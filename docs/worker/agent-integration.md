# Agent와 공통 Worker 연결

공통 코드는 src/worker, 호스트에 종속된 연결은 src/agent/integrations에 있다.
현재는 전환 1단계다. 기존 분석 Agent는 아직 새 진입점에 연결되지 않았다.
아래 TODO와 그래프 계약 연결을 완료한 뒤 배포해야 한다.

## 1. 실행과 작성 지점

| 파일 | 역할 | 개발자가 작성할 부분 |
|---|---|---|
| src/agent/worker_main.py | 자원 수명, 시작·종료, 어댑터 조립 | 추가 자원 연결이 필요한 경우 |
| integrations/worker_hooks.py | 그래프·핸들러 구성 | factory·registry·진행 처리 |
| integrations/langgraph_adapter.py | 이벤트와 checkpoint 연결 | State 계약이 다를 때 |
| 호스트 그래프 | 실행 제출, 대기, 반영, 업무 분기 | 아래 State·receipt 계약 |
| 호스트 API | 사용자 입력과 승인·취소 | 공통 guard와 요청 복구 계약 |

실제 main은 [worker_main.py](../../src/agent/worker_main.py)이고 실행 명령은
저장소 루트에서 python -m agent.worker_main이다. examples는 자동 로드하지 않는다.

## 2. create_graph — API와 같은 그래프를 연결

[worker_hooks.py](../../src/agent/integrations/worker_hooks.py)의
create_graph(checkpointer, bindings)에 있는 NotImplementedError를 실제 factory
호출로 바꾼다. 아래 my_agent는 인수자의 실제 패키지명으로 교체해야 한다.

```python
async def create_graph(checkpointer, bindings):
    from my_agent.graph import build_graph

    return build_graph(checkpointer=checkpointer, bindings=bindings)
```

async factory면 await한다. builder나 invoke 결과가 아닌, 공급된 checkpointer로
compile한 그래프를 반환한다. API와 Worker에서 같은 그래프 정의를 사용한다.
bindings는 현재 Worker의 Store이고 실행 제출 노드에 주입한다.
재개 후 다음 Execution을 만드는 노드도 동일하게 연결을 등록해야 한다.

main이 연결 자원을 닫으므로 factory에서 닫지 않는다. setup도 호출하지 않는다.
추가 HTTP client 등은 실행 종료까지 열린 상태로 유지하도록 main에 조립한다.
미구현 factory는 연결 자원을 열 수 있지만 소비·ACK 전에 실패한다.

## 3. build_handlers — 이벤트 타입별 처리

기본 등록은 참조 그래프 계약에 맞춘 다음 두 가지다.

```python
return {
    "execution.operation_completed": resume_graph,
    "execution.completed": resume_graph,
}
```

자기 그래프가 실제로 기다리는 경계만 재개에 연결한다.
최종 완료만 기다리는 그래프에 operation_completed를 무조건 전달하면 안 된다.
미등록 타입은 원본 보존 후 IGNORED 처리한다. 같은 group의 replica는 동일한
registry를 써야 한다.

진행 상황은 on_step_completed(context)를 구현한 뒤 주석 처리된 등록을 활성화한다.
정상 return은 DONE이다. pass/log만 있는 함수를 업무 핸들러로 등록하지 않는다.

```python
from worker import DeferEvent, EventContext, IgnoreEvent, RejectEvent
```

| 결과 | 처리 |
|---|---|
| 정상 return | DONE, ACK |
| DeferEvent | 준비 대기, 업무 실패 횟수 미차감 |
| IgnoreEvent | 명시적인 무시 기록 |
| RejectEvent | 최종 FAILED·DLQ |
| 기타 예외 | 재시도 예산 적용 후 소진 시 FAILED·DLQ |

context에는 session_id/task_id/execution_id/command_id와 Executor 원본 event가 있다.
graph_config의 thread_id는 session_id다. EventContext 자체가 LangGraph State는 아니다.
Dispatcher가 이미 guard를 잡으므로 핸들러에서 같은 guard를 다시 획득하지 않는다.
외부 DB 쓰기·알림·제출은 command_id + 작업명으로 멱등 처리한다.

## 4. 그래프 State·interrupt 계약

[어댑터](../../src/agent/integrations/langgraph_adapter.py)는 다음 필드를 사용한다.

| 필드 | 내용 |
|---|---|
| active_task_id | 현재 Task 문자열 |
| execution_id | 현재 Execution UUID 문자열 |
| ew_pending | 수락한 action |
| ew_receipts | command_id → event_id 처리 영수증 |
| ew_sequences | execution_id → 마지막 반영 순번 |

```python
action = interrupt(
    {
        "kind": "EXECUTOR_EVENT",
        "task_id": state["active_task_id"],
        "execution_id": state["execution_id"],
    }
)
```

action은 command_id/task_id/event이며, 어댑터는 대상 interrupt ID로 resume한다.
대기 노드가 action의 Task·실행을 검증하고 ew_pending을 반환한다.
다음 별도 반영 노드가 업무 처리 후 ew_receipts·ew_sequences를 저장한다.
중간 실패 복구는 이미 수락한 action으로 ainvoke(None)을 수행한다.
새 Task에서 이전 계획·결과는 초기화하되 영수증·순번은 보존한다.

[참조 그래프](../../examples/worker/session_graph.py)에 최소 구현이 있다.
실제 분석·제출·리포트는 예제에 없으며 호스트에서 구현한다.
execution.completed는 성공·실패·취소 모두 가능하므로 최종 결과를 조회하고 분기한다.
이전 Task의 늦은 이벤트와 사용자 승인 interrupt 보호도 회귀 검증해야 한다.

## 5. API 연결

API는 lifespan에서 ExecutorWorker(settings, {})를 자원용으로 열되 run하지 않는다.
별도 checkpoint 연결과 bridge.bindings로 같은 그래프를 만든다.
동일한 DB·Redis·EW_NAMESPACE를 사용하고 session_id 충돌을 방지한다.

```python
async with bridge.guard.hold(session_id):
    await graph.ainvoke(
        graph_input,
        {"configurable": {"thread_id": session_id}},
        durability="sync",
    )
```

호스트의 멱등 실행 제출 노드에서 Executor 응답 ID로 등록한다.
execution_id 인자는 UUID 타입이다.

```python
await bindings.register(
    execution_id=execution_id,
    session_id=session_id,
    task_id=task_id,
)
```

제출부터 binding·대기 checkpoint까지 같은 짧은 guard 안에서 진행한다.
API가 중간에 종료되면 같은 제출 키로 실행을 복원하고 binding을 다시 등록해야 한다.
[API 예제](../../examples/worker/api_integration.py)는 이미 존재하는 Execution을
연결하는 참조일 뿐 실제 접수·제출 API가 아니다.

장기 채팅 잠금, 사용자 요청 내구성, 취소 정책, 프론트 전달은 호스트 책임이다.
구체적인 전환 요구사항은 [전체 계획](../worker-centered-refactor.md)에 정리했다.

## 6. 환경과 배포

EW_DATABASE_URL/EW_REDIS_URL/EW_NAMESPACE는 API와 Worker에서 일치시킨다.
기본 main은 Worker DB를 checkpoint에도 사용하므로 별도 checkpoint DB를 쓰는
호스트는 saver 설정을 변경한다. EW_INSTANCE_ID는 replica마다 고유해야 한다.
EW_NAMESPACE는 DB 행·Redis group/key 범위이지 K8s namespace나 권한 경계가 아니다.

초기화는 worker_migrations/alembic.ini로 Alembic을 실행하고 checkpoint setup은
호스트 배포 단계에서 별도 수행한다. Worker 시작에서 자동 DDL을 하지 않는다.

[설치·실행 안내](README.md)와
[배포 템플릿](../../deploy/worker/deployment.yaml.example)을 참고한다.
현재 배포 템플릿은 실제 Agent 연결 후 적용할 자료이며 자동 전환되지 않는다.
