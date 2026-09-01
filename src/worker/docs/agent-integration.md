# Agent와 공통 Worker 연결

공통 코드는 src/worker, 호스트에 종속된 연결은 src/agent/integrations에 있다.
2E에서 이 저장소의 실제 분석 Agent runtime을 연결했다. 공통 Worker만 이식하는
서비스는 handler 계약을 그대로 구현할 수 있고, 이 Agent는 `agent.runtime` factory를
사용한다. 기존 FastAPI 라우터 전환 전에는 구 Worker와 새 Worker를 함께 띄우지 않는다.

## 1. 실행과 작성 지점

| 파일 | 역할 | 개발자가 작성할 부분 |
|---|---|---|
| src/agent/worker_main.py | 자원 수명, 시작·종료, runtime supervision | 운영 진입점 |
| agent/runtime | API/Worker 공통 그래프·복구 조립 | 호스트 설정과 lifespan |
| integrations/worker_hooks.py | 이벤트 타입 registry | 진행 이벤트를 추가할 때 |
| integrations/langgraph_adapter.py | 이벤트와 checkpoint 연결 | State 계약이 다를 때 |
| 호스트 그래프 | 실행 제출, 대기, 반영, 업무 분기 | 아래 State·receipt 계약 |
| 호스트 API | 사용자 입력과 승인·취소 | 공통 guard와 요청 복구 계약 |

실제 main은 [worker_main.py](../../agent/worker_main.py)이고 실행 명령은
저장소 루트에서 python -m agent.worker_main이다. examples는 자동 로드하지 않는다.

## 2. 공통 runtime factory

이 저장소는 [factory.py](../../agent/runtime/factory.py)의
`open_agent_runtime(settings, worker, checkpointer)`를 API와 Worker가 함께 쓴다.
아래처럼 업무 서비스, binding, guard와 복구 루프가 같은 경계에서 만들어진다.

```python
async with open_agent_runtime(settings, bridge, saver) as runtime:
    graph = runtime.graph
    api_calls = runtime.admission
    event_handler = runtime.event_handler
```

factory는 `SessionWorkflowServices`, Executor effect journal, AdmissionService,
FailureService를 구성하고 실패 보호가 적용된 handler를 반환한다. 시작 시 테이블과
checkpoint를 읽기 검증하지만 DDL은 수행하지 않는다. `worker_hooks.create_graph()`는
외부 이식 코드의 호환성을 위해 예외를 내는 deprecated symbol로만 남아 있다.

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

[어댑터](../../agent/integrations/langgraph_adapter.py)는 다음 필드를 사용한다.

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

[참조 그래프](../../../examples/worker/session_graph.py)에 최소 구현이 있다.
실제 분석·제출·리포트는 예제에 없으며 호스트에서 구현한다.
execution.completed는 성공·실패·취소 모두 가능하므로 최종 결과를 조회하고 분기한다.
이전 Task의 늦은 이벤트와 사용자 승인 interrupt 보호도 회귀 검증해야 한다.

## 5. API 연결

API는 lifespan에서 `ExecutorWorker(worker_settings, {})`를 bridge 자원으로만 열고
run하지 않는다. 별도 checkpoint 연결과 `open_agent_runtime()`으로 같은 그래프를
만들고 `recovery_lifespan()`을 실행한다. 동일한 DB·Redis·namespace를 사용한다.

```python
async with open_agent_runtime(settings, bridge, saver) as runtime:
    async with recovery_lifespan(
        runtime.lifecycle,
        shutdown_timeout_seconds=settings.worker_shutdown_grace_seconds,
    ):
        yield runtime.admission
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
[API 예제](../../../examples/worker/api_integration.py)는 이미 존재하는 Execution을
연결하는 참조일 뿐 실제 접수·제출 API가 아니다.

장기 채팅 잠금, 사용자 요청 내구성, 취소 정책, 프론트 전달은 호스트 책임이다.
구체적인 전환 요구사항은 [전체 계획](../../../docs/worker-centered-refactor.md)에 정리했다.

## 6. 환경과 배포

현재 Agent main은 `AGENT_DATABASE_URL`, `AGENT_CHECKPOINT_DATABASE_URL`,
`AGENT_REDIS_URL`을 원본으로 사용하고 `build_worker_settings()`가 공통 Worker 설정을
만든다. Stream/group 이름도 Agent 설정을 명시적으로 전달하므로 namespace 문자열을
추측하지 않는다. `WORKER_INSTANCE_ID`는 replica마다 고유해야 하며 미설정 로컬 실행은
Worker의 UUID 기본값을 사용한다. namespace는 DB 행·Redis key 범위이지 K8s
namespace나 권한 경계가 아니다.

초기화는 worker_migrations/alembic.ini로 Alembic을 실행하고 checkpoint setup은
호스트 배포 단계에서 별도 수행한다. Worker 시작에서 자동 DDL을 하지 않는다.

[설치·실행 안내](../README.md)와
[배포 템플릿](../../../deploy/worker/deployment.yaml.example)을 참고한다.
현재 배포 템플릿은 실제 Agent 연결 후 적용할 자료이며 자동 전환되지 않는다.
