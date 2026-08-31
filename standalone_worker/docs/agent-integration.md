# 실제 Worker 시작과 Agent 연결 가이드

이 문서는 인수할 서비스가 `API + Agent / background Worker`로 나뉘고,
`session_id = LangGraph thread_id`인 경우를 기준으로 한다.

## 1. 실행 파일과 수정할 파일

정식 시작 파일은 루트의 [main.py](../main.py)다. 더 이상 examples를
실행하지 않는다. Kubernetes Worker 컨테이너도 이 파일을 실행한다.

| 파일·위치 | 역할 | 개발자가 할 일 |
|---|---|---|
| main.py | 자원 생성, 그래프 연결, 소비 시작, 종료 신호 처리 | 기본적으로 수정 불필요 |
| agent_app.py:create_graph | API와 동일한 그래프 생성 | TODO 1: 자기 그래프 factory 연결 |
| agent_app.py:build_handlers | 이벤트 타입별 처리 등록 | TODO 2: 처리할 타입·핸들러 확정 |
| agent_app.py:on_step_completed | 진행 상황 저장·알림 확장 지점 | TODO 3: 필요하면 구현 후 등록 |
| 호스트 Agent 그래프 | 실행 제출, 대기, 결과 반영·업무 분기 | State와 interrupt 계약 연결 |
| 호스트 API | 그래프 최초 호출, 승인, 접수·세션 정책 | bindings와 guard 사용 |
| src/executor_worker/ | 소비, Inbox/Outbox, 순서·중복·복구 처리 | 일반적인 연결 작업에는 수정 불필요 |

`create_graph()`를 구현하지 않으면 시작 과정에서 오류로 종료한다.
DB 자원은 열릴 수 있지만 Redis 소비·ACK는 시작하지 않는다. 데모 그래프나
로그만 출력하는 핸들러로 업무 이벤트를 완료 처리하는 기본 동작은 없다.

`examples/`는 계약을 설명하는 참조 코드와 테스트 자료로만 남는다.
운영 Docker runtime에는 복사하지 않으며 `main.py`도 import하지 않는다.

## 2. TODO 1 — 자기 그래프를 연결한다

[agent_app.py](../agent_app.py)의 `create_graph()` 안에 있는
`raise NotImplementedError(...)`를 자기 서비스의 그래프 생성 호출로 바꾼다.
아래 `my_agent`는 제공되는 모듈이 아닌 **인수자의 실제 패키지 경로**다.

```python
async def create_graph(checkpointer, bindings):
    from my_agent.graph import build_graph

    return build_graph(
        checkpointer=checkpointer,
        bindings=bindings,
    )
```

호스트 factory가 async면 호출을 `await`한다. StateGraph builder나 실행 결과가
아니라 **checkpointer를 연결해 compile한 그래프**를 반환해야 한다.

- API와 Worker는 같은 그래프 정의·노드 버전을 사용한다.
- `checkpointer`는 main이 열고 닫는다. 다른 임시 체크포인터로 교체하지 않는다.
- `bindings`는 현재 Worker의 실행 연결 저장소다. 실행 제출 노드에 주입한다.
  Worker에서 재개된 MULTI 노드가 새 Execution을 제출할 때도 등록이 필요하다.
- 이 두 자원을 factory에서 닫거나 `checkpointer.setup()`을 호출하지 않는다.
- 호스트 그래프에 별도 HTTP client 등 수명 관리가 필요한 의존성이 있으면,
  main의 자원 컨텍스트에 연결한다. 그래프만 만들고 client를 닫는 factory는 안 된다.

## 3. TODO 2·3 — 이벤트별 핸들러를 정한다

기본 등록은 다음 두 가지다. 둘 다 같은 그래프 어댑터를 호출한다.

```python
return {
    "execution.operation_completed": resume_graph,
    "execution.completed": resume_graph,
}
```

기본 등록은 참조 그래프의 대기 계약을 따른다. 자기 그래프가 최종 완료만
기다린다면 operation_completed를 무조건 재개에 연결하지 말고 등록을 조정한다.
등록하지 않은 타입은 원본 저장 후 IGNORED 처리되며 순번은 진행한다.
같은 consumer group을 사용하는 모든 replica의 등록 목록은 같아야 한다.

진행 상황을 별도로 저장하려면 `on_step_completed(context)`를 구현하고,
`build_handlers()`의 주석 처리된 등록 한 줄을 활성화한다.

```python
async def on_step_completed(context):
    # progress_service는 인수자가 구현하는 서비스다.
    await progress_service.save_once(
        idempotency_key=f"{context.command_id}:progress",
        session_id=context.session_id,
        task_id=context.task_id,
        execution_id=str(context.execution_id),
        payload=context.event.payload,
    )
```

EventContext는 LangGraph State가 아니다. 실행 연결과 Inbox 원본으로 만들어져
핸들러에 전달되는 인자다. `context.graph_config`에는 session_id를 사용한
thread_id가 있고, `context.event`는 Executor 원본 이벤트다.

| 핸들러 결과 | Worker의 처리 |
|---|---|
| 정상 return | Command DONE, 내부 메시지 ACK |
| DeferEvent | 준비될 때까지 대기, 업무 실패 횟수 미차감 |
| IgnoreEvent | 명시적으로 무시한 처리 기록 저장 |
| RejectEvent | 최종 FAILED·DLQ |
| 그 외 예외 | 실패 횟수 증가, 재시도 후 소진 시 FAILED·DLQ |

예외는 `executor_worker`에서 import한다. 미구현 함수를 `pass`나 로그만으로
등록하면 성공 처리되어 버린다. 기본 진행 핸들러는 그래서 미등록 상태다.
Dispatcher가 이미 세션 guard를 잡으므로 핸들러에서 다시 획득하지 않는다.

외부 저장·발행은 `command_id + 안정적인 작업명`으로 멱등 처리한다.
Worker의 Inbox/Outbox가 임의의 호스트 API 호출까지 exactly-once로 만들지는 않는다.

## 4. 호스트 그래프가 맞춰야 하는 계약

main은 기본 [SessionGraphAdapter](../src/executor_worker/langgraph_adapter.py)를
쓴다. 임의의 그래프에 그대로 붙는 어댑터가 아니므로 아래 State·노드 계약이 필요하다.

| State 필드 | 내용 |
|---|---|
| active_task_id | 현재 Task ID 문자열 |
| execution_id | 현재 Execution UUID의 문자열 표현 |
| ew_pending | 대기 노드가 수락해 checkpoint에 저장한 action |
| ew_receipts | command_id 문자열 → 처리한 event_id 문자열 |
| ew_sequences | execution_id 문자열 → 마지막 반영 순번 |

실행 결과 대기 지점에는 식별 가능한 interrupt 하나를 둔다.

```python
action = interrupt(
    {
        "kind": "EXECUTOR_EVENT",
        "task_id": state["active_task_id"],
        "execution_id": state["execution_id"],
    }
)
```

수락 노드는 action의 실행·Task를 확인한 뒤 `{"ew_pending": action}`을 반환한다.
이 노드와 실제 결과 반영 노드를 분리해야 수락 내용이 먼저 checkpoint에 남는다.
action에는 command_id, task_id, Executor event가 들어간다.
반영 노드는 `ew_pending`을 읽어 업무 처리 후 ew_receipts와 ew_sequences를 갱신한다.
필드별 실제 값과 재개 형태는 어댑터 소스를 기준으로 확인한다.

[session_graph.py](../examples/session_graph.py)에 대기·반영 최소 코드가 있다.
필요한 계약을 자기 그래프에 반영하되, 이 예제는 실제 Executor 제출·결과 조회나
성공 리포트를 구현하지 않는다. `execution.completed`도 성공뿐 아니라 실패·취소일
수 있으므로 호스트가 최종 결과를 확인하고 분기해야 한다.

새 Task가 시작되어도 기존 ew_receipts와 Execution별 ew_sequences를 지우지 않는다.
재시도 시 이미 수락된 노드는 `ainvoke(None)`으로 이어 가고 새 resume를 넣지 않는다.
이전 실행 이벤트와 사용자 승인 interrupt의 보호도 어댑터가 처리한다.
State 이름·대기 구조가 다르면 어댑터와 호스트 대기·반영 노드를 함께 조정하고,
중복·중간 실패·사용자 승인 보호 테스트를 그대로 유지한다.

## 5. API와 실행 제출 노드에서 할 일

API는 자기 lifespan에서 별도의 `ExecutorWorker(settings, {})`를 자원용으로
열 수 있다. API에서는 `run()`을 호출하지 않는다. checkpoint 연결도 API가 별도로
열어 같은 그래프 factory에 `checkpointer`와 `bridge.bindings`를 전달한다.
두 프로세스가 Python 객체를 공유하는 것은 아니며 DB·Redis 기록을 공유한다.

API의 최초 invoke·사용자 승인 resume 모두 같은 guard로 감싼다.

```python
async with bridge.guard.hold(session_id):
    await graph.ainvoke(
        graph_input,
        {"configurable": {"thread_id": session_id}},
        durability="sync",
    )
```

그래프의 멱등 실행 제출 노드에서 Executor가 반환한 ID로 연결을 등록한다.
`bindings.register()`의 execution_id는 UUID 타입을 사용한다.

```python
await bindings.register(
    execution_id=execution_id,
    session_id=session_id,
    task_id=task_id,
)
```

제출 호출부터 binding 등록·실행 대기 checkpoint 저장까지 같은 짧은 guard 안에서
진행한다. API가 제출 직후 종료돼도 같은 제출 idempotency key로 Execution을
복원하고 연결을 다시 등록할 수 있어야 한다. 세션·Task 발급, 중복 접수 방지,
며칠간의 채팅 금지 잠금은 호스트 API의 별도 업무 정책이다.

[api_integration.py](../examples/api_integration.py)는 이미 존재하는 Execution을
연결하는 참조다. 실제 제출 API가 아니므로 통째로 라우터 대신 사용하지 않는다.

API와 Worker의 필수 일치 항목:

- PostgreSQL DB·체크포인트 테이블/search_path와 그래프 버전.
- Redis DB와 EW_NAMESPACE: 같은 세션 잠금과 업무 저장 범위.
- session_id = thread_id. 공유 체크포인트 DB에서 세션 ID가 충돌하면 안 된다.
- Worker replica의 핸들러 등록 목록. EW_INSTANCE_ID는 각 replica마다 고유하게 둔다.

EW_NAMESPACE는 DB 행·Redis group/key의 범위이며 K8s namespace나 권한 경계가 아니다.
기본 main은 EW_DATABASE_URL로 Worker 저장소와 체크포인트를 모두 연다.
호스트가 체크포인트 전용 DB를 쓰면 main의 saver 연결 설정도 맞춰 변경해야 한다.

## 6. 설치·초기화·실행

이 디렉터리 전체를 복사하되 .env, .venv, 캐시는 제외한다. 또는
`src/executor_worker/`, main.py, agent_app.py, 의존성, Alembic 파일을 자기 프로젝트
구조에 편입한다. 호스트의 my_agent 패키지도 Worker 이미지 안에 설치·복사해야 한다.

standalone_worker 디렉터리에서:

```bash
uv sync --frozen --all-extras --no-editable
cp .env.example .env
# .env의 DB/Redis/Executor 주소 수정 + agent_app.py의 TODO 구현
uv run --no-sync --env-file .env alembic upgrade head
# 체크포인트 초기화는 기존 Agent 배포 절차에서 별도 수행
uv run --no-sync --env-file .env python main.py
```

워커 테이블은 [Alembic 가이드](../migrations/README.md)대로 배포 Job에서 초기화한다.
LangGraph의 AsyncPostgresSaver.setup()은 호스트 배포 단계에서 한 번 수행한다.
각 Worker 시작 때 자동 DDL을 실행하지 않는다. 삭제된 CLI/Store.migrate 경로 대신
Alembic을 사용한다.

배포용 이미지 빌드 대상은 runtime이다. 마지막 test stage를 배포하지 않는다.

```bash
docker build --target runtime -t your-agent-worker:version .
```

현재 Dockerfile의 기본 CMD는 `["python", "main.py"]`, 작업 경로는 /worker다.
[Deployment 예시](../deploy.yaml.example)의 Worker 명령은 다음과 같다.

```yaml
command: [python, /worker/main.py]
```

같은 Pod에 API+Agent와 Worker를 각각 컨테이너로 띄운다. API 컨테이너는 uvicorn,
Worker 컨테이너는 main.py를 실행한다. 호스트 이미지의 경로가 다르면 command도
맞춘다. 기존 Secret·이미지명·API 모듈명은 예시 값을 실제 배포 값으로 교체한다.

SIGTERM/SIGINT는 신규 소비 중단과 진행 중 핸들러 drain을 요청한다.
그래프 처리 종료 후 체크포인트·DB·Redis·HTTP 자원을 닫는다. Pod 종료 유예는
EW_SHUTDOWN_SECONDS보다 길어야 한다. Worker 종료는 Executor 실행 취소가 아니다.

## 7. 인수 후 확인 목록

- [ ] create_graph의 미구현 예외를 제거하고 실제 API 그래프 factory 연결.
- [ ] State·interrupt·receipt 계약 연결, API/Worker가 같은 세션 checkpoint 조회.
- [ ] 실행 제출 노드의 멱등 제출과 bindings.register 구현.
- [ ] 이벤트 registry 확정. 진행 핸들러는 구현한 경우에만 등록.
- [ ] API의 invoke/resume에 같은 guard 적용; 핸들러 내부 중복 잠금 금지.
- [ ] 외부 효과 멱등성, 성공/실패/취소 분기, 프론트 전달 로직 구현.
- [ ] Alembic과 checkpoint 초기화, Secret, runtime 이미지·프로브 설정.
- [ ] 실제 그래프로 중복 이벤트·재시작·중간 실패·다음 Task 시나리오 검증.

현재 검증 범위와 삭제된 운영 도구의 잔여 테스트 문제는
[VALIDATION.md](../VALIDATION.md)와 [기능 목록](features.md)을 참고한다.
