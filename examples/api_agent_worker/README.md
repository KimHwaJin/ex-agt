# API+Agent / Worker 연결 예제

**API는 최초 입력과 사용자 승인을 직접 실행하고, Worker는 Executor 이벤트로
같은 Task의 그래프를 이어 간다.** 기존 Agent 개발자에게 연결 방법을 전달하기
위한 참조 구현이다. 운영 서비스 전환이나 완성된 Worker 제품이 아니다.

전체 정책은 [인수인계 가이드](../../docs/worker-handoff-guide.md)를 읽는다.
Kubernetes 구성은 [같은 Pod 배포](../../deploy/handoff/README.md)를 참고한다.

## 바로 실행

저장소 루트에서 실행한다.

```bash
uv sync --no-editable
uv run --no-sync python -m examples.api_agent_worker
```

```text
API: PLAN_REVIEW
API: WAITING
Worker: applied
Replay: duplicate
Final: SUCCEEDED
```

실제 FastAPI 라우터를 ASGI 테스트 클라이언트로 호출하고 실제 LangGraph를
실행한다. **단일 프로세스의 메모리 checkpoint/잠금/저장소와 가짜 Executor**를
사용한다. Redis, LLM, Executor, HTTP 서버, 컨테이너는 시작하지 않는다.
두 그래프 인스턴스를 분리해 호출 책임을 보여 주는 예제다.

## 파일 역할

| 파일 | 역할 |
|---|---|
| `api.py` | 직접 start/review 호출 라우터와 필수 admission callback |
| `worker.py` | DB의 EXECUTOR_SIGNAL만 재개하는 Redis Handler |
| `runner.py` | thread/interrupt 검증, receipt, pending 노드 복구 |
| `workflow.py` | review → submit → wait → apply_event 최소 그래프 |
| `contracts.py` | 분석 도메인과 분리된 Executor 경계 신호 모델 |
| `ports.py` | 인수 서비스가 구현할 admission/잠금/Executor/DB 계약 |
| `testing.py` | 테스트 대역. 운영에서 사용 금지 |
| `__main__.py` | 라우터와 Handler를 직접 연결하는 데모 |

현재 모델을 유지해서 가져가려면 다음 파일도 필요하다.

- `src/ex_agent/transport/consumer.py`: Redis 수신/lease/ACK/DLQ 기반.
- `examples/durable_event_to_langgraph/contracts.py`, `ports.py`:
  최소 Command 모델 및 저장소 계약. 자기 DB 모델로 대체해도 된다.
- `examples/durable_event_to_langgraph/memory_store.py`:
  데모 실행/테스트에서만 사용한다.

모듈을 복사한 뒤 `ex_agent.*`, `examples.*` import를 자신의 패키지 경로로 바꾼다.
`workflow_id` 필드는 이 예제에서 `str(task_id)`다. 재사용 분석 워크플로우 ID가 아니다.

## API 연결

API 프로세스의 lifespan에서 자기 PG 연결/pool, saver, 그래프를 만든 뒤
`SharedGraphRunner`를 만든다. `create_router(runner, guard, admit)`를
기존 FastAPI에 연결한다. `guard`와 `admit`은 호스트가 제공해야 한다.

데모 라우터:

- `POST /handoff/tasks/{task_id}/start`: `request_id`, `objective`.
- `POST /handoff/tasks/{task_id}/review`: `request_id`, `approved`.
- 인증된 BFF가 전달하는 `X-User-ID` 필수. 그 자체가 인증 수단은 아니다.
- 실행 소유권 충돌/다른 입력 경계는 409로 응답한다.
- START/사용자 RESUME Command는 생성하지 않는다.

운영 Task/Session 조회·SSE·취소 API를 대체하지 않는다. `admit`이 요청을
영속화하고 권한, 동일 request ID의 내용, 계획 버전, 세션 잠금을 검증해야 한다.
단순히 HTTP 연결이 끊겼다는 이유로 새 Task나 새 제출 키를 만들지 않는다.
HTTP 제한보다 긴 계획/모델 호출은 미완료 입력 조회·재개 정책이 필요하다.

## Worker 연결

Worker는 자기 PG pool/saver/graph로 Runner를 별도로 생성한다.
양쪽 모두 같은 checkpoint DB/schema와 `thread_id=str(task_id)`를 사용한다.
프로세스 간 Python 객체 공유는 없다. `MemoryRunGuard` 대신 동일한 분산
RunGuard 계약을 구현한다. receipt와 binding도 영속 저장한다.

기존 공통 소비기에 연결하는 핵심 형태는 다음과 같다.
이 코드는 **호스트가 만든 어댑터 변수가 있다고 가정하는 연결 예시**다.

```python
from ex_agent.transport.consumer import (
    RedisStreamConsumer,
    RedisStreamConsumerConfig,
)
from examples.api_agent_worker.worker import ExecutorResumeHandler

consumer = RedisStreamConsumer(
    redis,
    RedisStreamConsumerConfig(
        stream="handoff-agent.commands",
        group="handoff-agent-command-workers",
        consumer_prefix=pod_name,
        concurrency=len(worker_runners),
        dead_letter_stream="handoff-agent.commands.dlq",
    ),
    lambda slot: ExecutorResumeHandler(
        command_store, worker_runners[slot], shared_run_guard
    ),
)
await consumer.run()
```

Redis client는 `decode_responses=True`로 구성한다. 슬롯별 DB session을
동시에 공유하지 말고 요청/처리 단위로 얻는다. saver/graph의 슬롯별 구성은
현재 `src/ex_agent/workers/runtime.py`를 참고한다.

이 Handler는 **이미 DB에 확정된 내부 Command**를 소비한다.
앞단의 Executor event consumer, Inbox/순번 검증, Outbox relay까지 제거하는
예제가 아니다. 그 부분은 기존 서비스의 DB 계약을 참고해 호스트에 이식한다.
Stream의 `task_id`, `command_id`로 DB를 조회하고 Stream payload는 무시한다.
START/사용자 RESUME은 이 Handler에서 영구 오류로 처리한다.

종료 신호를 받으면 `consumer.request_stop()`으로 새 소비를 멈추고
`await consumer.shutdown(grace_period_seconds=25)`로 drain한 뒤 pool을 닫는다.
실제 signal wiring과 두 소비 루프,
Outbox/지표 루프의 생명주기는 호스트 Worker entrypoint가 구성한다.

## 순서와 장애 복구

- `RunGuard`: API/Worker의 동일 Task 동시 쓰기를 막는다. Worker에서는
  DB `mark_done`까지 유지한다. 세션 잠금과 다르고, 명령 FIFO도 보장하지 않는다.
- `ReadyCommandStore.get_command()`: DONE 명령 또는 Task의 다음 실행 가능한
  명령만 반환한다. 아직 차례가 아니면 `None`을 반환해 재시도시킨다.
- `receipts`: action ID + payload fingerprint를 checkpoint에 보관한다.
  같은 ID에 다른 payload를 넣으면 영구 오류다.
- `pending_action`: wait를 통과한 뒤 다음 노드가 실패한 경우 같은 입력인지
  확인하고 `ainvoke(None)`으로 복구한다. 사용자/이벤트 resume을 섞지 않는다.
- 종료 뒤 같은 execution의 후속 경계는 receipt만 기록한다. 종료 그래프를
  재실행하지 않는다. 실제 Agent의 리포트가 남아 있으면 종료로 취급하면 안 된다.
- `ExecutorPort.submit`: 동일 입력에 동일 제출 키를 쓰고 execution/task
  연결을 DB에 기록한다. 예제는 제출 1회이므로 task 기반 키를 쓴다.
  실제 MULTI 후속 요청은 plan/operation 단위의 안정적인 키를 사용한다.

처리 실패는 `mark_retry` 후 Redis `RETRY`다. 처리 재시도는 PEL만 맡고
Outbox는 초기 전달 복구만 맡는다. 기존 서비스처럼 DB 재발행 방식으로 바꿀 경우
Handler/DB 상태/relay를 함께 바꾼다. 두 정책을 혼합하지 않는다.

미도착/역순 Command와 일시적인 잠금 충돌도 예제에서는 RETRY다.
기본 재시도 한도를 운영 대기 시간에 맞춰 조정하고, DLQ 시 DB의 보류/종료와
순서 해제를 반드시 구현한다. 예제의 단순 상태 모델만으로 운영하지 않는다.
receipt 보관량과 만료 정책도 인수 서비스에서 정해야 한다.

## 구현 범위와 테스트

```bash
uv run --no-sync python -m pytest \
  tests/test_api_agent_worker_example.py -q
uv run --no-sync ruff check .
uv run --no-sync ruff format --check .
uv run --no-sync ty check
docker compose --profile test build test-migrate test
docker compose --profile test run --rm test
```

단위 테스트는 승인 재전송, 잘못된 경계, 조기 이벤트, 늦은 중복,
Command 역순, 노드 실패, checkpoint와 DB 완료 사이 장애를 검사한다.
PostgreSQL 테스트는 API saver 연결 종료 뒤 **새 연결과 그래프**에서 복구한다.
실제 Redis 소비기의 lease/DLQ/재전달은 기존 transport 통합 테스트가 검사한다.
새 예제 전체를 두 프로세스와 실제 Executor로 실행하는 E2E는 제공하지 않는다.

미포함: 실제 분석/LLM·계획 수정·리포트·취소, 운영 인증/입력 원장/세션 잠금,
분산 RunGuard 구현, 운영 CommandStore/Inbox/Outbox 어댑터, 자동 API 복구,
실행 가능한 운영 entrypoint. 이 부분은 호스트 서비스와 결합해야 한다.
현재 ex-agent의 `src/`와 런타임 동작은 변경하지 않는다.
