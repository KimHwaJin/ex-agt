# 실패 보상 — 전환 2D

이 모듈은 API 요청이 `BLOCKED`가 되거나 Worker command가 최종 `FAILED`가 된
뒤, 불확실한 Executor 실행을 남긴 채 세션 잠금을 풀지 않도록 보상한다. 성공한
분석에만 보고서를 만든다는 정책을 유지하며 실패·취소 보고서는 생성하지 않는다.

## 처리 순서

```text
원본 처리 실패
  → 실패 기록과 세션 잠금 확보
  → Execution ID 복구·고정
  → Executor 상태 조회·취소 요청
  → 터미널 상태 재조회
  → 실패 checkpoint로 안전하게 종료
  → Task 실패·사용자 메시지·요청 상태·잠금 해제를 한 트랜잭션으로 반영
  → Worker FAILED command를 IGNORED로 감사 종료
```

Executor가 취소 요청을 접수했어도 상태가 아직 터미널이 아니면 `PENDING`과 잠금을
유지한다. `SUCCEEDED`, `FAILED`, `CANCELLED` 확인 후에만 완료한다. 제출 journal은
있지만 응답이 유실된 경우 Executor의 Task 조회 API로 정확한 실행을 찾는다. 조회
결과가 없다는 사실은 실행 부재의 증명이 아니며, 결과가 없거나 둘 이상이면 자동
완료하지 않고 `BLOCKED`로 보존한다. 같은 코드를 새 멱등 키로 재제출하지 않는다.

복구한 `execution_id`는 취소 전에 실패 기록·Task·SessionLock에 함께 고정한다.
checkpoint 정리에는 실패했던 업무 노드를 다시 실행하는 `ainvoke(None)`을 쓰지
않는다. pending task를 `END`로 닫고 전용 `failure_settled` 노드 상태를 기록한 뒤,
해당 Task와 세션의 소유권 및 실패 영수증을 다시 검증한다.

## 파일별 책임

| 파일 | 기능 |
|---|---|
| `models.py` | `agent_failure_cleanups` 상태·원인·Executor 증거·감사 필드 |
| `store.py` | 실패 최초 기록, 시도 fencing, 실행 ID 고정, 원자적 최종 반영 |
| `executor.py` | 효과 journal/Task 조회로 실행 식별, 취소와 터미널 확인 |
| `graph.py` | 대상 checkpoint 검증, 업무 노드 재실행 없는 실패 종료 |
| `service.py` | API/Worker 실패 포착, 세션 guard 아래 단일 보상 수행 |
| `recovery.py` | 제한된 동시성과 keyset cursor를 쓰는 DB 복구 루프 |
| `operations.py` | 운영자 allowlist, BLOCKED cursor 조회, 멱등 재시도·즉시 검증 |
| `contracts.py` | 감사 필드를 포함한 운영 조회·명령 API 계약 |

## 상태

| 상태 | 의미 |
|---|---|
| `PENDING` | 보상 대기 또는 Executor 터미널 상태 확인 대기 |
| `BLOCKED` | 식별 충돌·증거 부족·시도 한도로 자동 처리 중단 |
| `DONE` | checkpoint와 업무 DB 종료, 잠금 해제까지 완료 |

API 요청은 실제 입력 적용 증거가 있으면 `APPLIED`로 복구한다. 보상으로 Task가
종료되면 `COMPENSATED`가 된다. 이미 성공·실패·취소로 완료된 Task는 결과를
덮어쓰지 않고 그대로 보존한다. 모든 행은 `created_at/by`, `updated_at/by`를 가진다.

## 호스트 연결

`FailureRecovery`는 API lifespan이나 Worker 호스트의 Agent 조립 계층에서 실행할
수 있다. 별도 Redis 스트림이나 세 번째 소비기를 만들지 않는다. API·Worker·복구
루프는 같은 Agent DB, checkpoint DB, Redis guard namespace를 사용해야 한다.

```python
from agent.failure.executor import FailureExecutor
from agent.failure.recovery import FailureRecovery
from agent.failure.service import FailureService
from agent.failure.store import FailureStore

failure = FailureService(
    graph,
    shared_session_guard,
    FailureStore(agent_session_factory),
    request_store,
    FailureExecutor(executor_client, effect_store, effect_sender),
)
recovery = FailureRecovery(failure, worker_store=worker_store)

# Dispatcher에 등록하기 전에 Agent 업무 handler만 감싼다.
event_handler = failure.protect(session_graph_adapter)
```

`recovery.run(stop_event)`의 lifecycle은 `RequestRecovery`와 같은 호스트가
관리한다. 일반 Worker 패키지는 Agent 실패 모듈을 import하지 않는다. Worker 쪽
추가는 기존 command 테이블을 읽는 `failed_page()`뿐이며 별도 Worker migration은
없다. Agent DB에는 Alembic `0010_failure_operations`까지 필요하다.

## BLOCKED 운영 API

`AGENT_FAILURE_OPERATOR_USER_IDS`에 쉼표로 구분한 운영자 user ID를 설정해야
접근할 수 있다. 기본값은 빈 allowlist이므로 인증된 일반 사용자도 `403`을 받는다.
초기 allowlist는 향후 BFF role/permission policy로 교체 가능한 application
경계다.

```text
GET  /api/v1/operations/failure-cleanups
GET  /api/v1/operations/failure-cleanups/{task_id}
POST /api/v1/operations/failure-cleanups/{task_id}/retry
POST /api/v1/operations/failure-cleanups/{task_id}/finalize
```

목록은 `updated_at, task_id` keyset cursor를 사용한다. 명령은 Task 범위의
`idempotency_key`, 조회한 `expected_version`, 필수 운영 사유를 받는다. 요청자,
사유, 이전 오류, operation ID는 `task.failure_cleanup_operator_requested` 이벤트와
failure row의 마지막 operation 필드에 기록된다.

`retry`는 시도 횟수를 초기화하고 기존 복구 루프에 다시 넣을 뿐 Task나 잠금을
직접 변경하지 않는다. `finalize`도 같은 복구 로직을 즉시 한 번 실행한다. Executor
터미널 상태와 LangGraph failure receipt가 모두 확인되어 기존 `finish()`가 성공한
경우에만 `DONE`과 Session unlock이 발생한다. 증거가 여전히 모호하면 `409`를
반환하고 다시 `BLOCKED`로 남긴다. 동일 멱등 키는 재실행하지 않으며 다른 조건으로
다시 시도하려면 새 키와 최신 version을 사용한다.

## 운영 제한

- Task의 모든 비최종 상태와 `current_interrupt` 화면 projection은 아직 공통 운영
  factory에 연결하지 않았다.
- 실제 Executor/Jupyter, K8s 프로세스 종료, 장기 실행 종단 검증은 수행하지 않았다.
  테스트는 실제 PostgreSQL/Redis/checkpointer와 결정적 Executor HTTP 대역을 쓴다.
- 실패 원문에는 내부 오류나 사용자 데이터가 포함될 수 있다. 일반 로그나 인증 없는
  상태 API에 노출하지 않는다.
