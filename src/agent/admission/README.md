# API 직접 호출의 접수·복구 — 전환 2C

`AdmissionService`는 API가 그래프를 직접 호출하기 **전에** 요청을 DB에 보관한다.
API가 종료돼도 `RequestRecovery`가 같은 세션 guard와 checkpoint를 확인해 이어 간다.
새 Redis 스트림이나 컨슈머는 추가하지 않는다. Executor 이벤트 전달은 기존
`src/worker`의 Inbox/Outbox/Dispatcher가 담당한다.

현재는 연결 가능한 내부 서비스다. 운영 FastAPI 라우터·factory·Chat UI의 실행
경로를 바꾸지는 않았다. 최종 실패 보상과 화면 복원까지 연결한 후 전환한다.

## 파일별 책임

| 파일 | 역할 |
|---|---|
| `contracts.py` | 신뢰된 START/RESUME/CANCEL 입력과 처리 기록 |
| `models.py` | Agent 소유 `agent_api_requests` 테이블과 인덱스 |
| `store.py` | Task·요청 원자적 접수, 재시도 상태, 처리 시도 번호 검증 |
| `graph.py` | 순수 입력 노드 출력과 수락 영수증을 함께 checkpoint |
| `service.py` | 세션 잠금, 승인 검증, 직접 invoke, 안전한 재개 판정 |
| `recovery.py` | 호스트가 관리하는 동시성 제한 DB 복구 루프 |

요청에는 원래 Task 입력, 대상 interrupt ID, 승인 계획 revision/hash, 최초 입력
해시가 저장된다. 동일 request_id에 다른 입력을 넣으면 거부한다. 생성·수정 시각과
주체는 `created_at/by`, `updated_at/by`에 기록한다. 요청 원문/오류에는 사용자
내용이 있을 수 있으므로 일반 로그나 인증 없는 상태 조회로 노출하지 않는다.

## 연결 조각

아래는 인증·인가 및 자원 초기화를 마친 호스트 안에서 사용하는 코드다.
독립 실행 스크립트나 그대로 공개할 HTTP request body가 아니다.

```python
from agent.admission.contracts import ApiRequest
from agent.admission.recovery import RequestRecovery
from agent.admission.service import AdmissionService
from agent.admission.store import RequestStore

requests = RequestStore(agent_session_factory)
api_calls = AdmissionService(graph, shared_session_guard, requests)
recovery = RequestRecovery(api_calls, concurrency=4, batch_size=32)

# authenticated_turn은 서버가 소유권을 검증한 TaskTurn이다.
# request_id는 요청자가 재전송해도 동일하게 유지할 UUID다.
record = await api_calls.handle(
    ApiRequest(
        request_id=request_id,
        turn=authenticated_turn,
        kind="START",
    )
)
```

`graph`는 `SessionWorkflowServices`와 `build_session_graph()`로 생성하고,
영속 checkpointer는 `checkpoint_serializer()`를 사용한다. `agent_session_factory`
는 업무 repository와 같은 DB다. API/Worker/복구 루프의 guard는 **동일한 Redis와
namespace**를 사용해야 한다. Dispatcher 안에서 다시 guard를 잡지 않는다.

START는 Task·사용자 메시지·접수 이벤트·요청을 한 트랜잭션으로 생성한다.
호출 전에 기존 `AgentRepository.create_task()`를 호출하면 안 된다. 그것은 구형
START 커맨드까지 발행하므로 새 경로와 중복된다. 이 접수 경로는 구형
`WorkflowCommand`에 START/RESUME를 저장하지 않는다.

사용자 승인/수정/거절은 `kind="RESUME"`, 취소는 `kind="CANCEL"`로 보낸다.
현재 checkpoint가 제시한 `interrupt_id`와 해당 응답 계약의 `payload`가 필요하다.
`turn`은 원래 접수한 Task 입력을 재사용한다. 수정 의견은 원본 메시지를 바꾸지
않고 payload에 넣는다. 사용자 입력으로 임의의 State/config를 받지 않는다.
Executor 이벤트 payload는 이 API로 보낼 수 없다.

```python
record = await api_calls.handle(
    ApiRequest(
        request_id=approval_request_id,
        turn=authenticated_turn,
        kind="RESUME",
        interrupt_id=current_interrupt_id,
        payload=validated_plan_review_payload,
    )
)
```

## 복구 루프의 실행 위치

API lifespan 또는 Worker 호스트에서 `recovery.run(stop_event)`를 지속 실행한다.
API/Worker 두 프로세스 구조를 유지할 수 있고, 세 번째 프로세스는 필요 없다.
Worker 호스트에 둘 경우 일반 소비기가 아니라 Agent 자원 조립 계층에서 붙인다.
해당 호스트에도 동일한 그래프·업무 DB·checkpointer·guard가 필요하다.

```python
stop_event = asyncio.Event()
recovery_task = asyncio.create_task(recovery.run(stop_event))

# 호스트 종료 시에는 신규 접수를 먼저 막는다.
stop_event.set()
await recovery_task
```

예제에는 `import asyncio`가 필요하다. stop은 유휴 대기를 즉시 끝내지만 진행 중인
invoke는 완료까지 기다린다. 호스트 종료 grace를 초과하면 recovery_task를 취소하고
종료를 기다린 뒤 DB/Redis/checkpointer를 닫는다. `CancelledError`를 삼키지 않는다.
호출 중 중단된 요청은 RUNNING으로 남아 재시작 후 복구된다.
기본 invocation_timeout=360초는 **한 번의 그래프 호출** 제한이다. 며칠짜리 코드
실행은 Executor 대기 interrupt에서 반환되므로 API 호출을 며칠 열어 두지 않는다.
계획 생성 시간과 호스트 종료 정책에 맞춰 timeout을 명시적으로 조정한다.

API는 `handle()`로 직접 호출할 수도, `accept()`를 await해 DB 접수를 확정한 뒤
202로 응답하고 복구 루프에 넘길 수도 있다. `execute(request_id)`는 내부용이다.
외부 상태 조회에는 인증·Task 소유권 검증이 추가로 필요하다. 결과 목록 HTTP API를
붙일 때는 커서 페이지네이션을 사용한다. `due(limit=...)`는 사용자 목록 조회가 아닌
내부 작업 선별용 제한 배치다.

## 처리 상태와 복구 근거

| 상태 | 의미 |
|---|---|
| PENDING | DB 접수 완료 또는 재시도 대기 |
| RUNNING | 세션 guard 아래 처리 시도 중; 프로세스 중단 시에도 남음 |
| APPLIED | API 입력 처리 후 다음 승인·Executor 대기·종료 지점에 도달 |
| REJECTED | 접수 후 원래 interrupt/세션이 바뀌어 입력 적용을 거부 |
| BLOCKED | 복구 근거 불일치 또는 재시도 한도 도달; 추가 자동 처리 금지 |

**APPLIED는 분석 작업 성공이나 취소 완료가 아니다.** 작업 결과와 execution_id는
Task/그래프의 현재 상태를 별도로 조회한다. 취소는 Executor 종료 확인 전까지 장기
잠금과 대기 상태를 유지한다. 성공 리포트 역시 Worker 후속 처리에 속할 수 있다.

- 입력 노드가 반환할 때 `api_receipts[request_id]`에 Task/입력 해시를 함께 저장한다.
- 그 다음 실행 소유자는 `invocation_owner={source: API, id: request_id}`다.
- 수락 전 재시도는 원래 interrupt에만 응답한다. 같은 승인으로 다음 계획을 승인하지 않는다.
- 수락 후 미완료 노드가 해당 요청 소유라면 `ainvoke(None)`으로 이어 간다.
- Executor 이벤트가 수락되면 실행 소유자는 Worker command로 바뀐다.
  이전 API 요청은 APPLIED로만 정리하고 Worker 미완료 노드를 대신 실행하지 않는다.
- 반대로 이전 Worker command의 완료 기록을 놓쳤어도, 이후 API 승인이 소유한
  미완료 노드를 오래된 Worker 재전달이 실행하지 않는다.
- 마지막 허용 시도 후 중단돼도 완료 checkpoint를 확인할 수 있으면 추가 invoke 없이
  APPLIED로 정리한다. 모호한 상태는 자동 재시작하지 않는다.
- 세션 guard 경합은 재시도 횟수에 포함하지 않는다. 한 세션의 미해결 요청은 DB의
  부분 unique index로 하나만 허용하며, BLOCKED도 접수 제한을 유지한다.

DB 상태와 checkpoint는 서로 다른 트랜잭션이다. 이 구조는 exactly-once HTTP가
아닌 멱등 재처리이며, 실제 외부 효과는 [효과 기록](../effects/README.md)이 보호한다.
짧은 guard와 실행 중 장기 채팅 금지는 별도다. 구/신 경로를 같은 세션에 섞지 않는다.

## 마이그레이션과 남은 작업

Agent DB의 `0008_api_requests`가 필요하다. 루트 Alembic의 `upgrade head`에 포함된다.
Worker만 이식하는 개발자는 이 테이블이 필요 없으며 `ew_0001`도 변경하지 않는다.
운영 DB 적용은 배포 Job/전환 절차에서 수행한다. 이번 테스트는 격리 DB에만 적용했다.

남은 필수 작업은 BLOCKED 최종 실패 보상(실행 조회·취소 확인·사용자 안내·잠금 정리),
공통 factory/lifespan 연결, FastAPI/Chat UI 상태 복원, 운영 설정과 readiness다.
특히 비최종 Task 상태·current_interrupt의 DB 반영은 기존 runner에 남아 있다.
새 호스트에서 그래프 대기/취소 상태를 화면용 Task 상태에 반영하는 연결이 필요하며,
현재 모듈만으로 기존 Task 조회 API가 모든 새 진행 상태를 보여주는 것은 아니다.
BLOCKED를 성공/실패로 강제 바꾸거나 요청 ID를 바꿔 재실행하는 운영 우회는 제공하지 않는다.
불확실한 외부 실행이 살아 있는 상태에서 세션을 해제해서는 안 된다.

## 검증 범위

- `tests/agent/test_admission.py`: 수락 노드·재개 판단·복구 동시성 제한.
- `tests/agent/test_admission_postgres.py`: 접수 원자성, API 종료, 승인 응답 유실,
  수정 후 재승인 분리, 마지막 재시도, actor 기록, Q&A 결과 중복 방지.
- `tests/worker/test_agent_admission_recovery.py`: 실제 Redis guard, 별도 PostgreSQL
  checkpointer 연결, API→Worker 소유권 이전, 취소 후 종료 확인과 잠금 유지.
- `tests/agent/test_effect_migration.py`: 구 revision 데이터 보존과 신규 테이블 생성.

모델과 Executor HTTP는 결정적 대역이며 실제 Jupyter/LLM/K8s 종단 테스트는 아니다.
