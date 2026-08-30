# Proposed System Architecture

상태: `ACCEPTED_IMPLEMENTED_V1`

이 문서는 인터뷰 과정에서 합의할 Agent/BFF/Executor 통신 구조의 초안이다.

## 1. 권장 배치 구조

```text
Browser
  |  POST command / GET state / SSE events
  v
BFF Service
  |
  +-> API Process
  |     | bounded QA / planning / HITL interaction
  |     | persist Message/Task/Plan/checkpoint
  |     | approval commit -> durable command/outbox
  |     v
  |   BFF PostgreSQL
  |     |
  |     +-> BFF internal Redis Stream -> Agent Workflow Worker
  |                                      | load/resume LangGraph checkpoint
  |                                      | submit/append/finalize
  |                                      v
  |                                   Executor
  |                                      |
  |                                      +-> PostgreSQL authoritative state
  |                                      +-> Redis execution events
  |                                              |
  |                                              v
  |                                  BFF Executor Event Subscriber
  |                                      | order/dedupe/gap recovery
  |                                      | persist event + enqueue resume
  |                                      v
  |                                  Agent Workflow Worker
  |
  +-> SSE / Task State -> Browser
```

핵심 선택은 다음과 같다.

- BFF가 Agent application을 포함하는 하나의 서비스 경계다. 별도 Agent API 서비스는 두지
  않는다.
- 같은 BFF 코드베이스를 API process, workflow worker, Executor event subscriber 역할로
  실행한다.
- BFF router는 일반 질의응답, 실행 의도 분류, 계획 생성, HITL 대기까지처럼 시간이 제한된
  foreground graph 구간을 직접 stream/invoke할 수 있다.
- 실행 승인 commit부터는 BFF DB에 승인·Session lock·durable command/outbox를 원자적으로
  저장하고 `202 Accepted`를 반환한다. 실제 실행 구간은 background workflow worker가 맡는다.
- foreground graph도 Message/Task와 LangGraph checkpoint를 사용한다. API process 장애 시 같은
  사용자 명령을 멱등하게 복구할 수 있어야 한다.
- LangGraph가 Executor 결과를 기다리며 5일 동안 Python coroutine을 유지하지 않는다.
- Executor 제출 뒤 checkpoint하고 현재 worker run을 종료한다.
- Executor event subscriber가 boundary event를 받으면 동일 `thread_id`의 그래프 재개를
  durable command로 enqueue한다.

이 구조에서 HTTP/SSE는 Browser와 BFF 사이의 계약이고, Redis Stream은 같은 BFF 서비스 안의
API/Worker/Event Subscriber를 분리하는 내부 wake-up 계약이다. API와 Worker는 같은 배포
이미지를 사용하되 process type과 autoscaling 정책을 별도로 둘 수 있다.

## 2. 왜 BFF 내부 서비스 호출만으로 부족한가

단순 질의응답은 BFF 요청 안에서 동기 응답하거나 token stream을 proxy할 수 있다. 하지만
분석/코드 실행은 다음 이유로 request lifecycle에서 분리해야 한다.

- HITL 승인까지 수분 또는 수일 멈출 수 있다.
- 데이터 레이크 download Step이 수시간 또는 수일 걸릴 수 있다.
- 전체 실행이 5일까지 지속될 수 있다.
- BFF/Agent 배포, autoscaling, connection timeout과 무관하게 복구되어야 한다.
- 동일 Task 명령의 네트워크 재시도에서 중복 실행을 막아야 한다.

따라서 BFF router의 직접 graph 호출은 대화·계획·HITL interrupt까지의 bounded 구간에 사용하고,
승인 후 Executor lifecycle은 durable worker 구간으로 넘긴다. 이 경계가 foreground 응답성과
5일 실행의 복구 가능성을 함께 보장한다.

## 2.1 Same-Pod 배포 변형

BFF API와 Agent Worker가 반드시 같은 Kubernetes Pod에 있어야 해도 application 경계는
유지할 수 있다. 권장 순서는 다음과 같다.

### 권장: 한 Pod, 두 Container

```text
BFF Pod
  ├─ api container
  │    └─ HTTP / SSE / Task command persistence
  └─ worker container
       ├─ LangGraph workflow consumer
       └─ Executor event subscriber
```

두 container는 같은 image를 서로 다른 command로 실행할 수 있다. 배포와 scaling의 운명은
공유하지만 event loop, CPU/memory, lifecycle signal을 분리할 수 있다.

### 허용: 한 Pod, 한 Container, 한 Process

API lifespan에서 workflow consumer와 Executor event subscriber를 background task로 시작할
수 있다. 다만 다음 조건이 필요하다.

- Uvicorn/Gunicorn web worker를 여러 개 띄울 경우 각 process가 background consumer를
  중복 시작하지 않도록 한다.
- 가장 단순한 초기 구성은 Pod당 web process 한 개와 고유 Redis consumer name이다.
- blocking/CPU-bound 작업은 API event loop에서 직접 실행하지 않는다.
- LLM/Executor I/O는 async이며, 실제 Python 장기 실행은 Executor/Jupyter가 담당한다.
- Pod 종료 시 새 command claim을 멈추고 in-flight graph node를 bounded drain한다.
- drain deadline을 넘긴 node는 checkpoint/idempotency를 통해 다른 Pod가 복구한다.

### Same-Pod에서 감수할 제약

- API 트래픽과 Agent 처리량을 독립적으로 autoscale할 수 없다.
- API 장애/재배포가 Worker와 SSE connection을 함께 끊는다.
- Agent 부하가 API latency와 같은 Pod resource를 경쟁한다.

기능적 안전성은 PostgreSQL checkpoint, durable Task command, Redis pending reclaim,
idempotency key, SSE event replay로 보완한다. Browser는 Pod 재시작 후 SSE를 다시 연결하고
`Last-Event-ID` 이후 이벤트를 받는다.

5일짜리 Executor 작업 동안 Agent coroutine이나 Worker slot을 계속 점유하지 않는 원칙은
Same-Pod에서도 동일하다. Agent는 Executor command를 제출하고 checkpoint한 뒤 현재 graph
invocation을 끝내며, Executor boundary event가 왔을 때만 다시 깨어난다.

## 3. Browser와 BFF의 명령 계약 초안

### Task 생성

```http
POST /tasks
```

```json
{
  "request_id": "bff-generated-idempotency-key",
  "task_id": "...",
  "user_id": "...",
  "project_id": "...",
  "session_id": "...",
  "message": "...",
  "attachments": [],
  "preferences": {}
}
```

응답:

```http
202 Accepted
Location: /tasks/{task_id}
```

### HITL 결정

```http
POST /tasks/{task_id}/decisions
```

```json
{
  "request_id": "...",
  "plan_version": 3,
  "decision": "APPROVE | REVISE | REJECT",
  "feedback": "..."
}
```

명령은 모두 idempotency key와 optimistic version을 사용한다. BFF와 Browser의 재시도가 새
Task나 중복 승인을 만들면 안 된다.

## 4. Frontend 전달

권장 transport:

- 명령: HTTP POST
- 현재 상태/복구: HTTP GET
- 진행 이벤트: SSE
- polling: SSE를 사용할 수 없는 환경의 fallback

WebSocket은 양방향 실시간 협업이 필요해질 때 추가한다. 승인/수정/거절은 HTTP 명령이면
충분하므로 5일 작업을 위해 WebSocket을 계속 유지할 필요가 없다.

SSE 예시:

```http
GET /tasks/{task_id}/events
Last-Event-ID: 42
```

필수 특성:

- 모든 사용자 이벤트에 Task별 단조 증가 `sequence`를 부여한다.
- 이벤트 이력을 durable storage에 보존한다.
- reconnect 시 `Last-Event-ID` 또는 `after_sequence`부터 replay한다.
- UI가 장시간 offline이어도 `GET /tasks/{task_id}`로 snapshot을 복구할 수 있다.
- 각 Browser connection이 Executor Redis consumer group을 직접 소비하지 않는다.

BFF가 사용자-facing event history와 접근 권한을 직접 소유하고 자체 SSE를 제공한다. Agent
workflow에서 발생한 product event는 BFF DB에 저장된다. Task별 Redis Pub/Sub은 SSE
wake-up에만 사용하며 SSE replay 원본으로 사용하지 않는다.

## 5. Task 상태 초안

```text
ACCEPTED
CLASSIFYING
ANSWERING
PLANNING
WAITING_FOR_APPROVAL
REVISING_PLAN
QUEUED_FOR_EXECUTION
EXECUTING
WAITING_FOR_EXECUTOR_EVENT
FINALIZING_EXECUTION
GENERATING_REPORT
SUCCEEDED
REJECTED
FAILED
CANCEL_REQUESTED
CANCELLED
```

Agent Task 상태와 Executor Execution 상태는 같은 enum이 아니다. Agent는 Executor 상태를
자신의 Task/Run view로 projection한다.

### 실행 중 Session 잠금 (`ACCEPTED`)

코드 실행 단계에 진입하면 해당 `session_id`에는 새로운 채팅 메시지를 받지 않는다.

확정된 잠금 구간:

```text
사용자 실행 승인 커밋
  -> session lock 획득
  -> QUEUED_FOR_EXECUTION
  -> EXECUTING / WAITING_FOR_EXECUTOR_EVENT
  -> FINALIZING_EXECUTION
  -> GENERATING_REPORT
  -> 성공: 최종 리포트 저장
     실패: 실패 원인 사용자 메시지 저장
     취소: Executor 취소 확인 후 취소 사용자 메시지 저장
  -> terminal Task 상태 저장
  -> session lock 해제
```

실행 중에도 허용할 API:

- Task/Execution 상태 조회
- SSE reconnect와 event replay
- 실행 취소 요청
- Artifact/중간 결과 조회

실행 중 차단할 API:

- 같은 Session의 새로운 사용자 채팅
- 새로운 분석/코드 실행 Task 생성
- 현재 실행과 무관한 Graph resume

잠금은 process memory나 만료 기반 Redis lock만으로 관리하지 않는다. BFF PostgreSQL에
Session의 active Task/Execution과 잠금 상태를 저장하고, 승인 반영과 잠금 획득을 하나의
transaction으로 처리한다. 필요하면 active 상태에 대한 partial unique constraint로 한
Session에 하나의 실행만 허용한다.

Pod 장애가 lock을 해제하지 않으며, 복구 worker가 Executor/Task 상태를 reconcile한 뒤에만
해제한다. 성공은 최종 리포트, 실패는 실패 원인 메시지, 취소는 Executor 취소 완료 확인과
취소 메시지를 저장한 후 lock을 해제한다.

Agent graph/command 자체가 재시도 한도를 초과해 실패했는데 Executor가 아직 terminal이
아니면 Task를 곧바로 `FAILED`로 만들지 않는다. 원래 durable command를
`FAILURE_COMPENSATION`으로 바꾸고 Task를 `CANCEL_REQUESTED`로 유지한다. 복구 Worker는
동일 idempotency key로 Executor 취소를 요청하고 `CANCELLED`, `FAILED` 또는 `SUCCEEDED`
terminal 상태를 REST로 확인한다. 확인된 Executor 상태, 실패 메시지, command `FAILED`,
Task `FAILED`, Session unlock은 하나의 Agent DB transaction으로 확정한다. Executor 통신이
복구되지 않으면 보상 command를 계속 재처리하며 잠금을 유지한다.

새 메시지 요청의 응답은 `409 Conflict` 또는 제품 계약상 `423 Locked`를 사용할 수 있다.
응답에는 최소한 `session_id`, `active_task_id`, 현재 상태, 상태/SSE 조회 URL, 취소 가능 여부를
포함한다.

## 6. 사용자 이벤트 초안

- `task.accepted`
- `intent.classified`
- `answer.delta`, `answer.completed`
- `plan.proposed`, `plan.revised`
- `approval.required`, `approval.recorded`
- `risk.review_completed`, `risk.warning_required`
- `execution.submitted`
- `operation.started`, `step.started`, `step.progress`, `step.completed`
- `execution.waiting`, `execution.completed`
- `report.started`, `report.completed`
- `task.failed`, `task.cancelled`

Executor 이벤트를 그대로 Browser에 노출하지 않는다. Agent가 사용자에게 필요한 안정적인
product event contract로 변환한다.

## 7. 장기 작업과 LangGraph

Production checkpointer는 PostgreSQL을 사용하고 모든 invocation/resume에 동일한
`thread_id`를 전달한다. 확정된 식별 관계:

```text
LangGraph thread_id = Agent task_id
Agent task_id = 한 코드 실행 요청부터 최종 리포트까지의 ID
Executor context.task_id = Agent task_id
```

Frontend 대화 복원은 BFF의 user-visible Message/Event 테이블을 사용하고, Agent workflow
복원은 LangGraph PostgreSQL checkpoint를 사용한다. Checkpoint에는 내부 ToolMessage와 실행
상태가 포함될 수 있으므로 Frontend가 직접 읽지 않는다.

Worker가 종료되어 Redis consumer pending에 남은 command는
`XAUTOCLAIM`으로 idle 기준 이후 다른 worker가 회수한다. 기존 worker의
task lock이 유효하면 중복 실행하지 않고 ack 없이 양보하며, lock 만료 후
같은 PostgreSQL command와 LangGraph checkpoint에서 재개한다.

한 Task에서 여러 Executor Execution을 허용할지는 별도 정책으로 결정한다. 허용한다면
`run_id`를 추가하고 Task와 Execution을 1:N으로 저장한다. Executor event의 `task_id`는 BFF
mapping을 통해 `session_id`/LangGraph thread를 찾는다.

코드 실행 중 Session chat lock으로 사용자 메시지에 의한 동시 resume는 차단한다. 하지만
at-least-once Executor 이벤트, 중복 resume command, 복구 sweep은 여전히 동시에 도착할 수
있으므로 Task 단위 single-flight lease와 idempotency는 유지한다.

`interrupt()` 재개 시 node가 처음부터 재실행되므로 approval node에는 외부 mutation을 두지
않는다. Executor mutation은 승인 뒤 별도 node에서 안정적인 idempotency key로 호출한다.

## 8. 데이터 레이크 다운로드

데이터 레이크 조회 함수는 분석 Skill/Tool catalog의 첫 번째 Tool이다. 실제 환경에서는
외부 작업자가 작성한 query를 입력받아 데이터를 조회한다. Agent의 초기 범위에는 데이터
레이크 query 자체의 생성/검증을 포함하지 않는다. 실행은 Executor/Jupyter Step으로 수행한다.

예시 Tool catalog:

| Skill | Tool | 목적 |
|---|---|---|
| data-access | `fetch_dataset` | 외부 query로 데이터 레이크 데이터를 실행 workspace에 저장 |
| data-inspection | `inspect_dataset` | 파일 형식, schema, row/column, 크기 확인 |
| data-quality | `profile_missing_values` | 결측치와 기본 품질 요약 |
| descriptive-analysis | `summarize_numeric_columns` | 기술통계와 이상치 후보 계산 |
| descriptive-analysis | `group_aggregate` | 범주별 집계와 비교 |
| visualization | `plot_distribution` | 분포 시각화와 artifact 생성 |

초기 개발에서는 실제 데이터 레이크 adapter 대신 `fetch_dataset` Fake를 제공한다. Fake는
query 문자열을 입력받지만 외부 시스템을 호출하지 않고, 고정 seed를 사용해 재현 가능한
샘플 분석 데이터를 workspace 파일로 생성한다. 반환값에는 파일 경로, format, row/column
수, schema 요약과 생성 seed를 포함한다. 이후 실제 함수가 전달되면 같은 계약의 adapter로
교체한다.

실제 조회가 수일 걸릴 수 있으므로 progress callback 또는 주기적 progress artifact/상태
갱신 가능 여부는 실제 함수 수령 후 확정한다. 함수가 완료 시점에만 반환하면 UI는 Executor의
Step RUNNING 상태와 경과시간만 표시한다.

## 9. 위험 판정 Guardrail

올해 범위에서 sandbox/정책 엔진은 구현하지 않고 LLM 위험 판정을 advisory guardrail로
사용한다. 이는 보안 경계가 아니며 prompt injection이나 판단 오류를 차단하지 못한다.

권장 node:

```text
classify_request_risk
  -> generate_plan_and_code
  -> classify_generated_code_risk
  -> plan_validation
  -> human_review_with_warnings
  -> submit_to_executor
```

위험 결과는 structured schema로 저장한다.

```text
risk_level: LOW | MEDIUM | HIGH | CRITICAL
categories: network, filesystem, credential, destructive_write,
            external_side_effect, resource_exhaustion, data_exfiltration
summary: user-facing explanation
evidence: relevant code locations or request fragments
recommended_action: ALLOW | WARN | REQUIRE_CONFIRMATION | BLOCK
model/version/prompt_version: audit metadata
```

확정 정책:

| Risk | 동작 |
|---|---|
| `LOW` | 별도 경고 없이 계획 승인 흐름 진행 |
| `MEDIUM` | 계획 승인 화면에 경고 표시 |
| `HIGH` | 실행 전 명시적 추가 확인 필요 |
| `CRITICAL` | 실행 차단 |

MULTI의 후속 Step은 승인된 분석 전략 범위에서는 자동 실행한다. 분석 목적/방법의 중대한
변경, 새로운 데이터 접근, 위험도 상승 또는 새로운 외부 부수효과가 발생하면 Session 잠금을
유지한 채 다시 승인을 요청한다.

LangChain `create_agent()` middleware는 planning context와 관련 Skill/Tool manifest 주입,
model audit, timeout/budget, structured PlanDraft 검증과 Tool allowlist를 적용한다. 사용자에게
보여야 하는 위험 판정과 제품 상태를 바꾸는 그래프 분기는 명시적 LangGraph node가 소유한다.

## 10. 실행 mode와 계획 표시

자유 코드 실행의 `SINGLE` 또는 `MULTI`는 Agent가 자동 선택하지 않고 사용자가 직접 선택한다.
데이터 분석 실행은 승격 Workflow를 사용자가 선택하면 `SINGLE`, 선택하지 않거나 적합한 후보가
없으면 동적 `MULTI`로 정한다. Agent는 이 규칙 밖에서 mode를 임의 변경하지 않는다.

선택된 mode와 요청의 필수 의미가 동시에 성립할 수 없을 때만 clarification을 요청한다. 예를
들어 SINGLE을 선택하면서 이전 셀 결과를 본 뒤 다음 코드를 새로 생성하라고 요구하면, 고정된
사전 계획으로 수행할지 사용자가 mode를 변경할지 확인한다. 이는 최적화 추천이 아니라 계약
모순 해소다.

사용자 숙련도를 별도로 분류하지 않는다. 승인 계획에는 code source를 표시하지 않고 Step
설명, 선택된 Skill/Tool, 주요 parameter, 예상 결과와 위험 판정을 표시한다. 긴 query는
요약/checksum으로 표시한다. 자유 코드는
`CUSTOM_CODE`로 표시한다.

Tool 기반 실행 코드는 Agent 서비스의 canonical 함수 source를 셀에 포함하고 한 번 호출하는
형태로 결정론적으로 컴파일한다. 세부 계약은 `docs/runtime-tool-contract.md`를 따른다.

## 11. 실행계획 영속성과 추적성

실행계획은 LangGraph checkpoint 외에 BFF PostgreSQL의 versioned Plan 도메인으로 영속화한다.
계획을 수정하지 않고 새 revision을 추가하며, 각 Step의 Skill/Tool 선택 또는 Custom Code 생성
근거를 사용자에게 공개 가능한 구조화된 설명으로 저장한다.

승인된 PlanStep은 compiled source checksum, Executor execution/operation/step receipt,
Attempt/result/artifact와 연결한다. MULTI 후속 계획도 이전 결과를 근거로 append-only revision을
추가한다. 상세 계약은 `docs/plan-audit-contract.md`를 따른다.

## 12. LLM intent routing

사용자는 자연어 query만 전달한다. LangGraph 첫 판단 node가 LLM structured output으로 일반
Q&A, 데이터 분석 Q&A, 데이터 분석 실행, 자유 코드 실행을 분류한다. 의미 분류에는 keyword나
rule routing을 사용하지 않는다. 실행이 필요하고 mode가 없으면 HITL로 사용자의 SINGLE/MULTI
선택을 받는다. 상세 계약은 `docs/intent-routing-contract.md`를 따른다.

## 13. 성공 리포트

리포트 형식은 prompt, Pydantic structured output, authoritative data assembler와 결정론적
Markdown renderer가 함께 관리한다. 성공한 Execution에서만 Executor REPORT Artifact API를
호출하고 `append_to_notebook=true`로 notebook에 추가한다. Artifact 저장과 사용자-facing
Markdown message 저장을 완료한 뒤 Session 잠금을 해제한다. 상세 계약은
`docs/report-contract.md`를 따른다.

## 14. 승격 Workflow 검색

데이터 분석 실행은 동적 계획 전에 BFF PostgreSQL/pgvector의 Promoted Workflow를 검색한다.
Workflow는 사용자가 성공한 Tool 기반 Plan을 명시적으로 승격한 immutable version이다.

LLM intent 분류 후 query embedding과 tenant/compatibility filter를 적용해 top Workflow를
제안한다. 초기 공개 범위는 서비스 전체이며 향후 ACL을 위한 access policy 확장점을 둔다.
Workflow의 Skill/Tool version/hash와 parameter template를 모두 추적한다. 사용자가 binding
parameter와 Step을 확인하고 선택하면 그 행위를 계획 승인으로 기록하고 Session을 잠근 뒤
Workflow를 현재 Task Plan으로 복제해 SINGLE로 실행한다. 선택하지
않거나 후보가 없으면 동적 Skill/Tool 계획을 MULTI로 수행한다. 상세 계약은
`docs/promoted-workflow-retrieval.md`를 따른다.

검색은 상위 3개 Workflow를 기본 제안하고 cursor 기반 추가 후보 조회를 제공한다. Workflow
승격은 현재 모든 인증 사용자에게 허용하지만 versioned `PromotionPolicy` port를 항상 거치며,
향후 역할/조직/프로젝트 기반 자격을 추가할 수 있다.

## 15. 초기 구현 경계

초기 구현에는 Agent core, PostgreSQL/pgvector, Redis worker/event subscriber, Executor REST
adapter와 최소 FastAPI/SSE를 포함한다. FastAPI router는 동일 application interface를 호출하고
Graph node나 repository를 직접 조립하지 않는다. 배포는 같은 Pod 안의 `api`/`worker` 두
container로 분리한다.
