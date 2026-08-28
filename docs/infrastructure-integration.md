# Infrastructure Integration

상태: `IMPLEMENTED_V1_BASELINE`

이 문서는 Agent가 Executor 개발 인프라의 PostgreSQL, Redis, REST API와 shared workspace를
어떻게 함께 사용할지 정의한다. Executor 저장소의 2026-08-27 Compose 설정을 기준으로 한다.

## 1. 확인된 Executor 개발 인프라

| Component | 현재 설정 | Agent 사용 방식 |
|---|---|---|
| Executor REST | container port `8000`, base path `/api/v1` | 실행 생성/추가 Operation/finalize/cancel/result/artifact 호출 |
| PostgreSQL | `pgvector/pgvector:pg17`, 기본 DB `executor`, port `5432` | server를 공유하고 Agent database와 credential은 분리 |
| Redis | `redis:7.4-alpine`, DB 0, AOF | Executor event 구독 및 Agent wake-up transport |
| Executor event stream | 기본 `executor.events` | Agent 전용 consumer group으로 읽기 |
| Executor work stream | 기본 `executor.work` | Agent가 직접 소비하지 않음 |
| Shared workspace | Executor/Jupyter가 공유 mount 사용 | PATH source와 result manifest 교환에 같은 volume mount 필요 |

## 2. PostgreSQL과 pgvector

Executor Compose는 PostgreSQL 17 호환 pgvector image로 갱신되었고 fresh `executor` database에
vector extension을 만드는 init script를 포함한다. 이 script는 기본 database에만 적용되므로,
Agent bootstrap은 별도 `agent` database와 role을 만들고 Agent DB에서도 다음 migration을
수행한다.

```sql
CREATE EXTENSION IF NOT EXISTS vector;
```

권장 논리 분리:

```text
one PostgreSQL server
  ├─ executor database  # Executor 소유
  └─ agent database     # BFF domain + LangGraph checkpoint + workflow vectors
```

- Agent는 Executor table을 읽거나 migration하지 않는다.
- `agent` database는 별도 role/password와 migration history를 가진다.
- LangGraph PostgresSaver, BFF domain/outbox/audit table, pgvector Workflow table은 모두 Agent
  migration 범위에 둔다.
- 현재 개발 Compose의 image는 `pgvector/pgvector:pg17`이다. 재현 가능한 release/E2E 환경에서는
  검증된 extension version tag로 pin한다. 기존 Executor volume에 image 변경을 적용하기 전에는
  backup과 PostgreSQL major/data-directory 호환성 검증을 한다.
- 운영에서는 동일한 논리 경계를 유지하되 managed PostgreSQL cluster와 secret manager를
  사용할 수 있다.

공식 pgvector 프로젝트는 PostgreSQL 17용 `pgvector/pgvector:pg17` 계열 tag를 제공한다.
실제 구현에서는 floating tag 대신 검증된 extension version tag를 pin한다.

## 3. Redis Stream 경계

Redis는 durable business state의 원본이 아니라 wake-up과 delivery transport다.

초기 stream/consumer 이름:

| Stream | Producer | Consumer | 목적 |
|---|---|---|---|
| `executor.events` | Executor | `agent-executor-events-v1` group | Executor lifecycle event 수신 |
| `agent.commands` | BFF outbox relay | workflow worker group | 승인 후 실행/resume command 전달 |
| `agent.product-events` | Agent/BFF | SSE projection relay | 저장된 사용자 이벤트의 wake-up |
| `agent.commands.dlq` | workflow worker | 운영자/replayer | 반복 처리 실패 command 격리 |

규칙:

- consumer name은 Pod/process마다 고유하고 consumer group은 deployment 역할별로 고정한다.
- `executor.events` message는 `event_id`와 Executor `event_sequence`로 중복 제거/순서 검증한다.
- Redis ACK 전에 BFF PostgreSQL의 inbox/event checkpoint와 필요한 side effect를 commit한다.
- gap이 있으면 Executor REST event history를 조회해 복구한다.
- 장시간 idle pending entry는 claim하고, 이미 처리된 command/event는 멱등하게 ACK한다.
- Agent는 `executor.work`를 읽거나 쓰지 않는다.
- Redis 장애 중에도 PostgreSQL outbox에 command가 남아 재발행될 수 있어야 한다.

## 4. Executor REST Adapter

Agent의 Executor adapter는 REST만 사용하고 다음 operation을 제공한다.

```text
create_execution
append_operation
finalize_execution
cancel_execution
get_execution
get_execution_events
get_execution_result
create_report_artifact
get_notebook_metadata / download_notebook
```

- container network 내부 base URL 기본값: `http://executor:8000/api/v1`
- host 개발 기본값: `http://127.0.0.1:8000/api/v1`
- 실제 값은 `EXECUTOR_BASE_URL`로 주입한다.
- mutation 요청은 Task/Plan revision/Step에서 파생한 안정적인 idempotency key를 사용한다.
- connect/read timeout과 재시도는 endpoint 특성별로 나눈다. HTTP 요청 자체가 5일간 열려 있지는
  않으며, 제출 응답 후 lifecycle은 Redis event와 REST reconciliation으로 추적한다.
- transport error에서 payload를 바꾸지 않고 같은 idempotency key로 제한 재시도한다.

## 5. Shared Workspace

Agent가 큰 source를 `PATH`로 제출하거나 Executor result manifest를 읽으려면 Executor와 동일한
shared directory가 같은 absolute container path에 mount되어야 한다.

초기 path convention:

```text
/workspace/shared/
  requests/{task_id}/{plan_revision}/{step_id}/source.py
  # Executor가 관리하는 result/artifact path는 Executor 계약을 따른다.
```

- Agent가 쓰는 상대경로는 canonicalize한 뒤 shared root 내부인지 검사한다.
- source file은 원자적으로 생성하고 SHA-256을 Executor request에 함께 보낸다.
- result는 Executor `result_ref`의 checksum/size/identity 검증 후에만 모델 context나 report에
  사용한다.
- Agent와 Executor가 서로 다른 Pod라면 `ReadWriteMany`가 가능한 shared storage 또는
  Executor가 지원하는 동등한 object storage adapter가 필요하다.

## 6. BFF foreground/background 경계

```text
request/message
  -> foreground: classify -> answer or plan -> HITL interrupt
  -> approval transaction: approval + session lock + outbox
  -> background: Executor REST submit -> checkpoint/suspend
  -> executor.events -> inbox/reconcile -> graph resume
  -> success report artifact or failure/cancel message
  -> terminal state + unlock
```

Foreground 호출은 bounded LLM 작업에만 쓴다. Executor 결과 대기는 process-local coroutine,
HTTP connection 또는 Redis blocking call에 묶지 않는다.

## 7. 테스트 최소선

- 단위 테스트: repository/Executor/Redis fake로 graph 분기와 domain invariant 검증
- PostgreSQL integration: migration, PostgresSaver resume, outbox/inbox, session lock race,
  pgvector top-k/filter 검증
- Redis integration: consumer group, duplicate, gap, pending reclaim, ACK-after-commit 검증
- Executor contract test: REST idempotency, SINGLE/MULTI lifecycle, cancel, report artifact,
  notebook lookup 검증
- restart test: 승인 직후, REST 응답 유실 직후, Redis event 수신 직후, report 저장 직전 process를
  중단하고 중복 실행/중복 메시지 없이 복구되는지 검증

## 8. 확정된 배치와 남은 운영값

- 로컬/초기 배포에서는 Executor PostgreSQL server와 Redis instance를 공유한다.
- Agent는 별도 database/credential을 사용하고 Executor와 table을 공유하지 않는다.
- BFF는 같은 Pod, 같은 image의 `api` container와 `worker` container로 실행한다. Executor event
  subscriber는 worker 역할에 포함한다.
- 초기 Jupyter runtime profile은 `basic`으로 고정한다.

운영 환경에서 정해야 할 값:

- production shared storage 종류와 mount path
- Redis retention/maxlen, pending reclaim timeout, DLQ/replay 운영값
