# V1 Implementation Status

상태: `FIRST_IMPLEMENTATION_COMPLETE`

## 구현됨

- FastAPI는 Task/resume/cancel을 PostgreSQL durable outbox에 기록하고 즉시
  `202`를 반환한다. Worker relay가 `SKIP LOCKED` claim과 Redis pipeline으로
  command/event를 배치 발행한다.
- Background worker만 LangGraph를 invoke/resume한다.
- `create_agent(tools=[])` Planner와 Skill context, risk prerequisite, model
  audit, timeout, output-validation middleware가 연결되어 있다.
- 의미 기반 LLM intent 분기, 명시적 request/code risk node, HITL interrupt,
  SINGLE/MULTI 실행, 최대 3회 MULTI 보정, 취소, 성공 전용 report flow가
  Graph edge로 표현되어 있다.
- Plan/Revision/Step에는 사용자 공개 계획, 선택 이유, Skill/Tool version과
  hash, parameter, 컴파일 source hash/path가 저장된다.
- promoted Workflow는 pgvector cosine search로 상위 3개를 검색하며 서비스
  전체 공개를 기본값으로 하고 미래 권한 필드를 포함한다.
- Executor 제출/append/finalize/cancel/result/report REST 계약과
  `executor.events` consumer가 구현되어 있다.
- 실행 코드와 성공 리포트는 공유 입력 루트에 원자적으로 materialize한 뒤
  PATH와 SHA-256으로만 제출한다. Agent Executor 경계는 INLINE source를
  거절한다.
- Executor event는 `event_id`로 중복 제거하고 Execution별 순번을 DB에서
  원자적으로 전진시킨다. gap은 Executor event-history REST pagination으로
  복구한 뒤에만 Graph resume command를 만든다.
- SSE는 PostgreSQL event ID와 `Last-Event-ID`로 재연결할 수 있다. 평상시에는
  Task별 Redis Pub/Sub 알림으로 깨어나며 PostgreSQL을 1초마다 polling하지
  않는다.
- 서로 다른 Task는 설정 가능한 bounded concurrency로 처리한다. 동일 Task는
  Redis lock으로 직렬화하고 lock과 Stream lease를 주기적으로 갱신한다.
  Worker 슬롯별 LangGraph checkpointer는 독립적이며 PostgreSQL pool을 공유한다.
- Executor event는 서로 다른 Execution을 bounded concurrency로 처리하고 같은
  `execution_id`는 분산 락과 Stream lease로 직렬화한다. transport 장애에는
  지수 backoff를 적용하며 실패 event는 ACK하지 않고 재claim한다.
- Redis Stream 소비 런타임을 Agent/LangGraph와 분리했다. command와 Executor
  event가 같은 공개 consumer contract를 사용하며 cursor 기반 batch reclaim,
  malformed envelope DLQ, pending 없는 idle consumer GC, grace period 기반 drain과
  취소 복구를 지원한다. 단일 파일 standalone import도 회귀 테스트로 보장한다.
  실행 binding보다 먼저 도착한 event는 ACK하지 않고 binding 생성 후 재claim한다.
- retry 가능한 handler 실패는 lock contention과 분리된 Redis counter로 추적한다.
  stream별 상한을 소진한 poison message는 마지막 retry 사유와 횟수를 포함해
  DLQ로 이동하고 원본을 ACK한다.
- versioned DLQ envelope와 비동기 관리 모듈을 제공한다. 운영 CLI의 replay와
  discard는 source/audit/DLQ/action marker를 원자적으로 변경하며 응답 유실 후
  반복 요청도 중복 발행하지 않는다.
- Redis Stream safe trim 모듈과 dry-run/실행 CLI를 제공한다. 보존기간, 최근
  최소 entry 수, 모든 group의 진행 ID와 가장 오래된 pending ID 중 가장 보수적인
  경계를 Lua 안에서 다시 계산하고 exact trim까지 원자적으로 수행한다.
- 성공한 Tool-only Task를 서비스 공개 Workflow v1으로 승격하는 draft/confirm API와
  versioned 정책 port를 제공한다. Executor 성공 경계에서 검증된 SINGLE/MULTI Step만
  추적하고, CUSTOM_CODE와 불완전 lineage를 차단한다. 원본 parameter는 입력 template로
  바꾸며 명시적 공개 기본값 외 실제 값은 Workflow snapshot과 embedding text에서
  제거한다. 동일 idempotency key 재요청은 같은 immutable version을 반환한다.
- 기존 Workflow의 새 immutable version 생성, 검토 승인/거절, 승인 version 간
  원자적 활성 전환, Workflow 비활성화/재활성화 API를 제공한다. 새 version은
  `PENDING_REVIEW`로 시작하며 승인된 version만 활성화할 수 있다. Workflow owner
  정책 port, 요청 hash 기반 멱등성, 변경 사유와 결과 snapshot 감사 기록을 모든
  상태 변경에 공통 적용한다.
- Workflow 운영 조회 API는 owner 정책을 적용해 Workflow/활성 version 요약,
  immutable version 목록과 상세 공개 Plan·Skill/Tool·source lineage, lifecycle
  감사 이력을 제공한다. version 번호와 `(created_at, action_id)` keyset을 불투명
  cursor로 전달해 offset pagination 없이 안정적으로 조회한다.
- FastAPI HTTP 계약 테스트가 BFF `X-User-ID`, owner `403`, resource `404`,
  cursor/body `422`, idempotency `409`와 정상 pagination 직렬화를 검증한다.
  누락된 `X-User-ID`는 FastAPI validation이 아닌 identity provider에서 일관되게
  `401`로 처리한다.
- 공개 resource API는 `created_at/updated_at/created_by/updated_by`를 공통
  계약으로 사용한다. Task·Workflow·WorkflowVersion은 actor를 영속화하고 기존
  데이터는 migration에서 lineage로 backfill한다. 내부 변경은 `AGENT`와
  `EXECUTOR` actor로 구분한다. 다건 일반 조회는 `items/next_cursor/has_more`
  envelope와 keyset cursor를 사용하고 SSE는 `Last-Event-ID`를 cursor로 사용한다.
- Worker entrypoint는 SIGTERM/SIGINT를 durable shutdown으로 변환한다. readiness를
  먼저 내리고 두 consumer와 maintenance loop를 전역 grace deadline 안에서 drain한
  뒤 DB/Redis/HTTP/metrics 자원을 닫는다.
- 실제 Redis 통합 테스트가 처리 중 runtime 종료 시 ACK 없이 PEL 유지와 lock 해제,
  다른 runtime의 `XAUTOCLAIM`, 단일 처리와 최종 pending 0건을 검증한다.
- application workflow state를 LangGraph adapter 밖으로 이동해
  `application → graph` 역의존을 제거했다. Worker command/event processor와
  Stream handler, FastAPI container/router, LLM factory와
  delivery/catalog/audit/execution repository를 기능 단위 모듈로 분리하면서
  기존 공개 import façade를 유지한다. execution repository는 binding, inbox
  중복 제거와 event sequence 전진을 같은 transaction 경계로 보존한다.
  Task 생성·resume·Session lock·interrupt·메시지·event와 Plan revision/step
  저장도 각각 전용 repository로 분리했다. Command lifecycle repository는
  상태 전이와 failure compensation을 소유하며 Command, Task와 Session lock
  변경을 하나의 transaction에 유지한다. system command의 idempotency 충돌은
  기존 Command ID를 반환하고 payload가 다르면 거절한다.
  `DefaultWorkflowServices`는 대화, 계획, 실행, 리포팅 capability를 조립하는
  façade로 축소했으며 Graph가 사용하는 20개 service 메서드 계약을 유지한다.
  `WorkflowNodes`도 대화, 계획, 실행, 종료 node group으로 분리하되 façade와
  31개 node 이름, state partial update 및 기존 edge 구성을 유지한다.
  `WorkflowWorker`는 dependency 조립만 담당하고 lifecycle, Redis consumer
  구성, outbox/readiness/metrics maintenance, handler 호환 메서드는 전용 worker
  모듈이 담당한다. 기존 worker runtime 메서드 계약은 façade가 유지한다.
  architecture test가 domain 순수성과 금지된 package 의존을 검사한다.
- API/Worker Prometheus endpoint와 API 동시 요청 부하 스크립트가 있으며,
  active slot, 처리시간, retry, DB outbox backlog, Redis pending/lag와 checkpoint
  pool 상태를 관측한다.
- 프로덕션 graph를 그대로 사용하는 결정론적 Fake LLM/Fake Executor 전체
  수명주기 benchmark가 SINGLE/MULTI의 계획, HITL, Executor 재개, 리포트 구간을
  분리 측정한다.
- Compose Redis/PostgreSQL 통합 테스트가 서로 다른 Execution의 병렬 처리와
  역순 event history 복구, 뒤늦은 중복 ACK, 최종 sequence를 검증한다.
- 승인 command와 Session lock은 같은 transaction으로 저장된다. 잠금은 성공
  report 완료, 실패 확인 또는 취소 완료 후 해제된다.
- 실행이 남아 있는 상태에서 Agent command가 최종 실패하면 같은 durable
  command를 `FAILURE_COMPENSATION`으로 전환한다. Worker는 AGENT actor로
  Executor 취소를 요청하고 terminal 상태를 확인한 transaction에서만 Task를
  `FAILED`로 확정하고 Session lock을 해제한다.
- uv, Ruff 79자, ty, Docker multi-stage build, Alembic, pgvector PostgreSQL,
  Redis Compose 통합 테스트가 구성되어 있다.
- 내부 vLLM `qwen38-27b-fp8`의 LangChain 일반 호출과 구조화 출력을 실제
  컨테이너에서 검증했다.
- 실제 임베딩 모델이 없는 개발 단계에는 결정적 `dummy-hash-v1` 1024차원
  임베딩으로 pgvector 인덱싱·검색 계약을 검증한다.
- 실제 Agent REST/HITL/worker/Executor/Jupyter/Redis event/report 전체를 잇는
  SINGLE 코드 실행 E2E에서 PATH source, checksum, stdout, Notebook Markdown,
  Task 감사 매핑을 검증했다.
- 실제 qwen 모델과 Executor/Jupyter를 사용한 동적 MULTI 분석 E2E에서 첫
  `fetch_dataset` 셀의 검증된 result manifest를 다음 계획에 전달하고, 실제
  출력 path로 `inspect_dataset` Operation을 append한 뒤 finalize와 한국어
  Markdown 성공 리포트까지 검증했다.
- Executor result manifest와 representation은 shared root 이탈, identity,
  complete flag, size와 SHA-256을 검증한 뒤 bounded preview만 모델 계획과
  리포트 증거에 전달한다. 파일 읽기는 event loop 밖에서 수행한다.
- 실제 Worker `SIGKILL` 장애 주입에서 계획 command 복원, 실행 중 Session
  lock 유지, Executor terminal event 복원, 성공 리포트와 lock 해제를
  검증했다. stale event 재claim 시 Executor history 최신 sequence까지 한 번에
  catch-up해 실행 단계 복구 지연을 153.6초에서 62.1초로 줄였다.
- 실제 실행 중 Redis와 PostgreSQL을 각각 중단하고 재가동하는 장애 주입에서
  프로세스 재시작 없이 동일 Task/Execution 복구, Session lock 유지·해제,
  중복 방지와 pending event 해소를 검증했다. 20초 설정 기준 재가동 후
  성공까지 Redis 47.4초, PostgreSQL 16.2초가 걸렸다.
- `WORKER_INSTANCE_ID`를 consumer 이름에 반영해 Pod별 stream 소유자를 추적할
  수 있다. 실제 2-Worker 장애 주입에서 active command owner를 종료한 뒤 다른
  Worker가 attempt 2로 복구했고, 두 Executor 실행의 동시 `RUNNING`, 멱등 저장,
  Session unlock과 두 consumer group pending 0건을 검증했다.
- 장기 Executor 작업 중 두 Worker container를 한 대씩 네 차례 교체하는 rolling
  restart soak에서 일반 질의 3건을 병행했다. 모든 Task와 Executor가 성공했고,
  중복 binding/event, failure compensation, Session lock 누수 없이 두 consumer
  group pending이 0건으로 수렴했다. Task 완료 직후 지연될 수 있는 마지막
  Executor 감사 경계의 수렴 시간도 별도로 측정한다.
- 전용 kind 클러스터에서 단일 Worker의 정상 `rollout restart`와 grace 0 강제
  Pod 삭제를 실제 Executor/Jupyter 작업에 주입했다. 두 번 모두 Pod UID가
  교체된 뒤 같은 Task/Execution이 성공했고, Session checkpoint 32개, Agent
  binding·완료 event 각 1개, Worker sequence 6, Redis pending·미완료
  command·미전송 outbox·Session lock 0건을 확인했다.
- 레거시 Worker와 통합 Worker는 command envelope, 전달 DB, checkpoint
  `thread_id`가 달라 같은 consumer group의 혼합 rolling 배포를 금지한다.
  `ex-agent-cutover-check`는 인증된 BFF 접수 차단 증거와 비종료 Task·구 command·
  제품 event·Session lock·두 Redis group pending/lag를 두 번 읽어 drain이
  안정적으로 끝났을 때만 성공한다. 전환 runbook과 Kubernetes Job 예시는
  `deploy/worker-cutover/`에 둔다.
- 실제 Git snapshot `391a818` 레거시 이미지와 현재 통합 이미지를 별도 kind
  cluster에서 전환했다. 레거시 smoke 성공과 두 번의 안정 drain 후 API/Worker
  Pod 0개를 확인한 다음에만 새 migration과 통합 Pod를 기동했다. 통합 Executor
  smoke는 Task/Execution 성공, Session checkpoint 25개, Agent/Worker binding
  단일성, 완료 event 1개, Redis pending·미완료 command·outbox·Session lock
  0건으로 수렴했다. 재현 스크립트와 기록은 `deploy/cutover-e2e/`에 있다.
- 운영 preflight는 더 이상 운영자의 freeze assertion만으로 통과하지 않는다.
  인증된 BFF 증거 API에서 `freeze_id`, revision, scope, 만료 시각을 두 번 직접
  확인한다. 격리 리허설 escape hatch는 이름에 `unsafe`를 명시하고 운영 Job에서는
  사용하지 않는다. `TARGET_STARTED` 이후 레거시 재기동을 금지하는 단계별 롤백
  판정 CLI와 배포 증거 계약은 `deploy/worker-cutover/`에 있다.
- allowlist로 제한된 BLOCKED failure cleanup 운영 API는 상세·cursor 목록,
  멱등 재시도와 즉시 검증 종료를 제공한다. 즉시 종료도 기존 Executor 터미널
  증거와 LangGraph failure receipt를 통과해야만 잠금을 해제하며 요청자·사유를
  Task event와 cleanup operation 필드에 감사한다.
- 등록된 논리 Redis Stream만 대상으로 dry-run과 비동기 trim 작업 API를 제공한다.
  trim은 API가 아니라 통합 Worker lifecycle에서 실행하며, 실제 Stream별 활성 작업
  unique lock, 멱등 키, stale claim 복구, 보존정책 하한과 at/by 감사를 적용한다.
- 운영 BFF 요청은 method, raw path/query, user ID, timestamp, nonce와 body hash를
  HMAC으로 결속한다. Redis nonce를 원자적으로 한 번만 수락하고 장애 시 fail
  closed하며 key ID 복수 등록으로 무중단 회전을 지원한다. production은 단순
  `X-User-ID` 신뢰 모드로 시작할 수 없다.
- API는 liveness `/healthz`와 dependency-aware `/readyz`, 통합 Worker는
  `/health/live`와 `/health/ready`를 분리한다.
  API PostgreSQL·Redis probe와 Worker의 Redis·DB·Agent runtime 확인은 timeout을
  적용한다. backlog/pending/lag는 readiness에서 제외하고 Prometheus 지표와
  warning/critical rule로 관리한다.

## 다음 구현 범위

- Stream maintenance API를 호출할 운영 CronJob/스케줄과 관리자 인증 체계 확정
- 실제 embedding 모델 확보 후 Workflow 의미 검색 품질 검증 및 재인덱싱
- transient token delta를 위한 별도 ephemeral streaming channel
- pgvector ANN index와 Workflow risk 사전 계산은
  `docs/performance-backlog.md`의 benchmark 선행 작업으로 관리
