# Redis Stream 정리 관리 API

이 API는 임의 Redis key를 받지 않는다. 서버 설정으로 등록된 다음 논리 이름만
허용한다.

- `agent_commands`
- `agent_command_dlq`
- `executor_events`
- `executor_event_dlq`
- `product_events`

`STREAM_MAINTENANCE_OPERATOR_USER_IDS`에 등록된 BFF user ID만 요청할 수 있다.
빈 값은 전체 거부다. 현재 인증 경계는 신뢰된 BFF의 `X-User-ID`이며, 외부에 API를
직접 공개하려면 별도 service-to-service 인증을 먼저 적용해야 한다.

## API

- `POST /api/v1/operations/stream-maintenance/plans`: Redis를 변경하지 않고
  안전 경계를 계산하고 요청·결과를 DB에 감사한다.
- `POST /api/v1/operations/stream-maintenance/jobs`: trim 작업을 DB에 `PENDING`으로
  등록하고 `202`를 반환한다. API 프로세스는 trim을 수행하지 않는다.
- `GET /api/v1/operations/stream-maintenance/jobs`: 생성 시각·ID 기반 opaque cursor
  목록이다.
- `GET /api/v1/operations/stream-maintenance/jobs/{job_id}`: 작업 상태와 결과를
  조회한다.

모든 POST에는 요청자 범위의 `idempotency_key`와 운영 사유가 필요하다. 같은 키와
같은 입력은 기존 작업을 반환하고, 입력이 다르면 `409`다. 각 레코드는
`created_at`, `updated_at`, `created_by`, `updated_by`를 보존한다.

## 안전성과 복구

실제 trim은 Worker lifecycle의 `StreamMaintenanceRecovery`만 수행한다. 같은 실제
Stream key에는 `PENDING` 또는 `RUNNING` trim이 하나만 존재할 수 있다. Worker는
DB claim 이후 기존 `SafeStreamTrimmer`의 Lua를 실행한다. Lua는 실행 순간에 다음
경계 중 가장 오래된 ID를 다시 계산해 exact `XTRIM MINID`를 수행한다.

- 설정된 최소 보존 시간
- 설정된 최소 tail 건수
- 모든 consumer group의 last-delivered ID
- 모든 consumer group의 oldest pending ID

요청은 서버의 `STREAM_RETENTION_SECONDS`와
`STREAM_MINIMUM_RETAINED_ENTRIES`보다 공격적인 값을 지정할 수 없다. Worker가 Redis
trim 뒤 DB 결과 저장 전에 종료되면 claim 만료 후 안전하게 다시 계산한다. 이때
실제 첫 실행의 삭제 건수는 복구할 수 없으므로 결과의
`result_recalculated_after_retry`가 `true`가 된다.

## 배포

Agent Alembic `0011_stream_maintenance`가 필요하다. API와 Worker는 같은 Agent DB와
Redis를 사용해야 한다. API만 띄우면 계획 조회는 가능하지만 등록된 trim은 진행되지
않는다. 정기 CronJob 연계는 아직 하지 않으며, 운영 스케줄과 호출 주체가 확정된 뒤
이 API를 호출하도록 구성한다.
