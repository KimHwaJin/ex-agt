# Executor 인프라 공유 설정

기본 `docker compose up -d --build`는 Agent의 `migrate`, `api`, `worker`만
실행한다. PostgreSQL·Redis는 먼저 실행된 Executor 인프라를 사용한다.
기존 Agent 전용 DB/Redis 컨테이너는 자동으로 중지하거나 삭제하지 않는다.

## 연결 설정

macOS Docker Desktop에서 Executor 포트가 호스트에 공개된 경우 `.env`:

```dotenv
AGENT_DATABASE_URL=postgresql+psycopg://agent:agent@host.docker.internal:5432/agent
AGENT_CHECKPOINT_DATABASE_URL=postgresql://agent:agent@host.docker.internal:5432/agent
AGENT_REDIS_URL=redis://host.docker.internal:6379/0
EXECUTOR_BASE_URL=http://host.docker.internal:8000/api/v1
EXECUTOR_SHARED_DIR=/actual/path/to/executor/shared_dir
```

DB/Redis 호스트·포트·인증 정보는 배포 환경에 맞게 변경한다. Redis 서버와 DB
번호는 Executor의 이벤트 발행 대상과 같아야 한다. Compose의 `environment`
설정은 `.env` 값을 실제 컨테이너에 전달하므로, 수정 후 컨테이너를 재생성한다.

PostgreSQL 서버는 공유하지만 DB는 `executor`와 `agent`로 구분한다.
`AGENT_DATABASE_URL`에 Executor DB를 넣으면 안 된다. Agent 도메인 테이블과
checkpoint는 Agent DB에만 저장한다.

## 최초 Agent DB 생성

기존 Agent 데이터를 이전해야 한다면 이 절차로 빈 DB를 사용하기 전에
아래 전환 주의사항부터 확인한다. 다음 명령은 개발 환경의 기본 credential인
`agent/agent`를 사용한다. 운영 환경에는 별도 credential을 준비해야 한다.

Executor 컨테이너 이름과 관리자 role이 각각 `executor-postgres-1`,
`executor`인 로컬 환경:

```bash
docker exec -i executor-postgres-1 \
  psql -U executor -d executor -v ON_ERROR_STOP=1 \
  < deploy/postgres/bootstrap-agent.sql
```

스크립트는 없는 `agent` role/DB만 생성하고 기존 비밀번호나 owner는 변경하지
않는다. pgvector extension은 관리자 권한으로 Agent DB에 설치한다.
기존 role이 있다면 `.env`에는 해당 role의 실제 비밀번호를 사용해야 한다.

```bash
docker compose up -d --build
docker compose ps
curl -fsS http://localhost:8010/readyz
curl -fsS http://localhost:8011/readyz
```

`migrate`는 Agent DB에 migration 후 종료한다. 정상 종료 시 API와 worker가
기동한다. 외부 PostgreSQL/Redis는 Compose `depends_on`으로 관리하지 않으므로
먼저 실행되어 있어야 한다. readiness는 LLM/Executor REST 연결은 검사하지 않는다.

## 기존 Agent 전용 인프라에서 전환할 때

- 먼저 API/worker의 진행 작업과 미처리 command를 확인한다.
- 새 DB로 바꾸면 예전 작업, 승인 대기, checkpoint, execution 연결은 자동으로
  이동하지 않는다. 새 DB에서는 예전 작업을 이어갈 수 없다.
- 기존 작업을 이어가야 하면 API/worker를 정지한 일관된 상태에서 Agent DB
  전체(checkpoint 포함)와 Redis 전달 상태의 이전/복구 계획을 세운다.
  DB만 복사하면 이미 `PUBLISHED`인 command의 Redis 메시지가 빠질 수 있다.
- 빈 DB로 새 테스트를 시작할 경우에도 기존 volume은 보존한다.
  `docker compose down -v`, Redis `FLUSHDB` 등은 사용하지 않는다.
- 새 DB를 기존 Executor Redis에 붙이면 과거 이벤트도 도착할 수 있다.
  새 DB에 binding이 없는 이벤트는 자동으로 새 작업이 되는 것이 아니라
  재시도/DLQ 대상이 될 수 있으므로 consumer group 시작 위치도 확인한다.

기존 컨테이너가 더 이상 사용되지 않는다고 확인한 뒤 중지만 한다:

```bash
docker compose --profile local-infra stop postgres redis
```

## 격리 테스트와 선택적 로컬 인프라

`test` profile은 기존처럼 `test-postgres`, `test-redis`를 별도로 사용한다.
Executor DB/Redis를 통합 테스트 대상으로 사용하지 않는다.

```bash
docker compose --profile test build test-migrate test
docker compose --profile test run --rm test
```

Agent 전용 인프라가 필요한 경우에만 `local-infra` profile을 명시한다.
이때 `.env` DB 호스트는 `postgres`, Redis 호스트는 `redis`로 변경하고,
먼저 인프라를 기동한 뒤 준비 상태를 확인하고 앱을 실행한다.

```bash
docker compose --profile local-infra up -d --wait postgres redis
docker compose up -d --build
```
