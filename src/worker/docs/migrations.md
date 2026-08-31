# 워커 DB 초기화: Alembic

## 전달물

아래 경로는 이 문서 위치가 아닌 저장소 루트 기준이다.

- `worker_migrations/alembic.ini`: ini 위치 기준으로 동작하는 독립 설정.
- `worker_migrations/env.py`: EW_DATABASE_URL로 PostgreSQL에 비동기 연결.
- `worker_migrations/versions/0001_worker_tables.py`: 독립 초기 스키마.
- `worker_migrations/script.py.mako`: 이후 수동 리비전 생성 템플릿.
- `deploy/worker/migrate-job.yaml.example`: Kubernetes 초기화 Job 예시.

리비전 `ew_0001`은 `ew_bindings`, `ew_inbox`, `ew_commands`, `ew_outbox`,
`ew_audit`를 생성한다. 기본값, PK/FK, UNIQUE/CHECK, 부분 인덱스까지 포함한다.
버전 관리는 `ew_alembic_version`에 기록한다. 기존 서비스의 `alembic_version`,
LangGraph checkpoint 테이블이나 기존 Agent 테이블은 수정하지 않는다.

`EW_NAMESPACE`는 행을 구분하는 값이지 테이블 구분자가 아니다. 같은 DB/schema의
워커 namespace들은 동일 테이블을 공유하므로 migration은 배포 스키마 단위다.

## 새 DB에서 실행

저장소 루트에서 실행한다. 기존 Agent의 alembic.ini와 혼동하지 않는다.

```bash
uv sync --frozen --no-editable
uv run --no-sync --env-file .env \
  alembic -c worker_migrations/alembic.ini upgrade head
uv run --no-sync --env-file .env \
  alembic -c worker_migrations/alembic.ini current
```

`EW_DATABASE_URL=postgresql://USER:PASSWORD@HOST:5432/DB`를 설정한다.
`postgresql+psycopg://`도 허용하며 다른 DB 드라이버는 거부한다. 비밀번호의
URL 인코딩은 유지하고 ConfigParser 설정 문자열로 URL을 재조립하지 않는다.
Alembic만 실행할 때는 EW_REDIS_URL이나 LangGraph extra가 필요 없다.
`.env`는 Alembic이 자동으로 읽지 않으므로 위 uv 옵션이나 배포 환경변수를 쓴다.

이미 head인 DB에 다시 실행해도 업무 데이터를 초기화하지 않는다. PostgreSQL
트랜잭션 안에서 DDL과 버전을 함께 반영하고 advisory lock으로 동시 migration을
직렬화한다. Worker가 실행 중일 때 임의 변경하지 말고 배포 순서를 관리한다.

SQL만 확인하려면:

```bash
uv run --no-sync --env-file .env \
  alembic -c worker_migrations/alembic.ini upgrade head --sql
```

이는 DB를 연결하거나 변경하지 않는다. 출력 SQL을 직접 실행하면 Alembic online
경로의 advisory lock은 적용되지 않으므로 실행 순서를 운영자가 보장해야 한다.

## Kubernetes 배포

이미지에 `/app/worker_migrations`를 포함했다. 제공된
Dockerfile이 아닌 호스트 이미지에 이식하면 경로와 의존성도 맞춰야 한다.
`migrate-job.yaml.example`의 image/Secret을 치환한 후 다음 명령을 Job에서 실행한다.

```bash
alembic -c /app/worker_migrations/alembic.ini upgrade head
```

Job 성공을 확인한 후 API+Agent와 Worker를 배포한다. manifest가 자동으로 두
Deployment의 선후 관계를 만들어 주는 것은 아니다. 재배포 시 Job 이름/릴리스
관리 방식도 호스트 배포 파이프라인에 맞춘다. 이번 작업에서 K8s에 적용하지 않았다.
LangGraph checkpoint 초기화는 호스트가 별도로 수행한다.

## 이미 SQL 초기화를 한 DB

기존 `executor-worker migrate` / `Store.migrate()`는 schema.sql을 실행하는
초기화 도구였다. 현재 CLI와 schema.sql 및 Store.migrate()는 제거되었다.
이후 스키마 변경은 Alembic으로만 관리한다.

초기 리비전은 이미 있는 테이블을 자동으로 채택하거나 `IF NOT EXISTS`로
무시하지 않는다. 부분 생성/스키마 차이를 정상 초기화로 오인하지 않도록 실패한다.

기존 데이터를 보존하며 전환하려면 먼저 백업하고, **5개 테이블의 컬럼·기본값·
PK/FK·UNIQUE/CHECK·인덱스가 ew_0001과 일치하는지 확인**해야 한다. 정확히
일치함을 확인한 환경에서만 다음과 같이 기준 버전을 등록한다.

```bash
alembic -c worker_migrations/alembic.ini stamp ew_0001
alembic -c worker_migrations/alembic.ini upgrade head
```

stamp는 DDL을 실행하거나 스키마를 검증하지 않는다. 일부 테이블만 있거나
스키마가 다르면 위 명령을 실행하지 말고 별도 데이터 보존 전환 작업이 필요하다.
옛 `ew_schema_migrations`는 자동 삭제하지 않는다. 새 초기화 경로는 이를 만들지
않으며, 전환 이후 authoritative 버전 기록은 ew_alembic_version이다.

## 호스트에 Alembic이 있는 경우

호스트에서 새 리비전을 생성한 후 `0001_worker_tables.py`의 테이블/인덱스 생성
작업과 필요한 helper를 복사하고 호스트의 revision/down_revision 체계에 편입한다.
원본 파일은 runtime 모델이나 schema.sql을 읽지 않으므로 독립 복사가 가능하다.
이 경우 호스트의 버전 테이블만 사용하고, 독립 Alembic도 중복 실행하지 않는다.

## 이후 변경과 downgrade

```bash
alembic -c worker_migrations/alembic.ini revision -m "worker schema change"
```

기존 ew_0001은 수정하지 않고 새 리비전에 변경을 작성한다. ORM metadata를
제공하지 않으므로 이 구성은 `--autogenerate` 대신 명시적인 변경 스크립트를 쓴다.
삭제된 schema.sql을 복원해 초기화하지 않으며 새 변경의 기준으로 사용하지 않는다.

초기 리비전의 downgrade는 전체 namespace의 업무 테이블과 데이터를 삭제하므로
기본 거부한다. 새로 만든 폐기 가능한 테스트 DB 또는 별도 승인·백업된 환경에서만
`-x allow_worker_table_drop=true`를 명시해 실행할 수 있다. 운영 rollback 수단으로
사용하지 않는다. 런타임 시작/종료에 downgrade를 연결하지 않는다.

## 검증 및 참고

테스트용 DB는 Alembic으로 생성한다. 신규/반복/동시 upgrade, 기존 서비스 버전
테이블 보존, 실패 시 트랜잭션 rollback, 명시적 downgrade 보호를 검증한다.
삭제된 SQL의 동등성 검증은 제거하고 기존 스키마의 명시적 채택 보호를 검증한다.
현재 결과는 [전환 계획](../../../docs/worker-centered-refactor.md)에 있다.

비동기 연결과 트랜잭션 연결은
[Alembic 공식 가이드](https://alembic.sqlalchemy.org/en/latest/cookbook.html#using-asyncio-with-alembic),
버전 테이블 설정은
[Runtime API](https://alembic.sqlalchemy.org/en/latest/api/runtime.html)를 참고했다.
