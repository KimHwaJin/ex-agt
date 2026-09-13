# Management API

Python **3.11** 기반 사용자·프로젝트·세션 관리 API입니다.
기존 Agent, Worker, 전달 패키지와 구버전 운영 자료는 제거했습니다.
현재 소스에는 에이전트 실행, Redis 소비, 메시지 및 메모리 처리가 없습니다.

## 브랜치 운영

이후 개발의 기준 브랜치는 `agt`입니다. 새 작업은 최신 `agt`에서
`feature/<작업명>` 브랜치를 생성하여 진행하고, 검증 후 `agt`로 합칩니다.
기존 `main`에는 별도 요청 없이 병합하지 않습니다.

## 구조

```text
app.py                       # 외부 템플릿과 동일한 루트 실행 진입점
config_dev.yaml              # 개발 설정
config.yaml                  # 운영 설정 (인증·로깅 연결 후 사용)
src/
  api_service/               # HTTP, 요청 스키마, 인증 의존성
  agent_service/
    application/             # 사용자·프로젝트·세션 유스케이스
    domain/                  # 엔티티와 도메인 오류
    infrastructure/database/ # PostgreSQL 저장소
    runtime/                 # 프로세스별 DB 풀 수명 관리
    bootstrap/               # YAML 설정·외부 로깅 초기화
    settings.py              # 설정 검증
migrations/                  # 현재 관리 API 전용 마이그레이션
tests/                       # 현재 구현만 검증
```

## 개발 환경

`uv sync --locked --python 3.11`로 설치합니다.
설정 프로파일은 `SERVICE_ENV=development`(기본값),
`SERVICE_ENV=production`에 따라 선택됩니다.
운영 설정이 잘못되면 개발 설정으로 대체하지 않고 시작을 중단합니다.

DB 연결은 YAML `database_url` 또는 `MANAGEMENT_DATABASE_URL`,
커서 서명 키는 YAML `cursor_secret` 또는 `MANAGEMENT_CURSOR_SECRET`로
설정합니다. 환경변수를 못 쓰는 배포에서는 YAML에 값을 주입하고
실제 비밀값은 Git에 커밋하지 않습니다.

현재 변경은 개발용 새 기준선입니다. 과거 DB를 이 마이그레이션으로
업그레이드하지 마세요. 기존 실행 서비스 및 DB는 정리 대상이 아닙니다.

## 실행 및 검증

```sh
uv sync --locked --python 3.11
docker compose up -d postgres
uv run --locked python -m alembic upgrade head
uv run --locked python app.py
```

Python 실행 환경이 이미 활성화돼 있으면 `python app.py`로 실행합니다.
설치는 src layout 패키지 설치 방식이며, `PYTHONPATH` 조작은 불필요합니다.
기본 포트는 8020이고 Swagger는 `/docs`, 헬스체크는 `/health/live`,
`/health/ready`입니다. DB 풀은 FastAPI lifespan에서 열고 닫습니다.
테이블은 서버가 자동 생성하지 않고 배포 전 Alembic으로 생성합니다.

컨테이너만으로 실행하려면 다음 명령을 사용합니다.
소스를 마운트하지 않고 이미지에 패키지를 설치하는 방식입니다.

```sh
docker compose up --build -d api
docker compose --profile test run --build --rm test
docker compose down
```

Compose의 `management_test` DB는 **tmpfs 기반 임시 DB**입니다.
컨테이너를 내리거나 재시작하면 데이터가 소실됩니다.
개발/테스트에만 사용하고 운영 DB는 외부 PostgreSQL로 연결합니다.
통합 테스트는 임의 사용자를 생성하며 기존 행을 초기화하지 않습니다.
테스트 중인 API에서 같은 테스트 DB를 수동 사용하지 마세요.

```sh
uv run --locked ruff check .
uv run --locked ruff format --check .
uv run --locked ty check
uv run --locked pytest -q -m 'not postgres'
```

로컬 PostgreSQL 통합 테스트는 마이그레이션 후 다음과 같이 실행합니다.
테스트 DB 설정 없이 실행하면 PostgreSQL 테스트는 명시적으로 skip됩니다.

```sh
export MANAGEMENT_TEST_DATABASE_URL=\
postgresql://management:management@127.0.0.1:55439/management_test
uv run --locked pytest -q
```

## 관리 API 계약

공통 prefix는 `/api/v1`입니다. 관리 API에는 사용자 사번이나 UUID를
body/query에 섞지 않고 인증 의존성을 통해 전달합니다.
향후 에이전트 실행 API와 Gaia 호환 입력은 별도 구현 범위입니다.

| 메서드 | 경로 | 입력 | 응답 |
| --- | --- | --- | --- |
| POST | `/me` | `X-User-Id`: 사번 | 200, 사용자 + 기본 프로젝트 |
| POST | `/projects` | name, description | 201, 프로젝트 |
| GET | `/projects` | limit, cursor | 200, 페이지 |
| GET | `/projects/{project_id}` | - | 200, 프로젝트 |
| PATCH | `/projects/{project_id}` | version, name/description | 200 |
| DELETE | `/projects/{project_id}` | - | 204 |
| POST | `/sessions` | project_id, title | 201, 세션 |
| GET | `/sessions` | project_id, limit, cursor | 200, 페이지 |
| GET | `/sessions/{session_id}` | - | 200, 세션 |
| PATCH | `/sessions/{session_id}` | version, title | 200 |
| DELETE | `/sessions/{session_id}` | - | 204 |

- `/me` 이외에는 `X-User-UUID`에 `/me`에서 받은 내부 UUID를 전달합니다.
- `/me`는 중복/동시 호출해도 사용자와 기본 프로젝트를 하나씩 보장합니다.
  세션은 자동 생성하지 않습니다.
- 프로젝트/세션 생성에는 `Idempotency-Key` 헤더가 필요합니다.
  같은 사용자·작업·키·본문이면 최초 생성 응답을 반환하고,
  같은 키에 다른 본문이면 409입니다. 삭제 후 재호출은 404입니다.
  키는 1~200자 영숫자 또는 `.`, `_`, `:`, `-`를 허용합니다.
- 프로젝트 응답에는 소유자의 `user_uuid`와 사번 `user_id`가 포함됩니다.
- 사용자·프로젝트·세션에 `created_at/by`, `updated_at/by`를 반환합니다.
  by는 내부 사용자 UUID이고 timestamp는 타임존 포함 시각입니다.
- PATCH의 version은 현재 조회 값입니다. 경합/구버전 수정은 409입니다.
  프로젝트 description은 명시적 null로 지울 수 있습니다.
- 삭제는 논리 삭제입니다. 기본 프로젝트는 삭제 불가(409)이고,
  프로젝트 삭제 시 하위 세션도 조회/수정/신규 생성할 수 없습니다.
- 소유하지 않거나 삭제된 리소스는 404입니다. 비활성 사용자는 403입니다.
- 목록은 `items`, `next_cursor`, `has_more`를 반환합니다.
  limit 기본 20/최대 100, 정렬은 생성일 내림차순 + UUID 내림차순입니다.
  커서는 서명되며 소유자와 목록 범위가 다르면 422입니다.
- 일반 오류는 code/message/request_id, 검증 오류는 errors도 포함합니다.
  오류 응답에는 원본 요청 본문·인증 토큰을 넣지 않습니다.

## 외부 템플릿 연동

템플릿의 루트 `app.py`와 기존 FastAPI 앱을 그대로 두고, 시작 전에
`api_service.factory.install_management_api(app, settings)`를 호출합니다.
기존 lifespan을 보존하며, API 코드는 Agent/Worker를 import하지 않습니다.

인증은 `api_service.security.IdentityProvider`의 비동기 메서드
`employee_id(request) -> str`, `user_uuid(request) -> UUID`를 구현해
교체합니다. 팩토리는 `factory(settings) -> IdentityProvider` 형식이며,
YAML의 `identity_provider_factory: package.module:function`으로 연결합니다.

- `development_header`: 로컬 개발용이며 인증 기능이 아닙니다.
- `trusted_header`: 신뢰 게이트웨이만 접근 가능하도록 제한하고,
  게이트웨이가 클라이언트 identity 헤더를 제거·재설정해야 합니다.
  `trusted_proxy_cidrs`는 직접 연결한 게이트웨이 주소 범위입니다.
  ASGI 서버의 proxy header 해석을 끄고 네트워크 접근도 제한해야 합니다.
- `external`: 플랫폼 인증/미래 SSO 어댑터입니다. 실제 토큰 검증 후
  내부 UUID로 매핑해야 합니다. 클라이언트 헤더만 믿으면 안 됩니다.

운영 logging은 `logging_mode: host`,
`logging_initializer: package.module:function`, `logging_yaml`을 설정합니다.
initializer는 YAML `Path` 하나를 받아 내부 로깅 라이브러리를 호출하는
얇은 어댑터입니다. 템플릿이 이미 로깅을 초기화했다면 설치 함수의
기본값인 `initialize_host_logging=False`로 유지합니다.

현재 `config.yaml`은 실제 인증·로깅 어댑터/DB/비밀키를 연결하기 전까지
시작이 실패하도록 돼 있습니다. 사내 라이브러리를 임의로 모사하지 않습니다.
현재 버전은 관리 API만 구현하며 메시지, 에이전트 실행, 프로젝트 메모리,
SSO 자체 구현, 논리 삭제/멱등 기록의 물리 정리 정책은 포함하지 않습니다.
