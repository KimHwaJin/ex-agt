# 앱 시작 시 빈 DB 초기화

`database_bootstrap: initialize_if_empty`를 명시한 경우에만 동작한다.
기본값은 `off`다. API와 별도 worker 모두 공유 런타임의 시작 단계에서
DB 풀/그래프/작업 소비를 시작하기 전에 수행한다.
YAML에 끄는 값을 명시할 때는 `database_bootstrap: "off"`처럼 따옴표를 쓴다.
따옴표 없는 off는 YAML 로더에서 boolean으로 해석될 수 있다.

## 내부 템플릿 설정

```yaml
AGENT_SERVICE:
  database_bootstrap: initialize_if_empty
  database_bootstrap_timeout_seconds: 60
  database_migration_config: alembic.ini
```

로컬 `config_dev.yaml` / `config.yaml`에서는 위 세 항목을 최상위에 둔다.
템플릿은 기존대로 HCP_ACTIVE_PROFILE에 맞는 YAML을 읽고 Settings에 전달한다.
`GaiaService.core` 수정이나 라우터별 초기화 코드는 필요 없다.

전달 시 최신 `src/d_test/`, 루트 `alembic.ini`, `migrations/` 전체가 필요하다.
`migrations/env.py`도 반드시 교체한다. 설정 파일 경로는 프로세스 작업 경로를
기준으로 해석하므로 다른 경로에서 실행한다면 절대 경로를 지정한다.
마이그레이션 디렉터리는 alembic.ini 위치를 기준으로 찾는다.

PostgreSQL 서버와 `chatapp` DB는 미리 준비해야 한다. DB 생성, 계정 생성,
기존 데이터 삭제, DB 이름 변경은 수행하지 않는다. 최초 초기화 계정에는
스키마/테이블/인덱스 생성 권한이 필요하다. 체크포인트 DB를 따로 지정했다면
그 DB에도 같은 준비가 필요하다. 권한을 제한하려면 배포 단계에서 초기화하고
앱의 설정은 `off`로 유지한다.

## 상태별 동작

| 확인 결과 | 동작 |
| --- | --- |
| 관리 스키마와 버전 테이블 모두 없음 | Alembic head까지 생성 |
| 체크포인트 스키마 없음 | 공식 AsyncPostgresSaver.setup으로 생성 |
| 버전과 필수 테이블/컬럼이 준비됨 | DDL 없이 계속 시작 |
| 버전이 이전 버전 또는 앱보다 새 버전 | MIGRATION_REQUIRED로 시작 실패 |
| 빈 스키마만 있거나 일부 테이블/컬럼이 없음 | UNMANAGED_OR_PARTIAL_SCHEMA로 시작 실패 |
| 마이그레이션 파일 없음 | MIGRATION_FILES_MISSING으로 시작 실패 |
| 설정한 전체 대기/작업 시간 초과 | BOOTSTRAP_TIMEOUT으로 시작 실패 |
| 연결/권한/DDL 등 기타 오류 | BOOTSTRAP_FAILED로 시작 실패 |

`management`와 `public.management_alembic_version`, 설정된 체크포인트 스키마를
검사한다. 다른 애플리케이션의 스키마가 있다는 이유로 실패하지는 않는다.
스키마를 미리 빈 상태로 만들어 두지는 않는다. 관리 또는 체크포인트 한쪽만
정상이고 다른 쪽이 완전히 없다면, 없는 쪽만 생성할 수 있다.
두 저장소를 사전 검사한 뒤 DDL을 시작하므로, 이미 있는 쪽이 불완전하다면
정상적으로 빈 다른 쪽에도 새 테이블을 만들지 않는다.

검사는 버전과 필수 테이블/컬럼 존재 검사다. 모든 컬럼 타입, 인덱스,
제약조건 또는 업무 데이터의 무결성을 완전 검증하는 도구는 아니다.

## 동시 시작과 실패 복구

관리 DB는 `chatapp:management-schema`, 체크포인트 DB는
`checkpoint-setup:<schema>` 키의 PostgreSQL advisory lock으로 직렬화한다.
잠금은 DDL을 실행하는 바로 그 연결이 보유한다. 같은 DB를 쓰는 여러 프로세스나
Pod가 동시에 시작하면 첫 프로세스가 초기화하고 나머지는 확인 후 건너뛴다.
명시적 Alembic/체크포인트 초기화 명령도 같은 잠금 규약을 사용한다.
잠금 획득은 짧은 try-lock과 비동기 재시도로 처리한다. 잠금 대기 SQL이
트랜잭션 스냅샷을 오래 보유해 체크포인트의 동시 인덱스 생성을 막지 않게 한다.

`database_bootstrap_timeout_seconds`는 잠금 대기를 포함한 전체 작업 제한이다.
기본 60초, 허용 범위 1~600초다. 취소/실패 시 전용 연결을 닫아 잠금을 해제한다.
관리 스키마 생성은 트랜잭션으로 수행한다. 체크포인트 공식 마이그레이션에는
CREATE INDEX CONCURRENTLY가 있어 autocommit으로 실행한다. 따라서 프로세스가
초기화 중 중단되면 체크포인트가 부분 생성될 수 있다. 두 DB 전체의 초기화를
하나의 원자적 트랜잭션으로 보장하지는 않는다.

부분 생성 상태를 다음 시작에서 자동 삭제하거나 복구하지 않는다. 상태와
백업을 확인한 뒤 운영자가 명시적 마이그레이션/복구 여부를 결정한다.
연결·권한 오류도 무시하지 않으므로 초기화에 실패한 앱은 요청을 받지 않는다.
로그는 `agent_service.database` 로거의 `database_bootstrap_started`,
`database_initialized component=...`, `database_current component=...`로
진행 단계를 보여준다. 예외 메시지에는 연결 URL과 SQL 파라미터를 포함하지 않는다.
실패는 `database_bootstrap_failed code=...`로 기록한다.

기존 버전 업그레이드는 여전히 별도 배포 단계에서 수행한다.

```python
from d_test.agent_service.migrate import main as migrate

migrate(settings)  # 동기 배포 진입점에서 명시적으로 호출
```

## 격리 테스트

`tests/test_database_bootstrap.py`의 PostgreSQL 테스트는 별도의
`MANAGEMENT_BOOTSTRAP_TEST_DATABASE_URL` 설정이 있어야 실행된다.
이 URL은 반드시 테스트 전용 서버의 chatapp DB를 가리켜야 한다.
테스트는 같은 서버에 UUID 이름의 임시 DB를 만들고 자신이 만든 DB만 삭제한다.
운영/공유 DB 서버에서는 실행하지 않는다.

구현 시 검증 결과:

- Python 3.11 파일 설치형 Docker 이미지에서 전체 테스트 153개 통과.
- 같은 빈 DB에 비동기 초기화 4개 / 별도 프로세스 3개 동시 시작 통과.
- 기존 데이터와 버전 행 보존, 구버전/부분 생성 거부, 잠금 시간 초과 후 재시도.
- 관리 DDL 실패 롤백, 체크포인트 부분 생성 후 다음 시작 차단.
- 마이그레이션 명령 없이 루트 app.py 시작 후 ready와 POST /me 모두 200.
- API 재시작 시 양쪽 모두 database_current 기록, 같은 사용자 UUID 보존.
- ruff(79자), ty 검사 통과. 실제 private Gaia 라이브러리는 모의 호스트로만 검증.
