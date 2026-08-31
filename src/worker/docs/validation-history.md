# 검증 매핑

> 이동 이전의 역사 기록이다. 이 문서의 옛 경로·명령을 현재 실행 안내로
> 사용하지 않는다. 현재 소스는 src/worker이고, 실행·검증은
> [README](../README.md), 현재 결과는 [전환 계획](../../../docs/worker-centered-refactor.md)을 본다.

기존 기능을 독립 모듈에서 재검증하기 위한 목록이다. 원본 서비스 테스트의
과거 통과 결과를 이 전달물의 통과 결과로 대신하지 않는다.

## 현재 검증 기록: 2026-08-31 (정식 main 추가 후)

- 로컬 Ruff lint/format 전체 통과. 코드 줄 길이 79자.
- 로컬·이미지 ty: 삭제된 모듈을 참조하는 테스트 4개를 제외한 범위 통과.
- 로컬 선별 회귀: 53 passed, 34 skipped, 1 deselected.
- Compose 선별 회귀: 87 passed, 1 deselected, skip 없음.
- 신규 main 검증 13개: 시작·연결·실패·종료 단위 테스트 12개와 실제
  Redis → Inbox/Outbox → 세션 checkpoint 재개 통합 테스트 1개.
- API/Worker는 통합 테스트에서도 별도 PostgreSQL saver를 사용했다.
  동일 Executor 이벤트 중복 수신 시 처리 기록·업무 결과가 하나인지 확인했다.
- 이미지 Ruff format 통과, lint는 삭제 모듈 관련 4개 테스트 제외 후 통과.
- 운영 runtime 이미지를 별도 빌드했다. main과 agent_app을 포함하며
  examples 없이 import 가능하고 core는 site-packages에서 로드되는지 확인했다.
- 원본 서비스 소스·실행 컨테이너는 변경하지 않았다. 격리 테스트 DB·Redis만
  사용했고 종료 후 테스트 DB·Redis를 중지했다. K8s rollout은 수행하지 않았다.

전체 테스트 통과라는 뜻은 아니다. 삭제된 DLQ/trim 운영 모듈을 참조하는
아래 4개 테스트 파일 때문에 전체 pytest 수집과 ty 검사에 오류가 발생한다.
컨테이너의 전체 Ruff lint도 같은 삭제 모듈의 import 분류에서 오류가 발생한다.

- tests/test_dlq.py
- tests/test_dlq_integration.py
- tests/test_stream_maintenance.py
- tests/test_stream_maintenance_integration.py

또한 test_legacy_schema_matches_revision_without_silent_adoption은 삭제된
schema.sql을 Store.migrate()로 읽는 테스트라 제외했다. 운영 시작 경로는
이 helper를 사용하지 않는다. CLI 명령 등록은 정리했지만 삭제된 기능이나
legacy 초기화 helper·테스트 자체를 이번 작업에서 복원/삭제하지는 않았다.

### 재현 명령

standalone_worker 디렉터리에서 파일 기반 설치 후 실행한다.

```bash
uv sync --frozen --all-extras --no-editable
uv run --no-sync ruff check .
uv run --no-sync ruff format --check .
uv run --no-sync ty check \
  --exclude 'tests/test_dlq*.py' \
  --exclude 'tests/test_stream_maintenance*.py'
uv run --no-sync python -m pytest -q \
  --ignore=tests/test_dlq.py \
  --ignore=tests/test_dlq_integration.py \
  --ignore=tests/test_stream_maintenance.py \
  --ignore=tests/test_stream_maintenance_integration.py \
  -k 'not test_legacy_schema_matches_revision_without_silent_adoption'
docker compose run --build --rm test python -m pytest -q \
  -p no:cacheprovider \
  --ignore=tests/test_dlq.py \
  --ignore=tests/test_dlq_integration.py \
  --ignore=tests/test_stream_maintenance.py \
  --ignore=tests/test_stream_maintenance_integration.py \
  -k 'not test_legacy_schema_matches_revision_without_silent_adoption'
docker compose run --rm --no-deps test ruff check --no-cache \
  --extend-exclude 'tests/test_dlq*.py,tests/test_stream_maintenance*.py' .
docker compose run --rm --no-deps test ruff format --check --no-cache .
docker compose run --rm --no-deps test ty check \
  --exclude 'tests/test_dlq*.py' \
  --exclude 'tests/test_stream_maintenance*.py'
docker compose stop postgres redis
```

### 과거 기록과의 구분

아래 92개 통과는 운영 도구 삭제 전 기록이며 현재 전체 통과를 의미하지 않는다.

## 과거 검증 기록: 2026-08-31 (Alembic 추가 후·운영 도구 삭제 전)

- 독립 모듈: Ruff lint/format, ty 통과. 코드 줄 길이 79자.
- 독립 모듈 로컬: 54 passed, 38 skipped (DB/Redis 미연결 항목).
- 독립 모듈 Compose: 92 passed, skip 없음.
- Compose 내부에서도 Ruff lint/format, ty 통과.
- Compose 통합 테스트의 DB 초기화는 Alembic upgrade head를 사용했다.
- 이번 검증은 격리 테스트 DB/Redis에만 적용했다. 실제 Agent DB와 K8s는
  변경하지 않았다.

Alembic 추가 이전에 완료한 회귀 검증 기록:

- 독립 모듈 Compose: 84 passed.
- 원본 프로젝트 로컬 회귀: 261 passed, 52 skipped.
- 원본 프로젝트 Compose 전체 회귀: 284 passed, 29 skipped.
- 원본의 skip은 실제 LLM/실서비스 smoke 등 opt-in 항목이다.
- Alembic 추가는 독립 디렉터리에 한정되어 원본 전체 회귀를 다시 실행하지 않았다.
- 원본 API/Worker 및 Executor 컨테이너는 교체·재시작하지 않았다.

두 프로젝트는 별도 uv 환경/lockfile/Compose 테스트를 사용한다. 루트 ty는
독립 디렉터리를 제외하고, 이 디렉터리의 ty를 별도로 실행한다.
테스트 이미지에는 이 폴더의 소스만 포함했고 no-editable로 설치했다.

## 기능별 근거

| 기능 | 독립 모듈 테스트 |
|---|---|
| 정식 main 연결·미설정 차단·종료·실제 세션 재개 | test_main |
| ACK/PEL/reclaim/재시도 상한/DLQ | test_stream_consumer, integration |
| 잠금·PEL lease 유지, 동시 처리 수, 종료 대기 | test_stream_consumer |
| DLQ 원자적 replay/discard·audit·cursor | test_dlq, integration |
| 안전한 Stream trim | test_stream_maintenance, integration |
| 초기 binding 전 이벤트 저장 | test_durable_pipeline |
| Inbox 중복/identity 충돌·원자적 Outbox 생성 | test_durable_pipeline |
| 발행 성공 후 DB 오류·복수 relay claim·순서 | test_durable_pipeline |
| 처리 완료 중복 수렴·실패/대기 구분·운영 복구 | test_durable_pipeline |
| 역순 이벤트 history 보충·복구 불가능 gap | test_durable_pipeline |
| Worker 교체 후 pending 복구 | test_durable_pipeline |
| API/Worker 공통 세션 guard·소유권 상실 | test_guard |
| session=thread / Agent 및 LangGraph 없는 core import | test_contracts |
| 이전 Task 이벤트, Execution별 순번, 사용자 승인 보호 | test_session_graph |
| 중간 노드 복구, PG checkpoint 후 DONE 실패 | test_session_graph |
| 선택적 취소 보상: 접수와 종료 확인 분리 | test_failure_cleanup |
| Alembic 신규/반복/동시 초기화, 기존 스키마 보존 | test_migrations |
| legacy 스키마 동등성, DDL rollback, downgrade 보호 | test_migrations |

기존 core 소비기에서 달라진 부분:

1. DEFER 결과 추가: ACK하지 않고 PEL에 유지하되 재시도 횟수는 쓰지 않는다.
2. 종료 중 이미 취소 중인 task를 다시 cancel하지 않는다. 중복 취소로 세션
   guard의 finally 정리가 중단되는 것을 막는다. Worker 교체 테스트로 확인한다.
3. 운영 CLI의 import/env/명령 이름만 독립 모듈로 변경했다.

실제 PostgreSQL, Redis, PostgreSQL LangGraph checkpoint를 사용한다.
HTTP history는 Executor 계약에 맞춘 MockTransport로 격리한다. 실제 LLM이나
Executor의 코드 실행을 호출하지 않는다. Worker 교체 테스트는 독립 runtime과
pool을 닫고 다시 여는 방식이다. OS SIGKILL/노드 장애까지 검증했다고 주장하지 않는다.

인수 서비스에서 추가 확인할 사항:

- 실제 Agent 노드의 부수 효과가 command_id 기준으로 멱등한가.
- API와 Worker가 같은 checkpoint/namespace/guard/그래프 버전을 사용하는가.
- 신규 Task 시작·승인·취소 경로 모두 짧은 실행 잠금과 긴 채팅 잠금을 구분하는가.
- Executor 제출 직후 프로세스가 종료돼도 제출 키와 execution binding을 복원하는가.
- 최종 실패가 남으면 DLQ뿐 아니라 DB FAILED와 뒤 실행 대기까지 운영자가 확인하는가.
- Redis AOF/복제/백업, Executor history 보존 기간, 세션 영수증 정리 정책이 맞는가.
- 여러 서비스가 공용 Stream을 사용할 때 서로 다른 consumer group/namespace인가.
- 동일 group replica의 핸들러 registry가 같은가.

새 배포의 초기화는 독립 Alembic `ew_0001`을 사용한다. 기존 SQL 파일과 CLI는
삭제되었고 Store.migrate()의 잔여 참조는 정리 대상이다.
앞으로 변경은 Alembic 새 리비전에 기록한다.
현재 기존 Agent DB와의 데이터 이관이나 원본 Worker 교체는 수행하지 않는다.
