# 전달용 Worker Redis 6.0.8 재검증

## 현재 상태

아래는 **수정 전** 발견 기록이다. 후속 작업에서 동일성 비교를 수정했고,
실제 Executor 이력 보충과 새 SINGLE/MULTI 실행·Pending 회수까지 통과했다.
현재 판정과 전달 방법은 [보완 후 재검증 기록](
verification-event-identity-fix-2026-09-06.md)을 따른다.

## 수정 전 판정: 실제 정상 실행 성공, 이력 보충 복구는 보완 필요

아래 첫 회귀 검증 이후 실제 공유 Redis도 6.0.8로 교체하고, 최신 Executor
`1f89ce7`과 기존 Jupyter에 연결하여 전달 패키지를 검증했다.
**Redis 명령 호환 경로는 동작하지만 실제 REST/Stream 이벤트의 동일성 비교에
결함이 있어 전달용 Worker 전체 검증 완료로 판정하지 않는다.**

| 검증 | 결과 |
| --- | --- |
| 기존 독립 패키지 테스트 | 54 passed, 2 skipped, 6.33초 |
| Ruff lint/format, ty | 모두 통과, Python 파일 35개 |
| 실제 SINGLE 제출 → Jupyter 실행 → Worker → 그래프 재개 | 성공 |
| 실제 MULTI 2회 operation → finalize → 그래프 완료 | 성공 |
| Worker 프로세스 종료/재시작 및 실제 Pending 4건 회수 | 회수·재개 성공, 이력 보충 오류 동반 |
| 동일 실제 이벤트를 Redis 수신 후 REST에서 재수신 | 실패 |
| 실제 terminal 이벤트만 도착한 Inbox의 누락 prefix 보충 | 실패, last_sequence=0 유지 |

2개 skip은 실제 Executor URL/Execution ID를 명시해야 실행하는 신규 opt-in
테스트다. 실제 환경변수를 넣은 별도 실행에서는 해당 2개가 모두 실패했다.
54개 통과 결과만으로 실제 Executor 연동 전체가 통과한 것으로 해석하면 안 된다.

## 실제 실행 증거

- SINGLE: `cc3a3895-103f-47cb-904f-610cc363bce0`, `SUCCEEDED`.
  - `[10, 20, 30, 40]`의 개수 4, 평균 25.0 출력 확인.
  - operation 완료, execution 완료 이벤트 2개를 그래프에서 처리.
- MULTI: `8ea2c204-ac9f-405b-a6d0-80dc6925daf1`, `SUCCEEDED`.
  - 첫 operation에서 만든 `values`를 다음 operation에서 재사용, 평균 25.0.
  - 첫 결과 처리 후 Worker 프로세스를 정상 종료.
  - 종료 중 실제 Executor가 발행한 두 번째 operation 이벤트 4개를 별도
    consumer가 읽고 ACK하지 않아 PEL에 남김. 이벤트를 합성하거나 본문을
    변경하지 않았다. 프로세스 강제 kill 테스트와는 구분한다.
  - 새 Worker 프로세스가 Redis 6.0.8 호환 회수 경로로 처리하고 그래프 재개.
  - operation 완료 2개, execution 완료 1개를 처리.
- 실제 Executor REST 이력의 event_id 순서와 그래프 수신 event_id 순서가 일치.
- 두 실행 합계 command `DONE=5`, outbox `SENT=5`, 그래프 receipt 5개.
- 해당 ingress Pending 0. 단 MULTI 종료 직후 복구 상태는
  `catch_up_version=4`, `caught_up_version=0`, `last_error`가 남아 있었다.
- 실행 노트북은 실제 Executor notebook API로 조회해 코드와 출력까지 확인.
- LangGraph는 PostgreSQL checkpointer, `thread_id=session_id`, 동기 내구성
  저장을 사용했다. 업무 처리와 receipt 기록 노드를 분리한 기존 경계를 사용했다.
- LLM이나 원본 `ex_agent`/`agent.runtime`은 사용하지 않았다. 수령자 역할의
  최소 테스트 그래프와 **non-editable 설치된** 전달 패키지로 실행했다.

처음 두 번의 테스트 드라이버에는 상태명 오타와 비동기 이벤트 발행 대기 누락이
있어 정리 후 재실행했다. 해당 미완료 MULTI 실행 2개는 테스트 cleanup에서
취소했다. 위 최종 두 실행과 구분한다.

## 발견한 결함: 실제 REST와 Stream 표현을 동일성 충돌로 오인

관련 파일:

- `src/worker/contracts.py`: `extra="allow"`, `occurred_at: str`.
- `src/worker/ingress.py`: REST items 전체를 ExecutorEvent로 검증한 뒤 ingest.
- `src/worker/store.py`: 이미 저장된 JSON과 새 model_dump 전체를 직접 비교.

실제 같은 event_id의 차이:

| 필드 | Redis 원문 | REST 이력 |
| --- | --- | --- |
| occurred_at | `2026-09-06T10:47:18.990828+00:00` | `2026-09-06T10:47:18.990828Z` |
| created_at/by/type, updated_at/by/type | 없음 | 있음 |
| delivery | 없음 | 발행 상태, 재시도 수, published_at 등 있음 |

핵심 이벤트 ID·타입·순번·payload는 같다. 그런데 표현/조회 메타데이터 차이로
`ValueError: Conflicting event identity or sequence`가 발생한다.
실제 terminal 이벤트(순번 10)만 격리 Inbox에 넣고 REST로 1~10을 보충하면,
마지막 이벤트 재수신에서 충돌하여 routing 커서가 0에 남는 것을 재현했다.
공유 Redis 원문이나 다른 서비스 consumer group은 변경하지 않는 테스트다.

이는 Redis 6.0.8 전용 명령 문제가 아니라 **전달 Worker의 실제 Executor
이벤트 정규화/동일성 비교 문제**다. 이번 요청은 전환·테스트이므로 운영 코어는
수정하지 않았고, 결함을 숨기지 않는 opt-in 재현 테스트만 추가했다.

다음 보완은 REST/Stream의 불변 이벤트 필드를 같은 형태로 정규화하고, UTC
동일 시각을 같게 비교하되 실제 payload/순번/ID 충돌 검증은 유지하는 것이다.
기존 Inbox에 저장된 이벤트에도 일관되게 적용해야 한다.

### 실제 계약 재현 테스트

`tests/test_live_event_contract.py`를 추가했다. 먼저 격리 PostgreSQL에
마이그레이션할 수 있는 `TEST_DATABASE_URL`을 준비한다. 기존 Agent/Executor
업무 DB를 테스트 DB로 사용하지 않는다. 실제 Redis는 읽기만 한다.

```bash
TEST_DATABASE_URL='<isolated-worker-test-db-url>' \
TEST_REDIS_URL='<actual-executor-redis-url>' \
TEST_EXECUTOR_URL='http://localhost:8000/api/v1' \
TEST_EXECUTION_ID='8ea2c204-ac9f-405b-a6d0-80dc6925daf1' \
uv run pytest -q tests/test_live_event_contract.py --tb=short
```

실제 수행은 Docker test 이미지에서 위 변수를 전달하여 실행했다.
결과: **2 failed in 1.27s**. 테스트를 xfail 처리하거나 우회하지 않았다.
이벤트가 retention으로 삭제되면 보존 중인 다른 소규모 완료 실행 ID를 사용한다.

## 로컬 Redis 전환 및 보존

- 구 서버: 7.4.10, 볼륨 `executor_executor-redis` 보존.
- 현재 서버: **6.0.8**, 볼륨 `executor_executor-redis-608`.
- Agent API/Worker와 Executor를 멈춰 발행·소비를 정지한 뒤 논리 이관.
- 11개 Stream, 메시지 2,641건, Group 9개, Pending 0.
- 모든 원문 ID/필드, Stream last-generated-id, Group last-delivered-id 일치 확인.
- 사용하지 않던 consumer 이름 메타데이터는 복사하지 않았으며 실제 consumer는
  재기동하면서 생성된다. Group의 진행 위치는 보존했다.
- 기존 PostgreSQL 컨테이너와 pgvector 이미지, Jupyter 컨테이너는 교체하지 않았다.
- 최신 Executor 기동 시 자체 migration `0003 → 0004` 적용.
- 원본 Agent의 누락 migration `0009 → 0011`은 DB 백업 후 적용했고
  API/Worker/migrate 이미지를 최신으로 맞췄다.
- 기존 Jupyter 세션 3개는 건드리지 않고 테스트 중 실행 한도만 1→4로 임시 변경.
  테스트 후 원래 한도 1로 복구했다. 따라서 기존 세션을 정리하지 않는 한
  일반 신규 실행은 한도에 막히는 기존 상황이 유지된다.

구 Redis 볼륨은 **전환 시점 스냅샷**이다. PostgreSQL은 이후에도 진전하므로
구 볼륨으로 단순 재연결하면 일관성을 보장하지 못한다. 롤백은 재접수 중지,
최신 DB/Redis 백업 및 상태 대조 후 수행해야 한다. 상위 Redis 재전환 시에도
우선 Agent `AGENT_REDIS_STREAM_MODE=compat`을 유지한다.
현재 Executor Compose는 이미지가 `redis:6.0.8`로 명시되어 있으므로
`REDIS_IMAGE` 환경변수만 바꿔서는 서버 이미지가 바뀌지 않는다.

테스트 드라이버, 실행별 notebook/detail JSON 및 DB 백업은 로컬
`/private/tmp/redis608-switch.5cfH3C/`에 있다. 이 경로는 임시 보관소이며
장기 보관 또는 Git 전달물로 간주하지 않는다. 핵심 결과는 본 문서에 남겼다.
테스트 전용 PostgreSQL/Redis 및 Worker 컨테이너, 이번 테스트 namespace의
consumer group 3개와 command Stream 3개는 증거 백업 후 정리했다.
실제 Executor 실행 이력·노트북 및 공유 Stream 원본은 보존했다.

## 앞서 수행한 격리 회귀 검증

2026-09-06 독립 전달 패키지의 Docker test 이미지를 다시 빌드해 확인했다.

- 실제 서버: Redis 6.0.8 (`INFO server` 확인)
- PostgreSQL: 격리된 postgres:17-alpine, 테스트 전용 tmpfs DB
- Python 클라이언트: redis-py 5.3.1
- 설치: `uv sync --frozen --no-editable`, PYTHONPATH 주입 없음
- 전체 테스트: **54 passed in 6.73s**, 실패/skip 없음
- `ruff check --no-cache .`: 통과
- `ruff format --no-cache --check .`: 34개 파일 통과
- `ty check`: 통과

검증 범위는 Worker 소비·Inbox/Outbox·중복/역순·재시도·runtime 교체 복구,
Pending 페이지 회수·동시 회수 경쟁·heartbeat·삭제된 본문·신규 메시지 공정성,
ACL 오류·일시 연결 오류·health 및 최소 LangGraph 연결이다.
패키지가 원본 `ex_agent` / `agent.runtime`을 import하지 않는지와 실제 설치된
distribution을 사용하는지도 테스트에서 확인했다.

## 재현 명령

전달 패키지 디렉터리에서 실행한다.

```bash
TEST_REDIS_IMAGE=redis:6.0.8 docker compose -f compose.test.yaml \
  run --build --rm test
docker compose -f compose.test.yaml run --rm --no-deps \
  --entrypoint /bin/sh test -c \
  'ruff check --no-cache . && ruff format --no-cache --check . && ty check'
docker compose -f compose.test.yaml exec -T redis redis-cli INFO server
docker compose -f compose.test.yaml down
```

## 첫 격리 검증 당시의 범위

이 검증은 실제 Redis/PostgreSQL과 테스트 이벤트/HTTP 대역을 사용한다.
업데이트된 실제 Executor/Jupyter에서 발행한 이벤트의 종단 검증은 아니다.

첫 격리 검증 시점에는 최신 Executor 코드가 확인되지 않아 공유 Redis를
재시작하지 않았다. 이후 최신 `1f89ce7`을 확인했고, 실제 전환·연동 결과는
이 문서 상단에 추가했다. 상단 결과가 현재 상태다.
