# Performance Testing and Metrics

## 목적

성능 변경은 API 수락 지연, Worker 처리시간, backlog와 consumer lag를 함께
측정한다. 단일 latency 숫자만으로 Agent 전체 성능을 판단하지 않는다.

## API 부하 생성

API와 Worker를 실행한 뒤 다음 명령으로 서로 다른 Session의 Task를 동시에
생성한다.

```bash
uv run --no-sync python scripts/load_test.py \
  --requests 100 \
  --concurrency 10 \
  --output load-result.json
```

결과에는 처리량, HTTP status 분포와 min/mean/p50/p95/p99/max 수락 지연이
포함된다. 이 스크립트는 실제 Task를 생성하므로 격리된 성능시험 환경에서
사용한다. 생성 이후 Agent 처리 완료시간은 Prometheus Worker 지표와 Task
event history로 측정한다.

권장 baseline matrix:

| 요청 수 | 동시성 | 목적 |
|---:|---:|---|
| 20 | 1 | 직렬 기준선 |
| 100 | 10 | 일반 부하 |
| 500 | 50 | pool/backpressure 확인 |

각 조합은 최소 3회 실행하고 중앙값을 비교한다. 모델 endpoint와 Executor의
동시성 제한, 데이터 크기, 실행 모드를 결과와 함께 기록한다.

## 전체 Agent 수명주기 benchmark

API `202` 수락 이후의 LangGraph 비용은 결정론적 Fake LLM/Fake Executor로
분리해 측정한다. 프로덕션 graph, route, interrupt/`Command(resume=...)`는
그대로 사용하고 외부 경계의 응답과 지연만 고정한다.

```bash
uv run --no-sync python scripts/lifecycle_benchmark.py \
  --scenario single_custom \
  --requests 20 \
  --concurrency 4 \
  --llm-delay-ms 5 \
  --executor-delay-ms 5

uv run --no-sync python scripts/lifecycle_benchmark.py \
  --scenario multi_analysis \
  --requests 20 \
  --concurrency 4 \
  --llm-delay-ms 5 \
  --executor-delay-ms 5
```

`single_custom`은 실행모드 선택, 계획 승인, 한 번의 Executor 완료 경계와
리포트를 지난다. `multi_analysis`는 동적 분석계획 승인, 두 번의 셀 완료에
따른 적응 판단, finalize, 전체 완료와 리포트를 지난다. 출력은 다음 구간별
mean/p50/p95/max와 전체 처리량을 제공한다.

- `planning_seconds`: 최초 요청부터 승인 가능한 계획까지
- `approval_to_executor_seconds`: 계획 승인부터 Executor 대기 interrupt까지
- `executor_resume_seconds`: 모든 Executor 경계 재개와 MULTI 적응 처리
- `report_seconds`: 증거 수집 시작부터 성공 상태 commit까지

이 benchmark는 LLM/Executor의 실제 성능이나 PostgreSQL/Redis 지연을 나타내지
않는다. 동일 코드 변경 전후의 Graph orchestration 회귀를 잡는 기준선이다.

## 실제 MULTI 분석 E2E

개발 Compose의 Agent API/Worker와 별도 Executor/Jupyter Compose, 내부 qwen
모델을 모두 연결한 검증은 다음 스크립트로 실행한다.

```bash
uv run --no-sync python scripts/live_multi_e2e.py \
  --output /tmp/ex-agent-live-multi-e2e.json
```

스크립트는 동적 MULTI를 선택하고 최초 Plan을 승인한 뒤 다음을 검증한다.

- 첫 `fetch_dataset` Operation과 두 번째 `inspect_dataset` Operation 성공
- 첫 셀 result manifest의 실제 path가 두 번째 셀 parameter에 사용됨
- Executor 최종 상태와 Agent Task 상태가 모두 `SUCCEEDED`
- Notebook에 code cell 2개와 Markdown 성공 리포트 1개 이상 존재

실행 전에 Executor runtime target이 `ACTIVE`이고 Jupyter image의 storage
extension 계약이 현재 Executor와 일치하는지 확인한다.

## Prometheus endpoint

- API: `GET /metrics` (`8010`)
- Worker: `GET /metrics` (`8011`)

주요 지표:

| Metric | 의미 |
|---|---|
| `ex_agent_worker_active` | command/event 활성 슬롯 수 |
| `ex_agent_worker_operation_seconds` | Worker 처리시간 histogram |
| `ex_agent_worker_retries_total` | Redis/DB transport 재시도 |
| `ex_agent_lock_contention_total` | Task/Execution 분산 락 충돌 |
| `ex_agent_delivery_backlog` | DB outbox 상태별 건수 |
| `ex_agent_redis_stream_pending` | consumer group pending 건수 |
| `ex_agent_redis_stream_lag` | consumer group lag |
| `ex_agent_checkpoint_pool` | checkpoint pool 사용·대기 통계 |
| `ex_agent_database_pool` | API/Worker SQLAlchemy pool 사용량 |
| `ex_agent_sse_connections` | API process의 활성 SSE 연결 수 |

## 장애 검증

다음 조건에서 Task 또는 event 유실이 없어야 한다.

1. Redis를 잠시 중단한 뒤 재시작한다.
2. Worker 처리 중 PostgreSQL 연결을 중단했다가 복구한다.
3. Worker container를 command 처리 중 종료하고 재시작한다.
4. 동일 Executor event를 중복 발행한다.
5. 같은 Execution의 event sequence를 역순으로 전달한다.

Redis/DB transport 오류에는 지수 backoff가 적용된다. 실패한 Executor event는
ACK하지 않으며 idle timeout 이후 다른 event consumer가 다시 claim한다. 같은
`execution_id`는 Redis lock으로 직렬화되고 sequence gap은 Executor REST history로
복구한다.

실제 Worker 재시작 복구는 다음 스크립트로 검증한다. 개발 Compose의 Worker를
두 차례 `SIGKILL`하므로 격리된 검증 환경에서만 실행한다.

```bash
uv run --no-sync python scripts/live_worker_restart_e2e.py \
  --output /tmp/ex-agent-worker-restart-e2e.json
```

스크립트는 계획 생성 중 Worker를 종료해 동일 Task가 승인 대기 상태로
복원되는지 확인한다. 이어서 Executor 실행 중 Worker를 다시 종료하고 다음을
검증한다.

- 실행 중인 Session의 새 Task 요청이 `409` 또는 `423`으로 거절됨
- Worker가 없는 동안 완료된 Executor event가 재시작 후 동일 Task에 반영됨
- Agent Task와 Executor가 모두 `SUCCEEDED`로 종료됨
- 성공 리포트 완료 후 Session lock이 해제되어 후속 일반 질의가 성공함

2026-08-30 실제 qwen/Executor/Jupyter 환경 측정에서 stale event를 하나씩
30초 간격으로 처리하던 기준 구현은 실행 단계 복구에 153.6초가 걸렸다.
재claim된 첫 event에서 Executor REST history의 최신 sequence까지 한 번에
catch-up하도록 변경한 뒤 62.1초로 줄었다(59.6% 감소). 이 값에는 의도적인
20초 Worker 중단, 30초 claim idle, 성공 리포트 모델 호출 시간이 포함된다.
같은 실행의 Redis pending event는 검증 종료 후 0건이었다.

4번과 5번 및 서로 다른 Execution의 실제 병렬 처리는 다음 Compose 테스트가
Redis consumer group/lock/ACK와 PostgreSQL binding/inbox를 함께 사용해 검증한다.

```bash
docker compose --profile test run --rm test \
  uv run --no-sync pytest -q tests/test_worker_event_integration.py
```
