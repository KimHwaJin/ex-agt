# Local Performance Baseline — 2026-08-28

이 결과는 macOS의 로컬 Docker Compose 환경에서 측정한 개발 기준선이다.
운영 용량 산정 자료가 아니며 API가 PostgreSQL에 Task/outbox를 commit하고 `202`를
반환하는 구간만 측정한다.

## 결과

| 요청 수 | 동시성 | 처리량 | p50 | p95 | p99 | 실패 |
|---:|---:|---:|---:|---:|---:|---:|
| 20 | 1 | 127.20 req/s | 3.4 ms | 8.7 ms | 75.1 ms | 0 |
| 100 | 10 | 353.38 req/s | 18.1 ms | 86.4 ms | 87.3 ms | 0 |

두 실행 모두 HTTP `202` 성공률은 100%였다. 동시성 10 실행 직후 Worker는
설정된 command slot 4개를 모두 사용했고 Redis command consumer lag는 104였다.
이는 API가 부하를 빠르게 수락하고 Worker bounded concurrency가 downstream
LLM 처리량에 맞춰 backpressure를 형성한 상태다.

## 해석 제한

- 기존 개발 DB와 Redis volume을 재사용했다.
- Agent 완료시간과 모델 token 처리량은 이 표에 포함하지 않았다.
- 일반 질의 문장으로 실제 Agent Task를 생성했다.
- 운영과 동일한 CPU/memory limit, 네트워크, PostgreSQL 또는 Redis 구성이 아니다.

다음 비교에서는 동일한 요청 문장과 concurrency matrix를 사용하고 Worker의
`ex_agent_worker_operation_seconds`, Redis lag, checkpoint pool wait를 함께
기록한다.

## Worker 재시작 복구 확인

command slot 4개가 모두 처리 중일 때 Worker container를 교체했다. 재시작 직후
새 Worker가 신규 backlog를 먼저 처리해 Stream lag는 0이 되었고, 중단 시점의
pending message 4건은 기존 Task lock 때문에 대기했다. 시험 Task의 stale lock
만료를 명시적으로 재현한 뒤 네 message가 모두 `XAUTOCLAIM`되어 처리됐으며
pending은 4에서 0, 성공 operation counter는 4에서 8로 변경됐다.

이 결과를 반영해 기본 lock TTL/renewal을 60초/10초로 조정하고 renewal이 Stream
claim idle 30초보다 항상 빠르도록 설정 검증을 추가했다. 비정상 종료 시 복구
상한은 lock TTL과 claim idle의 합으로 제한된다.

## 결정론적 Agent 전체 수명주기

프로덕션 LangGraph에 5 ms Fake LLM/Fake Executor 지연을 주고 요청 20개,
동시성 4로 측정했다. DB/Redis나 실제 모델/Executor 용량 산정값이 아니라 graph,
HITL interrupt/resume, SINGLE/MULTI 분기의 회귀 기준선이다.

| 시나리오 | 처리량 | 전체 p50 | 전체 p95 | 계획 p95 | Executor 재개 p95 | 경계 수 |
|---|---:|---:|---:|---:|---:|---:|
| `single_custom` | 55.44/s | 68.5 ms | 72.2 ms | 37.1 ms | 10.6 ms | 1 |
| `multi_analysis` | 32.70/s | 119.3 ms | 123.6 ms | 35.7 ms | 62.2 ms | 3 |

MULTI는 두 operation 완료 재계획, finalize와 execution 완료까지 세 번
checkpoint를 재개하므로 SINGLE보다 Executor 재개 구간이 길다. 동일한 fake
지연과 요청 조합을 유지한 상태에서 코드 변경 전후 값을 비교해야 한다.

Compose 통합 테스트에서는 서로 다른 두 `execution_id`가 실제 Redis lock을
각각 획득해 동시에 PostgreSQL에 기록될 수 있음을 확인했다. 또한 sequence 2를
먼저 발행했을 때 fake Executor history에서 sequence 1을 보충하고, 뒤늦게 온
sequence 1 중복을 추가 기록 없이 ACK해 최종 binding sequence 2와 pending 0을
확인했다.

## 실제 qwen + Executor/Jupyter MULTI 분석

동적 분석 요청 한 건을 Agent REST로 생성하고 HITL 응답을 자동 제출해 전체
수명주기를 측정했다. 내부 `qwen38-27b-fp8`, Agent PostgreSQL/Redis,
Executor PostgreSQL/Redis와 Jupyter를 모두 실제로 사용했다.

| 항목 | 결과 |
|---|---:|
| API 수락 | 0.10 s |
| 최초 승인 가능 계획 | 33.59 s |
| 전체 완료 | 146.33 s |
| Executor Operation | 2개, 모두 성공 |
| Notebook | code 2개, Markdown 1개 |

첫 Operation은 `fetch_dataset`으로 500행 CSV를 만들었다. Agent는 checksum,
size와 identity가 검증된 `text/plain` result representation에서 실제
`artifacts/datasets/agent_multi_e2e.csv` path를 읽고 두 번째
`inspect_dataset(sample_rows=3)` Operation을 생성했다. 두 번째 결과 이후
Executor finalize, 성공 event reconciliation, 한국어 리포트 작성을 거쳐 Agent와
Executor가 모두 `SUCCEEDED`가 됐다.

이 단일 표본은 성능 목표가 아니라 실제 경계 연결의 기능 기준선이다. 모델 호출
횟수와 출력 길이에 민감하므로 운영 성능 판단에는 반복 실행과 percentile 수집이
필요하다.
