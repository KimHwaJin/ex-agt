# Executor Worker 인수인계 모듈

이 디렉터리만 전달하면 된다. 기존 `ex_agent` 소스, Task 테이블, Skill,
프롬프트, API는 필요 없다. PyPI 배포를 요구하지 않는 로컬 Python 패키지다.

- 공통 기반: 실제 PostgreSQL Inbox/Outbox/실행 연결, Redis 소비·복구·DLQ.
- 연결 방식: 이벤트 타입별 `async` 핸들러 등록.
- 그래프: `session_id = thread_id`. `task_id`와 `execution_id`도 추적한다.
- 예제: API가 직접 그래프를 시작하고 Worker가 Executor 이벤트로 재개한다.
- 원본 서비스: 이 모듈로 교체하지 않았고, 기존 API/Worker 동작도 그대로다.

## 기능 참조 문서

[전체 기능 목록](docs/features.md)에서 기능 이름, 역할, 담당 소스와
호스트 에이전트의 구현 범위를 확인할 수 있다.

[실제 시작·Agent 연결 가이드](docs/agent-integration.md)에 정식 main,
개발자가 채울 함수, 그래프/API 연결, Kubernetes 실행 방법을 정리했다.
삭제된 운영 도구 관련 잔여 테스트·구형 helper 참조는
[삭제 후 정리 사항](docs/features.md#9-삭제-후-정리-사항)을 확인한다.

## 개발자가 수정할 곳

1. `agent_app.py:create_graph()`에서 API와 동일한 자기 그래프를 로드한다.
2. `agent_app.py:build_handlers()`에서 처리할 타입을 정하고 핸들러를 채운다.
3. API의 실행 제출 노드에서 실행 연결을 등록한다.
4. API와 Worker에 동일한 세션 실행 잠금을 적용한다.

`main.py`가 자원 연결·소비 시작·종료를 담당한다. 개발자는 위 연결 지점을 수정한다.
그래프 factory가 미구현이면 소비 시작 전에 오류로 종료하며 데모로 대체하지 않는다.
진행 핸들러는 구현 후 명시적으로 등록한다. 정상 return은 DONE 처리이므로
로그만 출력하거나 pass인 함수를 업무 핸들러로 등록하면 안 된다.

API 측 연결 등록:

```python
await worker.bindings.register(
    execution_id=execution_id,
    session_id=session_id,
    task_id=task_id,
)
```

`worker` 객체를 API에서 만들어도 `run()`을 호출하지 않으면 소비하지 않는다.
API lifespan에서 `async with`로 열어 DB/Redis 자원을 재사용할 수 있다.
자기 DB pool을 사용하는 경우 `Store(pool, namespace)`만 조립해도 된다.

동일 실행 ID의 연결은 변경 불가다. 한 Task에 여러 Execution은 허용한다.
같은 Task의 모든 Execution은 동일 세션에 속하도록 호스트가 관리한다.
세션 ID는 공유 checkpoint DB 안에서 서비스 간에도 충돌하지 않아야 한다.

## 실행 방법

이 디렉터리를 작업 디렉터리로 사용한다. Python 3.12+, PostgreSQL, Redis가
필요하다. 기존 Executor DB에 별도 Agent DB를 쓰거나, 기존 Agent DB 안에
`ew_*` 테이블을 만들 수 있다. URL은 SQLAlchemy 형식이 아닌 psycopg 형식이다.

```bash
uv sync --frozen --all-extras --no-editable
cp .env.example .env
# .env의 DB/Redis/Executor 주소와 EW_NAMESPACE를 수정한다.
uv run --no-sync --env-file .env alembic upgrade head
# agent_app.py 구현 + checkpoint 테이블을 호스트 배포 절차에서 별도 초기화
uv run --no-sync --env-file .env python main.py
```

새 배포의 DB 초기화는 Alembic으로 수행한다. 버전 테이블은
`ew_alembic_version`이며 기존 Agent의 `alembic_version`과 분리된다.
Alembic은 `EW_DATABASE_URL`만 필요하고 Redis·Agent·LangGraph를 로드하지 않는다.
Worker 시작 시에는 자동 DDL을 실행하지 않는다. 체크포인트 테이블은 기존
Agent 배포 절차의 `AsyncPostgresSaver.setup()` 등으로 별도 초기화한다.

설치 구조, Kubernetes Job, 기존 SQL 초기화 DB의 전환 주의사항은
[Alembic 가이드](migrations/README.md)를 참고한다. 이전 CLI와 schema.sql은
삭제되었다. 잔여 `Store.migrate()`는 동작하지 않으므로 호출하지 않는다.

예제는 이벤트 수신/그래프 재개를 보여주며, 실제 코드 실행을 제출하지 않는다.
`examples/api_integration.py`는 이미 제출된 execution을 예제 그래프에 연결하는
함수다. 실제 Agent에서는 멱등 제출 노드에서 `register()`를 호출하고,
그 노드부터 실행 대기 checkpoint까지 동일한 guard 안에서 진행해야 한다.
Executor 제출 후 API가 죽는 경우 같은 제출 idempotency key로 execution을
복원하고 binding을 재등록해야 한다. Worker는 누락된 제출 의도를 추측하지 않는다.

폴더 복사 방식으로 이식하려면 `src/executor_worker/` 전체를 자기 프로젝트의
Python 패키지 경로로 옮긴다. 정식 시작 구성을 쓰려면 `main.py`와 `agent_app.py`도
함께 옮긴다. 의존성은 `pyproject.toml`을 반영한다. 이 main은 LangGraph 어댑터를
사용하므로 langgraph extra가 필요하다. core만 이식할 때는 선택 사항이다.
Alembic으로 배포하려면 `alembic.ini`와 `migrations/`도 함께 전달하거나,
초기 리비전의 작업 내용을 호스트 Alembic 체계로 편입해야 한다.
테스트/예제까지 그대로 확인하려면 이 디렉터리 전체를 전달하는 편이 간단하다.
전달할 때 `.venv/`, 캐시 디렉터리와 실제 `.env`는 제외한다.

## 처리 흐름과 책임

```text
Executor Redis Stream
  → Inbox 원본 저장 → 원본 ACK
  → binding 확인 + 순서 복구
  → Inbox 처리 표시 + Command + Outbox를 한 DB 트랜잭션으로 저장
  → Outbox relay → 내부 Redis Stream
  → 세션 잠금 → 타입별 핸들러 → DB 완료 표시 → 내부 ACK
```

Inbox에 받은 원본을 먼저 확정하므로 binding 등록 전 도착해도 잃지 않는다.
연결이 없으면 DB에서 대기하고, 등록되면 다시 라우팅된다. 이 대기는 실패
횟수를 소비하지 않는다. 업무 테이블과 checkpoint는 서로 다른 트랜잭션이다.

| 상황 | 처리 |
|---|---|
| DB 저장 전 종료 | 원본 ACK 없음, Redis pending에서 복구 |
| Inbox 저장 후 라우팅 전 종료 | DB에 남은 원본을 다시 라우팅 |
| Command/Outbox commit 전 종료 | 둘 다 rollback, Inbox 처리 표시도 rollback |
| commit 후 발행 전 종료 | Outbox에서 다시 발행 |
| XADD 후 발행 확정 전 종료 | claim 만료 후 동일 command ID 재발행 가능 |
| 처리 완료 후 ACK 전 종료 | DB의 DONE 확인 후 중복 핸들러 호출 없이 ACK |
| 핸들러 중간 종료 | PEL reclaim으로 재호출; 외부 효과는 멱등 처리 필요 |
| 처리 재시도 소진 | DB FAILED + Redis DLQ, 뒤 이벤트는 대기 |

Outbox는 **발행 재시도만** 담당한다. 핸들러 실행 재시도는 PEL이 담당한다.
실행 실패 시 Outbox를 PENDING으로 돌리지 않는다. DB failure_attempts는
실제 핸들러 실패만 센다. 세션 충돌/DB 장애/`DeferEvent`는 예산에서 제외한다.
최종 FAILED 상태는 원본 메시지가 재전달돼도 다시 실행하지 않고 DLQ로 보낸다.

원본 소비기 자체의 RETRY/DLQ 기능도 유지했다. 새 Dispatcher는 재시도 정책
충돌을 피하려고 일반 핸들러 실패를 DB에 세고 DEFER로 PEL에 남긴다.
DLQ의 retry_attempts는 transport 카운터이고, 업무 실패 횟수는 DB에서 확인한다.

정렬은 Execution별 순번 기준이다. 해당 Execution의 앞 Command가 DONE 또는
IGNORED가 된 뒤 다음 Command를 발행한다. 서로 다른 Execution은 병렬 처리하되,
같은 세션의 핸들러는 동시에 실행하지 않는다. 서로 다른 Execution 간의 업무
선후 관계는 호스트가 관리한다. Task 전체의 동시 시작 방지도 호스트 책임이다.

## 이벤트 계약

원본은 Executor의 schema_version `1.0`, event_id, execution_id, event_type,
event_sequence, occurred_at, JSON payload를 받는다. 이벤트 타입은 다음과 같다.

- `execution.started`
- `execution.operation_started`
- `execution.step_started`
- `execution.step_completed`
- `execution.operation_completed`
- `execution.completed`

등록하지 않은 타입은 원본과 IGNORED 상태를 저장하고 순번을 진행한다.
등록한 타입은 **모두** 해당 핸들러로 전달한다. 진행 이벤트를 그래프에 넘길지,
DB/화면에만 반영할지는 호스트가 결정한다. 처리 중 registry가 달라진 replica는
이미 만들어진 Command를 조용히 버리지 않고 대기한다. 같은 group의 모든
replica에는 같은 registry를 배포해야 한다.

순번이 누락되면 `GET /executions/{id}/events?after_sequence=...&limit=...`로
이력을 보충한다. 한 회차의 페이지 크기는 제한되고 다음 회차에서 이어 간다.
reclaim된 기존 이벤트도 history catch-up을 요청한다. gap을 복구하지 못하면
순번을 건너뛰지 않으며 binding의 last_error와 DB 대기 상태에 남긴다.
Executor 이력이 이미 정리되어 사라진 경우 자동으로 복원할 수는 없다.

내부 메시지에는 schema_version `1`, namespace, command_id, generation만 있다.
업무 payload/session/task는 DB에서 읽는다. API에서 임의 내부 Command를
발행하는 용도가 아니다. 핸들러에는 EventContext의 세 ID, 원본 이벤트,
command_id, session 기반 graph_config가 전달된다.

## LangGraph 연결 계약

스킬의 영속성/HITL 지침에 따라 워커와 그래프를 분리했다. 기본 모듈은
LangGraph를 import하지 않는다. `langgraph_adapter.py`는 선택 가능한 **참조
어댑터**이며, 모든 Agent 그래프에 그대로 붙는 마법 같은 어댑터는 아니다.

`examples/session_graph.py`는 다음 상태 계약을 구현한다.

- active_task_id / execution_id: 현재 실행 식별.
- ew_pending: 대기 노드가 받아 checkpoint한 현재 Command.
- ew_receipts: 처리된 command_id → event_id 영수증.
- ew_sequences: execution_id별 마지막 반영 순번.
- EXECUTOR_EVENT interrupt: task_id와 execution_id를 함께 표시.

이벤트 수락 노드와 업무 수행 노드를 분리한다. 그래프가 중간 노드에서 실패하면
같은 Command의 `ainvoke(None)`으로 이어 가고 새 resume를 주입하지 않는다.
업무 완료 영수증 뒤에 남은 노드도 같은 Command만 이어 갈 수 있다.
checkpoint 완료 후 DB DONE 기록이 실패해도 영수증을 확인해 중복 수렴한다.

이미 처리 이력이 있는 이전 실행의 늦은 이벤트는 무시한다. 아직 그래프에
등장하지 않은 새 binding은 API checkpoint를 기다린다. 사용자 승인 interrupt를
Executor 이벤트가 승인하지 않는다. 새 Task로 넘어갈 때 영수증/Execution별
순번을 지우지 않는다. 장기 세션의 영수증 보존/정리는 호스트의 재전달 가능 기간과
맞춰 별도로 결정한다.

API도 반드시 `async with worker.guard.hold(session_id)` 안에서 invoke/resume한다.
이것은 **짧은 그래프 실행 잠금**이지 코드 실행 며칠 동안의 채팅 금지 잠금이
아니다. 이미 동등한 분산 guard가 있다면 Dispatcher와 API 양쪽에 같은 구현을
연결한다. 핸들러 안에서 같은 guard를 다시 잡으면 안 된다.

Redis lease 상실 시 작업 취소를 전달한다. 핸들러는 async 취소에 협조해야 한다.
네트워크 분할, Redis failover, 취소를 무시하는 외부 호출까지 exactly-once로
보장하지 않는다. 외부 쓰기는 command_id + 안정적인 작업 구분자로 멱등성을
확보해야 한다. 함수의 모든 외부 부수 효과를 모듈이 자동 중복 제거하지는 않는다.

## 운영

기본 Worker 포트 `8011`: `/health/live`, `/health/ready`, `/metrics`.
`EW_HEALTH_PORT=0`이면 HTTP listener를 열지 않는다. readiness는 소비 루프와
DB/Redis를 확인하며, LLM/Executor 업무 성공 여부까지 의미하지는 않는다.
metrics는 처리 수/지연/활성 핸들러/DB backlog/Redis pending·lag를 제공한다.

운영 CLI와 DLQ replay/discard·Stream trim 도구는 삭제된 상태다. 명령 등록도
제거했으며 이 README는 더 이상 해당 명령을 실행 방법으로 안내하지 않는다.
소비기의 DLQ 발행과 Store의 Command retry/skip·audit 기반은 남아 있다.
이를 노출할 운영 API나 절차는 호스트에서 별도로 구현해야 한다.
DB FAILED는 Redis 메시지 재발행만으로 재실행되지 않는다. skip도 업무 성공이나
Executor 취소를 의미하지 않으므로 뒤 이벤트를 진행시켜도 안전한지 판단해야 한다.

Redis Cluster는 다중 키 원자 연산의 hash slot 설정이 필요하며, 이 전달물의
기본 검증 대상은 standalone Redis다. 공용 Executor Stream은 임의 trim하지 않는다.

API+Agent와 Worker는 Kubernetes에서 같은 Pod의 두 컨테이너로 띄울 수 있다.
추가 Inbox/Outbox 서비스는 필요 없다. `deploy.yaml.example`은 배포 조립 예시이며
API 앱이나 운영 Secret을 만들지는 않는다. Worker는 SIGTERM 수신 후 신규 소비를
멈추고 기본 25초 동안 처리 종료를 기다린다. Pod 종료 유예는 그보다 길게 둔다.
며칠 걸리는 코드는 Executor에서 수행되므로 Worker가 해당 기간 HTTP를 유지하지
않는다. 프로세스 재시작으로 Executor 작업을 취소하지 않는다.

## 검증

```bash
uv run --no-sync ruff check .
uv run --no-sync ruff format --check .
uv run --no-sync ty check
uv run --no-sync python -m pytest -q
docker compose run --build --rm test
docker compose stop postgres redis
```

Compose는 별도 프로젝트명, 노출 포트 없는 임시 DB/Redis를 쓴다. 기존 Executor와
원본 Agent 컨테이너는 건드리지 않는다. Docker 테스트는 파일 기반 설치
(`--no-editable`)이며 이 디렉터리 밖 소스를 이미지에 넣지 않는다.
위 전체 검사 명령은 삭제된 DLQ/trim 모듈을 참조하는 테스트가 남아 있어 현재
그대로 통과하지 않는다. 현재 유효 범위의 재현 명령과 제외 사유는 검증 문서에 있다.

검증 항목과 한계는 [검증 매핑](VALIDATION.md)을 참고한다.

## 포함하지 않는 업무 기능

Agent 계획/승인/SINGLE·MULTI/보고서, BFF 화면 복원·SSE, Task 접수 및 채팅 잠금,
실제 Executor 제출/취소 정책은 인수자의 Agent에 남긴다. `execution.completed`는
성공·실패·취소 모두 가능하므로 REST 결과를 확인해야 한다. 실패 보상에서 Executor
취소 요청 후 종료를 확인하는 기존 정책은 호스트 핸들러에 연결할 영역이다.
`examples/failure_cleanup.py`에 실제 REST 취소/종료 확인 참조 함수를 제공한다.
자동 활성화하지 않으며, 취소 미확인 시 DeferEvent로 대기하고 성공을 반환하지 않는다.
이 패키지를 import하는 것만으로 그런 업무 정책까지 활성화되지는 않는다.

API 재시도/세션 admission, 사용자의 실제 Agent 외부 효과, 실제 Executor·LLM,
5일 장기 실행, Redis 장애조치, K8s 롤링 배포는 인수 환경의 최종 E2E 검증 대상이다.
전달용 DB 스키마는 원본 agent 테이블을 마이그레이션하거나 데이터를 옮기지 않는다.
