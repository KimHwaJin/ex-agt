# 워커 중심 통합 전환 계획

기준 보존 커밋: `391a818`. 작업 브랜치: `codex/worker-centered-refactor`.

## 원칙과 완료 기준

- 공통 기반의 유일한 원본은 `src/worker`다. agent/api/LangGraph를 import하지 않는다.
- 에이전트 업무와 연결 코드는 `src/agent`, HTTP는 최종적으로 `src/api`에 둔다.
- API와 Worker는 동일한 그래프 정의·체크포인트 DB·세션 실행 guard를 사용한다.
- session_id = thread_id. task_id는 업무 단위, execution_id는 Executor 실행 단위다.
- 기존 배포는 새 경로의 검증 완료 전까지 유지한다. 구·신 워커가 같은 업무를
  이중 처리하도록 띄우지 않는다. 이 작업에서 운영 DB·체크포인트를 삭제하지 않는다.
- LangChain create_agent·middleware와 명시적 LangGraph 제어 흐름을 유지한다.
- uv/파일 기반 설치, Ruff·ty, 79자 코드 형식, 격리 Compose 검증을 유지한다.
- 배포 단위는 하나이므로 루트 pyproject/lockfile을 사용한다. 여러 모듈을 설치하는
  [uv 설정](https://docs.astral.sh/uv/configuration/build-backend/)을 명시한다.
  인수자는 worker 모듈만 복사할 수 있다. 이번에 별도 PyPI 배포 체계는 만들지 않는다.

## 단계별 계획

| 단계 | 범위 | 검증 후 다음 단계로 이동하는 조건 |
|---|---|---|
| 1 | 공통 워커 편입·테스트/문서/마이그레이션 이동 | 설치·독립 import·기존/워커 회귀 통과 |
| 2 | 세션 그래프·업무 capability·이벤트 핸들러 연결 | 중복·순서·재시작·다음 Task 격리 검증 |
| 3 | API 직접 호출·승인·취소·호출 복구 전환 | API 장애 경계와 UI 복원·잠금 정책 검증 |
| 4 | Docker/Compose/K8s 실행 경로 전환 | 실제 구성에서 전체 업무 시나리오 통과 |
| 5 | 구형 워커·중복 저장 경로·임시 호환 코드 제거 | 역의존·구 import·미사용 배포 참조 없음 |

1단계에서는 ex_agent의 실행 동작, API 응답 계약, 기존 DB migration을 바꾸지 않는다.
`src/agent/worker_main.py`는 인수용 시작 코드를 옮긴 것으로 아직 기존 Agent에
연결되지 않았다. 미구현 factory가 소비 전에 실패하는 보호를 유지한다.
`ex-agent-api`, `ex-agent-worker`, langgraph dev의 기존 실행 경로는 그대로다.

## 저장과 실행 계약 — 단계별 구현 기준

### 워커와 업무 저장소

워커는 ew_bindings/inbox/commands/outbox/audit의 전달 상태를 소유한다.
Agent는 Task/Message/Plan/Skill·Tool lineage/Workflow/Report를 소유한다.
체크포인트는 그래프 상태이며 화면 복원용 업무 테이블을 대체하지 않는다.
업무 쓰기와 체크포인트가 한 트랜잭션이 아니므로 외부 효과는 멱등 처리한다.
구 task thread와 새 session thread를 자동으로 같은 상태로 취급하지 않는다.
활성 작업 drain·명시적 상태 변환 등 전환 절차와 직렬화된 구 클래스 경로의
호환 여부를 결정한 뒤 배포한다. 코드 폐기 승인을 DB 삭제 승인으로 해석하지 않는다.

### 시작·승인·취소 요청의 내구성

Executor 이벤트 수신 경로와 사용자 요청 접수 경로는 구분한다.
API 직접 invoke로 전환하면서 기존 START/RESUME 커맨드 처리만 제거하면,
Executor 제출 전 API 종료 시 복구할 이벤트가 없는 공백이 생긴다.
요청 ID/멱등 키·입력·대상 interrupt·실행 상태를 먼저 기록하고 같은 세션 guard
안에서 checkpoint를 확인해 재실행/재개하는 계약이 필요하다.
접수 전/접수 후/제출 후 binding 전/checkpoint 후 응답 전 각각을 테스트한다.
기존 커맨드를 재발행하는 계층을 무조건 추가하지 않고 요청 복구 책임을 명확히 한다.
2C에서 Agent 요청 접수 모듈을 구현했다. 호스트/factory와 화면용 상태 반영은
아래 진행 기록의 남은 전환 작업에 해당한다.

### 세션 상태와 결과 반영

새 Task 시작 시 이전 계획·실행·오류·리포트 등 작업 상태를 초기화한다.
대화와 처리 영수증·Execution별 순번은 보존한다. 늦은 이벤트가 다음 Task에
영향을 주거나 사용자 승인 interrupt를 통과시키면 안 된다.
수락 action을 checkpoint한 후 별도 노드에서 결과를 반영하고 receipt를 저장한다.
이미 수락된 노드의 복구는 ainvoke(None)이며 새 resume를 다시 주입하지 않는다.

### 잠금·성공·실패·취소

짧은 분산 실행 guard와 장기 채팅 금지 상태를 구분한다.
코드 실행부터 성공 리포트 저장 또는 최종 실패/취소 확인까지 장기 잠금을 유지한다.
사용자 취소는 Executor의 실제 종료 확인 후 완료로 알린다.
성공일 때만 리포트를 생성하며 실패는 원인 안내로 끝낸다.
동일 command·operation의 제출/append/리포트/업무 저장은 멱등하게 수렴해야 한다.

## 반드시 보존할 시나리오

- 일반 대화·분석 Q&A·분석 실행·자유 코드 실행의 LLM 기반 분기.
- workflow 검색·선택, skill/tool 선택 이유와 파라미터·계획 revision 추적.
- 승인·수정·거절, 자유 코드 전환, SINGLE/MULTI 선택 정책.
- SINGLE 전체 계획 수행, MULTI 결과 기반 다음 셀 구성과 오류 처리.
- 실행 ID 기반 노트북·아티팩트 접근, 성공 리포트 저장과 화면 전달.
- 중복 이벤트·역순·binding 전 도착·lease 상실·처리 중 종료·DONE 전 장애.
- API 중복 접수·승인 재전송·Executor 제출 응답 유실·취소 중 재시작.
- UI 관찰 중단과 실제 작업 취소의 구분, 재연결 후 이력·진행 복원.

## 기존 삭제 항목 정리

사용자가 삭제한 dlq/stream_maintenance 운영 도구를 복원하지 않는다.
이 모듈들만 검증하던 4개 잔여 standalone 테스트는 제거한다. 소비기 자체의
DLQ 발행·ACK·재시도 테스트 및 기존 서비스 운영 도구 테스트는 보존한다.
삭제된 schema.sql을 읽는 Store.migrate와 legacy 전용 테스트는 제거한다.
대신 Alembic의 기존 스키마 자동 채택 금지와 명시적 stamp 보존을 검증한다.
초기 ew_0001 리비전·버전 테이블은 변경하지 않는다.

## 진행 기록

- 준비: 기존 서비스 로컬 기준 261 passed, 52 skipped.
- 준비: 현재 소스와 인수인계 자료를 391a818에 보존. 개인 .env·캐시 및
  별도 복사 문서 `worker-handoff-guide 2.md`는 포함하거나 변경하지 않았다.
- 1단계: 소스 편입과 검증 완료. 기존 src/ex_agent, docker-compose.yml,
  langgraph.json, 루트 uv.lock은 보존 커밋 대비 변경하지 않았다.
- 공통 워커: 격리 Compose 93 passed, 제외·skip 없음. 삭제 모듈 관련 수집 오류를
  정리했고 기존 스키마의 명시적 stamp 보호 검증과 독립성 테스트 5개를 포함한다.
- 전체 로컬: 319 passed, 87 skipped. DB/Redis 및 opt-in 실서비스 미연결 항목이다.
- 전체 격리 Compose: 377 passed, 29 skipped. skip은 실제 모델·API 연결 항목이다.
- Ruff lint/format, ty 전체 통과. 이미지의 Ruff lint·ty도 통과했다.
- root uv 파일 기반 설치로 worker와 agent가 site-packages에서 로드됨을 검증했다.
  모든 worker 하위 모듈을 Agent/LangGraph import 차단 상태에서 불러왔다.
- 기존 langgraph dev 의존성도 잠긴 버전으로 유지했다. 별도 CLI·하위 lockfile은
  제거했지만 기존 서비스의 ex-agent-* 명령은 이번 단계에서 변경하지 않았다.
- Docker에서 하위 Python 캐시를 제외했다. 테스트 이미지가 호스트의 오래된
  bytecode를 재사용하지 않도록 했다. 운영 컨테이너·데이터에는 적용하지 않았다.

### 1단계 전체 회귀 재현

저장소 루트에서 실행한다. 운영 Compose 프로젝트 이름을 사용하지 않는다.

```bash
uv sync --frozen --no-editable --group chat-ui
uv run --no-sync ruff check .
uv run --no-sync ruff format --check .
uv run --no-sync ty check
uv run --no-sync python -m pytest -q
docker compose -p ex-agent-refactor-regression --profile test \
  run --build --rm test python -m pytest -q -p no:cacheprovider
docker compose -p ex-agent-refactor-regression --profile test \
  stop test-postgres test-redis
```

Worker만 검증하는 격리 Compose 명령은 [워커 안내](../src/worker/README.md)에 있다.
실제 Executor·LLM·K8s 배포 검증은 아직 수행하지 않았다.

### 1단계 후속 — 워커 문서 통합

- 워커 안내는 `src/worker/README.md`, 상세 문서는 `src/worker/docs/`로 모았다.
  마이그레이션 안내도 함께 이동했지만 실행 파일은 `worker_migrations/`에 유지한다.
- 남은 `standalone_worker` 안내 파일을 제거했다. 구 가상환경·캐시는 저장소 밖의
  임시 백업으로 이동했다. 워커 동작과 DB 스키마는 변경하지 않았다.
- 문서의 패키지 포함 여부와 로컬 링크를 검사하는 회귀 테스트를 추가했다.
  테스트 이미지에도 링크 검증에 필요한 배포 예시와 전환 계획을 포함한다.
- 전체 로컬 320 passed, 87 skipped. 워커 격리 Compose 94 passed, skip 없음.
  Ruff lint/format, ty 전체 통과. 실제 Executor·LLM·K8s 검증은 포함하지 않는다.
- 이 정리는 문서·경로 통합이다. 아래 2단계 Agent 연결은 아직 완료되지 않았다.

### 2A — 세션 그래프와 워커 연결 경계

- 1단계 변경은 main의 `075543f`로 머지·푸시했다.
  후속 브랜치는 `codex/session-agent-worker-integration`이다.
- `src/agent/graph`에 세션 그래프를 추가했다. 현재 Task 상태는 workflow 객체로
  분리해 매 Task 교체하고, 세션 대화와 워커 영수증/Execution별 순번은 보존한다.
- 기존 31개 업무 노드와 라우팅은 공통 topology로 재사용한다. 구 그래프의
  실행 경로·체크포인트 필드·배포 명령은 바꾸지 않는다.
- 제출 후 binding 등록을 별도 노드로 분리했다. 이벤트 수락 → 결과 조회·반영 →
  영수증 저장 이후 MULTI/실패/취소/리포트로 분기한다.
- API 측 호출 도구 SessionCoordinator는 동일한 SessionGuard 안에서 Task 입력
  중복·소유권·interrupt ID·계획 revision을 검증한다. 요청 큐/복구 루프는 아니다.
- 자세한 구현 계약과 남은 제한은 [Agent 안내](../src/agent/README.md)에 있다.

#### 2A 검증 기록

- uv 파일 기반 설치, Ruff lint/format, ty 통과.
- 전체 로컬: 338 passed, 89 skipped. DB/Redis 및 opt-in 실서비스 항목 제외.
- 전체 격리 Compose: 398 passed, 29 skipped. 실제 모델/API 항목만 제외.
- 세션 Agent + 공통 Worker 범위의 격리 Compose: 114 passed, skip 없음.
- 세션 Q&A, 승인/수정/거절, SINGLE/MULTI, 실패/취소 시 리포트 미생성,
  등록 노드 복구, 수락 후 결과 처리 실패, 영수증 후 리포트 실패를 검증했다.
- 실제 PG/Redis에서는 별도 graph/checkpointer 연결로 API/Worker를 구분하고,
  checkpoint 저장 후 Worker DONE 기록 실패·재전달·다음 Task 격리를 검증했다.
- 업무 서비스는 테스트 대체 구현이다. 실제 Executor/Jupyter·LLM 및 K8s 배포
  검증은 수행하지 않았으며, 운영 진입점도 전환하지 않았다.
- 작업 중 미추적 `파일명 2.py` 등의 복사본이 다수 나타나 테스트 수집·중복
  Alembic revision에 간섭했다. 복사본은 변경·커밋하지 않았다. 최종 검증은
  Git 관리 파일과 이번 변경만 내보낸 임시 스냅샷에서 수행했다.
- 기존 구조 테스트의 AST 리터럴 탐색은 topology 추출에 맞춰 실제 컴파일된
  그래프의 31개 노드 검증으로 바꾸었다. 테스트를 제외한 것이 아니다.

### 2B — Agent 외부 요청과 결과 반영 복구

- 2A를 main `ba03dfe`로 머지·푸시했다. 후속 작업 브랜치는
  `codex/durable-agent-effects`다.
- `src/agent/services.py`의 SessionWorkflowServices를 추가했다. 기존 분류·계획·
  위험 검토 capability는 재사용하고, Executor 쓰기와 실행 완료 저장을 보완한다.
- `src/agent/effects`는 Agent DB에 첫 요청과 응답을 저장한다. 같은 입력을 재시도할
  때 expected_version·코드·파일 경로·Markdown·멱등 키를 다시 만들지 않는다.
  새 메시지 큐나 컨슈머를 추가한 것이 아니며 `src/worker`는 변경하지 않았다.
- append 키는 가변 DB 순번 대신 Task+직전 Operation으로 고정한다. 응답 후 DB
  반영이 실패하면 영수증을 재사용하고, 반영 후 실패하면 계획 revision·실행 순번·
  최종 메시지가 중복/역행하지 않게 한다. 승격 안내 이벤트도 중복 생성하지 않는다.
- 추가 셀의 실제 계획/revision을 그래프 상태에 돌려줘 후속 결과의 lineage를 유지한다.
  단독 계획의 로컬 순번은 0부터, Executor의 전체 순번은 제출 단계에서 지정한다.
  이 구분 누락으로 checkpoint 복원 시 Step이 dict로 남던 경로도 수정했다.
- 코드/리포트 입력은 내용 해시 기반 PATH이며 준비한 원문으로 파일 복구가 가능하다.
  컴파일/파일 IO는 스레드로 분리하고 HTTP/LLM 대기 중 DB 트랜잭션을 유지하지 않는다.
- 신규 Agent migration은 `0007_executor_effects`다. 기존 baseline의 동적
  create_all이 미래 테이블을 미리 만들지 않도록 별도 ORM metadata를 사용한다.
  기존 migration이나 공통 Worker의 `ew_0001`은 변경하지 않았다.
- 사용법·복구 경계·보존 주의점은 [효과 모듈 안내](../src/agent/effects/README.md)에
  기록했다. 이 기록은 자동 실행 스케줄러나 API 요청 접수 기록을 대체하지 않는다.

#### 2B 검증 기록

- uv 파일 기반 설치, 전체 Ruff lint/format, ty 통과.
- 전체 로컬: **349 passed, 108 skipped**. DB/Redis 및 opt-in 실서비스 항목 제외.
- 전체 격리 Compose: **428 passed, 29 skipped**. opt-in 실제 모델/API 항목 제외.
- 실제 PostgreSQL에서 제출/append/finalize/cancel/report 응답 유실, 요청 저장 실패,
  binding 반영 전후 장애, 동일 키 입력 변경 거부, 단조 순번/버전, 동시 중복 계획·
  최종 메시지·승격 안내 저장을 검증했다.
- 실제 PG/Redis/별도 checkpointer 연결로 MULTI append binding 반영 후 중단과
  완료 메시지 반영 후 중단을 재전달해 복구했다. HTTP 요청·리포트 생성은 중복되지 않았다.
- 빈 DB 초기화 및 직전 Agent migration에서 Task 데이터를 보존한 upgrade/repeat를
  검증했다. migration 테스트는 자신이 생성한 별도 임시 DB만 제거한다.
- 모델은 결정적 출력, Executor는 멱등 영수증과 응답 유실을 구현한 HTTP 대역이다.
  실제 Executor/Jupyter·LLM·K8s 종료/재시작 검증을 수행했다는 의미는 아니다.
- 미추적 복사본은 그대로 보존했다. Git 관리 파일과 이번 변경의 스냅샷에서 검증했으며,
  운영 DB·기존 컨테이너에는 적용하지 않았다.

### 2C — API 요청 접수와 직접 호출 복구

- 2B는 main `6aef717`로 머지·푸시했다. 후속 브랜치는
  `codex/durable-api-admission`이다.
- `src/agent/admission`에 START/RESUME/CANCEL 접수 기록, 직접 호출 도구와
  호스트 관리 복구 루프를 추가했다. 새 Redis 스트림·컨슈머는 만들지 않았다.
- START의 Task·최초 메시지·접수 이벤트·요청은 한 트랜잭션으로 저장한다.
  구형 START/RESUME WorkflowCommand는 만들지 않는다.
- 요청 입력 해시·대상 interrupt·수락 영수증으로 재개 근거를 구분한다.
  승인 후 미완료 노드만 재개하고 같은 승인으로 다음 계획을 통과시키지 않는다.
- API와 Worker의 실행 소유권을 양방향으로 확인한다. Worker DONE 기록을 놓친
  오래된 이벤트도 이후 API 승인 소유의 미완료 노드를 대신 실행하지 않는다.
- 재시도 동시성·횟수·대기 시간을 제한한다. 세션 guard 경합은 횟수를 소비하지
  않으며, 마지막 시도 후에도 완료 checkpoint가 있으면 추가 실행 없이 정리한다.
- 한도 초과/근거 불일치는 BLOCKED로 보존하고 새 요청이 덮어쓰지 못하게 한다.
  이것은 Task 실패 완료가 아니며 Executor 취소와 잠금 정리 보상은 다음 단계다.
- `0008_api_requests` 마이그레이션과 생성·수정 at/by를 추가했다.
  새 테이블은 Agent 소유이고 공통 Worker DB 스키마는 변경하지 않았다.
- Q&A 최종 메시지 DB 반영도 멱등화했다. 원래 응답을 checkpoint한 뒤 저장하므로
  최종 저장 직후 장애를 복구해도 답변 생성과 결과 메시지가 중복되지 않는다.
- [API 접수 안내](../src/agent/admission/README.md)에 연결 코드, 요청 상태,
  복구 루프 lifecycle, 동작 범위와 미완료 연결을 정리했다.

#### 2C 검증 기록

- uv `--no-editable` 파일 기반 설치. 전체 Ruff lint/format·ty 통과.
  최종 Docker 테스트 이미지 안의 Ruff lint·ty도 통과했다.
- 전체 로컬: **356 passed, 123 skipped**. DB/Redis 및 opt-in 실서비스 항목 제외.
- 전체 격리 Compose: **450 passed, 29 skipped**. opt-in 실제 모델/API 항목 제외.
- Task/요청 원자적 접수·DB 동시 접수 경합·원래 interrupt 검증·입력 checkpoint
  직후 장애·승인 수락 출력 직전 장애·승인 후 HTTP 응답 유실을 검증했다.
- 수정 요청 완료 후 API 종료와 이전 요청 재전송이 새 승인을 건드리지 않음을
  검증했다. 마지막 시도 완료 증거 확인과 잠금 경합/시도 번호 보호도 포함한다.
- 실제 PG/Redis/별도 checkpointer 연결로 API 중단 후 Worker 처리, Worker DONE
  누락 후 API 승인 처리, 양방향 재전달 소유권을 검증했다.
- 취소 요청 APPLIED와 Task CANCELLED를 구분하고, Executor 종료 이벤트 처리
  전까지 잠금을 유지하며 취소 리포트는 생성하지 않음을 검증했다.
- 0006→0007→0008 upgrade 및 반복 적용에서 기존 Task 데이터 보존을 검증했다.
- 모델·Executor HTTP는 결정적 대역이며 실제 Jupyter 실행/K8s 종료 검증은 아니다.
  운영 DB/서비스와 미추적 복사본은 건드리지 않고 Git 스냅샷에서 테스트했다.

### 다음 단계 — 실패 보상과 운영 호스트 전환

1. API BLOCKED/Worker 최종 처리 실패 시 Executor 조회·취소/종료 확인·사용자
   안내·장기 세션 잠금 정리의 업무 보상을 연결한다. 불확실한 실행을 남긴 채
   성공/실패 완료로 처리하거나 새 멱등 키로 우회하지 않는다.
2. 비최종 Task 상태·current_interrupt의 DB 반영을 새 호스트에 붙인다.
   현재 이 부분은 구 runner에 남아 있으므로 새 그래프 모듈만으로 기존 Task 조회
   API의 진행/취소/승인 대기 표시가 자동 완성되는 것은 아니다.
3. API와 Worker의 공통 factory·자원 lifecycle·RequestRecovery 실행·readiness를
   연결한다. API+Agent / Worker 두 프로세스를 유지할 수 있다.
4. FastAPI/Chat UI·Docker/Compose/K8s 진입점을 전환하고 실제 Executor·모델을
   검증한 뒤 구 Worker/임시 import를 제거한다. 활성 작업 전환 정책을 먼저 정한다.

`worker_hooks.create_graph`의 미연결 보호와 기존 운영 배포 경로는 아직 유지한다.
2C 검증 통과만으로 운영 전환하거나 `ex_agent`를 삭제하지 않는다.
