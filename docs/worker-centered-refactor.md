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

## 저장과 실행 계약 — 이후 단계에서 구현할 내용

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

Worker만 검증하는 격리 Compose 명령은 [워커 안내](worker/README.md)에 있다.
실제 Executor·LLM·K8s 배포 검증은 아직 수행하지 않았다.

### 다음 단계

2단계에서 기존 Agent 업무 capability를 새 그래프 구성에 연결하고,
session State 초기화/보존, 실행 binding 등록, 수락·receipt 노드를 구현한다.
단순 인사 → 계획 승인 → 실행 대기 → 이벤트 재개를 최소 통합 시나리오로 먼저
검증한 뒤 MULTI·취소·리포트를 확장한다. 이후 API 호출·접수 복구를 전환한다.
