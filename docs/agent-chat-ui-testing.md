# Agent Chat UI로 기존 Worker 테스트

## 실행 구조와 범위

Agent Chat UI → 로컬 `langgraph dev`의 `agent` 연결 그래프 → 기존 Agent API
→ PostgreSQL outbox → Redis → 기존 Worker → Executor 순서로 실행한다.
Executor 이벤트 수신과 업무 그래프 resume 역시 기존 Worker가 담당한다.

이 모드는 **Chat UI를 테스트 프런트로 붙이는 어댑터**다. 업무 그래프를
dev 서버로 옮기거나 Worker의 체크포인터를 공유하지 않는다. Redis 소비 그룹,
inbox/outbox, 세션 잠금, 실패 보상과 Executor 실행 경로는 변경하지 않는다.
추가 큐, 추가 Worker, 추가 DB도 필요 없다.

- `langgraph dev` 상태: UI messages, 현재 task ID, 조회 커서, 승인 카드.
- 기존 Agent DB: 업무 Task, 계획, 승인, execution ID, 결과와 이벤트.
- 기존 Worker PostgreSQL 체크포인트: 실제 분석 그래프의 상태와 재개 지점.

Studio에는 UI 연결 그래프의 노드가 보인다. 내부 분석 그래프 노드에 대한
브레이크포인트/핫리로드는 제공하지 않는다. 업무 코드 변경을 반영하려면
기존 Worker 이미지를 갱신해야 한다. **개발 서버만 재시작해서 Worker 코드가
바뀌는 것은 아니다.**

## 1. 기존 컨테이너 확인

기존 `api`, `worker`와 Executor 서비스를 실행해둔다. 이 문서의 명령은
새 컨테이너를 실행하지 않는다. 현재 설정에서 API는 호스트 `8010` 포트다.

```bash
curl --fail http://127.0.0.1:8010/readyz
curl --fail http://127.0.0.1:8011/readyz
```

API/Worker의 PostgreSQL/Redis 접속은 기존 `.env`와 Compose 설정 그대로다.
Executor 측 PostgreSQL에 분리된 Agent DB를 사용하는 구성은
[공유 인프라 문서](shared-executor-infrastructure.md)를 참고한다.

## 2. dev 서버 준비와 실행

저장소 루트에서 실행한다.

```bash
uv sync --group chat-ui --no-editable
cp .env.chat-ui.example .env.chat-ui
uv run --no-sync langgraph dev \
  --host 127.0.0.1 --port 2024 --no-browser
```

이미 `.env.chat-ui`를 조정했다면 복사 명령은 생략한다. 이 파일과
`.langgraph_api/`는 Git에서 제외한다. `chat-ui` 의존성 그룹은 선택 사항이며
기존 운영/테스트 Docker 이미지에는 개발 서버 의존성을 추가하지 않는다.

설정 기본값:

- `CHAT_UI_API_URL=http://127.0.0.1:8010`: 기존 API 서버의 origin.
  `/api/v1`은 어댑터가 붙이므로 입력하지 않는다.
- `CHAT_UI_USER_ID=chat-ui-test-user`: 테스트용 `X-User-ID`.
- `CHAT_UI_PROJECT_ID=chat-ui-test-project`: 테스트 작업을 구분하는 프로젝트.
- `CHAT_UI_WATCH_SECONDS=30`: SSE 상태 재확인·재연결 간격.
  시간이 지나도 승인 카드 없이 자동으로 다음 관찰 구간으로 이어진다.
- `CHAT_UI_REQUEST_TIMEOUT_SECONDS=10`: 개별 REST 요청 제한 시간.
- `LANGSMITH_TRACING=false`: 외부 추적 전송 기본 비활성화.

user/project ID는 Executor 경로 계약에 맞게 영문·숫자로 시작하는
영문·숫자·마침표·밑줄·하이픈 조합만 허용한다. session ID는 UI thread에서
결정론적 UUID로 생성한다. 콜론이 들어간 임의 접두사를 붙이지 않는다.

이 dev 서버는 모델, DB, Redis, 공유 파일에 직접 접근하지 않는다. 따라서
호스트에 `model.frodo.com` hosts 등록이나 Jupyter 경로 설정을 추가할 필요가
없다. 모델/파일 설정은 기존 Worker 컨테이너에서만 유효하면 된다.

로컬 테스트용이며 인증 서버가 아니다. 신뢰된 테스트 user ID를 대신 넣으므로
외부 공개, `--tunnel`, 공용 네트워크 바인딩을 사용하지 않는다.

## 3. Agent Chat UI 연결

[Agent Chat UI](https://agentchat.vercel.app/)의 연결 화면에 입력한다.

- Deployment URL: `http://127.0.0.1:2024`
- Assistant/Graph ID: `agent`
- LangSmith API Key: 로컬 dev 연결에서는 비워둔다.
- Built with Agent Builder: 끈다.

브라우저가 로컬 네트워크 접근 권한을 요청하면 로컬 테스트 연결에 한해
허용한다. 브라우저 정책으로 hosted UI에서 localhost 접근이 막히면
공식 UI를 로컬로 실행한다. 이 경우에도 API URL은 `2024`이며, `8010`은
LangGraph API가 아니므로 Chat UI에 직접 연결하면 안 된다.

별도 Agent Chat UI 저장소에서 사용할 환경변수:

```dotenv
NEXT_PUBLIC_API_URL=http://127.0.0.1:2024
NEXT_PUBLIC_ASSISTANT_ID=agent
NEXT_PUBLIC_AUTH_SCHEME=
```

UI 설치/실행은 [공식 README](https://github.com/langchain-ai/agent-chat-ui)를
참고한다. 현재 `action_requests`/`review_configs`와
`command.resume.decisions`를 사용하는 버전이 필요하다. 오래된
`action_request`/`HumanResponse[]` 규격은 지원하지 않는다.

호환 규격 확인 기준 커밋: `2a76b8e0da2be9115348eaba0a08dd2020967fe8`.

## 4. 질문, 선택, 승인

일반 질문은 채팅 입력창으로 보낸다. UI thread를 테스트 session으로 사용하고,
각 질문은 별도 Task로 접수한다. message ID와 내용으로 결정론적 작업 ID를
생성해 동일 입력 재전송 시 API idempotency를 활용한다.

인사·칫챗·일반 지식 질문과 분석 개념 질문은 Worker에서 분류 후 답변으로
종료한다. 의도 판단은 LLM이 수행하며, 인사 키워드에 의한 우회 분기는 없다.
처리 중에는 상태 메시지 하나를 갱신하고 완료 시 실제 답변으로 교체한다.
내부 Task ID나 Worker 접수 안내는 답변에 붙이지 않는다.
Task ID는 UI 그래프의 `task_id`/`snapshot`과 기존 API에 보존한다.
회사 FAQ/문서를 검색하는 RAG는 아직 없으며, 일반 Q&A와 구분해야 한다.

작업이 입력을 기다리면 Chat UI의 HITL 카드가 나타난다. 계획에는 목적,
선택한 스킬·함수, 선택/생성 이유, 파라미터와 예상 결과를 표시한다.
실행 소스 코드는 UI 체크포인트에 복사하지 않는다.

### 실행 모드

`EXECUTION_MODE` 카드에서 Approve는 SINGLE이다. MULTI를 선택하려면
Edit에서 `mode`를 `MULTI`로 바꾸고 제출한다.

입력에 "싱글모드로 샘플 데이터 생성, EDA, 플롯까지 실행해줘"처럼 모드를
명시해도 된다. LLM 분류가 모드를 추출하고, 별도의 모드 카드 없이 계획 승인으로
진행한다. 분석 요청도 해당 모드를 보존한다. 셀 개수만으로 모드를 정하지 않는다.

### 워크플로우

`WORKFLOW_SELECTION` 카드의 설명에 후보의 ID, 스킬·함수, 입력 계약이 나온다.
Edit에서 `workflow_version_id`와 `input_values`를 입력하면 해당 워크플로우를
SINGLE로 실행하도록 승인한다. 후보를 선택하지 않고 Approve하면 입력에서
명시한 모드의 동적 계획으로 넘어가며, 모드를 지정하지 않았으면 MULTI다.
카드 설명에 실제 동적 모드를 표시한다.
후보 ID와 payload hash는 서버 제안을 기준으로 검증한다.

기존에 생성된 Task의 실행 모드를 소급 변경하지 않는다. 모드 보존 수정 후에는
API/Worker 이미지를 갱신하고 새 Task로 검증한다. 이미 제출된 execution의
모드를 바꾸거나 실행을 재제출하지 않는다.

### 실행계획

- Approve: 현재 revision 승인.
- Reject: 현재 계획 거절.
- Edit에서 `feedback` 입력: 수정 요청 전달 → 재계획 → 다시 승인.
- HIGH 위험 승인: Edit에서 `risk_acknowledged`를 `true`로 설정.
  수정 요청 없이 위험만 승인하려면 `feedback`은 비워둔다.

계획 revision/hash는 수정할 수 없다. `feedback`에 자유 코드 생성 요청을
입력하면 기존 Agent의 계획 수정 로직으로 전달된다.

### 추가 정보와 요청 위험

`CLARIFICATION`은 Edit에서 `answer`를 입력한다.
`REQUEST_RISK_CONFIRMATION`은 Approve로 동의하거나 Reject로 중단한다.

승인 대기 중 입력창에 `승인`이라고 쓰는 방식은 지원하지 않는다. 카드로
명시적으로 결정해야 하며, 추가 채팅을 보내도 새 작업이나 자동 승인이
발생하지 않고 기존 작업 상태를 다시 보여준다.

## 5. 장시간 실행, 복원, 취소

실행 중에는 상태 메시지 하나가 `코드 실행 중 → 결과 정리 → 리포트 작성`
등 현재 Task 단계로 갱신된다. 진행률을 추정하거나 가짜 퍼센트를 표시하지
않는다. 완료 시 임시 상태 메시지를 제거하고 기존 Worker의 결과를 표시한다.
Q&A가 오래 걸려도 사용자 클릭 없이 답변까지 기다린다.

`OBSERVE_EXECUTION`/`OBSERVE_RESPONSE` 조회용 승인 카드는 새 실행에서
생성하지 않는다. 30초 관찰 구간 종료는 UI의 HITL 중단이 아니라 자동
상태 재확인과 SSE 재연결 시점이다. 최초 계획, 중요한 계획 변경, 추가 정보,
위험 확인 등 실제 사용자 결정이 필요한 경우에만 기존 HITL 카드를 유지한다.

- MULTI의 승인 범위 내 다음 셀 실행에는 사용자 클릭이 필요 없다.
- **입력창 옆 Cancel/Stop, Mark as Resolved, 탭 닫기와 dev 서버 종료는
  Executor 취소가 아니다.**
- 화면 관찰 중에는 Chat UI run이 실행 중이며 일반 입력은 UI에서 차단된다.
  우회 입력이 들어와도 기존 활성 Task에 새 작업이나 자동 승인을 만들지 않는다.
- UI 재연결은 Agent Chat UI의 resumable stream을 사용한다. Stop이나 dev
  서버 재시작으로 관찰 run이 종료된 경우 기존 thread에서 다시 실행하면
  같은 Task를 조회한다. 그래프 호출 기준 `ainvoke(None, config)`로도
  관찰 체크포인트에서 재개할 수 있다.
- 이전 버전에서 이미 중단된 조회 카드는 새로고침만으로 삭제되지 않는다.
  기존 카드를 한 번 제출하면 이후부터 자동 관찰로 전환된다. 새 채팅에는
  처음부터 조회 카드가 없다. 이전 기록은 삭제하지 않는다.

### 실제 작업 취소

조회 카드를 제거했으므로 신규 실행에는 카드의 Edit 취소 입력도 없다.
실제 취소는 기존 API를 사용한다. `task_id`는 Studio의 그래프 상태 또는
기존 API에서 확인한다. UI에 표시한 `execution_id`와 혼동하지 않는다.

```bash
curl --fail-with-body -X POST \
  "http://127.0.0.1:8010/api/v1/tasks/<task_id>/cancel" \
  -H "X-User-ID: chat-ui-test-user" \
  -H "Content-Type: application/json" \
  -d '{"idempotency_key":"cancel-<task_id>","reason":"테스트 중단"}'
```

API 접수만으로 취소 완료를 표시하지 않는다. 자동 관찰은 `CANCEL_REQUESTED`
상태를 보여주며 Executor 확인 후 `CANCELLED` 결과를 표시한다.

조회에는 기존 Task SSE와 `Last-Event-ID`를 사용한다. 재연결 시 저장된 이벤트
커서를 전달하며, 진행률 이벤트마다 DB 상태를 polling하지 않는다.
Task 상태 변경 이벤트는 즉시 snapshot을 조회해 화면에 반영하며, 이벤트가
없는 구간에는 설정된 간격으로 상태를 재확인한다. Redis를 추가로 소비하거나
Executor 결과 조회/업무 resume를 중복 수행하지 않는다.
UI 연결 장애는 Agent 업무 실패나 자동 Executor 취소로 처리하지 않는다.

관찰은 비동기 I/O이며 매 구간의 Task snapshot과 커서를 UI 체크포인트에
저장한다. 관찰 반복은 LLM/업무 계획 반복이 아니므로 UI 그래프에만 별도
`recursion_limit=1_000_000`을 둔다. 기본 30초 기준 5일의 유휴 관찰은
14,400구간이다. 호출자나 Agent Server가 실행 config를 별도로 주면 그 제한이
우선하므로 장기 테스트에서는 실제 run 설정도 확인해야 한다.
Worker의 실행·재계획 예산은 변경하지 않는다.
이 방식은 로컬 테스트용 활성 관찰 run이다. 장기간 서버 run 제한, 탭 복원과
네트워크 단절을 모두 보장하는 운영용 BFF 프런트를 구현한 것은 아니다.

성공 시 기존 Worker가 생성한 결과 리포트를 표시한다. 실패/거절/취소는 기존
terminal message만 표시하고 별도 성공 리포트를 생성하지 않는다.
Execution ID가 있는 경우 결과 메시지에 표시하므로 기존 notebook 조회/
다운로드 API에서 사용할 수 있다. 내부 Task ID는 상태에서 조회한다.

## 테스트 범위와 제한

```bash
uv run --no-sync python -m pytest tests/test_dev_chat_client.py \
  tests/test_dev_chat_decisions.py tests/test_dev_chat_graph.py
```

- 입력은 텍스트만 지원한다. 파일 첨부/이미지는 거절한다.
- Worker 내부 모델 토큰은 이 API에 노출되지 않아 토큰별 실시간 출력이 아닌
  진행 상태·승인·결과 메시지 단위로 표시한다.
- 각 질문을 별도 Task로 전달하며 이전 턴 대화 전체를 QA 모델에 주입하는
  대화 메모리는 아직 없다. 앞선 답변을 지칭하는 후속 질문에는 필요한
  내용을 함께 넣어야 한다.
- UI time travel/메시지 편집은 외부 Executor 실행을 되돌리지 않는다.
  실행 중인 thread에서 분기/재생하지 말고 별도 새 채팅으로 테스트한다.
- UI thread를 삭제해도 업무 Task/Executor 작업을 삭제하거나 취소하지 않는다.
- 이 어댑터는 기존 API가 제공하는 후보를 표시한다. 추가 워크플로우 검색,
  승격 관리 화면이나 완전한 BFF 프런트를 구현하는 범위는 아니다.
- 이 테스트 경로를 이용해도 5일 실행과 장애 복구의 운영 검증을 대체하지 않는다.

실제 smoke 테스트 질문 예시:

1. `평균과 중앙값의 차이를 짧게 설명해줘.`
2. `Python으로 1부터 10까지 합을 계산해서 출력해줘.`
3. `샘플 매출 데이터를 받아 결측치를 점검하고 요약 통계를 내줘.`

실제 성공한 함수 코드 입력, 검증 결과와 기존 Agent 생성 품질의 후속
점검 사항은 [검증 기록](agent-chat-ui-verification.md)을 참고한다.

## 코드 변경 후 다시 테스트

업무 분류/답변 변경은 Worker 이미지에 반영해야 한다. UI 어댑터 변경은
파일 기반 패키지를 재설치한 뒤 실행 중인 dev 서버를 재시작한다.
관찰 UI 변경만 반영할 때는 Worker 재빌드 명령을 생략해도 된다.

```bash
docker compose up -d --build --no-deps worker
uv sync --group chat-ui --no-editable --reinstall-package ex-agent
uv run --no-sync langgraph dev \
  --host 127.0.0.1 --port 2024 --no-browser
```

이전 오분류로 중단된 Task는 새 프롬프트가 자동 재분류하지 않는다.
Chat UI에서 **새 채팅**을 만들어 확인한다. 기존 기록은 삭제하지 않는다.

실제 모델 분류 평가(업무 Task/Executor 생성 없음):

```bash
docker compose --profile test build test
docker compose --profile test run --rm --no-deps \
  -e EX_AGENT_TEST_LIVE_MODEL_URL=http://model.frodo.com/v1 \
  test python -m pytest tests/test_conversation_live.py -q --tb=short
```

기존 API/Worker를 통한 Q&A smoke(별도 테스트 사용자/프로젝트에 Task 생성):

```bash
EX_AGENT_TEST_LIVE_API_URL=http://127.0.0.1:8010 \
  uv run --no-sync python -m pytest tests/test_dev_chat_live.py -q -s
```
