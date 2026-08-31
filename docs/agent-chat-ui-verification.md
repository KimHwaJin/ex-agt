# Agent Chat UI 연결 검증 — 2026-08-31

## 후속 보완: 조회 승인 제거와 자동 진행 표시 — 2026-08-31

`OBSERVE_EXECUTION`/`OBSERVE_RESPONSE`를 새로 생성하지 않도록 변경했다.
LangGraph 스킬의 상태 reducer·HITL 경계 지침을 적용해 실제 사용자 결정만
`review` 노드로 보내고, 실행 관찰은 `observe`로 자동 연결했다.
기존 관찰 카드 체크포인트의 제출 규격은 호환을 위해 유지한다.

- 같은 ID의 상태 메시지 하나를 갱신하고 완료 시 실제 결과로 교체한다.
- Task 상태 변경 이벤트는 SSE로 받고 snapshot을 다시 조회한다.
  관찰 시간이 지나면 사용자 클릭 없이 자동 재확인·재연결한다.
- 최초 계획·중요 변경의 실제 승인, 버전/hash 검증은 유지한다.
- UI Stop은 관찰만 중단한다. 실제 취소는 기존 취소 API를 사용하고
  Executor 확인 후에만 취소 완료를 표시한다.
- Worker/Executor, Redis 소비, 업무 체크포인트는 변경하지 않았다.

기본 UI는 `values` 스트림을 표시하므로 별도 프런트 컴포넌트나 가짜
LLM/tool 이벤트 없이 상태 메시지를 갱신한다. 참고:
[Agent Chat UI 소스](https://github.com/langchain-ai/agent-chat-ui/blob/main/src/components/thread/index.tsx),
[LangGraph 스트리밍](https://docs.langchain.com/oss/python/langgraph/streaming).

검증:

- 파일 기반 `uv --no-editable` 설치 후 전체 로컬: 218 passed, 43 skipped.
- Compose 격리 test-postgres/test-redis 전체: 240 passed, 21 skipped.
- 실제 기존 API/Worker Q&A smoke: 3 passed. 중간 승인 없이 최종 답변까지
  자동 도달하고 최종 HumanMessage 1개 + AIMessage 1개를 확인했다.
- 관찰 100구간, 중간 중요 변경 재승인, SSE timeout 3회 자동 재연결,
  끊긴 SSE 커서 복원, UI 중단 후 같은 Task 재관찰, 취소 확인 대기를 검증했다.
- Ruff check/format(79자), ty, git diff --check 통과.

이번 UI 동작 검증은 그래프 `values` 스트림/HTTP 클라이언트/실제 Q&A
서비스 기준이다. 브라우저 버튼 렌더링, 실제 MULTI EDA 재실행 또는
수일간 장기 실행을 새로 검증한 것은 아니다. 이전 EDA 실행 검증과 구분한다.
파일 기반 설치는 갱신했고, 사용자의 dev 서버와 Worker는 종료하지 않았다.

사용법과 취소 API, 이전 조회 카드 복원 방법은
[테스트 안내](agent-chat-ui-testing.md)를 따른다.

---

## 후속 보완: 칫챗·분석 Q&A — 2026-08-31 15:24 KST

아래 초기 연결 검증 이후 사용자 인사 `ㅎㅇㅎㅇ`가 CLARIFICATION으로
오분류되어 실행 모드를 묻는 사례를 확인했다. Task
`41a25d2a-510e-5378-9ece-a42a229c499e`는 원래 상태로 보존했다.

이번 보완:

- LLM 분류 프롬프트/구조화 출력 스키마에 네 의도의 의미와 대화 사례를
  명시했다. 실행 모드 선택은 기존 전용 노드에 맡긴다. 키워드 분기 없음.
- 일반 답변 프롬프트에 자연스러운 인사, 질문 길이/언어 존중, 실행 결과와
  회사 FAQ 근거를 지어내지 않는 기준을 추가했다.
- 접수 AI 메시지와 본문의 Task ID를 제거했다. 추적 정보는 상태/API에 유지.
  Execution ID는 노트북 조회를 위해 계속 표시한다.
- 실행 전 대기에는 `OBSERVE_RESPONSE` 조회 전용 카드를 사용한다.
  LangGraph HITL 지침에 따라 조회와 승인/취소 신호를 분리 유지했다.
- 분류/답변 변경을 반영한 Worker 이미지를 빌드해 교체했다. 기존 API,
  Executor, 공유 DB/Redis는 재시작하지 않았다. 로컬 파일 기반 설치도 갱신했다.
  사용자가 실행 중인 dev 서버는 직접 종료하지 않았다.

검증 결과:

- Ruff check/format(line length 79), ty: 통과.
- 파일 기반 로컬 회귀: 183 passed. 통합/실제 서비스 검증은 별도 실행.
- Compose 전용 test-postgres/test-redis 사용 전체 테스트:
  **205 passed, 18 skipped**. skip 18개는 아래 별도 실행한 실서비스 테스트.
- 실제 `qwen38-27b-fp8` 분류 평가: **15 passed**. 인사, 일반 지식,
  분석 개념, 실행하지 않는 코드 예시, 실제 분석/코드 실행, 모호한 요청 포함.
- 수정된 API/Worker를 이용한 Chat 그래프 smoke: **3 passed**.
  아래 세 요청 모두 `SUCCEEDED`, `execution_id=null`, interrupt 없음,
  접수 메시지 없이 HumanMessage 1개와 최종 AIMessage 1개를 확인했다.

| 입력 | Task ID | 결과 |
| --- | --- | --- |
| `ㅎㅇㅎㅇ` | `621465e5-c5a2-542f-9a6c-b0cfb4f7c900` | 자연스러운 인사 |
| 평균·중앙값 차이를 두 문장으로 설명 | `9125e8d4-28a4-5d67-8e8d-255a681b6ffc` | 두 문장 설명 |
| 무슨 일을 도와줄 수 있는지 질문 | `84e03dc6-fc01-5d12-819f-9b1b73a77cec` | 기능 안내 |

실서비스 smoke는 `chat-qa-smoke-user` / `chat-qa-smoke-project`에 별도
Task를 만들었다. 기존 사용자 Task를 수정/삭제하지 않았다.
이번 검증은 연결 그래프/API와 실제 모델 기준이며, 변경 후 hosted UI를
브라우저 버튼으로 다시 검증한 것은 아니다. 아래 초기 코드 실행 smoke와
이번 승인/취소 회귀 테스트를 구분해야 한다.

검증 도중 환경 오류도 바로잡았다. 모델 IP를 URL에 직접 사용한 호출은
호스트 라우팅 때문에 404였고, Compose hosts 설정의 도메인으로 재실행했다.
또 전용 DB/Redis 없이 실행한 컨테이너 전체 테스트는 통합 22개가 접속
실패했다. 전용 테스트 서비스를 기동한 뒤 전체 205개 통과를 확인했다.
공유 Executor DB/Redis로 통합 테스트를 우회하지 않았다.

범위 제한: 문서 검색 기반 FAQ/RAG와 이전 턴을 주입하는 대화 메모리는
아직 없다. 모델 품질을 15개 사례 밖에서도 보장하는 것은 아니며, 초기
자유 코드 셀 규약 실패(아래 2번)는 이번 변경 범위 밖이다.

---

아래는 보완 전의 초기 연결 검증 기록이다.

## 변경 범위

UI 연결 그래프와 REST/SSE/HITL 어댑터만 추가했다. 기존 API/Worker 컨테이너는
재생성하거나 재시작하지 않았다. 실제 업무 그래프 실행, Redis consumer,
PostgreSQL 체크포인트와 Executor 코드는 그대로 사용했다.

스킬의 interrupt 재실행/체크포인트 경계 지침에 따라, 승인 카드 노드와 API
쓰기 노드를 분리하고 재전송에는 안정적인 idempotency key를 사용했다.

## 자동 테스트

- `ruff check .`: 통과.
- `ruff format --check .`: 통과, line length 79.
- `ty check`: 통과.
- 파일 기반 설치 후 `python -m pytest -q`: **168 passed, 22 skipped**.
- 새 연결 테스트: **33 passed**. 메시지 입력, 승인/수정/거절 변환,
  워크플로우 선택, SINGLE/MULTI 선택, 위험 확인, 취소 확인 대기,
  중복 전송 방지, SSE 커서와 timeout/cancellation, 경로 계약 검증 포함.

22개 skipped는 별도 `TEST_DATABASE_URL`/`TEST_REDIS_URL`이 필요한 기존
통합 테스트다. 이번에는 추가 컨테이너를 띄우지 않았으며 해당 테스트를
공유 Executor DB/Redis에 강제로 연결하지 않았다.

`pytest` 실행 파일을 직접 호출하면 기존 `examples` import가 실패할 수 있어
저장소 루트에서 `python -m pytest`를 사용한다. 설치된 ex-agent는
`uv sync --group chat-ui --no-editable --reinstall-package ex-agent`로 검증했다.

## 실제 서비스 smoke

local dev 서버와 기존 Compose API/Worker, 실제 vLLM/Executor를 사용했다.
브라우저에서는 hosted Agent Chat UI의 연결 화면, 질문 전송, 접수 메시지,
실행 모드 HITL 카드 렌더링을 확인했다. 전체 승인/재개 수명주기는
Chat UI와 같은 LangGraph SDK `messages`/`command.resume.decisions` 규격으로
검증했다. 브라우저 버튼만으로 전체 수명주기를 검증한 것은 아니다.

### 일반 대화 성공

- Task ID: `32fb8fcb-9d5b-55e7-ad3d-0964517b03e2`
- 인사 요청에 실제 Worker가 응답했으며 `SUCCEEDED` 종료.

### 실제 코드 실행과 리포트 성공

- UI thread: `01a05665-3446-7072-ab85-f4aa1137423b`
- Task ID: `bfabf60c-95db-5622-9734-f67306a73819`
- Execution ID: `df2d65a7-a71c-4d37-a880-df5808ddf2fe`
- Session ID: `db003a51-52b3-53ad-a5f7-17086108db2d`
- 흐름: EXECUTION_MODE → PLAN_REVIEW → OBSERVE_EXECUTION → SUCCEEDED.
- 코드 출력: `55`.
- 기존 Worker의 성공 리포트 본문과 Execution ID가 UI 메시지로 반환됨.

사용한 입력:

```text
SINGLE 모드 자유 코드 실행 요청입니다.
한 셀에 함수 정의 하나와 호출 하나를 반드시 함께 넣어주세요.
다음 코드만 실행해주세요:
def calculate_sum():
    return sum(range(1, 11))

result = calculate_sum()
print(result)
```

실제 MULTI 전체 수명주기와 수일 실행은 이번 smoke 범위에 포함하지 않았다.
MULTI 선택 신호 변환은 자동 테스트에서 검증했다.

## 구현 중 발견하여 수정한 연결 문제

초기 어댑터의 `chat-ui:<thread_id>` session ID가 Executor의 안전 경로
규격에 맞지 않아 `WorkspacePathError`가 발생했다. 최종 구현은 session을
결정론적 UUID로 만들며 user/project ID도 Executor 경로 규격에 맞게 검증한다.
수정 후 위 실제 코드 실행/리포트 성공으로 재검증했다.

실패 기록: Task `e0a62c8f-6ee2-59af-b9e7-a2dc40c7cc1e`,
Execution `0bfd978e-e42f-41f3-8dd1-e835c2de08e0`.
이 기록은 수정 전 테스트 이력으로 보존했고 재실행/삭제하지 않았다.

## 별도 후속 점검: 기존 Agent 생성 품질

아래는 UI 연결과 별개로 기존 Worker에서 실제로 관찰된 오류이며,
이번 변경에서는 업무 프롬프트/컴파일러/재시도 정책을 수정하지 않았다.

1. 분석 개념 Q&A가 계획 생성 경로로 진입하고 미등록 함수를 선택함.
   - 입력: `평균과 중앙값의 차이를 두 문장으로 설명해줘.`
   - Task: `1372f298-4d95-524a-a82e-789f348cfce2`.
   - 로그: `Unknown Tool: text_answer`, 최종 `Unknown Tool: llm_response`.
   - 후속: intent 분류/라우팅, 실제 registry 제한 전달, 잘못된 함수 선택의
     검증 피드백을 점검할 것. 단일 재현 사례이므로 일반적인 실패율은 미측정.

2. 짧은 자유 코드 요청에서 셀 규약을 만족하지 않는 코드를 생성함.
   - 입력: `Python으로 1부터 10까지 합을 계산하고 출력해줘.` 형태.
   - Task: `f127e17f-e20d-587f-a970-a5ed6ee6e845`,
     `e069e2b8-0228-5892-aa6e-05004360d78e`.
   - 오류: `Custom cell must define exactly one function`.
   - 후속: 함수 정의 1개+호출 1개 규약의 프롬프트와 컴파일 검증 피드백을
     점검할 것. 함수 형태를 명시한 입력은 위 smoke에서 성공했다.

실패는 UI에 terminal message로 표시됨을 확인했다. 실패 리포트는 만들지
않았으며, 해당 실패 테스트들은 Executor 실행 전에 종료됐다.

테스트 이력은 `chat-ui-test-user` / `chat-ui-test-project`로 식별할 수 있다.
