# Intent Routing Contract

상태: `ACCEPTED`

## 1. 원칙

사용자는 자연어 query만 전달한다. BFF request에 `CHAT`/`EXECUTION` 같은 명시적 request type을
요구하지 않는다. Keyword, 정규식 또는 고정 문구 기반 rule routing을 사용하지 않는다.

LangGraph의 첫 판단 node가 LLM structured output으로 의도를 분류한다.

## 2. Intent schema

```text
intent:
  GENERAL_QA
  DATA_ANALYSIS_QA
  DATA_ANALYSIS_EXECUTION
  CODE_EXECUTION
confidence: 0.0 .. 1.0
decision_summary: 사용자에게 공개 가능한 짧은 판단 이유
requires_clarification: bool
clarification_question: string | null
requires_execution_mode: bool
requested_execution_mode: SINGLE | MULTI | null
```

`decision_summary`는 비공개 모델 사고과정이 아니라 감사 가능한 구조화된 분류 근거다.

## 3. Routing

```text
GENERAL_QA -------------> answer without Executor
DATA_ANALYSIS_QA -------> answer with relevant domain Skill context, no Executor
DATA_ANALYSIS_EXECUTION -> promoted Workflow retrieval/proposal
                           -> selected: fixed Workflow SINGLE
                           -> declined/no match: requested mode, else MULTI
CODE_EXECUTION ---------> explicit mode or collect missing choice -> planning
ambiguous --------------> clarification interrupt
```

명시적인 실행 모드는 의도와 함께 LLM이 의미 기반으로 추출한다. 키워드 룰이나
추가 모델 호출을 넣지 않는다. 분석 요청도 "싱글로 실행"이라고 지정하면
워크플로우 후보가 없거나 선택하지 않아도 SINGLE을 유지한다.
모드를 지정하지 않은 분석 요청의 동적 계획은 기존처럼 MULTI다.

고정 워크플로우를 사용자가 선택하면 화면에 안내된 SINGLE 실행을 승인한다.
벡터 검색만으로 자동 선택하지 않는다. 자유 코드 실행은 워크플로우 검색을 하지
않고, 명시 모드가 없을 때만 HITL 모드 선택을 받는다.
모드 지정은 계획 실행 승인이 아니므로 계획 검토/승인은 계속 필요하다.

`requested_execution_mode`는 단순 설명/QA에는 null이다. 셀 개수나 작업의
복잡도만으로 추론하지 않는다. `requires_execution_mode`는 자유 코드 실행에
명시 모드가 없을 때만 true다. 기존 저장된 분류에는 새 필드가 없어도 null로 읽는다.

계획의 `execution_mode`는 그래프 상태의 확정 모드와 같아야 한다.
출력 미들웨어에서 불일치를 거부하고 원래 시간 예산 안에서 한 번만 전체 계획을
재생성한다. MULTI의 일부 단계에 SINGLE 이름만 붙이는 보정은 하지 않는다.
컴파일·승인·Executor 제출 전에도 일치 여부를 검사한다.
SINGLE은 분석에 필요한 전체 셀을 제출하고 MULTI는 다음 셀만 제출한다.

분류 결과는 model/provider, prompt template version, input message ID, structured output, trace ID,
token usage와 함께 저장한다.

## 4. Rule이 허용되는 경계

Intent 의미를 판단하는 rule은 사용하지 않는다. 다음은 의미 분류가 아니라 schema/security
validation이므로 결정론적으로 처리한다.

- 필수 ID와 문자열 크기 검증
- enum/schema 검증
- Session execution lock 확인
- 권한 확인
- request idempotency 확인
- 지원하지 않는 payload 형식 거부

## 5. 모드 보존 검증 (2026-08-31)

- Ruff lint/format, ty 통과. Python 줄 길이 79자 유지.
- 로컬 전체: 261개 통과, 외부 연동 조건 없는 52개 제외.
- Compose 전체: 284개 통과, opt-in live 테스트 29개 제외.
- 실모델 별도 검사: 의도/명시 모드 추출 21개, 계획 생성 2개 통과.
- SINGLE: `fetch_dataset → inspect_dataset → plot_distribution` 전체 계획.
- MULTI: `fetch_dataset` 다음 한 셀만 생성.
- 실모델 계획은 등록 함수 계약에 맞는 코드로 컴파일했지만 실행하지 않았다.
  Executor 제출 모드와 전체 셀 전달은 HTTP 대역 테스트로 확인했다.

수정은 새 Task에 적용한다. 이미 생성된 checkpoint/승인된 계획/Executor 실행을
소급 변경하지 않는다. 실행 중인 API/Worker 컨테이너는 별도 이미지 갱신이 필요하다.
Chat UI의 로컬 `langgraph dev`도 재시작하면 갱신된 카드 설명을 사용한다.

```bash
docker compose up -d --build --no-deps api worker
```

현재 진행 중인 작업을 확인하고 안전한 시점에 위 명령으로 교체한다.
DB migration이나 기존 데이터 초기화는 필요하지 않다.
