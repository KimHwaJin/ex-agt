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
```

`decision_summary`는 비공개 모델 사고과정이 아니라 감사 가능한 구조화된 분류 근거다.

## 3. Routing

```text
GENERAL_QA -------------> answer without Executor
DATA_ANALYSIS_QA -------> answer with relevant domain Skill context, no Executor
DATA_ANALYSIS_EXECUTION -> promoted Workflow retrieval/proposal
                           -> selected: fixed Workflow SINGLE
                           -> declined/no match: dynamic MULTI planning
CODE_EXECUTION ---------> collect user SINGLE/MULTI choice -> planning
ambiguous --------------> clarification interrupt
```

데이터 분석 실행의 mode는 Workflow 선택 결과로 결정된다. 벡터 검색만으로 자동 선택하지 않고
사용자 선택을 요구한다. 자유 코드 실행은 Workflow 검색을 사용하지 않고 HITL로 사용자의
SINGLE/MULTI 직접 선택을 받는다.

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
