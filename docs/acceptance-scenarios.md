# Acceptance Scenarios

상태: `IMPLEMENTED_V1_BASELINE`

1차 구현이 완료됐다고 판단하기 위한 end-to-end 기준이다. 외부 데이터 레이크는 결정론적 Fake
Tool로 대체하지만 PostgreSQL/pgvector, Redis, Executor/Jupyter는 실제 component를 사용한다.

## 1. 질의응답

### A1. 일반 질의응답

- 일반 질문이 `GENERAL_QA`로 분류되고 Executor Execution을 만들지 않는다.
- 사용자/Assistant message가 저장되고 session snapshot과 SSE replay에서 동일하게 복원된다.

### A2. 데이터 분석 질의응답

- 실행이 필요 없는 분석 개념 질문이 `DATA_ANALYSIS_QA`로 분류된다.
- 분석 Skill을 참고할 수 있지만 Plan/Executor Execution은 생성하지 않는다.

## 2. 데이터 분석 실행

### A3. 승격 Workflow 선택

- query embedding으로 service-visible, compatible Workflow 상위 3개가 제안된다.
- 사용자가 Workflow와 binding parameter를 선택하면 그 행위가 승인으로 기록된다.
- 승인 transaction에서 session lock과 outbox command가 함께 저장되고 SINGLE로 실행된다.

### A4. 동적 분석 계획

- Workflow를 선택하지 않으면 관련 Skill/Tool을 이용한 MULTI Plan이 생성된다.
- 화면/API에는 코드 대신 Step 설명, Skill/Tool, parameter와 선택 이유가 표시된다.
- 승인된 Tool source version/hash와 실제 Jupyter cell source hash가 추적된다.

### A5. Fake 데이터 수집과 분석

- `fetch_dataset`이 query checksum과 고정 seed를 기반으로 재현 가능한 sample dataset을 shared
  workspace에 생성한다.
- 후속 inspect/profile/aggregate/plot Step이 이전 Step의 검증된 result를 이용한다.
- 하나의 Executor Step/Jupyter cell에는 하나의 함수 정의와 하나의 호출이 들어간다.

## 3. 자유 코드 실행

### A6. Mode 선택과 승인

- 자유 코드 실행 intent는 Workflow 검색과 registered analysis Tool을 사용하지 않는다.
- 사용자가 SINGLE/MULTI를 직접 선택하고 생성 코드의 목적·단계·parameter를 승인한다.
- LLM이 함수 정의와 호출을 생성하되 코드 생성 전/실행 전 risk review를 통과해야 한다.

## 4. HITL과 추적성

### A7. 수정/거절

- REVISE는 새 PlanRevision과 structured diff/변경 이유를 만들고 다시 승인을 요청한다.
- 사용자가 Tool을 쓰지 말고 자유 코드를 만들라고 수정하면 새 revision이 CUSTOM_CODE lineage를
  가진다.
- REJECT는 Executor를 호출하지 않고 사용자 메시지를 저장한 뒤 종료한다.

### A8. 계획 감사

- 최초/사용자 수정/Agent 수정/MULTI 적응 revision을 모두 조회할 수 있다.
- 각 Step에서 선택/생성 이유, Skill/Tool version/hash, model/template version, Executor ID
  mapping과 결과 artifact를 따라갈 수 있다.
- raw chain-of-thought와 원본 model prompt/response는 저장하지 않는다.

## 5. 장기 실행, 실패와 취소

### A9. 재시작 복구

- Executor 대기 중 API/worker Pod를 재시작해도 열린 coroutine 없이 checkpoint와 DB state로
  복구된다.
- Redis event 중복/누락/역순을 처리해 graph resume과 사용자 event가 한 번만 적용된다.
- 처리 중 worker가 종료되면 원본 message를 ACK하지 않고 lock을 해제한다. claim idle 이후
  다른 worker가 PEL message를 reclaim하여 완료하고 최종 pending은 0건이 된다.

### A10. Session 잠금

- 승인 commit부터 terminal 사용자 메시지와 상태 저장까지 새 chat/task가 거절된다.
- 상태 조회, SSE reconnect, artifact/notebook 조회와 cancel은 계속 허용된다.

### A11. 실패/취소

- SINGLE 실패는 자동 코드 수정 없이 실패 내용과 원인을 알리고 report를 만들지 않는다.
- MULTI는 승인 전략 범위 안에서 최대 3회 보정한 뒤 최종 실패하면 report 없이 실패 메시지를
  저장한다.
- cancel은 Executor 취소 완료를 확인한 뒤 취소 메시지와 terminal 상태를 저장하고 잠금을 푼다.

## 6. 성공 리포트와 Workflow 승격

### A12. 성공 리포트

- 성공 Execution에 대해서만 Pydantic output, authoritative assembler, deterministic renderer로
  Markdown report를 생성한다.
- Executor REPORT Artifact로 저장하고 notebook에 Markdown cell을 append한다.
- artifact/execution ID와 같은 Markdown을 BFF에 저장한 뒤 Task를 SUCCEEDED로 만들고 잠금을
  해제한다.

### A13. Workflow 승격

- 성공한 Tool-only Plan에 대해 Agent가 공개용 이름/설명/요청 예시/태그를 제안한다.
- 사용자가 확인하면 immutable Workflow version과 embedding이 저장된다.
- 공개 version에는 원본 query, 실제 data value와 user/project/session/task 식별자가 남지 않는다.
- CUSTOM_CODE Plan은 초기 버전에서 승격할 수 없다.
- 동일 idempotency key 재요청은 중복 Workflow를 만들지 않는다.
- 원본 parameter는 Workflow 입력 placeholder가 되고 사용자가 명시한 공개 기본값만
  version에 남는다.

### A14. Workflow version 운영

- 소유자는 다른 성공 Tool-only Task에서 기존 Workflow의 새 immutable version을
  만들 수 있으며, 새 version은 `PENDING_REVIEW`와 비활성 상태로 시작한다.
- 승인 transaction은 기존 활성 version을 내리고 승인 version 하나만 활성화한다.
- 거절된 version은 활성화할 수 없고, 승인된 과거 version은 운영 롤백 대상으로
  다시 활성화할 수 있다.
- Workflow 전체를 비활성화하면 공개 검색에서 빠지고, 재활성화하면 현재 활성
  version이 다시 검색된다.
- 생성·승인·거절·version 전환·Workflow 상태 전환은 요청자, 사유, 정책 version,
  요청 hash와 결과를 감사하며 동일 idempotency key 재요청은 상태를 중복 변경하지
  않는다.
- 소유자는 Workflow 요약, 모든 version의 상태와 원본 Task/Plan/Execution lineage,
  공개 Plan 및 Skill/Tool 선택 이유, lifecycle 감사 이력을 cursor pagination으로
  조회할 수 있다.

## 7. 완료 품질 게이트

- unit, PostgreSQL, Redis, Executor contract/E2E test 통과
- lint와 type check 통과
- migration을 빈 DB와 기존 DB 양쪽에 적용 가능
- API/worker 재시작 및 중복 command/event 주입 테스트 통과
- 최소 OpenAPI 예제와 로컬 실행 문서 제공
