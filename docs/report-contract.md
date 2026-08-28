# Successful Execution Report Contract

상태: `IMPLEMENTED_V1_BASELINE`

## 1. 생성 조건

최종 리포트는 Executor Execution이 `SUCCEEDED`이고 결과/manifest 정합성 검증이 끝난 Task에만
생성한다. 실패 또는 취소 Task는 리포트를 만들지 않고 사용자 메시지만 저장한다.

## 2. Prompt, schema, renderer의 역할

리포트 구조를 prompt에만 맡기지 않는다.

- Prompt: 대상 사용자에 맞는 설명 수준, 분석 해석 원칙, 근거 없는 주장 금지, 문체
- Pydantic structured output: LLM이 작성할 필수 narrative와 evidence reference 강제
- Application assembler: BFF Plan/Revision/Step과 Executor mapping/result를 결합
- Markdown renderer: 고정된 섹션 순서로 최종 Markdown 생성

LLM에게 Plan/Executor 감사 이력을 기억해 재서술하게 하지 않는다. 계획 revision, Skill/Tool,
실행 ID, Artifact 목록은 authoritative DB/API 결과에서 renderer가 삽입한다.

## 3. Structured output 초안

LLM이 생성할 필드:

```text
ReportNarrative
  title
  executive_summary
  objective_and_scope
  findings[]
    title
    summary
    evidence_refs[]
    caveats[]
  limitations[]
  next_recommendations[]
```

`evidence_refs`는 허용된 PlanStep/result/artifact reference만 사용한다. 존재하지 않는 reference는
schema 이후 application validation에서 거부한다.

Application이 삽입할 필드:

```text
task/session/execution identity
approved initial plan
plan revision history and change reasons
executed Step order
Skill/Tool/parameter and selection rationale
Custom Code generation rationale
Executor operation/step/result/artifact mapping
notebook and Artifact links
```

## 4. Markdown 섹션

최종 순서:

1. 요청과 분석 목적
2. 요약
3. 승인된 최초 계획
4. 계획 변경 이력
5. 실제 수행 과정
6. Skill/Tool 및 함수 선택·생성 이유
7. 핵심 분석 결과와 근거
8. 생성된 Artifact와 Notebook
9. 한계
10. 다음 추천 작업

## 5. Executor materialization

리포트 생성 흐름:

```text
Executor SUCCEEDED
  -> result/manifest/notebook reconciliation
  -> ReportNarrative structured generation
  -> reference validation
  -> deterministic Markdown rendering
  -> POST /api/v1/executions/{execution_id}/artifacts
       type=REPORT
       source=PATH
       media_type=text/markdown
       append_to_notebook=true
  -> artifact_id/checksum 저장
  -> GET /api/v1/artifacts/{artifact_id}/content 또는 생성 원문 검증
  -> BFF user-visible message/event 저장
  -> Session lock 해제
```

Executor 계약상 REPORT 기본 경로는 `reports/final-report.md`이고 `append_to_notebook=true`이면
동일 Markdown을 notebook Markdown cell로 추가한다. Materialization은 성공한 Execution에서만
허용된다.

호출은 Task/report version에서 파생한 안정적인 idempotency key를 사용한다.

## 6. Frontend 표시

BFF는 생성된 Markdown과 Executor가 반환한 `artifact_id`, checksum, `execution_id`를 연결해
저장한다. 화면에는 Markdown 내용을 렌더링하고 다음 링크를 제공할 수 있다.

- Jupyter notebook 조회/다운로드
- Report Artifact 다운로드
- Plan revision/diff
- Step/Artifact 상세

## 7. 실패 처리

리포트 생성 또는 Executor materialization이 일시적으로 실패하면 idempotent system retry를
적용한다. Execution 성공만으로 Task 성공을 사용자에게 확정하지 않는다. 리포트 Artifact와
사용자-facing message가 저장된 뒤 Task를 `SUCCEEDED`로 전환하고 Session lock을 해제한다.
