# Plan and Execution Audit Contract

상태: `IMPLEMENTED_V1_BASELINE`

## 1. 원칙

실행계획은 LangGraph checkpoint의 임시 필드가 아니라 BFF PostgreSQL이 소유하는 독립적인
도메인 객체다. 계획 생성, 사용자 수정, 승인, MULTI 후속 계획, Executor 제출, 실제 결과와
최종 리포트까지 append-only lineage로 연결한다.

다음을 구분한다.

- 내부 모델 사고과정 원문: 저장하거나 사용자에게 노출하지 않는다.
- 감사 가능한 판단 근거: 사용자에게 공개 가능한 짧고 구조화된 설명으로 생성하고 저장한다.

판단 근거에는 선택 목적, 근거가 된 사용자 요구/데이터 정보, 적용한 제약, 고려한 대안과
선택하지 않은 이유, 예상 결과와 검증 기준을 포함할 수 있다.

## 2. 핵심 객체

```text
Session
  └─ Task
      └─ Plan
          ├─ PlanRevision 1 (draft/rejected/superseded)
          ├─ PlanRevision 2 (approved)
          │   └─ PlanStep[]
          │       └─ CompiledStep
          └─ AdaptivePlanRevision[] (MULTI)
                  └─ PlanStep[]

PlanStep
  └─ ExecutorBinding
      ├─ execution_id
      ├─ operation_id
      ├─ executor_step_id
      └─ attempt/result/artifact references
```

Executor는 Agent의 Plan 테이블을 공유하거나 Plan ID를 소유하지 않는다. Executor가 반환한
`execution_id`, `operation_id`, `step_id`를 BFF의 `ExecutorBinding`에 저장한다.

## 3. Plan

Plan의 최소 필드:

```text
plan_id
task_id
session_id
execution_mode: SINGLE | MULTI
objective
strategy_summary
assumptions[]
constraints[]
success_criteria[]
created_at
```

Plan 자체는 논리적 계획의 identity이고 실제 변경 내용은 revision으로 관리한다.

## 4. PlanRevision

계획을 in-place update하지 않는다. 사용자 수정이나 재계획은 새 revision을 만든다.

```text
plan_revision_id
plan_id
revision_number
parent_revision_id
revision_type:
  INITIAL | USER_REVISED | AGENT_REVISED | MULTI_ADAPTIVE
status:
  DRAFT | PENDING_APPROVAL | APPROVED | REJECTED | SUPERSEDED
change_summary
change_rationale
approval_requirement:
  REQUIRED | NOT_REQUIRED_WITHIN_APPROVED_STRATEGY
approved_by
approved_at
approval_payload_hash
created_at
```

`approval_payload_hash`는 사용자에게 표시된 설명, Skill/Tool, 공개 parameter, 예상 결과,
위험도와 내부적으로 결합된 Tool/source version을 canonical JSON으로 hash한 값이다.

승인 뒤 해당 payload가 바뀌면 새 revision과 새 승인이 필요하다. MULTI의 승인 전략 범위 안
후속 Step은 `NOT_REQUIRED_WITHIN_APPROVED_STRATEGY`로 기록하지만 revision과 근거는 생략하지
않는다.

## 5. PlanStep

모든 Step은 다음 질문에 답할 수 있어야 한다.

- 무엇을 수행하는가?
- 왜 필요한가?
- 왜 이 Skill과 Tool을 선택했는가?
- 어떤 입력과 이전 결과를 근거로 선택했는가?
- 무엇을 만들거나 확인하려는가?
- 성공 여부를 어떻게 검증하는가?

필드 초안:

```text
plan_step_id
plan_revision_id
sequence
step_type: TOOL | CUSTOM_CODE
title
description
purpose
selection_rationale
evidence_refs[]
constraints_applied[]
alternatives_considered[]
expected_inputs[]
expected_outputs[]
expected_artifacts[]
validation_criteria[]
depends_on_step_ids[]
timeout_seconds

skill_name / skill_version / skill_content_hash
tool_name / tool_version / tool_source_hash
tool_parameters
public_parameter_summary

custom_code_generation_rationale
risk_review_id
```

Tool Step의 `selection_rationale`은 등록된 Tool을 선택한 이유를 설명한다. Tool 함수 구현은
개발자가 작성한 canonical source이므로 LLM이 함수 내부를 새로 만든 이유는 없다. 대신 해당
Task에서 그 Tool을 사용한 이유를 기록한다. Tool registry의 정의에는 Tool 자체가 처음
만들어진 목적(`creation_rationale`), owner, version별 변경 이유와 source hash를 별도로
보관한다.

Custom Code Step은 Skill/Tool 대신 다음을 기록한다.

- 기존 Tool로 요구사항을 충족할 수 없었던 이유 또는 사용자가 자유 코드를 요청했다는 근거
- 생성할 함수의 목적과 algorithm 요약
- 입력/출력 및 검증 기준
- 생성된 source reference/hash와 모델/prompt provenance

## 6. CompiledStep

PlanStep과 실제 Executor 입력 사이의 정확한 변환을 기록한다.

```text
compiled_step_id
plan_step_id
compiler_version
rendering_mode: INLINE_DEFINITION | RUNTIME_IMPORT
runtime_profile
source_ref
source_sha256
source_size_bytes
compiled_parameters_hash
compiled_at
```

전체 code source를 BFF PostgreSQL에 중복 저장하거나 BFF 감사 API로 제공하지 않는다.
Executor 제출을 위해 필요한 source는 Agent/Executor shared request storage의 `.py`로 만들고
source checksum과 제출 reference만 mapping에 기록한다. 실행 이후 실제 코드 확인의 원본은
Executor가 관리하는 Jupyter notebook 및 immutable executed-source snapshot이다.

## 7. ExecutorBinding

Executor submit/append 응답을 받은 즉시 다음 mapping을 저장한다.

```text
plan_step_id
compiled_step_id
execution_id
operation_id
executor_step_id
executor_sequence
executor_state_version
submit_idempotency_key
bound_at
```

실행 후에는 Attempt, result manifest, Artifact와 상태를 연결한다.

```text
attempt_id
fencing_token
step_status
result_ref
result_manifest_sha256
artifact_ids[]
started_at
finished_at
failure_type
error_summary
```

원본 상태는 Executor가 소유하고 BFF mapping은 조회/보고용 projection이다. 불일치가 의심되면
Executor Result API와 event history로 reconcile한다.

BFF는 `execution_id`를 항상 Task/Plan에 연결해 저장하고, 사용자 또는 운영자가 실제 코드를
확인하려 할 때 Executor의 execution 기반 notebook 조회/다운로드 API로 연결한다. 가능하면
`operation_id`, `executor_step_id`, sequence도 함께 사용해 PlanStep과 notebook cell의 대응을
표시한다.

## 8. SINGLE과 MULTI

### SINGLE

- 승인 전에 전체 PlanRevision과 모든 Step을 생성한다.
- 승인된 revision을 freeze한다.
- 모든 CompiledStep을 생성한 뒤 하나의 Executor Operation으로 제출한다.
- 실행 중 계획을 변경하지 않는다.
- 실패 시 Agent 코드 수정/재실행 없이 실패 Step과 실행 증거를 연결하고 사용자에게 실패
  내용과 원인을 알린다. 실패 리포트는 생성하지 않는다.

### MULTI

- 최초 승인에는 전체 전략과 첫 실행 Step을 포함한다.
- Operation 결과마다 새로운 `MULTI_ADAPTIVE` revision을 추가한다.
- 후속 Step은 어떤 이전 Step 결과/result reference를 근거로 만들었는지 기록한다.
- 최초 승인 전략 범위 안이면 자동 실행하되 `approval_requirement`와 판단 근거를 남긴다.
- 중대한 변경/새 데이터 접근/위험도 상승/새 외부 부수효과가 있으면 새 revision을
  `PENDING_APPROVAL`로 만든다.
- 이미 실행한 revision과 Step은 수정하거나 삭제하지 않는다.
- 오류 보정 Step은 Task당 최대 3회 자동 생성한다. 한도를 초과하거나 보정 불가능하면 실패
  내용과 원인을 사용자에게 알리고 종료한다. 실패 리포트는 생성하지 않는다.

## 9. 모델과 Prompt provenance

PlanRevision 및 Custom Code 생성에는 다음을 저장한다.

```text
model_provider
model_name
model_parameters_hash
prompt_template_name
prompt_template_version
skill_registry_snapshot_hash
tool_registry_snapshot_hash
input_message_ids[]
input_result_refs[]
structured_output
request_trace_id
token_usage
latency_ms
```

원본 prompt/response 저장 여부는 개인정보, 보안, 비용과 retention 정책을 별도로 합의한 뒤
결정한다. 초기 정책은 원본 prompt/response와 raw chain-of-thought를 저장하지 않는 것이다.
대신 template/model/version, 입력 참조, structured output, trace ID와 사용량을 저장한다.

## 10. 성공 Task 최종 리포트 lineage

최종 리포트는 성공한 Task에만 생성한다. 각 주요 결과는 가능한 경우 다음을 참조한다.

```text
report section/finding
  -> plan_step_id
  -> executor_step_id / attempt_id
  -> result_ref / artifact_id
  -> validation result
```

리포트에는 최소 다음을 포함한다.

- 최초 승인 전략과 revision history
- 실제 실행된 Step 순서
- Step별 Skill/Tool 또는 Custom Code
- Step별 선택/생성 근거
- 계획과 실제 실행의 차이
- 성공/실패/건너뛴 Step과 이유
- 핵심 결과와 근거 result/artifact
- 한계와 다음 추천 작업

실패 또는 취소 Task는 리포트를 생성하지 않는다.

- 실패: 실패한 Operation/Step, 안전한 오류 요약, 원인 분류와 사용자가 취할 수 있는 다음
  행동을 메시지로 저장한다.
- 취소: Executor 취소 완료를 확인한 뒤 취소되었다는 메시지를 저장한다.
- 취소 요청만 저장되고 Executor 확인이 끝나지 않은 동안은 `CANCEL_REQUESTED`와 Session
  잠금을 유지한다.

## 11. 사용자 조회

Frontend/BFF는 다음 정보를 조회할 수 있어야 한다.

- 현재/과거 PlanRevision
- revision 간 diff와 변경 이유
- 각 Step의 설명, Skill/Tool, 공개 parameter, 선택 근거
- 위험 판정과 승인 기록
- 실제 Executor mapping과 실행 상태
- 결과/Artifact와 최종 리포트 연결

내부 code source는 BFF에서 제공하지 않고 Executor notebook API로 연결한다. 전체 query 또는
민감 parameter는 권한과 별도 상세조회 정책을 적용한다.
