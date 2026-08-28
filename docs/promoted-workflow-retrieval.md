# Promoted Workflow Retrieval Contract

상태: `IMPLEMENTED_V1_SEARCH_BASELINE`

## 1. 정의

Promoted Workflow는 LLM이 현재 요청에서 동적으로 생성한 계획이 아니다. 사용자가 과거에
성공적으로 실행한 Tool 기반 Plan을 명시적으로 재사용 Workflow로 승격한 불변 버전이다.

데이터 분석 실행 요청은 동적 계획 전에 Workflow 검색 단계를 거친다.

```text
user query
  -> LLM intent classification: DATA_ANALYSIS_EXECUTION
  -> query embedding
  -> PostgreSQL/pgvector Workflow search
  -> compatibility/permission validation
  -> top Workflow proposal
      -> user selects: instantiate fixed Workflow -> SINGLE
      -> user declines/no eligible result: dynamic planning -> MULTI
```

벡터 검색 score만으로 Workflow를 자동 선택하거나 SINGLE을 자동 실행하지 않는다. 최종 선택은
사용자가 한다.

## 2. Workflow와 version

```text
Workflow
  workflow_id
  owner_user_id
  project_id
  name
  description
  visibility: SERVICE
  access_policy
  status
  latest_version
  created_at

WorkflowVersion
  workflow_version_id
  workflow_id
  version
  source_task_id
  source_plan_id
  source_plan_revision_id
  source_execution_id
  objective
  strategy_summary
  input_contract
  output_contract
  runtime_profile
  tool_registry_snapshot_hash
  searchable_text
  searchable_text_hash
  embedding
  embedding_model
  embedding_dimension
  promoted_by
  promoted_at
  promotion_policy_version
```

WorkflowVersion은 생성 후 수정하지 않는다. Workflow 내용 변경은 새 version을 만든다.

## 3. 승격 조건

초기 정책:

- 성공한 Task/Execution만 승격할 수 있다.
- 사용자가 명시적으로 승격해야 한다.
- 승격 요청 주체가 현재 `PromotionPolicy`를 통과해야 한다.
- 모든 실행 Step이 등록된 Skill/Tool 기반이어야 한다.
- `CUSTOM_CODE` Step이 포함된 Plan은 초기에는 승격할 수 없다.
- Plan/Executor/result lineage가 완전해야 한다.
- 사용한 Tool version/source hash와 runtime profile을 고정한다.
- MULTI 실행도 최종적으로 성공한 실제 Tool Step 순서를 고정 Workflow로 승격할 수 있다.
- 서비스 전체 공개 전에 원본 query, 실제 데이터 값, user/project/session/task ID와 민감
  parameter를 제거하고 재사용 가능한 parameter template로 변환한다.

승격은 원본 Plan을 수정하지 않고 별도 WorkflowVersion snapshot을 만든다.

## 4. WorkflowStep

```text
workflow_step_id
workflow_version_id
sequence
skill_name / skill_version / skill_hash
tool_name / tool_version / tool_source_hash
purpose
selection_rationale
parameter_template
required_parameters[]
expected_inputs[]
expected_outputs[]
validation_criteria[]
timeout_seconds
```

Workflow의 Step 순서와 Tool 선택은 고정이다. 현재 요청에 맞는 parameter binding은 허용하지만
Step 추가/삭제/재배열 또는 Tool 교체는 Workflow 선택으로 간주하지 않는다. 그런 변경이
필요하면 Workflow를 선택하지 않고 MULTI 동적 계획으로 간다.

각 WorkflowStep은 Skill과 Tool을 함께 추적한다. 이름만이 아니라 Skill/Tool version,
content/source hash, 선택 근거와 parameter template까지 WorkflowVersion snapshot에 고정한다.

## 5. Index document

한 WorkflowVersion을 하나의 검색 document로 사용한다. 임의 chunking은 하지 않는다. 검색
텍스트에는 다음을 포함한다.

- Workflow 이름과 설명
- 해결하는 분석 목적
- 적합한 사용자 요청 예시
- 필요한 입력 데이터 특성
- 생성하는 결과/Artifact
- Skill/Tool 이름과 기능 설명
- 제약과 부적합 조건
- 사용자가 작성한 승격 설명/태그

Tool source code, 전체 query, 실제 데이터 값, 사용자/프로젝트 식별자, 대용량 실행 결과는
embedding text에 넣지 않는다.

## 6. pgvector 검색

검색 전 의미 분류는 LLM이 수행한다. `DATA_ANALYSIS_EXECUTION`일 때만 Workflow retriever를
호출한다.

검색 순서:

1. user query를 index와 동일한 embedding model로 embed한다.
2. DB query에서 `SERVICE` 공개 및 현재 access policy를 filter한다.
3. `ACTIVE` WorkflowVersion, runtime profile, Skill/Tool availability를 filter한다.
4. pgvector similarity로 top-k 후보를 조회한다.
5. 최소 relevance threshold와 input compatibility를 검증한다.
6. 상위 eligible Workflow 3개를 사용자에게 제안한다.
7. 추가 후보는 안정적인 cursor 기반 pagination으로 조회한다.

초기 `k`와 threshold는 evaluation dataset으로 결정하고 환경설정으로 둔다. 서로 다른 embedding
model/dimension의 vector를 같은 index에서 비교하지 않는다. model 변경 시 새 embedding version
또는 재색인이 필요하다.

검색 결과와 판단 근거를 저장한다.

```text
workflow_search_id
task_id / session_id / input_message_id
embedding_model / index_version
filters
candidates[]: workflow_version_id, score, rank, eligibility
proposed_workflow_version_id
searched_at
```

`candidates`에는 검색 당시 전체 rank/score/eligibility를 감사용으로 남기되 사용자 응답은 기본
3개와 다음 cursor만 반환한다.

## 7. 사용자 제안 payload

사용자에게 상위 3개 Workflow card를 표시한다. 각 card에는 code가 아니라 다음을 표시한다.

- Workflow 이름과 설명
- 이 Workflow가 제안된 짧은 이유
- 고정된 Step 순서
- 각 Step의 Skill/Tool, 목적과 선택 이유
- 현재 요청에 binding한 공개 parameter
- 예상 결과/Artifact
- runtime/timeout 정보
- Tool version compatibility와 위험 판정

선택/거절 결정은 `workflow_version_id`, binding hash, proposal version에 묶어 멱등 저장한다.
사용자가 전체 Step과 binding parameter를 확인하고 선택한 행위가 곧 실행계획 승인이다.

초기 `PromotionPolicy`는 인증된 모든 사용자를 허용한다. promotion application service는
policy를 우회하지 않으며 다음 확장점을 가진다.

```text
can_promote(access_context, source_task, source_plan) -> PolicyDecision
```

향후 역할, 조직, 프로젝트, allowlist 또는 관리자 승인 정책을 추가할 수 있다. 승격 기록에는
판정한 policy version과 actor를 저장한다.

## 8. Workflow 선택

사용자가 선택하면:

1. 선택된 WorkflowVersion을 현재 Task의 새 Plan/PlanRevision으로 복제한다.
2. required parameter를 현재 query/context에서 binding하고 schema 검증한다.
3. 사용자에게 표시된 Workflow/parameter payload hash를 저장한다.
4. Workflow 선택을 PlanRevision 승인으로 기록한다.
5. 같은 transaction 경계에서 Session lock을 획득한다.
6. 고정 PlanStep을 compile한다.
7. Execution mode를 `SINGLE`로 설정하고 Executor 제출 절차를 수행한다.

사용자가 선택하지 않거나 eligible Workflow가 없으면:

1. Workflow proposal 결과를 감사 이력에 남긴다.
2. 데이터 분석 Skill/Tool을 사용해 동적 Plan을 생성한다.
3. Execution mode를 `MULTI`로 설정한다.
4. 기존 MULTI 계획/HITL 절차를 수행한다.

## 9. Lifecycle과 호환성

Tool이 삭제되거나 source/version/runtime dependency가 호환되지 않으면 해당 WorkflowVersion을
실행 후보에서 제외한다. 과거 감사 조회에는 그대로 보존한다.

새 Tool version으로 Workflow를 갱신하려면 compatibility 검증 및 테스트 후 새
WorkflowVersion을 만들어야 한다. 검색 embedding도 새 version 내용으로 생성한다.

## 9.1 공개 범위와 향후 권한

초기에는 인증된 모든 서비스 사용자가 모든 `SERVICE` Workflow를 검색하고 선택할 수 있다.
향후 migration 없이 `USER`, `PROJECT`, `ROLE` 범위를 추가할 수 있도록 다음 정보를 유지한다.

```text
visibility
owner_user_id
owner_project_id
access_policy_version
allowed_principal_refs[]
```

초기 authorization evaluator는 `SERVICE`를 모두 허용하지만 repository/search API는 항상
`AccessContext`를 받는다. 향후 정책 적용 시 vector search 전에 authorization filter를 추가한다.

## 10. 평가

최초 개발 시 다음 fixture가 필요하다.

- 관련 Workflow가 명확한 query
- 유사하지만 입력 계약이 맞지 않는 query
- 적합한 Workflow가 없는 query
- 향후 ACL 설정에서 접근 불가 Workflow가 섞인 query
- 비활성/Tool version 불일치 Workflow
- 유사한 Workflow가 여러 개인 query
- 첫 3개 이후 cursor pagination query

측정 항목:

- eligible top-1 precision
- no-match accuracy
- access-policy filter leakage 0건
- 사용자 선택/거절률
- Workflow 선택 후 성공률
- 동적 MULTI fallback 성공률
- promotion authorization enforcement
