# Executor Integration Reference

이 문서는 Agent 프로젝트에서 Executor를 연동할 때 지켜야 하는 계약을 요약한다.
Executor 저장소의 2026-08-27 `main` 브랜치(`c313b40`)를 기준으로 작성했다.

원본 저장소:

```text
/Users/a10054/SKAX_PROJECT/executor
```

## 1. 책임 경계

Agent가 소유한다.

- 사용자 대화, Task, 분석 의도 분류
- 실행 계획과 사용자 승인 상태
- 데이터 분석 Skill/Tool 선택과 코드 생성
- LangGraph checkpoint와 Executor 이벤트 소비 checkpoint
- 실행 결과 해석 및 최종 리포트

Executor가 소유한다.

- Execution, Operation, Step, Attempt, StepAttempt
- Runtime target/session, 실행 상태, lease와 fencing
- Jupyter 코드 실행, interrupt, timeout, cleanup
- 실행 결과 manifest, notebook projection, artifact lineage
- PostgreSQL authoritative state와 Redis integration event

Agent와 Executor는 데이터베이스 테이블을 공유하지 않는다. Agent는 Executor가 발급한
`execution_id`, `operation_id`, `step_id`를 자신의 Task/PlanStep에 연결해 저장한다.

PATH source는 Executor의 `request_storage_root`인
`/workspace/shared/requests`를 기준으로 한 상대경로를 전달한다. Agent가 실제 파일을
`/workspace/shared/requests/{task_id}/{revision}/step-0000.py`에 저장했다면 API에는
`{task_id}/{revision}/step-0000.py`를 보낸다.

## 2. 전송과 저장소 역할

- MCP: `POST /mcp`
- REST: `/api/v1`
- PostgreSQL: Executor 상태와 이벤트 이력의 원본
- Redis Streams: work/event wake-up 채널
- Agent/Executor shared volume: 요청 Python 파일과 Step 결과 manifest/output
- Jupyter shared volume: notebook, Runtime artifact, report

Redis payload에는 전체 코드, 전체 텍스트 출력, 이미지 Base64를 넣지 않는다.

## 3. 최초 실행 제출

REST:

```http
POST /api/v1/executions
```

MCP Tool:

```text
execution_submit
```

핵심 요청 필드:

```json
{
  "idempotency_key": "agent-generated-stable-key",
  "lifecycle": {
    "operation_mode": "SINGLE | MULTI",
    "operation_wait_timeout_seconds": 600
  },
  "trigger": {
    "type": "INTERACTIVE",
    "actor": {"type": "AGENT", "id": "analytics-agent"}
  },
  "runtime": {"type": "JUPYTER", "profile": "basic"},
  "context": {
    "user_id": "...",
    "task_id": "...",
    "project_id": "...",
    "session_id": "..."
  },
  "operation": {
    "operation_timeout_seconds": 600,
    "spec": {
      "schema_version": "1.0",
      "steps": []
    },
    "metadata": {}
  },
  "metadata": {}
}
```

규칙:

- `idempotency_key`는 Agent가 생성하고 같은 논리 명령의 재시도에서 재사용한다.
- 같은 키에 다른 payload를 사용하면 conflict다.
- SINGLE에는 `operation_wait_timeout_seconds`를 보내지 않는다.
- MULTI에는 `operation_wait_timeout_seconds >= 30`이 필수다.
- 현재 Runtime type은 `JUPYTER`다.
- `session_id`를 보내려면 `project_id`도 있어야 한다.
- Actor가 `USER`이면 actor ID와 context user ID가 같아야 한다.

## 4. Step 코드 계약

Operation은 하나 이상의 Step을 가진다. 최초 Step sequence는 `0`부터 연속 증가한다.

```json
{
  "sequence": 0,
  "payload": {
    "type": "PYTHON_EXECUTE",
    "source": {
      "type": "PATH",
      "path": "{task_id}/{revision}/step-0000.py",
      "sha256": "64-character lowercase hex"
    }
  },
  "step_timeout_seconds": 300,
  "lineage": {
    "skill_name": "eda",
    "tool_name": "describe_data",
    "input_parameters": {}
  }
}
```

Agent는 `EXECUTOR_SOURCE_MODE=PATH`만 허용한다. 코드와 성공 리포트를
Agent/Executor shared request root 아래에 원자적으로 저장하고, 상대경로와
SHA-256만 Executor에 전달한다. INLINE payload는 Agent 경계에서 거절한다.

PATH source는 Agent/Executor shared request root 기준 상대경로와 SHA-256을 사용한다.

```json
{
  "type": "PATH",
  "path": "{task_id}/{revision}/step.py",
  "sha256": "64-character lowercase hex"
}
```

Agent 설계 요구사항:

- 하나의 Step은 하나의 Jupyter cell에 대응한다.
- 데이터 분석 Tool 기반 Step은 `lineage.skill_name`, `tool_name`,
  `input_parameters`를 채운다.
- 자유 코드 Step은 lineage를 비우거나 자유 코드임을 나타내는 Agent metadata 정책을
  사용한다. 최종 정책은 인터뷰에서 확정한다.
- Executor는 Python 코드에서 Tool lineage를 역추론하지 않는다.

## 5. SINGLE 실행

SINGLE은 승인된 전체 계획의 모든 Step을 최초 Operation 하나로 제출한다.

권장 흐름:

```text
plan -> human approval -> execution_submit(SINGLE)
     -> wait for execution.completed
     -> execution_result_get
     -> validate result manifests
     -> final report
```

중간 Step 실패 시 Executor 수준 자동 보정 Operation은 없다. Agent는 실패 결과를 보고하고
해당 Task run을 종료한다. 별도의 새 실행/재시도 정책이 필요한지는 Agent 제품 정책으로
결정한다.

## 6. MULTI 실행

MULTI는 한 번에 한 Operation을 실행하고 결과를 본 뒤 다음 Operation을 결정한다. 현재
제품 요구사항에서는 Operation당 Step 한 개를 사용하는 것이 자연스럽다.

추가 Operation:

```http
POST /api/v1/executions/{execution_id}/operations
```

```text
execution_operation_create
```

필수 규칙:

- Execution이 `WAITING_FOR_OPERATION`이어야 한다.
- `expected_version`은 최신 Execution `state.version`과 일치해야 한다.
- 새 Step sequence는 기존 전체 Step 수부터 연속 증가한다.
- 409 conflict 발생 시 최신 상태를 조회하고 이미 반영된 명령인지 먼저 확인한다.

더 실행할 Step이 없으면 finalize한다.

```http
POST /api/v1/executions/{execution_id}/finalize
```

```text
execution_finalize
```

Finalize도 최신 `expected_version`과 안정적인 idempotency key가 필요하다. Finalize는
리포트를 만들어 주지 않으며 Execution을 닫고 notebook projection/session cleanup을
완료하는 명령이다.

## 7. 이벤트 소비와 LangGraph 재개

Agent 전용 Redis consumer group과 다음 checkpoint가 필요하다.

```text
execution_id + last_event_sequence
```

처리 규칙:

- 동일 Execution의 이벤트는 `event_sequence` 순서로 직렬 적용한다.
- `event_sequence <= checkpoint`는 중복이므로 ACK한다.
- gap이 있으면 Executor event history API/MCP Tool로 누락 이벤트를 복구한다.
- 상태 저장과 이벤트 부수효과를 완료한 뒤 ACK한다.
- `event_id`로 외부 부수효과도 중복 제거한다.
- Redis Stream ID나 timestamp로 lifecycle 순서를 추론하지 않는다.

LangGraph wake boundary:

- SINGLE: `execution.completed`
- MULTI Operation: `execution.operation_completed`
- MULTI terminal: `execution.completed`

LLM에게 Redis ACK, pending reclaim, gap recovery를 직접 맡기지 않는다. 결정론적인
`ExecutionEventSubscriber` 어댑터가 정렬·중복 제거·누락 복구 후 정규화된 결과만 그래프에
전달해야 한다.

## 8. 결과 조회와 검증

통합 결과:

```http
GET /api/v1/executions/{execution_id}/result
```

```text
execution_result_get
```

Operation 결과:

```text
execution_operation_result_get
```

응답은 상태, Operation, Step, Attempt, Artifact 인덱스이며 실제 출력 본문은 포함하지
않는다. Step의 `result_ref`를 통해 shared volume의 manifest를 읽는다.

Agent가 검증할 항목:

- 상대경로가 shared root 밖으로 이탈하지 않는지
- manifest checksum과 size가 `result_ref`와 일치하는지
- `complete=true`인지
- execution/step/attempt/fencing identity가 참조와 일치하는지
- manifest가 가리키는 각 representation의 checksum과 size가 일치하는지
- 허용된 MIME type과 모델 context 예산을 만족하는지

불완전하거나 검증되지 않은 출력은 성공 결과처럼 리포트하지 않는다.

Notebook 셀과 최종 Markdown 리포트는 execution ID로 조회한다.

```http
GET /api/v1/executions/{execution_id}/notebook?view=FULL
```

2026-08-28 라이브 E2E에서 확인한 현재 Executor 구현의 주의사항:

- REPORT를 `append_to_notebook=true`로 materialize하면 execution notebook
  조회 API는 추가된 Markdown cell을 정상 반환한다.
- 실행 완료 시 등록된 기존 NOTEBOOK Artifact의 size/checksum은 리포트
  append 후 갱신되지 않아, Artifact content 다운로드가 기존 size에서
  잘릴 수 있다. Frontend 노트북 복원은 우선 execution notebook API를
  사용하고, Executor에서 NOTEBOOK Artifact metadata 갱신/불변 snapshot
  정책을 보완해야 한다.

## 9. HITL과 부수효과 경계

LangGraph `interrupt()`가 재개되면 해당 node는 처음부터 다시 실행된다. 따라서 다음
경계를 지킨다.

```text
generate_plan              # side-effect free
prepare_approval_payload   # side-effect free
human_approval             # interrupt; side-effect free
submit_to_executor         # approval 뒤의 별도 node, idempotent command
```

승인 node에서 Executor submit을 수행하면 재개 시 중복 호출 위험이 생긴다. 모든 Executor
mutation에는 저장된 idempotency key를 사용하되, 승인 전에는 mutation 자체를 호출하지
않는다.

## 10. Runtime Target 준비

Executor는 Runtime Fleet이 빈 상태로 시작할 수 있고, 이 때도 `/readyz`는
정상이다. 호환되는 ACTIVE target이 없으면 Execution은 `QUEUED`에
남는다.

```http
GET  /api/v1/runtime-targets
POST /api/v1/runtime-targets
```

로컬 Compose의 Jupyter container를 시작하는 것과 Runtime Target 등록은
별개 작업이다. Target은 Executor container에서 접근 가능한
`http://jupyter:8888` 같은 endpoint, token, pool, capacity를 포함해야 한다.

Executor와 Jupyter image의 extension 계약도 같아야 한다. 예를 들어
Executor가 `/executor/storage/notebooks/prepare`를 호출하면 Jupyter image에
해당 route가 포함되어야 한다. 소스 변경 후 오래된 image를 재사용하면
Runtime probe는 통과하지만 실행 준비가 404로 실패할 수 있다.

2026-08-28 MULTI 라이브 검증에서는 현재 Executor source로 Jupyter image를
재빌드한 뒤 이 endpoint와 2개 연속 Operation, Notebook report append가 모두
정상 동작했다. Executor API 변경 뒤에는 Jupyter image도 함께 재빌드해야 한다.

## 11. 원본 문서

Executor 저장소의 다음 파일을 우선 참고한다.

```text
dev_docs/post-executions.md
dev_docs/post-execution-operations.md
dev_docs/post-execution-finalize.md
dev_docs/get-execution-result.md
dev_docs/agent-execution-event-consumer-guide.md
docs/architecture-decisions.md
docs/event-delivery.md
docs/execution-recovery.md
docs/shared-result-storage.md
```
