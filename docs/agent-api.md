# 메시지·Run API 계약

## 현재 범위와 제한

사용자·프로젝트·세션 관리 위에 메시지 저장, Run 접수·조회·재개·취소,
SSE 스트림과 개발 화면을 추가했습니다. 아래 계약은 실제 API입니다.
기본 실행기는 실제 LangGraph 대화입니다.
[체크포인트와 실행 설정](langgraph-runtime.md)을 함께 참고하세요.
실제 LangGraph도 동일한 승인/수정/거절 계약을 사용합니다. 아래 실행 대기와
리포트 단계 예시는 **DEMO 실행기**에 해당합니다. 개발에서만 활성화하며
운영 환경에서 `agent_backend: demo`를 지정하면 시작을 거부합니다.

Executor, Redis Worker, 프로젝트 공유 메모리,
파일·이미지 업로드, 노트북·리포트 생성은 아직 연결하지 않았습니다.
DEMO의 실행 ID는 `simulated: true`이며 Executor 조회에 사용하면 안 됩니다.
실제 그래프의 승인 계획은 LLM이 만든 초안이며 아직 Skill/Tool 선택 결과가 아닙니다.
승인 후 `failed`와 `EXECUTOR_NOT_CONFIGURED`를 반환하며 실제 실행은 하지 않습니다.
고정 테스트 계획은 `agent_backend: demo`에서만 사용합니다.

## API 목록

기본 경로는 `/api/v1`입니다. 관리 API와 같은 사용자 확인 의존성을 사용합니다.

| 메서드 | 경로 | 기능 |
| --- | --- | --- |
| GET | `/sessions/{session_id}/messages` | 저장된 메시지 목록 |
| POST | `/agent/runs` | 신규 메시지 실행 또는 HITL 재개 |
| GET | `/agent/runs/{run_id}` | 상태·입력 요청·실행 ID 조회 |
| GET | `/agent/runs/{run_id}/stream` | 저장 이벤트 재전송 및 진행 관찰 |
| POST | `/agent/runs/{run_id}/cancel` | 실제 Run 취소 요청 |
| GET | `/sessions/{session_id}/runs` | 해당 세션의 Run 목록 |

목록은 `limit`(기본 20, 최대 100), `cursor`를 사용합니다.
정렬은 `(created_at DESC, ID DESC)`입니다. 프론트는 메시지를 화면에서
시간순으로 배치합니다. 응답은 `items`, `has_more`, `next_cursor`입니다.
다른 사용자·세션·목록 종류의 커서는 재사용할 수 없습니다.
메시지, Run, 입력 요청, 실행 연결에는 `created_at/by`, `updated_at/by`가
있습니다. 변경하지 않는 스트림 이벤트는 `occurred_at`을 사용합니다.

## 식별자와 인증

1. `POST /me`는 기존처럼 사번을 받아 내부 사용자 UUID를 확보합니다.
2. 실행 요청 본문의 `user_id`는 **사번이 아닌 내부 UUID**입니다.
3. 개발 모드에서는 `X-User-UUID` 헤더도 같은 UUID로 보냅니다.
4. 본문 UUID는 인증 수단이 아닙니다. 인증 의존성에서 확인한 사용자와
   다르면 403이며, 타인 소유 리소스는 404입니다.
5. 운영에서는 기존 trusted-header 또는 external identity provider를
   연결합니다. SSO 구현을 본문 처리에 섞지 않습니다.

`project_id`는 클라이언트가 보내지 않습니다. 서버가 세션 소유권을 확인한
다음 프로젝트를 찾아 Run에 기록합니다. 별도 Task 테이블은 만들지 않았고,
외부 실행과의 연결은 `Run → run_executions → execution_id`입니다.
동일 Run에 여러 외부 실행을 연결할 수 있는 구조입니다.

## 신규 실행

```http
POST /api/v1/agent/runs
X-User-UUID: <사용자 UUID>
Idempotency-Key: <이 요청에 대해 생성한 고유 키>
Content-Type: application/json
```

```json
{
  "user_id": "<사용자 UUID>",
  "session_id": "<세션 UUID>",
  "stream": false,
  "input": {
    "type": "message",
    "content": [{"type": "text", "text": "샘플 데이터 분석해 줘"}]
  }
}
```

위 `<...>`는 설명용이며 실제 요청에는 유효한 UUID를 사용합니다.
`stream` 생략 시 false이며 HTTP 202 접수 응답을 반환합니다.

```json
{
  "run_id": "<Run UUID>",
  "session_id": "<세션 UUID>",
  "status": "queued"
}
```

`stream: true`이면 HTTP 200 `text/event-stream`을 반환합니다.
두 방식은 같은 작업을 접수하며, 스트리밍이 실행 수명을 결정하지 않습니다.
스트림 응답의 `X-Run-Id` 헤더로도 Run ID를 확보할 수 있습니다.
SSE 연결 실패는 접수가 취소됐다는 의미가 아닙니다.

### 중복 요청

`Idempotency-Key`는 신규 실행과 재개에 모두 필수입니다. 동일 사용자·키·본문의
재요청은 기존 접수 결과를 돌려주며 메시지/Run/재개를 다시 만들지 않습니다.
같은 키에 다른 본문은 409입니다. `stream` 값만 바꾸는 것은 허용합니다.
재개는 신규 실행 요청과 다른 키를 사용하고, 그 재개의 재시도에는 같은 키를
사용합니다. 재시도 응답 상태는 **최초 접수 당시 값**일 수 있으므로 현재 상태는
Run 조회 또는 스트림으로 확인합니다.

키를 잃어버린 경우 무조건 새 키로 재실행하지 말고 세션 Run 목록을 확인하세요.
실제 Executor 제출용 멱등 키는 별개이며, API의 키를 그대로 대체하지 않습니다.

## HITL 재개

Run 조회의 `pending_interrupts` 또는 `run.interrupted` 이벤트가 안내한
`interrupt_id`, `response_type`, `schema_version`, 응답 스키마를 사용합니다.

```json
{
  "user_id": "<사용자 UUID>",
  "session_id": "<세션 UUID>",
  "stream": true,
  "input": {
    "type": "resume",
    "run_id": "<기존 Run UUID>",
    "interrupt_id": "<현재 대기 중인 요청 UUID>",
    "response": {
      "type": "plan_review",
      "schema_version": 1,
      "decision": "modify",
      "instruction": "그래프는 제외해 줘"
    }
  }
}
```

- `approve`, `reject`에는 `instruction`을 넣지 않습니다.
- `modify`에는 공백이 아닌 `instruction`이 필수입니다.
- 수정 후에는 새 입력 요청 ID와 계획 버전으로 다시 승인을 요청합니다.
- 신규 Run을 만들지 않고 기존 Run을 이어갑니다.
- 이미 응답한 요청, 이전 버전, 다른 세션의 Run은 거절합니다.
- 동시에 서로 다른 승인 응답을 보내도 하나만 접수됩니다.
- 외부에서 LangGraph `Command`, `goto`, 임의 State를 주입할 수 없습니다.

복잡한 HITL은 `response.type`별 Pydantic 모델을 추가하고, 대기 요청에
그 JSON Schema를 안내하는 방식으로 확장합니다. 현재 실제 지원 타입은
`plan_review` 1개입니다. 임의 JSON을 무검증으로 받는 상태는 아닙니다.

요청 전체는 128 KiB, JSON 중첩은 32단계까지 허용합니다. 텍스트 블록은
1~32개, 블록당 최대 16,000자, 메시지 합계 32,000자이며 수정 지시는
8,000자까지입니다. 크기 초과는 413, 타입/필드/스키마 오류는 422입니다.

## 상태·잠금·취소

기본 성공 흐름은 다음과 같습니다.

```text
queued → running → awaiting_input → queued → running
       → waiting_execution → running(결과 정리) → completed
```

`awaiting_input`에서 수정하면 다시 승인 대기로, 거절하면 `rejected`로 갑니다.
실패는 `failed`, 취소는 즉시 `cancelled` 또는 `cancelling → cancelled`입니다.

세션의 `active_run_id`는 접수부터 종료까지 유지하여 Run 중첩을 막습니다.
`is_locked`는 외부 실행 제출 직전 켜고 결과 정리까지 유지합니다.
따라서 일반 처리/승인 대기 중에도 새 메시지는 409(`RUN_ACTIVE`)이며,
실행 잠금 중에는 409(`SESSION_LOCKED`)입니다. 승인 응답과 취소는 가능합니다.
활성 Run이 있는 세션 및 그 프로젝트의 삭제도 409입니다.

취소 API는 body가 없습니다. 즉시 확인 가능하면 200, 실행 취소 확인이
필요하면 202(`cancelling`)입니다. 확인 전에는 잠금을 풀지 않습니다.
이미 취소 중/취소 완료인 요청은 반복 가능하며, 다른 종료 상태는 409입니다.
취소된 Run의 재실행은 새 Run입니다. 취소·실패·거절 시 보고서를 만들지 않습니다.
현재 확인은 DEMO 내부 상태에만 해당하며 실제 Executor 취소 확인은 미연결입니다.

브라우저 연결 중단, 탭 종료, `AbortController`는 **작업 취소가 아닙니다**.

## 스트림과 화면 복원

SSE 프레임은 다음 형태입니다.

```text
id: <Run UUID>:5
event: message.delta
data: {"run_id":"<Run UUID>","type":"message.delta","data":{"message_id":"<메시지 UUID>","block_index":0,"delta":"일부 응답"},"occurred_at":"<시각>"}

```

주요 타입:

| 타입 | 의미 |
| --- | --- |
| `run.accepted` | 신규/재개 접수 |
| `run.status_changed` | 상태 전이 |
| `message.started` | assistant 메시지 작성 시작 |
| `message.delta` | 특정 블록에 추가할 텍스트 |
| `message.updated` | 재시작/복구 시 메시지 전체 내용 교체 |
| `message.completed` | 전체 메시지 스냅샷과 저장 상태 |
| `run.interrupted` | 사용자 입력 요청 |
| `run.progress` | 작업 단계 진행 안내 |
| `run.completed/rejected/failed/cancelled` | Run 종료 |
| `stream.closed` | 연결 수명 종료, 작업이 활성 상태면 재연결 |
| `stream.error` | 관찰 연결 오류. Run 실패와 다름 |

`message.completed` 이름만 보고 작업 성공으로 처리하면 안 됩니다.
사용자 메시지 저장 때도 발행하며, 중간 출력 종료 시 `status`가
`interrupted` 또는 `failed`일 수 있습니다. Run 상태를 따로 확인합니다.

신규 실행 첫 이벤트는 `run.accepted`입니다. 재개 스트림에서는 상태 변경
다음 `run.accepted`가 나오므로 순서 한 가지를 가정하지 말고 타입별 처리합니다.
입력 대기 또는 종료 이벤트까지 전달하면 연결을 닫습니다.
입력 대기에서는 재연결하지 말고 사용자 응답을 기다립니다.

GET 스트림 재연결 시 마지막 처리 ID를 `Last-Event-ID` 헤더로 보냅니다.
그 ID **이후**부터 재전송합니다. 같은 이벤트를 다시 받아도 중복 표시하지
않도록 Run별 순번을 추적합니다. 헤더가 없으면 처음부터 전달합니다.
일반 `EventSource`는 사용자 정의 헤더를 설정할 수 없으므로 개발 UI는
`fetch`와 응답 스트림을 사용합니다. 인증 정보를 쿼리로 옮기지 않습니다.

새로고침 시 권장 순서:

1. 세션 Run 목록에서 최신 Run을 찾고 상세 조회합니다.
2. 상세의 `last_event_id`를 기억한 뒤 메시지 목록을 조회합니다.
3. `pending_interrupts`가 있으면 승인 화면을 복원합니다.
4. 작업이 계속 진행 중이면 위 ID 다음부터 GET 스트림을 연결합니다.
5. 메시지의 `event_sequence` 이하 이벤트는 이미 저장된 내용이므로
   다시 추가하지 않습니다. 이 순번은 해당 메시지가 속한 Run 기준입니다.

이는 상세 조회 후 메시지 조회 사이에 출력이 더 저장되어도 글자가 중복되는
문제를 방지합니다. 과거 메시지 페이지도 ID로 합치고 서버 저장 순번을 비교합니다.

다른 Run ID 또는 미래 순번은 422, 보존 범위 밖 커서는 410입니다.
410이면 상태·메시지를 다시 조회하여 복원합니다. 현재는 이벤트를 자동 삭제하지
않으며 보존/정리 정책은 후속 작업입니다. SSE heartbeat는 10초이고,
기본 연결 수명은 60초입니다. 프록시도 응답 버퍼링을 끄고 적절한 타임아웃을
설정해야 합니다. 스트림 연결 1개가 DB 커넥션을 계속 점유하지는 않습니다.

## 메시지 저장 책임

라우터는 HTTP 계약만 처리합니다. `RunService`가 사용자 메시지와 Run 접수를
저장하고, 실행기는 `OutputWriter`를 통해 사용자에게 보여줄 메시지를 기록합니다.
메시지 변경과 이벤트 기록은 같은 PostgreSQL 트랜잭션입니다.

향후 실제 Agent 연결 시에는 출력 어댑터가 사용자 공개 메시지만 이 경로로
보내야 합니다. 모든 내부 노드/툴 메시지, 추론, 비밀값을 그대로 저장·방출하면
안 됩니다. `runs.checkpoint`는 실행 관리 정보입니다. 실제 LangGraph
상태는 별도 `agent_checkpoints` 스키마에 저장하며 메시지 DB와 분리합니다.

## 설정과 실행

`config_dev.yaml`에서 DEMO 흐름을 테스트할 때의 설정은 다음과 같습니다.

```yaml
agent_backend: demo
embedded_run_worker: true
demo_scenario: analysis
```

추가 옵션은 `demo_step_seconds: 0.5`, `stream_poll_seconds: 0.5`,
`stream_max_seconds: 60`입니다. `demo_scenario`는 `reply`(단순 응답),
`analysis`(승인·실행 대기), `failure`(중간 출력 후 실패)입니다.
의도 분류기가 아니라 고정 테스트 시나리오이며, 접수 시 Run에 고정합니다.

```sh
docker compose up --build -d api
docker compose stop api
docker compose --profile test run --build --rm test
docker compose up --no-deps -d api
```

기존 사용자 관리 DB에 `management_0002`, `management_0003`를 적용합니다.
`migrate` 서비스가 먼저 실행합니다. 직접 실행이면 `alembic upgrade head`가
필요합니다. 사용자 DB를 초기화할 필요는 없습니다.

개발 화면은 `http://localhost:8020/dev/`입니다. 사번 입력 → 새 대화 →
메시지 전송 → DEMO 계획 수정/승인/거절 → 진행 상태/취소를 확인할 수 있습니다.
새로고침하면 사번부터 다시 입력하고 기존 대화를 선택해 복원합니다.

독립 프로세스 실행을 시험하려면 `embedded_run_worker: false`로 바꾼 뒤,
같은 DB·설정·설치 소스를 사용하는 두 프로세스를 띄웁니다.

```sh
python app.py
# 별도 터미널, 저장소 루트에서:
python -m agent_service.worker_main
```

개발 구성은 편의를 위해 FastAPI lifespan에서 실행기를 시작합니다.
최종 운영 배포 구조를 이 구성으로 확정한 것은 아닙니다.
단독 worker 진입점도 설정에 따라 실제 LangGraph 또는 DEMO를 처리합니다.
DB 단계 상태와 짧은 트랜잭션/advisory lock으로 프로세스 재시작이나 중복
worker의 동일 단계 경쟁에 대응합니다. 외부 부작용의 exactly-once를
보장한다는 뜻은 아니며 실제 Executor 연동은 별도로 검증해야 합니다.

## 후속 구현

1. 분석 그래프 확장과 새로운 HITL 타입.
2. 실제 Run 실행기의 다중 인스턴스 부하 검증과 분리 배포 헬스체크.
3. Executor 제출·취소·결과 이벤트, Inbox/Outbox 및 실행 ID 복구 연동.
4. 복잡한 HITL 타입 추가, Skill/Tool 계획 추적, 성공 작업 리포트 작성.
5. 프로젝트 Store 메모리와 파일/이미지 메시지 블록·권한·첨부 저장소.
6. 스트림 부하/장기 운영 검증, 이벤트 보존·정리 정책.

현재 스트림은 서버 내부에서 PostgreSQL을 주기 조회합니다. 프론트의 상태 API
폴링은 아니지만 동시 연결 증가 시 조회 부하는 생깁니다. 실서비스 규모에 맞게
알림 기반 전달, 출력 청크 묶음 저장, 부하 제한을 검증해야 합니다.
DEMO 출력은 작은 고정 청크입니다. 실제 LLM의 매 토큰마다 동일 DB 작업을
그대로 실행하는 방식으로 연결하지 마세요.

## 검증

`tests/test_runs.py`는 실제 PostgreSQL로 중복 접수, 동시 재개, 수정 버전,
취소 확인/잠금, 중간 출력, DB 롤백, 이벤트 중복 제거·재전송·커서를 검사합니다.
`tests/browser/run-ui.cjs`는 개발 UI의 스트림, 수정, 승인 대기 복원, 완료,
취소·잠금 해제, 모바일 화면을 검사합니다. 실제 LLM/Executor 검증은 아닙니다.

```sh
CHATAPP_UI_TEST_URL=http://127.0.0.1:8020 node tests/browser/run-ui.cjs
```

브라우저 테스트만 Playwright와 Chrome이 설치된 Node.js가 필요합니다.
서비스와 개발 UI 실행에는 별도 프론트 빌드가 필요하지 않습니다.
통합 테스트 중에는 같은 DB의 다른 실제/DEMO worker를 중지해야 합니다.

실제 API 프로세스 재시작 복구 검증은 아래처럼 명시적으로 실행합니다.
로컬 Compose의 `api`만 중지/시작하며 PostgreSQL은 재시작하지 않습니다.
다른 사용자가 해당 개발 API를 사용하지 않을 때만 실행하세요.

```sh
CHATAPP_RESTART_TEST=1 CHATAPP_UI_TEST_URL=http://127.0.0.1:8020 \
  node tests/browser/restart-recovery.cjs
```
