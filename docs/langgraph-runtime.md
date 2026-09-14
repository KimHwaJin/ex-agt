# 요청 분기·계획 검토 실행기와 PostgreSQL 체크포인트

## 이번 단계에서 가능한 것

일반 대화/분석 설명, 요청 분류, 작업 계획과 셀 코드 준비를 실제 LLM으로 처리합니다.
`create_agent` 정의를
별도 `agents/`에 두고, `graphs/assistant/`에서 명시적 그래프로 조합합니다.
API의 신규 메시지 계약은 이전과 같으며 `session_id = thread_id`입니다.

```text
POST /agent/runs → Run 접수·사용자 메시지 저장 → 실행기
  → 세션 실행 잠금 획득 → PostgreSQL 체크포인트 조회
  → classify(create_agent)
      → 일반/분석 질문: respond(create_agent) → complete → END
      → 분석/코드 작업: plan → prepare_code → review_plan(interrupt)
          → modify: plan → prepare_code → review_plan (새 버전)
          → reject: review_outcome → complete → END (rejected)
          → approve: review_outcome → complete → END (failed: 실행 미연결)
  → 응답·Run 완료 저장 → 세션 활성 Run 해제
```

실제 분석/코드 실행, Executor, 리포트 생성은 아직 없습니다.
일반 대화는 승인 없이 답변합니다. 작업 요청은 LLM이 만든 **셀 코드 준비 계획**을
실제 LangGraph `interrupt`로 검토받습니다. 승인해도 실행 성공으로 처리하지 않고
`EXECUTOR_NOT_CONFIGURED`를 안내하며 종료합니다. 가짜 Execution ID나 리포트를
생성하지 않습니다. 워크플로우 검색·single/multi 선택은 다음 단계입니다.
Skill·Tool 선택과 코드 원문 저장은 [계획 준비](skill-tool-planning.md)를 참고하세요.
기존 DEMO의 승인/수정/취소 시나리오는 `agent_backend: demo`로 남겨두었습니다.
실제 대화 그래프에 DEMO의 승인 응답을 적용하지 않습니다.
DEMO에서 전환하기 전 활성 테스트 작업을 완료/취소하세요. 기존 Run은 접수 당시
backend를 유지하며 다른 backend로 재개/비동기 취소를 접수하지 않습니다.
남은 작업은 해당 backend 실행기를 다시 구동해 처리합니다. 실행 제출 전의
단순 승인 대기는 다른 backend 상태에서도 즉시 취소할 수 있습니다.

## 요청 분류와 실제 HITL

분류 결과는 `general_question`, `analysis_question`, `analysis_task`,
`code_task` 중 하나이며 키워드/정규식 규칙 없이 모델이 대화 의미로 판단합니다.
단순 설명·코드 예시는 질문 경로이고 실제 수행 요청만 계획 검토로 보냅니다.
분류가 불명확하면 질문 경로에서 확인하도록 프롬프트에 명시합니다.
모델 출력은 Pydantic 구조 검증에 실패하면 종료하며 임의 분류로 대체하지 않습니다.
분류·계획의 구조화 호출은 `temperature=0`을 사용합니다. 의미 분류의 완벽한
정확성을 보장하지는 않으며, 실제 도메인 평가 사례를 추가하는 작업은 계속 필요합니다.
분류 호출이 추가되어 질문 응답은 보통 분류 1회+답변 1회, 최초 작업 계획은
분류 1회+초안 1회+생성 전 위험 검토 1회+셀 계획 1회를 사용합니다.
수정은 초안·위험 검토·셀 계획 3회이며
승인/거절 자체는 모델 호출 없이 처리합니다.

`agents/intake.py`는 `create_agent`와 `NativeJSONOutput` 미들웨어를 사용합니다.
미들웨어가 `ProviderStrategy`의 JSON Schema 요청 형식을 적용하고 Pydantic으로
응답을 검증합니다. 기본 ProviderStrategy 실행 경로는 `tools: []`를 전송하는데,
현재 모델 게이트웨이가 이를 거절하므로 도구 필드를 보내지 않는 경로를 사용합니다.
모델 서버는 JSON Schema 구조화 출력을 지원해야 합니다. 현재 vLLM 구성으로
검증하며 제공자를 바꿀 경우 이 기능도 확인해야 합니다. 모델 호출의 컨텍스트 제한은
`ContextWindow` 미들웨어로 처리하고 원본 대화 체크포인트는 삭제하지 않습니다.
실행 도구가 없으므로 승인받기 위한 가짜 Tool은 만들지 않았습니다.
`HumanInTheLoopMiddleware`의 Tool 승인이 아니라 전체 계획 검토용 `interrupt`입니다.

계획에는 제목·요약·각 단계의 내용/선택 이유/예상 산출물·구현 방식이 들어갑니다.
`catalog`는 도메인 함수 조합, `generated_code`는 직접 코드 작성입니다.
원문은 승인 화면과 분리해 저장하며 실행하지 않습니다.
수정 요청은 이전 계획과 함께 모델에 전달하고 같은 `plan_id`의 버전을
증가시킵니다. 사용자가 직접 코드 작성을 요청하면 모델이 구현 방식을 바꿉니다.

API 계약은 기존 `input.type=resume`과 `response.type=plan_review`를 유지합니다.

1. LangGraph가 계획을 체크포인트에 저장하고 `interrupt(plan)`으로 멈춥니다.
2. 실행기가 interrupt를 `management.run_interrupts`와 `run.interrupted`에
   동일 트랜잭션으로 반영합니다. 실제 graph ID는 내부 컬럼 `graph_interrupt_id`에
   보관하고 공개 UUID는 해당 Run/interrupt로부터 안정적으로 생성합니다.
3. API는 소유자·세션·활성 Run·현재 승인 ID를 검증하고 응답을 영속 저장합니다.
4. 실행기는 정확한 graph ID를 대상으로 `Command(resume={id: value})`를 보냅니다.
5. 그래프는 적용한 공개 승인 ID를 상태에 남깁니다. 장애 재시도에서 이미 적용한
   승인으로 다음 계획을 승인하지 않으며, 수정 후 이전 카드의 응답은 409입니다.

`review_plan`은 외부 쓰기/모델 호출이 없는 노드입니다. 재개 시 노드가 처음부터
실행된다는 LangGraph 규칙에 맞게 계획 생성 노드와 분리했습니다.
대기 중에는 연결/그래프 잠금을 반납하지만 서비스의 `active_run_id`는 유지합니다.
따라서 새 메시지는 막고 승인 응답/취소만 받습니다. `is_locked=execution`은 실제
Executor 연동 때 사용하는 별도 상태이며 이번 단계에서는 설정하지 않습니다.
사람의 응답을 기다리는 시간에는 모델/Run 타임아웃과 worker 슬롯을 점유하지 않습니다.
새 승인 응답마다 복구 시도 횟수를 초기화하므로 정상적인 반복 수정은 실패 횟수가 아닙니다.

계획 버전과 응답은 승인 테이블에 남고 각 검토 단계는 별도 화면 메시지로 보존합니다.
내부 분류/계획 JSON 토큰은 사용자 답변 스트림에 노출하지 않습니다.
`run.classified`에는 최종 의도와 짧은 공개 분류 근거만 제공합니다.

새 Run은 `assistant-v3`를 사용하며 이미 접수된 v1/v2 Run은 기존
노드 경로로 완료할 수 있습니다. 세션의 기존 메시지 체크포인트는 계속 활용합니다.
구버전 코드로 롤백할 때는 v3 활성 Run을 먼저 종료해야 합니다. 구버전 실행기는
v3를 처리할 수 없습니다. 체크포인트 원본을 자동 변환/삭제하지 않습니다.

## 설정

개발 기본값은 `config_dev.yaml`, 운영은 `config.yaml`입니다.
운영 템플릿은 계속 `agent_backend: disabled`로 두어 미설정 실행을 막습니다.

```yaml
agent_backend: langgraph
embedded_run_worker: true
checkpoint_database_url: null
checkpoint_database_url_env: CHECKPOINT_DATABASE_URL
checkpoint_schema: agent_checkpoints
checkpoint_pool_max_size: 5
worker_concurrency: 2
model_name: qwen38-27b-nvfp4
model_name_env: AGENT_MODEL_NAME
model_provider: openai
model_provider_env: AGENT_MODEL_PROVIDER
model_base_url: http://model.frodo.com/v1
model_base_url_env: AGENT_MODEL_BASE_URL
model_api_key: EMPTY
model_api_key_env: AGENT_MODEL_API_KEY
model_extra_body:
  chat_template_kwargs:
    enable_thinking: false
```

`checkpoint_database_url: null`이면 관리 API와 같은 `chatapp` DB를 사용합니다.
별도 URL을 지정해 다른 DB를 쓸 수도 있지만 모든 API/worker 인스턴스가 같은
체크포인트 DB·스키마를 사용해야 합니다. 서로 다른 스키마를 쓰면 기억과 실행
잠금이 분리됩니다. 운영에서는 실제 인증키를 배포 설정에 주입하고 커밋하지 마세요.
`EMPTY`는 현재 개발 vLLM 서버용 값이지 일반 서비스에서 유효한 인증키가 아닙니다.

`*_env`는 컨테이너 **안에** 해당 환경변수가 있을 때 YAML 값을 덮어씁니다.
환경변수가 없는 배포에서는 YAML만 설정해도 됩니다. 모델은 `init_chat_model`로
생성합니다. 서버별 추가 옵션은 `model_extra_body`에 두며 다른 제공자로 바꾸면
해당 제공자 패키지/옵션도 맞춰야 합니다. 현재 통합 검증 대상은 OpenAI 호환
vLLM입니다.

Compose API의 `extra_hosts`는 호스트 `/etc/hosts`에서 확인한
`model.frodo.com → 10.250.110.99`를 기본값으로 사용합니다.
환경별로 `MODEL_HOST_IP`를 바꾸세요. DNS가 제공되는 운영 배포에서는 이 로컬
Compose 매핑이 필요하지 않습니다. 호스트의 `/etc/hosts`는 수정하지 않습니다.

## 체크포인트 초기화와 수명

```sh
uv sync --locked --python 3.11
uv run --locked python -m d_test.agent_service.migrate
uv run --locked python app.py
```

`d_test.agent_service.migrate`는 관리 Alembic 적용 후 체크포인트 라이브러리의
마이그레이션을 수행합니다. 기존 관리 DB는 초기화하지 않습니다.
승인 ID 매핑의 `management_0004`와 셀 계획 저장의 `management_0005`를
포함합니다.
업데이트한 API/worker를 시작하기 전에 적용해야 합니다.
체크포인트만 초기화/업그레이드하려면 다음 명령을 사용합니다.

```sh
python -m d_test.agent_service.checkpoint_main
```

기본값에서는 `AsyncPostgresSaver.setup()`을 배포 단계에서 호출합니다.
`database_bootstrap: initialize_if_empty`를 명시하면 빈 체크포인트 스키마에
한해서 앱 시작 시에도 호출합니다. 기존 버전 업그레이드는 하지 않습니다.
[자동 초기화 정책](database-bootstrap.md)을 참고하세요. `setup()`은
라이브러리가 제공하는 버전별 테이블/인덱스 마이그레이션이며 이를 임의의
Alembic DDL로 복제하지 않았습니다. 동시 초기화는 PostgreSQL advisory lock으로
직렬화하고, 런타임은 테이블과 라이브러리 마이그레이션 버전이 준비됐는지 확인합니다.
수동 배포 방식에서는 테이블 생성/인덱스 권한은 배포 계정에,
실행 중 필요한 접근 권한은 실행 계정에
부여하세요. 별도 계정을 쓴다면 스키마 USAGE와 기존 테이블 DML 권한도 필요합니다.
자동 초기화 시에는 앱 계정에도 최초 스키마/테이블/인덱스 생성 권한이 필요합니다.

기본 테이블:

- `agent_checkpoints.checkpoint_migrations`: 라이브러리 마이그레이션 버전
- `agent_checkpoints.checkpoints`: 그래프 단계 상태와 메타데이터
- `agent_checkpoints.checkpoint_blobs`: 채널 값
- `agent_checkpoints.checkpoint_writes`: 노드 쓰기/복구 정보

관리 API의 `management.messages`는 화면용 메시지이며 체크포인트와 별개입니다.
`management.runs.checkpoint` 컬럼은 실행기 버전/시도 ID/복구 횟수 등의
**실행 관리 정보**입니다. 실제 LangGraph 상태는 위 별도 스키마에 저장합니다.

프로세스가 시작하면 관리 DB 풀과 체크포인트 풀을 열고, 종료할 때 실행 작업을
먼저 중단·정리한 다음 풀을 닫습니다. 실행 중에는 풀에서 체크포인트 연결 하나를
빌려 `AsyncPostgresSaver`와 그래프를 만들고 그 연결의 유효 범위 안에서만 씁니다.
닫힌 `with ... as checkpointer` 밖으로 그래프를 반환해 재사용하지 않습니다.
그래프가 SQL을 수행할 때만 짧은 작업을 수행하며 LLM 대기 중 열린 SQL 트랜잭션은
없습니다. 대신 활성 그래프마다 체크포인트 커넥션 1개는 점유합니다.

## 배포 방식 독립성

개발 편의상 기본 API lifespan에서 실행기를 구동합니다. 실행 코드는 라우터나
요청 coroutine에 속하지 않습니다. 별도 프로세스로 실행하려면 공통 YAML을
`embedded_run_worker: false`로 설정하고 다음 두 진입점을 사용합니다.

```sh
python app.py
python -m d_test.agent_service.worker_main
```

같은 컨테이너의 두 프로세스 또는 별도 Deployment에서도 동일 소스를 사용할 수
있습니다. 프로세스 감시/배포 매니페스트를 이 단계에서 특정 방식으로 확정하지는
않았습니다. 별도 worker는 SIGTERM/SIGINT 시 활성 그래프를 정리하고 종료합니다.
API-only 구성의 readiness는 별도 worker가 살아 있다는 것까지 증명하지 않습니다.
분리 배포의 worker 헬스체크/백로그 모니터링은 추가 운영 작업입니다.

## 동시 실행과 복구

1. 공통 계층의 `active_run_id`로 세션에 Run 하나만 허용합니다.
2. 그래프 실행기는 체크포인트 DB에서 세션 advisory lock을 획득합니다.
3. 체크포인트 쓰기도 **그 잠금을 소유한 동일 연결**로 수행합니다.
4. 메시지 투영은 시도 ID를 확인해 이전 실행기의 뒤늦은 쓰기를 거부합니다.
5. 정상 종료/취소 때 실행이 멈춘 것을 확인한 후 세션을 해제합니다.

커넥션이 유실되면 이전 saver는 새 연결로 몰래 교체되지 않습니다. 새 실행기가
잠금을 획득하더라도 이전 saver가 체크포인트를 계속 쓰는 상황을 막습니다.
네트워크 장애 감지 시간이나 LLM 서버 자체의 연산 중단을 보장하는 것은 아닙니다.
실제 외부 부작용이 있는 툴은 아직 없으며, Executor 연동 시 별도의 멱등 처리와
취소 확인이 필요합니다. LLM HTTP 스트림을 닫았다고 서버 GPU 작업까지 중단됐다고
판단하지 않습니다.

실행기 상태의 `due_at`은 스케줄링 힌트입니다. 실행 중 주기적으로 갱신해 다른
worker가 활성 작업만 반복 스캔하지 않게 하고, 중단되면 기본 약 5초 이후 다시
후보가 됩니다. 정확성을 보장하는 잠금은 시간 기반 lease가 아닌 세션 DB 잠금입니다.

복구 때 해당 Run이 그래프에 이미 들어갔다면 `None` 입력으로 저장 위치에서
이어갑니다. 신규 Run만 새 HumanMessage를 넣으며 ID도 Run에서 안정적으로 만듭니다.
단, 아직 적용되지 않은 HITL 응답이 있으면 해당 interrupt에 `Command(resume=...)`를
전달합니다. 승인 대기를 일반적인 `None` 재실행으로 해제하지 않습니다.
`completed_run_id`가 일치하면 LLM을 다시 호출하지 않고 저장된 답변을 메시지 DB에
반영합니다. 단, LLM 호출 도중 프로세스가 종료돼 완료 체크포인트가 없다면 모델을
다시 호출할 수 있습니다. 외부 호출의 exactly-once는 보장하지 않습니다.

재시도 때 미완성 답변은 같은 메시지 ID의 `message.updated` 스냅샷으로 초기화하고
새 내용을 표시합니다. 서로 다른 생성 시도의 텍스트를 이어붙이지 않습니다.
완료된 답변은 체크포인트의 최종 텍스트로 맞춥니다. 전송 도중 버퍼에만 있던 일부
텍스트는 장애 때 유실될 수 있지만, 저장된 청크와 최종 응답은 복원할 수 있습니다.

## 기본 제한과 성능

| 설정 | 기본값 | 의미 |
| --- | --- | --- |
| `worker_concurrency` | 2 | 프로세스당 동시 그래프 수 |
| `checkpoint_pool_max_size` | 5 | 활성 실행+조회용 체크포인트 연결 상한 |
| `worker_poll_seconds` | 0.25초 | 작업 탐색·취소 상태 확인 간격 |
| `worker_reschedule_seconds` | 5초 | 작업 탐색 재노출 간격 |
| `run_timeout_seconds` | 180초 | 이번 대화 Run의 시도당 처리 제한 |
| `recovery_max_attempts` | 3 | 재시작/일시적 DB 장애 복구 시도 상한 |
| `model_timeout_seconds` | 60초 | 모델 클라이언트 타임아웃 |
| `model_max_retries` | 1 | 모델 SDK 재시도 설정 |
| `model_max_tokens` | 2048 | 모델 출력 토큰 상한 |
| `code_plan_max_tokens` | 8192 | 함수 원문 포함 코드 계획 출력 상한 |
| `context_message_limit` | 40 | 모델에 전달하는 최근 메시지 수 |
| `output_flush_chars` | 256자 | 출력 청크 저장 기준 |
| `output_flush_seconds` | 0.2초 | 다음 청크 도착 시 적용하는 시간 기준 |
| `output_max_chars` | 64000자 | 공개 응답 저장 상한 |

풀 상한은 worker 동시성보다 커야 합니다. 관리 API 풀은 별도입니다.
한 토큰마다 DB에 쓰지 않고 일정 분량을 모아 저장합니다. 아직 SSE 전달은 서버의
DB 조회 방식이므로 대규모 연결, 여러 worker 및 네트워크 분할 부하 검증은 후속입니다.
장기 Executor 실행은 향후 별도 대기 단계로 구현해야 하며 위 180초 대화 제한을
며칠짜리 코드 작업에 그대로 적용하지 않습니다.

대화 기록은 체크포인트에 유지하되 현재 모델 입력에는 최근 40개만 전달합니다.
토큰 기반 요약/압축과 프로젝트 공유 Store는 후속 구현입니다. 기존 DEMO 대화나
체크포인터 없이 저장했던 과거 메시지를 자동으로 그래프에 이전하지 않습니다.

## 검증

실제 DB 테스트는 같은 DB를 처리하는 API/별도 worker를 중지한 뒤 실행합니다.

```sh
docker compose stop api
docker compose --profile test run --build --rm test
docker compose up --build -d api
```

`tests/test_graph_runtime.py`는 실제 PostgreSQL saver와 비동기 테스트 모델로
대화 유지, 새 풀에서 복원, 다른 세션 격리, 완료 후 투영 재시도, 취소, 중복 worker,
프로세스 로컬 상태 상실, 부분 응답 실패를 검증합니다.

실제 모델+UI 검증은 합성 입력으로만 실행합니다. 재시작 옵션은 로컬 `api`만
중지/시작하며 PostgreSQL은 그대로 유지합니다.

```sh
CHATAPP_UI_TEST_URL=http://127.0.0.1:8020 \
node tests/browser/langgraph-chat.cjs
# 다른 사용자가 개발 API를 사용하지 않을 때만:
CHATAPP_RESTART_TEST=1 CHATAPP_UI_TEST_URL=http://127.0.0.1:8020 \
  node tests/browser/langgraph-chat.cjs
```

실제 모델의 요청 분류·계획 수정·거절·승인 미연결 안내는 아래로 확인합니다.

```sh
CHATAPP_UI_TEST_URL=http://127.0.0.1:8020 node tests/browser/hitl-chat.cjs
```

HITL 브라우저 테스트의 재시작 옵션은 8021 포트의 별도 테스트 컨테이너
`ex-agent-hitl-test-api`에만 허용됩니다. 8020 개발 API를 재시작하지 않습니다.

설계 참고: [LangGraph 메모리/체크포인터](https://docs.langchain.com/oss/python/langgraph/add-memory),
[영속성](https://docs.langchain.com/oss/python/langgraph/persistence).
실제 동작은 `uv.lock`에 고정된 패키지와 설치 소스로 확인했습니다.
HITL 참고: [interrupt/resume](https://docs.langchain.com/oss/python/langgraph/interrupts),
[구조화 출력](https://docs.langchain.com/oss/python/langchain/structured-output).
