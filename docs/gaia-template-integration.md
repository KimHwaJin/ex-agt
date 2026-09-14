# Gaia 템플릿 연결 계약 v1

## 범위와 현재 상태

이번 연결부는 사용자에게 설명받은 동적 workflow 탐색 규칙과
`ainvoke(payload, config=...)`,
`astream(payload, config=..., stream_mode=["custom", "updates"])`
호출 형태를 지원한다. private 패키지의 실제 소스/실행 환경은 없으므로
**내부 A2A·SuperAgent UI까지 검증된 구현은 아니다.**

우리 관리 REST API와 template workflow는 같은 RunService를 사용한다.
외부 wrapper가 LangGraph를 직접 호출해서 사용자 검증·세션 잠금·멱등성·
공개 메시지 저장을 우회하지 않는다. 모델 선택은 REST와 workflow 양쪽에서
동일한 계약을 적용한다.

## 이식 구조

```text
app.py                              # GaiaService 생성 후 우리 runtime 부착
src/routers/__init__.py              # 기존 get_routers()에 라우터 추가
src/workflows/analysis.py            # manager = WorkflowManager()
src/template_bindings.py             # private 인증/외부 요청 변환
src/d_test/api_service/template.py          # 같은 FastAPI에 lifecycle 연결
src/d_test/agent_service/bootstrap/template.py
src/d_test/agent_service/integrations/
  template_protocol.py              # 변경 가능한 외부 계약 v1
  template_workflow.py               # Run 접수/조회와 formatter 호환
```

복사 예제는 `examples/gaia_template/`에 있다. 정식 패키지는 그 안에 중복
복사하지 않는다. 실제 탐색 경로가 workflows가 아니면 예제 export 파일 위치와
app.py의 import만 실제 경로로 바꾼다. 발견되는 manager와 lifespan에 bind하는
manager는 **동일한 객체**여야 한다.

우리 구현은 `src/d_test/` 전체를 한 단위로 전달한다. 템플릿의 common,
gaia, lib, middleware를 이 아래로 옮기는 것이 아니다. 우리 Python 경로만
`d_test.api_service.*` / `d_test.agent_service.*`로 바뀌며 외부 API URL과
DB 식별자는 그대로다. 별도 worker/migration의 `python -m` 명령도
`d_test.agent_service.*`로 변경해야 한다.

## 앱 lifespan과 설정

GaiaService 생성자가 만든 FastAPI 객체를 그대로 사용한다.
`get_routers()`에는 `get_management_routers()` 반환값을 추가한다.
`install_template_runtime()`은 이미 등록된 라우터를 다시 등록하지 않는다.
기존 lifespan 진입 → 우리 pool/agent 시작 → workflow bind → 요청 처리 →
workflow unbind → 우리 자원 종료 → 기존 lifespan 종료 순서다.
시작에 실패해도 생성한 풀을 닫고, 종료한 runtime은 app.state에서 제거한다.
모듈 import 때 DB를 열거나, 이미 종료된 saver를 전역 graph에 넣지 않는다.

**GaiaService.main()은 호출하지 않는다.** 제공받은 main()이 lifespan을
덮어쓰기 때문이다. 루트 app.py에서 기존 OpenAPI·계측·uvicorn 설정을 수행한
뒤 우리 lifespan이 적용된 app을 실행한다. private gaia/core.py 수정은 없다.
호스트의 기존 lifespan이 향후 별도로 추가된다면 그대로 합성된다.
템플릿 main() 변경 시 예제 진입점도 비교해야 한다.
기존 health API는 덮어쓰지 않는다. 운영 readiness에서 DB/worker 상태까지
확인하려면 호스트 health 연결부에서
`await app.state.management_runtime.resources.ready()`를 호출해야 한다.

`common.config.config`가 읽어 둔 `AGENT_SERVICE` dict만 Settings로 변환한다.
private Config의 `__getattr__` 반환 None/평탄화 방식에 의존하지 않는다.
섹션이 dict가 아니면 명확히 실패한다. private helper가 섹션을 평탄화한다면
app.py에서 명시적 dict로 만들어 전달한다.

템플릿 YAML은 대문자 설정 키(`ENVIRONMENT`, `DATABASE_URL`, `MODEL_NAME` 등)를
사용한다. adapter가 `AGENT_SERVICE` 바로 아래의 키만 소문자 Settings 필드로
변환한다. 과거 소문자 설정도 읽지만 같은 항목의 대소문자 중복은 허용하지
않는다. 설정값과 `MODEL_EXTRA_BODY` 내부의 모델 요청 키는 그대로 전달한다.
기존 Gaia `PRIVATE_LLM_*`를 우리 `MODEL_*`로 자동 복사하지는 않는다.

| HCP_ACTIVE_PROFILE | 호스트가 읽을 파일 | AGENT_SERVICE.ENVIRONMENT |
|---|---|---|
| local | config.local.yml | local |
| dev | config.dev.yml | dev |
| stg | config.stg.yml | stg |
| prd | config.yml | prd |

호스트가 파일 선택을 담당하며, 우리 adapter는 `HCP_ACTIVE_PROFILE`을 받거나
다시 파일을 읽지 않는다. 이미 선택된 `config.AGENT_SERVICE`만 검증한다.
`local/dev`는 개발 기능, `stg/prd`는 운영 수준의 인증·비밀값 검증을 적용한다.
독립 실행의 `SERVICE_ENV`도 같은 네 값을 사용한다.

`LOGGING_MODE: preconfigured`는 호스트가 이미 logger.yml을 적용했다는
명시적 계약이다. logging 초기화를 아무것도 하지 않으므로 **진입점에서
init_logger가 호출되어 있어야 한다.** 내부 라이브러리의 설정 완료 여부를
자동으로 탐지할 수는 없다. 업무 코드는 기존 logging.getLogger를 사용한다.

## 인증/ID 매핑: 이식 시 반드시 채울 부분

`template_bindings.resolve_request(payload, config)`는 아래 결과를 반환한다.

```python
AuthorizedRequest(
    owner=verified_internal_user_uuid,
    request_id="stable-client-request-id",
    request=validated_run_request,
)
```

`owner`는 인증된 컨텍스트에서 얻어야 한다. body.user_id 또는 임의 metadata를
그대로 복사하면 인증이 아니다. 사번은 내부 user UUID로, 외부 세션 ID는 소유권이
확인된 내부 session UUID로 변환한다. project_id는 우리 세션 정보에서 얻는다.
외부 세션 자동 생성/기존 세션 연결 정책은 실제 플랫폼 매핑 구현에서 정한다.

우리 RunService는 owner=request.user_id, 활성 사용자, 세션/Run 소유권,
현재 interrupt_id, 재사용된 요청 키의 내용 일치를 다시 검증한다.
LangGraph thread_id는 **검증된 내부 session_id**로 실행기가 설정한다.
템플릿 config.metadata.thread_id를 configurable.thread_id로 무조건 복사하거나
외부 config의 checkpoint_id로 다른 체크포인트를 지정하지 않는다.
현재 template config의 recursion_limit·callback도 그래프에 그대로 넘기지 않는다.

예제 resolver는 private 인증 구현이 없으므로 503으로 닫혀 있다.
이 부분만 실제 내부 인증/매핑으로 연결하면 v1 manager를 호출할 수 있다.

## 임시 요청/재개 계약

공용 템플릿 payload 전체를 우리 스키마로 검증하지 않고, 충돌을 피하도록
`agent_request` 아래에 우리 계약을 넣는다. 기존 template의 message/parts만
전달하는 경우 resolver가 명시적으로 변환해야 한다.

```json
{
  "agent_request": {
    "protocol_version": 1,
    "request_id": "request-001",
    "request": {
      "user_id": "내부-user-UUID",
      "session_id": "내부-session-UUID",
      "main_model_name": "qwen38-27b-nvfp4",
      "input": {
        "type": "message",
        "content": [{"type": "text", "text": "분석 계획 만들어줘"}]
      }
    }
  }
}
```

새 HITL 응답은 새 request_id로, 전송 재시도는 기존 request_id로 보낸다.
매번 생성되는 trace_id를 request_id로 대신 쓰지 않는다.
복잡한 JSON도 input.response의 명시적 스키마로 다룬다. 현재 구현 범위는
plan_review 승인/거절/수정이며 나머지 5개 UI 패턴을 구현했다고 보지 않는다.

```json
{
  "agent_request": {
    "protocol_version": 1,
    "request_id": "request-002",
    "request": {
      "user_id": "동일-user-UUID",
      "session_id": "동일-session-UUID",
      "main_model_name": "서버-허용목록의-다른-모델",
      "input": {
        "type": "resume",
        "run_id": "응답의-run_id",
        "interrupt_id": "응답의-interrupt_id",
        "response": {
          "type": "plan_review",
          "schema_version": 1,
          "decision": "modify",
          "instruction": "내부 함수 대신 직접 코드를 작성해줘"
        }
      }
    }
  }
}
```

`decode_v1(payload, owner=verified_uuid)`는 이 envelope를 검증한다.
단순 텍스트 '승인'을 임의로 Command(resume=...)로 해석하지 않는다.
내부 실제 Command 및 graph interrupt ID 매핑은 기존 GraphDriver 책임이다.

## 출력과 스트림

manager.ainvoke는 완료/입력 대기까지 관찰한 public state를 반환한다.
관찰 상한(stream_max_seconds)을 넘으면 현재 running/queued 상태로 반환한다.
접수된 Run은 계속 살아 있으며 HTTP 취소/관찰 종료가 작업 취소는 아니다.
별도 worker 배포 시 API와 worker가 같은 DB/체크포인트 설정을 사용해야 한다.
worker가 없으면 queued가 반환되며 완료를 가장하지 않는다.

astream은 (`custom`, dict)만 방출한다. 상태 전환과 마지막 public state를
보내며 이 1차 template adapter에서는 LLM 토큰을 실시간 전달하지 않는다.
우리 REST `/agent/runs/{id}/stream`은 기존 메시지 스트리밍을 계속 지원한다.
DB 연결은 짧은 조회 동안만 사용하고 관찰 대기 중에는 반환한다.

최종 state 필드는 answer, status, run_id, session_id, last_event_id, error,
input_requests, inputRequest, metadata.agent다. 체크포인트/원본 셀 코드는
노출하지 않는다. inputRequest에는 protocol_version, pattern=review,
stage, question, run_id, interrupt_id, response_type, schema_version, payload를
넣는다. pending이 해소되면 명시적으로 null/빈 목록으로 내려 이전 카드를 지운다.

제공받은 formatter에서는 custom 문자열이 나오면 마지막 answer fallback을
생략한다. 그래서 진행 상태를 문자열로 보내지 않는다. 마지막 answer가
fallback으로 출력되는 경로를 사용한다. STREAM_DONE은 관찰 종료일 뿐,
분석 성공/코드 실행 완료라는 뜻이 아니다.

**확인되지 않은 부분:** private normalize_return_state/output_metadata가
추가 state 필드를 유지하는지, final_state_events가 inputRequest를 별도 이벤트로
전달하는지. metadata mirror만으로 SuperAgent 카드가 뜬다고 보장하지 않는다.

## A2A 경계: 후속 연결

알려준 가이드의 canonical 경로는
`status.message.parts[].data.inputRequest`이고 상태는 `input-required`다.
우리 결과를 이 경로로 변환하는 실제 private AgentExecutor 연결이 필요하다.
외부 taskId/contextId/owner와 내부 run_id/interrupt_id의 매핑은 영속 저장하고,
resume 때 동일 owner와 pending 상태를 확인해야 한다. 이번 v1은 A2A Task
생성/저장/상태 발행을 대신 구현하지 않으며 외부 ID를 내부 ID로 가정하지 않는다.
형식이 확정되면 protocol/resolver/송신 adapter를 수정하고 업무 그래프는 유지한다.

## 요청별 모델 선택

- `GET /api/v1/agent/models`: 인증 필요. 서버 설정 모델 목록 반환.
- `POST /api/v1/agent/runs`: `main_model_name` 선택 필드 추가.
- 새 요청에서 생략하면 서버 model_name, resume에서 생략하면 직전 선택 유지.
- 명시하면 resume에도 새 모델 적용. 진행 중인 LLM 요청은 중간 교체하지 않는다.
- 분류·대화·계획·위험 검토·코드 계획 모두 선택한 모델 bundle을 사용한다.
- model.selected 이벤트에 모델/제공자/선택자/재개 여부와 발생 시각을 저장한다.
- endpoint/provider/API key는 서버 설정만 사용한다. body override는 거절한다.
- ALLOWED_MODEL_NAMES에 기본 MODEL_NAME은 자동 포함되고 중복은 제거된다.
- 프로세스 시작 때 허용 모델들의 agent/client를 준비하고 종료 때 모두 닫는다.
  구성 상한은 기본 모델 포함 최대 32개다. HTTP 연결이 요청별로 계속 생성되지 않는다.
- 허용 목록에서 제거된 모델의 미완료 Run은 해당 모델 구성을 복원해야 처리된다.
  pending resume에서 사용자가 허용된 다른 모델을 선택하는 것은 가능하다.

현재 목록은 정적 설정이며 내부 모델 카탈로그 API의 실시간 조회 결과가 아니다.
API 응답에도 source=configured로 구분한다. 실제 모델 서버에 없는 이름을
허용 목록에 넣으면 접수 후 모델 호출은 실패할 수 있다.
**고려사항만:** 최초 모델 고정 정책은 미적용. 필요 시 향후 제품 정책으로 검토.

## migration / 별도 worker

빈 DB를 앱 시작 시 준비하려면 내부 YAML에 다음 값을 추가한다.
설치 함수가 사용하는 lifespan에서 처리하므로 GaiaService.core 수정은 없다.

```yaml
AGENT_SERVICE:
  DATABASE_BOOTSTRAP: initialize_if_empty
  DATABASE_BOOTSTRAP_TIMEOUT_SECONDS: 60
  DATABASE_MIGRATION_CONFIG: alembic.ini
```

`chatapp` 데이터베이스는 미리 생성한다. `src/d_test`뿐 아니라 최신
`alembic.ini`와 `migrations/` 전체도 루트에 배치한다. 특히 `migrations/env.py`는
앱이 잠근 연결을 전달받는 버전이어야 한다. 예전 env.py를 그대로 쓰지 않는다.
현재 버전은 검사 후 건너뛰며 구버전/부분 생성 상태는 시작을 중단한다.
기본값 `off`는 기존 명시적 배포 방식을 유지한다.
동시 시작·권한·실패 대응은 [DB 초기화 안내](database-bootstrap.md)를 참고한다.

기존 버전 업그레이드 또는 자동 초기화를 끈 경우에는 아래처럼
템플릿 config를 초기화하고 Settings를 만든 후 별도 배포 job에서 호출한다.
루트에 alembic.ini/migrations를 함께 배치하고 아래는 동기 진입점에서 실행한다.

```python
from d_test.agent_service.migrate import main as migrate

migrate(settings)  # management Alembic + official checkpoint setup
```

별도 worker 프로세스도 private logger 초기화 후 동일 Settings를 주입한다.

```python
import asyncio
from d_test.agent_service.worker_main import main as run_worker

asyncio.run(run_worker(settings))
```

이 경우 API에는 embedded_run_worker=false를 설정한다. 같은 컨테이너 2프로세스
또는 별도 Deployment 선택과 업무/그래프 코드는 무관하다. 독립 실행 옵션을
주지 않은 기존 CLI는 종전처럼 로컬 YAML 설정을 읽는다.

## 검증 기준

- Python 3.11, uv.lock의 실제 패키지, ruff(79자), ty.
- fake host의 lifespan 합성·초기화 실패·중복 설치·로그 미재설정.
- 실제 PostgreSQL의 Run 접수→HITL→재개, 멱등 재전송과 다른 owner 거절.
- 관찰 타임아웃 후 queued Run 보존.
- 실제 LangGraph/Postgres saver와 테스트 모델로 resume 시 모델 변경 검증.
- private Gaia/A2A 실제 통신 및 모델 카탈로그 API는 미검증.
