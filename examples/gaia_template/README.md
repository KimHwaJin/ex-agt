# 내부 Gaia 템플릿 연결 예제

이 디렉토리는 **내부 템플릿에 병합할 예제**다. private 라이브러리가 없어
여기서 `python app.py`만 실행할 수는 없다. 우리 서비스 로컬 실행은 기존
저장소 루트 `app.py`/Compose를 계속 사용한다.

## 옮길 파일

- 정식 구현: `src/d_test/` 전체 (`__init__.py` 포함), DB 마이그레이션.
- 이 예제의 `app.py`: 내부 루트 진입점에 반영.
- `src/workflows/analysis.py`: 실제 workflow 탐색 디렉토리에 반영.
- `src/template_bindings.py`: 인증·세션 매핑 및 외부 프로토콜 변환 구현.
- `config.dev.yml`: 제공받은 기본 항목과 대문자 `AGENT_SERVICE` 추가 설정을
  합친 예제. 실제 환경의 기존 값은 유지하고 추가 섹션을 병합한다.
- `config.stg.yml`, `config.yml`: 같은 항목 구성의 스테이징/운영 예제.
  `ENVIRONMENT`는 각각 `stg`, `prd`이며 인증은 `external`을 사용한다.
  빈 `DATABASE_URL`, `CURSOR_SECRET`, `IDENTITY_PROVIDER_FACTORY`,
  `MODEL_BASE_URL`, `MODEL_API_KEY`를 해당 환경 값으로 채운다.

기존 `common`, `gaia`, `lib`, `middleware`, `routers`를 덮어쓰지 않는다.
API 구조 변경 후에는 예전 `api_service/factory.py` 등의 파일을 남긴 채
새 디렉토리를 덧씌우지 않는다. 제거 대상과 새 import 경로는
`docs/api-service-layout.md`의 내부 템플릿 이식 절차를 참고한다.
예제 app.py의 workflow import 경로는 실제 탐색 경로와 일치시킨다.
서로 다른 이름으로 모듈을 중복 import하면 서로 다른 manager가 생성된다.

## 기존 routers/__init__.py에 추가

```python
from d_test.api_service import get_management_routers


def get_routers():
    routers = []
    # 기존 chat_router/include_router 및 기타 라우터 등록은 유지
    # ...
    routers.extend(get_management_routers())
    return routers
```

설치 함수는 위 라우터가 등록됐는지 확인하고 중복 등록하지 않는다.
기존 FastAPI 객체와 lifespan을 재사용한다. private `GaiaService.main()`은
lifespan을 다시 덮어쓰므로 호출하지 않는다. 현재 알려준 main()의
OpenAPI·instrumentation·uvicorn 설정은 예제 진입점에 명시했다.
템플릿 버전이 바뀌면 main()에 추가된 초기화가 있는지 비교해야 한다.

## 실행 및 초기화

내부 requirements/pyproject에 우리 의존성을 합치고 Python 3.11 사용.
호스트가 `HCP_ACTIVE_PROFILE=local/dev/stg/prd`에 맞는 YAML과 logger.yml을
이미 읽은 후 `settings_from_template(config.AGENT_SERVICE)`로 넘긴다.
우리 코드는 `HCP_ACTIVE_PROFILE`을 읽거나 private Config의 파일 선택을
재구현하지 않는다. 각 YAML의 `AGENT_SERVICE.ENVIRONMENT`에 같은 프로필을
명시한다. `stg/prd`는 운영 검증을 적용한다. 운영 인증은 실제 신뢰 경계에 맞는
`external` identity provider 또는 명시된 프록시 대역의 `trusted_header`
연결이 필요하다. 개발 헤더 인증을 그대로 배포하지 않는다.

```sh
HCP_ACTIVE_PROFILE=dev python app.py
```

윈도우에서도 `python app.py` 또는 `uv run python app.py`로 실행한다.
예제는 `d_test.api_service.run_server()`를 사용해 윈도우에서만
`SelectorEventLoop`로 서버와 lifespan 전체를 실행한다. Psycopg 비동기
연결은 윈도우 기본 Proactor 루프와 호환되지 않기 때문이다.
Linux에서는 기존 `uvicorn.run()` 경로를 유지한다. `gaia/core.py` 변경이나
추가 환경변수는 필요 없다. 이미 이식했다면 `src/d_test/` 변경과 함께
루트 app.py의 `uvicorn.run(...)`도 `run_server(...)`로 교체해야 한다.
윈도우에서 `uvicorn app:app`으로 직접 실행하면 이 처리를 우회한다.
이 진입점은 단일 프로세스용이며 Windows reload/다중 worker는 지원하지 않는다.
독립 migration/checkpoint/worker CLI에도 호환 루프가 적용된다.
Windows worker 종료는 콘솔 Ctrl+C를 사용한다. Linux SIGTERM 처리는 유지된다.

설정 키는 `ENVIRONMENT`, `DATABASE_URL`, `MODEL_NAME`처럼 대문자로 쓴다.
로더는 이 키만 내부 Settings의 소문자 필드에 대응시킨다. 이전 소문자 키도
호환되지만 같은 항목을 대소문자 두 가지로 중복 작성하면 오류가 발생한다.
값(`dev`, `langgraph`, `preconfigured`)과 `MODEL_EXTRA_BODY` 안의 실제
모델 요청 필드(`chat_template_kwargs.enable_thinking`)는 변환하지 않는다.

기본 `PRIVATE_LLM_*`는 Gaia 설정이고, 우리 Agent는 `AGENT_SERVICE.MODEL_*`를
읽는다. 같은 모델을 쓰려면 양쪽 주소/모델/키를 맞춘다. 예제의 localhost DB와
model.frodo.com은 기존 개발 환경 값이므로 내부 환경 값으로 교체한다.
PORT는 기존 Gaia의 5000을 사용한다. `S3_FULE_URL_ENABLED`는 제공받은 철자를
유지했으며, 실제 private 코드에서 사용하는 키를 확인해 맞춘다.

세 환경 예제 모두 `AGENT_SERVICE.DATABASE_BOOTSTRAP: initialize_if_empty`를 켠다.
`chatapp` DB는 미리 생성하고, 루트에 `alembic.ini`와 `migrations/`도 복사한다.
앱 시작 시 비어 있는 관리/체크포인트 스키마를 초기화한다. 현재 버전은
건너뛰며, 구버전이나 불완전한 스키마는 자동 수정하지 않고 시작에 실패한다.
기존 DB의 버전 변경은 내부 배포 job에서 명시적으로 수행한다.
자동 초기화를 원하지 않으면 `DATABASE_BOOTSTRAP: "off"`로 설정한다.
기존 CLI는 우리 YAML 로더를 쓰므로 private config를 자동으로 읽지 않는다.
이 차이는 `docs/gaia-template-integration.md`의 초기화 예제를 참고한다.
상세 정책은 `docs/database-bootstrap.md`를 참고한다.

## 반드시 구현할 한 곳

`template_bindings.resolve_request()`는 의도적으로 503을 반환한다.
private 인증 객체/사번·세션 매핑을 확인하지 못했기 때문이다.
임의 body UUID를 인증 결과로 간주하는 기본 구현은 제공하지 않는다.

기존 관리 REST API는 별도로 동작하며, workflow 호출은 이 resolver를
연결해야 한다. A2A taskId/contextId와 내부 run_id를 임의로 같게 두지 않는다.
HITL 출력의 실제 A2A `input-required` 변환은 아직 private 코드에서 확인되지
않았으므로, 이번 구현이 플랫폼 UI까지 자동 호환된다는 의미는 아니다.
