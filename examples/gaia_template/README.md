# 내부 Gaia 템플릿 연결 예제

이 디렉토리는 **내부 템플릿에 병합할 예제**다. private 라이브러리가 없어
여기서 `python app.py`만 실행할 수는 없다. 우리 서비스 로컬 실행은 기존
저장소 루트 `app.py`/Compose를 계속 사용한다.

## 옮길 파일

- 정식 구현: `src/d_test/` 전체 (`__init__.py` 포함), DB 마이그레이션.
- 이 예제의 `app.py`: 내부 루트 진입점에 반영.
- `src/workflows/analysis.py`: 실제 workflow 탐색 디렉토리에 반영.
- `src/template_bindings.py`: 인증·세션 매핑 및 외부 프로토콜 변환 구현.
- `config.dev.yml`: 기존 YAML에 `AGENT_SERVICE` 섹션만 추가.

기존 `common`, `gaia`, `lib`, `middleware`, `routers`를 덮어쓰지 않는다.
예제 app.py의 workflow import 경로는 실제 탐색 경로와 일치시킨다.
서로 다른 이름으로 모듈을 중복 import하면 서로 다른 manager가 생성된다.

## 기존 routers/__init__.py에 추가

```python
from d_test.api_service.factory import get_management_routers


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
호스트가 `HCP_ACTIVE_PROFILE=dev/stg/prd`에 맞는 YAML과 logger.yml을
이미 읽은 후 `settings_from_template(config.AGENT_SERVICE, profile=...)`
로 넘긴다. 우리 코드가 private Config의 파일 선택을 재구현하지 않는다.
`stg/prd`는 production 검증을 적용한다. 운영 인증은 실제 신뢰 경계에 맞는
`external` identity provider 또는 명시된 프록시 대역의 `trusted_header`
연결이 필요하다. 개발 헤더 인증을 그대로 배포하지 않는다.

```sh
HCP_ACTIVE_PROFILE=dev python app.py
```

DB migration은 앱 startup에서 자동 수행하지 않는다. 내부 배포 job에서
우리 기존 migration/checkpoint 초기화 절차를 별도 실행해야 한다.
기존 CLI는 우리 YAML 로더를 쓰므로 private config를 자동으로 읽지 않는다.
이 차이는 `docs/gaia-template-integration.md`의 초기화 예제를 참고한다.

## 반드시 구현할 한 곳

`template_bindings.resolve_request()`는 의도적으로 503을 반환한다.
private 인증 객체/사번·세션 매핑을 확인하지 못했기 때문이다.
임의 body UUID를 인증 결과로 간주하는 기본 구현은 제공하지 않는다.

기존 관리 REST API는 별도로 동작하며, workflow 호출은 이 resolver를
연결해야 한다. A2A taskId/contextId와 내부 run_id를 임의로 같게 두지 않는다.
HITL 출력의 실제 A2A `input-required` 변환은 아직 private 코드에서 확인되지
않았으므로, 이번 구현이 플랫폼 UI까지 자동 호환된다는 의미는 아니다.
