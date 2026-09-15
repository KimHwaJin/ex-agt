# API 서비스 패키지 구조

API는 HTTP 입력 검증·인증·응답을 담당하고, 업무 처리는 기존 application
서비스에 위임한다. 이번 변경은 파일/책임 분리이며 URL, 요청/응답 계약,
DB 스키마, 세션 잠금, Run 실행 정책은 변경하지 않는다.

## 디렉토리별 책임

```text
src/d_test/api_service/
  __init__.py                  # 외부에서 사용하는 공개 설치 함수
  bootstrap/
    application.py             # FastAPI 조립, 라우터 충돌 검사, lifespan
    runtime.py                 # lifespan이 소유하는 자원과 인증 제공자
    server.py                  # Windows/Linux 서버 진입점
  routers/
    __init__.py                # get_management_routers() 등록 목록
    users.py                   # POST/GET /me
    projects.py                # 프로젝트 생성/조회/수정/삭제
    sessions.py                # 세션 생성/조회/수정/삭제
    messages.py                # 세션 메시지 조회
    runs.py                    # Run 생성/조회/취소/스트림, 세션 Run 목록
    models.py                  # 선택 가능한 모델 목록
    health.py                  # 독립 앱 전용 liveness/readiness
    dev.py                     # 개발 UI 페이지와 정적 파일 응답
  schemas/
    common.py                  # 쓰기 요청 기본 모델, 페이지 응답
    users.py                   # 사용자 응답
    projects.py                # 프로젝트 요청/응답
    sessions.py                # 세션 요청/응답
    runs.py                    # Run/메시지 요청/응답
  dependencies/
    identity.py                # 요청의 인증 사용자와 사번 확인
    services.py                # lifespan의 업무 서비스 참조
    parameters.py              # 커서/limit/멱등성 키/Last-Event-ID
  auth/
    providers.py               # 인증 인터페이스, 헤더 인증, 제공자 생성
  middleware/
    body_limits.py             # Run 요청 본문 크기/깊이 제한
    request_context.py         # 요청 ID, no-store, 안전한 요청 로그
  handlers/
    exceptions.py              # 도메인/요청 검증/DB 예외를 HTTP로 변환
  streaming/
    runs.py                    # SSE 인코딩·heartbeat·재접속·종료 처리
  integrations/
    gaia.py                    # 선택적인 Gaia workflow 연결
  static/dev/                  # 개발 UI HTML/JS/CSS
```

`routers`는 요청을 받고 서비스 메서드를 호출한다. 직접 DB 쿼리나 그래프
실행을 구현하지 않는다. `streaming`은 저장된 Run 이벤트를 관찰할 뿐,
스트림 연결이 끊겼다고 Run을 취소하거나 다시 실행하지 않는다.

## 서비스와 모델은 어디에 있는가

| 종류 | 실제 위치 | 역할 |
| --- | --- | --- |
| 관리 업무 서비스 | `agent_service/application/management.py` | 사용자·프로젝트·세션, 소유권·버전 검증 |
| Run 업무 서비스 | `agent_service/application/runs.py` | Run 접수·조회·취소·메시지 조회 |
| 공통 도메인 모델 | `agent_service/domain/management.py`, `runs.py` | 엔티티·Run 계약·도메인 오류 |
| DB 접근 | `agent_service/infrastructure/database/` | PostgreSQL 저장소·풀·마이그레이션 연결 |
| HTTP 전용 입력 모델 | `api_service/schemas/` | 생성/수정 요청의 필드·제약 검증 |

기존 도메인 응답 모델과 Run 계약은 `schemas`에서 그대로 재노출한다.
동일한 필드를 가진 Pydantic 클래스를 다시 만들거나 서비스 호출만 전달하는
빈 `api_service/services` 계층은 추가하지 않았다. 따라서 현재 모델의 원본을
수정할 때는 도메인 파일을 봐야 한다. 업무 서비스의 소속 패키지 변경은 이번
범위가 아니며, 나중에 분리하더라도 API 계약 변경과는 별도로 진행할 수 있다.

## 새 API 추가 순서

1. HTTP 전용 요청 모델은 해당 `schemas/<자원>.py`에 정의한다.
2. 업무 규칙과 저장 처리는 application 서비스/저장소에 구현한다.
3. `routers/<자원>.py`에서 인증 의존성과 입력 모델을 받아 서비스를 호출한다.
4. 새로운 라우터 파일이면 `routers/__init__.py` 등록 목록에 추가한다.
5. 계약 테스트와 실제 DB 통합 테스트를 추가한다.

새 API마다 `application.py`나 Gaia 진입점을 수정할 필요는 없다.
인증 정책 변경은 `auth`, 공통 오류 응답 변경은 `handlers`에서 처리한다.

## 내부 템플릿 이식

외부 코드는 내부 파일 경로 대신 공개 진입점을 사용한다.

```python
from d_test.api_service import (
    create_app,
    get_management_routers,
    install_management_api,
    install_template_runtime,
    run_server,
)
```

실제로 사용하는 함수만 import하면 된다. Gaia의 `get_routers()`에는
`get_management_routers()` 결과를 기존처럼 추가한다.

API만 붙일 때는 다음과 같다.

```python
install_management_api(
    app,
    settings,
    include_routers=False,  # 호스트 get_routers()에서 이미 등록했을 때
    enable_dev_ui=False,
)
```

Gaia 기존 채팅 API까지 연결하려면 대신 `install_template_runtime()`을 쓴다.
이때만 workflow `manager`와 요청 변환 `resolve`가 필요하다. 두 설치 함수를
같은 앱에 중복 호출하지 않는다. 기존 Gaia `core.py`는 수정하지 않는다.

기존 `factory.py`, `server.py`, `template.py`, `security.py` 경로는 제거했다.
각각의 설치 함수는 위 공개 진입점으로, 인증 타입은
`d_test.api_service.auth.providers`에서 import한다. 인증 제공자를 YAML의
문자열 import 경로로 등록했다면 그 경로도 확인한다.

내부에 복사할 때 예전 파일을 그대로 둔 채 새 디렉토리를 덧씌우지 않는다.
특히 `routers.py`/`routers/`, `schemas.py`/`schemas/`,
`dependencies.py`/`dependencies/`를 동시에 두면 코드 탐색이 혼란스러워진다.
내부 수정사항을 보존하면서 아래 예전 파일을 정리하고 새 패키지를 반영한다.

```text
api_service/factory.py
api_service/server.py
api_service/template.py
api_service/security.py
api_service/routers.py
api_service/run_routes.py
api_service/schemas.py
api_service/dependencies.py
api_service/body_limits.py
api_service/dev_ui.py
```

DB 변경은 없으므로 이 리팩토링만을 위한 별도 데이터 마이그레이션은 없다.
