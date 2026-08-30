# Project Structure

이 프로젝트는 계층형 모듈러 모놀리스와 Agent workflow 전용 모듈을 결합한다.
API와 Worker는 같은 domain/application contract를 사용하지만 각각 독립적인
composition root와 프로세스로 실행된다.

## 주요 경계

- `domain`: 외부 framework에 의존하지 않는 enum과 Pydantic contract
- `application`: workflow service contract와 framework 독립 상태
- `graph`: LangGraph node, route, graph builder 및 state compatibility adapter
- `llm`: chat model과 embedding 생성
- `middleware`, `planners`, `tools`: 계획 생성과 Skill/Tool compilation
- `executor`: Executor REST와 Artifact/result contract
- `persistence`: SQLAlchemy model, transaction, repository facade
- `persistence/repositories`: outbox, Workflow catalog, model audit처럼 독립적인
  저장 기능
- `transport`: Redis publisher와 재사용 가능한 Stream consumer runtime
- `workers`: command/event processor, Stream handler, observer, checkpoint helper
- `api/routers`: health/metrics와 Task REST/SSE route

`worker.py`, `persistence/repository.py`, `api/app.py`, `models.py`의 기존 import
경로는 호환 façade로 유지한다. 새 코드는 각각 `workers`,
`persistence.repositories`, `api.routers`, `llm.factory`를 직접 사용한다.

## 허용 의존 방향

```text
api / workers / graph
        ↓
application
        ↓
domain

api / workers / application
        ↓
executor / persistence / transport / llm / tools
```

`application → graph`, `persistence → tools`, `domain → infrastructure` 의존은
허용하지 않는다. `tests/test_architecture.py`가 이 규칙과 독립 Redis consumer의
Agent domain 비의존성을 검사한다.

## 변경 원칙

- 이동 전후 공개 import와 runtime contract를 유지한다.
- transaction invariant를 공유하는 persistence 메서드는 함께 이동한다.
- LangGraph state field, node 이름과 edge는 구조 리팩터링에서 변경하지 않는다.
- source 변경 후 `uv sync --reinstall-package ex-agent --no-editable`로 설치하고
  테스트한다.
- PostgreSQL/Redis 관련 변경은 Compose 전체 테스트를 통과해야 한다.

## 후속 분리 후보

- `DefaultWorkflowServices` 구현을 conversation, planning, execution, reporting
  capability로 분리하고 현재 class는 façade로 유지
- execution binding, inbox, event sequence repository를 transaction 단위로 분리
- `WorkflowNodes`를 대화, 계획, 실행, 종료 node group으로 분리
- 테스트 수가 더 증가하면 `unit`, `integration`, `e2e` 디렉터리로 물리 분리

이 후보들은 현재 동작에 문제가 있어서가 아니라 여러 개발자의 병렬 변경 충돌과
대형 파일의 리뷰 비용을 줄이기 위한 작업이다.
