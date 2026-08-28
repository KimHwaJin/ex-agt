# ex-agent

LangGraph 상태 머신과 LangChain `create_agent()` middleware를 사용하는
데이터 분석/코드 실행 Agent BFF다. 승인된 코드는 Executor REST API로 제출하고
Executor Redis event에서 workflow를 재개한다.

## 개발 명령

```bash
uv sync --no-editable
uv run --no-sync ruff check .
uv run --no-sync ruff format --check .
uv run --no-sync ty check
uv run --no-sync pytest
```

실제 PostgreSQL/pgvector와 Redis 통합 테스트:

```bash
docker compose --profile test build
docker compose --profile test run --rm test
```

API와 worker 실행:

```bash
cp .env.example .env
docker compose up --build
```

Executor는 별도로 실행하고 `EXECUTOR_BASE_URL`을 설정한다. Agent와 Executor가
PATH source를 교환하려면 `EXECUTOR_SHARED_DIR`이 Executor의 `shared_dir`을
가리켜야 한다. `EXECUTOR_SOURCE_MODE=PATH`만 허용되며, 실행 코드와 성공
리포트는 공유 입력 파일의 상대경로와 SHA-256으로만 제출된다.

API와 worker는 반드시 같은 `AGENT_REDIS_URL`을 사용해야 한다.
Executor의 `executor.events`를 같은 Redis에서 소비하는 배치라면
`.env`에 `redis://host.docker.internal:6379/0` 같은 실제 공유 Redis URL을
저장한다. 일회성 shell 변수로만 주입하면 후속 `docker compose up`
시 worker가 기본 로컬 Redis로 복귀할 수 있다.

## API 흐름

- `POST /api/v1/projects/{project_id}/sessions/{session_id}/tasks`
- `GET /api/v1/tasks/{task_id}`
- `POST /api/v1/tasks/{task_id}/resume`
- `POST /api/v1/tasks/{task_id}/cancel`
- `GET /api/v1/tasks/{task_id}/events` (`Last-Event-ID` 기반 SSE)

BFF는 모든 요청에 신뢰된 `X-User-ID`를 전달한다. Task 생성 시 BFF가
채번한 `task_id`와 `input_message_id`를 body에 넣는다. API는 작업을
PostgreSQL에 먼저 저장하고 Redis Stream에 발행한 뒤 `202`를 반환한다.
Graph는 worker에서만 invoke/resume한다.

기본 LLM은 내부 vLLM OpenAI 호환 endpoint
`http://model.frodo.com/v1`의 `qwen38-27b-fp8`이다. Compose는
`model.frodo.com:10.250.110.99`를 각 Agent 컨테이너의 `/etc/hosts`에
추가한다. IP가 바뀌면 `MODEL_HOST_IP`, 모델이 바뀌면 `AGENT_MODEL`로
덮어쓴다. 서버의 `/v1/models`에는 `qwen38-28b-fp8`이 없고
`qwen38-27b-fp8`이 등록되어 있어 실제 등록 ID를 기본값으로 사용한다.

현재 Workflow 검색은 외부 모델 없이 `dummy-hash-v1` 결정적 임베딩을 사용한다.
동일한 1024차원 구현을 인덱싱과 질의에 함께 사용해 pgvector 흐름을 개발할 수
있지만, 의미 검색 품질을 보장하지는 않는다. 실제 임베딩 모델이 확보되면
`AGENT_EMBEDDING_PROVIDER=openai`와 모델 endpoint를 설정해 교체한다.
Embedding 생성 자체가 실패하거나 차원이 다르면 `workflow.search_degraded` Task
event를 남기고 동적 MULTI 계획으로 전환한다.

승인 시점부터 성공 리포트 완료, Executor 실패 확인 또는 취소 완료까지
Session lock을 유지한다. 성공 리포트는 Executor REPORT Artifact API로
Notebook의 Markdown cell과 함께 생성한다. 리포트 본문도 INLINE API
payload가 아니라 공유 입력 루트의 Markdown 파일로 전달한다.

상세 설계는 [LangGraph Workflow Design](docs/langgraph-design.md)을 참고한다.
