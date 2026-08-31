FROM ghcr.io/astral-sh/uv:0.11.29 AS uv

FROM python:3.12-slim-bookworm AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:${PATH}"

WORKDIR /app

RUN apt-get update \
    && apt-get install --yes --no-install-recommends tini \
    && rm -rf /var/lib/apt/lists/*

COPY --from=uv /uv /uvx /bin/
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src ./src
COPY migrations ./migrations
COPY worker_migrations ./worker_migrations
COPY alembic.ini ./
COPY skills ./skills
RUN uv sync --frozen --no-dev --no-editable

RUN useradd --uid 10001 --create-home agent \
    && mkdir -p /workspace/shared/requests \
    && chown -R agent:agent /app /workspace/shared

USER agent

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["ex-agent-api"]

FROM runtime AS test

USER root
COPY tests ./tests
COPY examples ./examples
COPY docker-compose.yml ./
COPY langgraph.json ./
COPY docs/worker-centered-refactor.md ./docs/worker-centered-refactor.md
COPY deploy/worker ./deploy/worker
RUN uv sync --frozen --no-editable
USER agent
