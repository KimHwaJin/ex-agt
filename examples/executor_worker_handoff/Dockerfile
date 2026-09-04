FROM ghcr.io/astral-sh/uv:0.11.29 AS uv

FROM python:3.12-slim AS base

ENV PATH="/app/.venv/bin:${PATH}" \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy

WORKDIR /app

COPY --from=uv /uv /uvx /bin/
COPY pyproject.toml uv.lock README.md alembic.ini ./
COPY migrations ./migrations
COPY src ./src

RUN uv sync --frozen --no-dev --no-editable

CMD ["executor-event-worker"]

FROM base AS test
RUN uv sync --frozen --no-editable
COPY tests ./tests
CMD ["python", "-m", "pytest", "-q"]

FROM base AS runtime
