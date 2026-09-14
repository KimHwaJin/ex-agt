FROM ghcr.io/astral-sh/uv:0.11.29 AS uv

FROM python:3.11-slim AS base
WORKDIR /app
COPY --from=uv /uv /usr/local/bin/uv
ENV UV_COMPILE_BYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PATH="/app/.venv/bin:$PATH"
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable --no-cache
COPY app.py config.yaml config_dev.yaml alembic.ini ./
COPY migrations ./migrations

FROM base AS test
RUN uv sync --frozen --no-editable --no-cache
COPY tests ./tests
COPY examples ./examples
CMD ["python", "-m", "pytest"]

FROM base AS runtime
RUN useradd --uid 10001 --create-home appuser
USER appuser
EXPOSE 8020
# Fail closed without explicit deployment configuration.
ENV SERVICE_ENV=production
CMD ["python", "app.py"]
