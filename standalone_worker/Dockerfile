FROM ghcr.io/astral-sh/uv:0.11.29 AS uv
FROM python:3.12-slim-bookworm AS runtime
COPY --from=uv /uv /bin/uv
WORKDIR /worker
ENV UV_LINK_MODE=copy PYTHONUNBUFFERED=1 \
    PATH="/worker/.venv/bin:${PATH}"
COPY pyproject.toml uv.lock ./
COPY src ./src
COPY main.py agent_app.py ./
COPY alembic.ini ./
COPY migrations ./migrations
RUN uv sync --frozen --no-dev --all-extras --no-editable
RUN useradd --uid 10001 --create-home worker
USER worker
CMD ["python", "main.py"]

FROM runtime AS test
USER root
COPY examples ./examples
COPY tests ./tests
RUN uv sync --frozen --all-extras --no-editable
USER worker
CMD ["python", "-m", "pytest", "-q", "-p", "no:cacheprovider"]
