from __future__ import annotations

from http.server import HTTPServer
from typing import Any

from prometheus_client import Counter, Gauge, Histogram, start_http_server

WORKER_ACTIVE = Gauge(
    "ex_agent_worker_active",
    "Currently active worker operations.",
    ["kind"],
)
WORKER_CONFIGURED_SLOTS = Gauge(
    "ex_agent_worker_configured_slots",
    "Configured worker concurrency slots.",
    ["kind"],
)
WORKER_OPERATIONS = Counter(
    "ex_agent_worker_operations_total",
    "Completed worker operations by outcome.",
    ["kind", "outcome"],
)
WORKER_OPERATION_SECONDS = Histogram(
    "ex_agent_worker_operation_seconds",
    "Worker operation duration in seconds.",
    ["kind"],
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 15, 60, 300),
)
WORKER_RETRIES = Counter(
    "ex_agent_worker_retries_total",
    "Worker transport iteration retries.",
    ["component"],
)
LOCK_CONTENTION = Counter(
    "ex_agent_lock_contention_total",
    "Distributed lock acquisition conflicts.",
    ["kind"],
)
OUTBOX_PUBLISHED = Counter(
    "ex_agent_outbox_published_total",
    "Durable outbox records published to Redis.",
)
OUTBOX_RELAY_SECONDS = Histogram(
    "ex_agent_outbox_relay_seconds",
    "Outbox relay iteration duration in seconds.",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 5),
)
DELIVERY_BACKLOG = Gauge(
    "ex_agent_delivery_backlog",
    "Durable delivery records by kind and state.",
    ["kind", "state"],
)
REDIS_STREAM_PENDING = Gauge(
    "ex_agent_redis_stream_pending",
    "Pending Redis Stream records by logical stream.",
    ["stream"],
)
REDIS_STREAM_LAG = Gauge(
    "ex_agent_redis_stream_lag",
    "Redis Stream consumer group lag by logical stream.",
    ["stream"],
)
CHECKPOINT_POOL = Gauge(
    "ex_agent_checkpoint_pool",
    "LangGraph checkpoint pool values.",
    ["stat"],
)
DATABASE_POOL = Gauge(
    "ex_agent_database_pool",
    "SQLAlchemy database pool values.",
    ["pool", "stat"],
)
SSE_CONNECTIONS = Gauge(
    "ex_agent_sse_connections",
    "Active SSE connections in this API process.",
)


def start_worker_metrics_server(host: str, port: int) -> HTTPServer:
    server, _ = start_http_server(port, addr=host)
    return server


def update_database_pool_metrics(name: str, engine: Any) -> None:
    pool = engine.sync_engine.pool
    for stat, value in {
        "size": pool.size(),
        "checked_in": pool.checkedin(),
        "checked_out": pool.checkedout(),
        "overflow": pool.overflow(),
    }.items():
        DATABASE_POOL.labels(pool=name, stat=stat).set(value)
