from __future__ import annotations

import json
import threading
from collections.abc import Callable, Iterable
from socketserver import ThreadingMixIn
from typing import Any
from wsgiref.simple_server import (
    WSGIRequestHandler,
    WSGIServer,
    make_server,
)

from prometheus_client import Counter, Gauge, Histogram, make_wsgi_app

from ex_agent.readiness import ReadinessResult, ReadinessState

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
REDIS_DEAD_LETTERED = Counter(
    "ex_agent_redis_dead_lettered_total",
    "Redis Stream records moved to a dead-letter stream.",
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
COMPONENT_READY = Gauge(
    "ex_agent_component_ready",
    "Whether the component is ready to receive traffic.",
    ["component"],
)
DEPENDENCY_READY = Gauge(
    "ex_agent_dependency_ready",
    "Whether a required component dependency is reachable.",
    ["component", "dependency"],
)
DEPENDENCY_PROBE_SECONDS = Histogram(
    "ex_agent_dependency_probe_seconds",
    "Dependency readiness probe duration in seconds.",
    ["component", "dependency"],
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2),
)
DEPENDENCY_PROBE_TIMESTAMP = Gauge(
    "ex_agent_dependency_probe_timestamp_seconds",
    "Unix timestamp of the latest dependency readiness probe.",
    ["component", "dependency"],
)


class _ThreadingWSGIServer(ThreadingMixIn, WSGIServer):
    daemon_threads = True


class _SilentHandler(WSGIRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:
        return


def start_worker_metrics_server(
    host: str,
    port: int,
    readiness: ReadinessState,
    *,
    stale_after_seconds: float,
) -> WSGIServer:
    application = _worker_http_application(
        readiness,
        stale_after_seconds=stale_after_seconds,
    )
    server = make_server(
        host,
        port,
        application,
        server_class=_ThreadingWSGIServer,
        handler_class=_SilentHandler,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def _worker_http_application(
    readiness: ReadinessState,
    *,
    stale_after_seconds: float,
) -> Callable[
    [dict[str, Any], Callable[..., Any]],
    Iterable[bytes],
]:
    metrics_application = make_wsgi_app()

    def application(
        environ: dict[str, Any],
        start_response: Callable[..., Any],
    ) -> Iterable[bytes]:
        path = environ.get("PATH_INFO", "")
        if path == "/metrics":
            return metrics_application(environ, start_response)
        if path == "/healthz":
            return _json_response(
                start_response,
                "200 OK",
                {"status": "ok"},
            )
        if path == "/readyz":
            payload = readiness.payload(stale_after_seconds)
            status = "200 OK" if payload["ready"] else "503 Unavailable"
            return _json_response(start_response, status, payload)
        return _json_response(
            start_response,
            "404 Not Found",
            {"status": "not_found"},
        )

    return application


def _json_response(
    start_response: Callable[..., Any],
    status: str,
    payload: dict[str, Any],
) -> list[bytes]:
    body = json.dumps(payload, separators=(",", ":")).encode()
    start_response(
        status,
        [
            ("Content-Type", "application/json"),
            ("Content-Length", str(len(body))),
        ],
    )
    return [body]


def record_readiness(component: str, result: ReadinessResult) -> None:
    COMPONENT_READY.labels(component=component).set(int(result.ready))
    for dependency, check in result.checks.items():
        labels = {"component": component, "dependency": dependency}
        DEPENDENCY_READY.labels(**labels).set(int(check.ready))
        DEPENDENCY_PROBE_SECONDS.labels(**labels).observe(
            check.latency_seconds
        )
        DEPENDENCY_PROBE_TIMESTAMP.labels(**labels).set(
            result.checked_at_epoch_seconds
        )


def update_database_pool_metrics(name: str, engine: Any) -> None:
    pool = engine.sync_engine.pool
    for stat, value in {
        "size": pool.size(),
        "checked_in": pool.checkedin(),
        "checked_out": pool.checkedout(),
        "overflow": pool.overflow(),
    }.items():
        DATABASE_POOL.labels(pool=name, stat=stat).set(value)
