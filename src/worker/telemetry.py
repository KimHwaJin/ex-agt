from __future__ import annotations

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram


class Telemetry:
    def __init__(self) -> None:
        self.registry = CollectorRegistry()
        self.operations = Counter(
            "ew_operations",
            "Consumer results",
            ["kind", "outcome"],
            registry=self.registry,
        )
        self.active = Gauge(
            "ew_active",
            "Active handlers",
            ["kind"],
            registry=self.registry,
        )
        self.duration = Histogram(
            "ew_duration_seconds",
            "Consumer duration",
            ["kind"],
            registry=self.registry,
        )
        self.backlog = Gauge(
            "ew_backlog",
            "Database backlog",
            ["state"],
            registry=self.registry,
        )
        self.stream = Gauge(
            "ew_stream",
            "Stream progress; lag=-1 means unknown; has_unread is 0 or 1",
            ["kind", "metric"],
            registry=self.registry,
        )

    def observer(self, kind: str) -> Observer:
        return Observer(self, kind)


class Observer:
    def __init__(self, telemetry: Telemetry, kind: str) -> None:
        self.telemetry, self.kind = telemetry, kind

    def operation_started(self) -> None:
        self.telemetry.active.labels(self.kind).inc()

    def lock_contended(self) -> None:
        self.telemetry.operations.labels(self.kind, "lock_contended").inc()

    def operation_finished(self, outcome: str, duration: float) -> None:
        self.telemetry.active.labels(self.kind).dec()
        self.telemetry.operations.labels(self.kind, outcome).inc()
        self.telemetry.duration.labels(self.kind).observe(duration)

    def transport_retry(self) -> None:
        self.telemetry.operations.labels(self.kind, "transport_retry").inc()

    def dead_lettered(self) -> None:
        self.telemetry.operations.labels(self.kind, "dead_lettered").inc()
