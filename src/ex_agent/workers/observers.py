from ex_agent.metrics import (
    LOCK_CONTENTION,
    REDIS_DEAD_LETTERED,
    WORKER_ACTIVE,
    WORKER_OPERATION_SECONDS,
    WORKER_OPERATIONS,
    WORKER_RETRIES,
)
from ex_agent.transport.consumer import ConsumerObserver


class WorkerConsumerObserver(ConsumerObserver):
    def __init__(
        self,
        *,
        kind: str,
        lock_kind: str,
        retry_component: str,
        stream: str,
    ) -> None:
        self._kind = kind
        self._lock_kind = lock_kind
        self._retry_component = retry_component
        self._stream = stream

    def operation_started(self) -> None:
        WORKER_ACTIVE.labels(kind=self._kind).inc()

    def lock_contended(self) -> None:
        LOCK_CONTENTION.labels(kind=self._lock_kind).inc()

    def operation_finished(self, outcome: str, duration: float) -> None:
        WORKER_ACTIVE.labels(kind=self._kind).dec()
        WORKER_OPERATIONS.labels(
            kind=self._kind,
            outcome=outcome,
        ).inc()
        WORKER_OPERATION_SECONDS.labels(kind=self._kind).observe(duration)

    def transport_retry(self) -> None:
        WORKER_RETRIES.labels(component=self._retry_component).inc()

    def dead_lettered(self) -> None:
        REDIS_DEAD_LETTERED.labels(stream=self._stream).inc()


__all__ = ["WorkerConsumerObserver"]
