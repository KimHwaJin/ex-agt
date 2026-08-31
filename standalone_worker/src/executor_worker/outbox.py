from __future__ import annotations

from redis.asyncio import Redis

from executor_worker.store import Store


class Outbox:
    def __init__(
        self,
        store: Store,
        redis: Redis,
        stream: str,
        *,
        batch_size: int = 100,
        lease_seconds: int = 30,
    ) -> None:
        self.store = store
        self.redis = redis
        self.stream = stream
        self.batch_size = batch_size
        self.lease_seconds = lease_seconds

    async def once(self) -> int:
        token, rows = await self.store.claim_outbox(
            self.batch_size,
            self.lease_seconds,
        )
        if not rows:
            return 0
        pipeline = self.redis.pipeline(transaction=False)
        for row in rows:
            pipeline.xadd(
                self.stream,
                {
                    "schema_version": "1",
                    "namespace": self.store.namespace,
                    "command_id": str(row["command_id"]),
                    "generation": str(row["generation"]),
                },
            )
        # On uncertain publish outcome leave the claim to expire. A repeated
        # XADD carries the same command ID, never a fresh business operation.
        results = await pipeline.execute(raise_on_error=False)
        sent, failed = [], []
        for row, result in zip(rows, results, strict=True):
            target = failed if isinstance(result, BaseException) else sent
            target.append(row["command_id"])
        await self.store.finish_publications(token, sent, sent=True)
        await self.store.finish_publications(token, failed, sent=False)
        return len(sent)
