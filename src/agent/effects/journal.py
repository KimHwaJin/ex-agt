from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID

from agent.effects.store import (
    EffectRecord,
    EffectStore,
    digest,
    validate_identity,
)


class EffectJournal:
    def __init__(self, store: EffectStore) -> None:
        self.store = store

    async def run(
        self,
        *,
        task_id: UUID,
        key: str,
        kind: str,
        inputs: dict[str, Any],
        prepare: Callable[[], Awaitable[dict[str, Any]]],
        send: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]],
    ) -> EffectRecord:
        fingerprint = digest(inputs)
        record = await self.store.get(key)
        if record is None:
            request = await prepare()
            record = await self.store.prepare(
                key=key,
                task_id=task_id,
                kind=kind,
                input_sha256=fingerprint,
                request=request,
            )
        validate_identity(record, task_id, kind, fingerprint)
        if record.response is None:
            # HTTP result can be lost: replay the identical saved request.
            response = await send(record.request)
            record = await self.store.complete(key, response)
        return record
