from __future__ import annotations

import json
from typing import Any
from uuid import UUID

from redis.asyncio import Redis

from ex_agent.config import Settings
from ex_agent.persistence.repository import AgentRepository


class CommandPublisher:
    def __init__(
        self,
        settings: Settings,
        repository: AgentRepository,
        redis: Redis,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._redis = redis

    async def publish_pending(self) -> int:
        published = 0
        for command in await self._repository.pending_commands():
            try:
                await self._redis.xadd(
                    self._settings.agent_command_stream,
                    {
                        "command_id": str(command.id),
                        "task_id": str(command.task_id),
                        "command_type": command.command_type,
                        "payload": json.dumps(command.payload),
                    },
                )
            except Exception as error:
                await self._repository.set_command_state(
                    command.id,
                    "PENDING",
                    f"{type(error).__name__}: {error}",
                )
                continue
            await self._repository.set_command_state(
                command.id,
                "PUBLISHED",
            )
            published += 1
        return published

    async def publish_task_event(
        self,
        *,
        task_id: UUID,
        event_type: str,
        payload: dict[str, Any],
    ) -> None:
        await self._redis.xadd(
            self._settings.agent_product_event_stream,
            {
                "task_id": str(task_id),
                "event_type": event_type,
                "payload": json.dumps(payload),
            },
        )
