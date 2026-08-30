from __future__ import annotations

import json
from typing import Any

from redis.asyncio import Redis

from ex_agent.config import Settings
from ex_agent.persistence.models import TaskEvent, WorkflowCommand
from ex_agent.persistence.repository import AgentRepository


class CommandPublisher:
    """Relay durable command and product-event outboxes to Redis."""

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
        command_count = await self._publish_pending_commands()
        event_count = await self._publish_pending_task_events()
        return command_count + event_count

    async def _publish_pending_commands(self) -> int:
        commands = await self._repository.claim_pending_commands(
            limit=self._settings.outbox_batch_size,
            claim_timeout_seconds=(
                self._settings.outbox_claim_timeout_seconds
            ),
        )
        if not commands:
            return 0
        claimed_at = commands[0].publish_claimed_at
        if claimed_at is None:
            raise RuntimeError("Claimed commands have no claim timestamp")
        pipeline = self._redis.pipeline(transaction=False)
        for command in commands:
            pipeline.xadd(
                self._settings.agent_command_stream,
                _command_fields(command),
            )
        try:
            results = await pipeline.execute(raise_on_error=False)
        except Exception as error:
            await self._repository.finish_command_publications(
                [command.id for command in commands],
                claimed_at=claimed_at,
                published=False,
                error=_error_message(error),
            )
            raise
        published_ids = [
            command.id
            for command, result in zip(commands, results, strict=True)
            if not isinstance(result, BaseException)
        ]
        failed_ids = [
            command.id
            for command, result in zip(commands, results, strict=True)
            if isinstance(result, BaseException)
        ]
        await self._repository.finish_command_publications(
            published_ids,
            claimed_at=claimed_at,
            published=True,
        )
        await self._repository.finish_command_publications(
            failed_ids,
            claimed_at=claimed_at,
            published=False,
            error="Redis command stream publication failed",
        )
        return len(published_ids)

    async def _publish_pending_task_events(self) -> int:
        events = await self._repository.claim_pending_task_events(
            limit=self._settings.outbox_batch_size,
            claim_timeout_seconds=(
                self._settings.outbox_claim_timeout_seconds
            ),
        )
        if not events:
            return 0
        claimed_at = events[0].delivery_claimed_at
        if claimed_at is None:
            raise RuntimeError("Claimed task events have no claim timestamp")
        pipeline = self._redis.pipeline(transaction=False)
        for event in events:
            pipeline.xadd(
                self._settings.agent_product_event_stream,
                _event_fields(event),
                maxlen=self._settings.product_event_stream_maxlen,
                approximate=True,
            )
        last_event_by_task: dict[Any, int] = {}
        for event in events:
            last_event_by_task[event.task_id] = event.id
        for task_id, event_id in last_event_by_task.items():
            pipeline.publish(
                task_event_channel(self._settings, task_id),
                str(event_id),
            )
        try:
            results = await pipeline.execute(raise_on_error=False)
        except Exception as error:
            await self._repository.finish_task_event_publications(
                [event.id for event in events],
                claimed_at=claimed_at,
                published=False,
                error=_error_message(error),
            )
            raise
        published_ids: list[int] = []
        failed_ids: list[int] = []
        task_ids = list(last_event_by_task)
        notification_results = {
            task_id: results[len(events) + index]
            for index, task_id in enumerate(task_ids)
        }
        for index, event in enumerate(events):
            stream_result = results[index]
            notification_result = notification_results[event.task_id]
            if isinstance(stream_result, BaseException) or isinstance(
                notification_result,
                BaseException,
            ):
                failed_ids.append(event.id)
            else:
                published_ids.append(event.id)
        await self._repository.finish_task_event_publications(
            published_ids,
            claimed_at=claimed_at,
            published=True,
        )
        await self._repository.finish_task_event_publications(
            failed_ids,
            claimed_at=claimed_at,
            published=False,
            error="Redis product-event publication failed",
        )
        return len(published_ids)


def _command_fields(command: WorkflowCommand) -> dict[Any, Any]:
    return {
        "command_id": str(command.id),
        "task_id": str(command.task_id),
        "command_type": command.command_type,
        "payload": json.dumps(command.payload),
    }


def _event_fields(event: TaskEvent) -> dict[Any, Any]:
    return {
        "event_id": str(event.id),
        "task_id": str(event.task_id),
        "event_type": event.event_type,
        "payload": json.dumps(event.payload, ensure_ascii=False),
    }


def task_event_channel(settings: Settings, task_id: Any) -> str:
    return f"{settings.agent_product_event_channel_prefix}:{task_id}"


def _error_message(error: Exception) -> str:
    return f"{type(error).__name__}: {error}"
