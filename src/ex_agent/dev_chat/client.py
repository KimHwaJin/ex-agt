from __future__ import annotations

import asyncio
from typing import Any
from urllib.parse import quote

import httpx

from ex_agent.api.contracts import TaskResponse
from ex_agent.dev_chat.settings import ChatSettings
from ex_agent.domain.enums import TaskStatus

HUMAN_INTERRUPTS = frozenset(
    {
        "CLARIFICATION",
        "EXECUTION_MODE",
        "REQUEST_RISK_CONFIRMATION",
        "WORKFLOW_SELECTION",
        "PLAN_REVIEW",
    }
)


def needs_input(task: TaskResponse, ignored_version: int = -1) -> bool:
    return bool(
        task.version > ignored_version
        and task.current_interrupt
        and task.current_interrupt.get("kind") in HUMAN_INTERRUPTS
    )


def _snapshot_ready(
    task: TaskResponse, ignored_version: int, known_version: int | None
) -> bool:
    return (
        needs_input(task, ignored_version)
        or TaskStatus(task.status).is_terminal
        or (known_version is not None and task.version != known_version)
    )


class AgentApiClient:
    """Only the existing BFF API is used; never consume Worker streams."""

    def __init__(
        self,
        settings: ChatSettings,
        *,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.settings = settings
        self._transport = transport

    def connection(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=str(self.settings.api_url).rstrip("/") + "/api/v1/",
            headers={"X-User-ID": self.settings.user_id},
            timeout=self.settings.request_timeout_seconds,
            transport=self._transport,
        )

    async def _request(
        self,
        client: httpx.AsyncClient,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
    ) -> httpx.Response:
        for attempt in range(3):
            try:
                response = await client.request(method, path, json=body)
                response.raise_for_status()
                return response
            except httpx.HTTPStatusError as error:
                if error.response.status_code < 500 or attempt == 2:
                    raise
            except httpx.TransportError:
                if attempt == 2:
                    raise
            await asyncio.sleep(0.2 * 2**attempt)
        raise AssertionError("Unreachable retry exit")

    async def get_task(self, task_id: str) -> TaskResponse:
        async with self.connection() as client:
            return await self._get_task(client, task_id)

    async def _get_task(
        self, client: httpx.AsyncClient, task_id: str
    ) -> TaskResponse:
        response = await self._request(client, "GET", f"tasks/{task_id}")
        return TaskResponse.model_validate(response.json())

    async def create_task(self, session_id: str, body: dict[str, Any]) -> None:
        project = quote(self.settings.project_id, safe="")
        session = quote(session_id, safe="")
        async with self.connection() as client:
            await self._request(
                client,
                "POST",
                f"projects/{project}/sessions/{session}/tasks",
                body,
            )

    async def send_signal(
        self,
        task_id: str,
        signal: dict[str, Any],
        idempotency_key: str,
        expected_version: int,
    ) -> None:
        cancel = signal["type"] == "CANCEL_REQUESTED"
        body: dict[str, Any] = {"idempotency_key": idempotency_key}
        if cancel:
            body["reason"] = signal.get("reason")
        else:
            body["signal"] = signal
        endpoint = "cancel" if cancel else "resume"
        async with self.connection() as client:
            try:
                await self._request(
                    client, "POST", f"tasks/{task_id}/{endpoint}", body
                )
            except httpx.HTTPStatusError as error:
                if error.response.status_code != 409 or cancel:
                    raise
                # A retried response may arrive after Worker advanced. Do not
                # reapply a decision to a newer interrupt or fail the task.
                task = await self._get_task(client, task_id)
                if (
                    task.version <= expected_version
                    and task.current_interrupt is not None
                    and not TaskStatus(task.status).is_terminal
                ):
                    raise

    async def watch(
        self,
        task_id: str,
        *,
        after_event_id: int,
        ignored_version: int,
        known_version: int | None = None,
    ) -> tuple[TaskResponse, int]:
        """Wait for a snapshot change, decision, or reconciliation deadline."""
        cursor = after_event_id
        async with self.connection() as client:
            task = await self._get_task(client, task_id)
            if _snapshot_ready(task, ignored_version, known_version):
                return task, cursor
            try:
                async with asyncio.timeout(self.settings.watch_seconds):
                    while True:
                        try:
                            async with client.stream(
                                "GET",
                                f"tasks/{task_id}/events",
                                headers={"Last-Event-ID": str(cursor)},
                                timeout=httpx.Timeout(
                                    self.settings.request_timeout_seconds,
                                    read=None,
                                ),
                            ) as response:
                                response.raise_for_status()
                                event_id, event_type = cursor, ""
                                async for line in response.aiter_lines():
                                    if line.startswith("id:"):
                                        event_id = int(line[3:].strip())
                                    elif line.startswith("event:"):
                                        event_type = line[6:].strip()
                                    elif not line:
                                        cursor = max(cursor, event_id)
                                        if event_type in {
                                            "task.interrupted",
                                            "task.completed",
                                            "task.status_changed",
                                        }:
                                            task = await self._get_task(
                                                client, task_id
                                            )
                                            if _snapshot_ready(
                                                task,
                                                ignored_version,
                                                known_version,
                                            ):
                                                return task, cursor
                                        event_type = ""
                        except httpx.TransportError:
                            pass
                        # An EOF/disconnect reconnects with the durable cursor.
                        # This is not a business failure or a cancel request.
                        await asyncio.sleep(0.5)
                        task = await self._get_task(client, task_id)
                        if _snapshot_ready(
                            task, ignored_version, known_version
                        ):
                            return task, cursor
            except TimeoutError:
                pass
            return await self._get_task(client, task_id), cursor
