"""Discoverable manager using durable Run admission, never raw graph input."""

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from time import monotonic

from d_test.agent_service.application.management import active_user
from d_test.agent_service.application.runs import RunService
from d_test.agent_service.domain.runs import TERMINAL, RunDetail
from d_test.agent_service.integrations.template_protocol import (
    AuthorizedRequest,
    input_request,
)

RequestResolver = Callable[[dict, dict], Awaitable[AuthorizedRequest]]


@dataclass(frozen=True)
class Binding:
    runs: RunService
    resolve: RequestResolver


class WorkflowManager:
    """Safe to export at module import: no clients, DB or graph are created."""

    def __init__(self):
        self._binding: Binding | None = None

    @asynccontextmanager
    async def bind(self, runs: RunService, resolve: RequestResolver):
        if self._binding is not None:
            raise RuntimeError("Workflow manager is already bound")
        binding = Binding(runs, resolve)
        self._binding = binding
        try:
            yield self
        finally:
            self._binding = None

    def require_binding(self) -> Binding:
        if self._binding is None:
            raise RuntimeError("Workflow is unavailable outside app lifespan")
        return self._binding

    async def ainvoke(self, payload: dict, config: dict | None = None):
        result = None
        async for _, state in self.astream(payload, config):
            result = state
        return result

    async def astream(
        self,
        payload: dict,
        config: dict | None = None,
        *,
        stream_mode: list[str] | None = None,
    ) -> AsyncIterator[tuple[str, dict]]:
        # Reject unsupported output modes before admitting work.
        modes = ["custom", "updates"] if stream_mode is None else stream_mode
        if "custom" not in modes or set(modes) - {"custom", "updates"}:
            raise ValueError("Template manager requires custom stream mode")
        binding = self.require_binding()
        authorized = await binding.resolve(payload, config or {})
        if not isinstance(authorized, AuthorizedRequest):
            raise TypeError("Resolver must return AuthorizedRequest")
        runs = binding.runs
        receipt, after = await runs.submit(
            authorized.owner, authorized.request_id, authorized.request
        )
        cursor = after
        started = monotonic()
        previous_status = None
        while True:
            if self.require_binding() is not binding:
                raise RuntimeError("Workflow lifespan changed")
            events, status, watermark = await runs.event_batch(
                authorized.owner, receipt.run_id, cursor
            )
            if events:
                cursor = events[-1].sequence
            if status != previous_status:
                # Never custom strings: formatter treats them as answer tokens.
                yield (
                    "custom",
                    {
                        "run_id": str(receipt.run_id),
                        "status": status,
                    },
                )
                previous_status = status
            settled = cursor >= watermark and (
                status in TERMINAL or status == "awaiting_input"
            )
            if settled or monotonic() - started >= (
                runs.settings.stream_max_seconds
            ):
                yield (
                    "custom",
                    await result_state(
                        runs, authorized.owner, receipt.run_id, after
                    ),
                )
                return
            if not events:
                await asyncio.sleep(runs.settings.stream_poll_seconds)


async def result_state(runs, owner, run_id, after):
    """Read a coherent public snapshot; no pool slot held between polls."""
    async with runs.repository() as repo:
        active_user(await repo.user(owner))
        run = await repo.run(owner, run_id, lock=True)
        detail = RunDetail.model_validate(
            {
                **run,
                "executions": await repo.executions(run_id),
                "pending_interrupts": await repo.pending(run_id),
                "last_event_id": f"{run_id}:{run['event_seq']}",
            }
        ).model_dump(mode="json")
        message = await repo.one(
            "SELECT content FROM management.messages WHERE run_id = %s "
            "AND role = 'assistant' AND event_sequence > %s "
            "ORDER BY created_at DESC, message_id DESC LIMIT 1",
            (run_id, after),
        )
    answer = "\n\n".join(
        block["text"] for block in (message or {}).get("content", [])
    )
    requests = [
        input_request(item, str(run_id))
        for item in detail["pending_interrupts"]
    ]
    state = {
        "protocol_version": 1,
        "run_id": str(run_id),
        "session_id": detail["session_id"],
        "status": detail["status"],
        "last_event_id": detail["last_event_id"],
        "error": detail["error"],
        "input_requests": requests,
    }
    return {
        **state,
        "answer": answer,
        "inputRequest": requests[0] if requests else None,
        "metadata": {"agent": state},
    }
