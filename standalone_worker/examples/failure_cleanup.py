"""Optional host policy; never automatically enabled by the Worker."""

import asyncio

import httpx

from executor_worker import DeferEvent, EventContext

TERMINAL = {"SUCCEEDED", "FAILED", "CANCELLED"}


async def cancel_and_confirm(
    http: httpx.AsyncClient,
    context: EventContext,
    *,
    reason: str,
    timeout_seconds: float = 30,
    poll_seconds: float = 0.5,
) -> str:
    """Return confirmed terminal status, not a cancellation receipt.

    Host decides when this policy runs and persists the user-facing failure.
    Never release the long chat lock merely because cancel POST was accepted.
    """
    if timeout_seconds <= 0 or poll_seconds <= 0:
        raise ValueError("Timeout and polling interval must be positive")
    path = f"/executions/{context.execution_id}"

    async def status() -> str:
        response = await http.get(f"{path}/result")
        response.raise_for_status()
        return response.json()["execution"]["state"]["status"]

    try:
        async with asyncio.timeout(timeout_seconds):
            current = await status()
            if current in TERMINAL:
                return current
            response = await http.post(
                f"{path}/cancel",
                json={
                    "idempotency_key": (
                        f"{context.namespace}:failure-cancel:{context.execution_id}"
                    ),
                    "reason": reason,
                    "actor": {"type": "AGENT", "id": context.namespace},
                },
            )
            if response.status_code != 409:
                response.raise_for_status()
            while True:
                current = await status()
                if current in TERMINAL:
                    return current
                await asyncio.sleep(poll_seconds)
    except (TimeoutError, httpx.TimeoutException) as error:
        raise DeferEvent("Executor terminal state is not confirmed") from error
