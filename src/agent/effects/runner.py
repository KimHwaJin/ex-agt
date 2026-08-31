import asyncio
from typing import Any

from agent.effects.files import restore_files
from ex_agent.config import Settings
from ex_agent.executor.client import ExecutorClient
from ex_agent.executor.contracts import ArtifactResponse, CommandResponse


class ExecutorEffectSender:
    """Validate and normalize receipts before they become durable."""

    def __init__(self, settings: Settings, executor: ExecutorClient) -> None:
        self.settings = settings
        self.executor = executor

    async def send(self, request: dict[str, Any]) -> dict[str, Any]:
        await asyncio.to_thread(
            restore_files,
            self.settings.executor_shared_storage_root,
            request.get("files", []),
        )
        raw = await self.executor.post_prepared(
            request["path"], request["body"]
        )
        if request["kind"] == "report":
            return ArtifactResponse.model_validate(raw).model_dump(mode="json")
        response = CommandResponse.model_validate(raw)
        expected = request.get("execution_id")
        if expected is not None and str(response.execution_id) != expected:
            raise ValueError("Executor response has another Execution ID")
        operation = (
            response.operation
            if request["kind"] in {"submit", "append"}
            else None
        )
        if request["kind"] in {"submit", "append"} and operation is None:
            raise ValueError("Executor response omitted Operation")
        return {
            "execution_id": str(response.execution_id),
            "execution_version": response.state.version,
            "operation_id": str(operation.operation_id) if operation else None,
        }
