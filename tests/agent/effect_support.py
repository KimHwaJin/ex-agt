"""Real business services/DB; deterministic model and HTTP fault injection."""

import json
import os
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import httpx
from langchain_core.language_models.fake_chat_models import FakeListChatModel

from agent.services import SessionWorkflowServices
from ex_agent.config import Settings
from ex_agent.domain.enums import ExecutionMode
from ex_agent.executor.client import ExecutorClient
from ex_agent.models import DeterministicHashEmbeddings
from ex_agent.persistence.database import create_engine, create_session_factory
from ex_agent.persistence.repository import AgentRepository
from ex_agent.tools.registry import ToolRegistry
from tests.agent.support import turn
from tests.test_execution_mode_policy import plan


class ExecutorHTTP:
    """Models Executor idempotency, not its Jupyter implementation."""

    def __init__(self):
        self.execution_id = uuid4()
        self.version = 0
        self.status = "CREATED"
        self.operations: list[dict[str, Any]] = []
        self.calls: list[tuple[str, dict]] = []
        self.receipts: dict[str, tuple[dict, dict]] = {}
        self.lose_responses = 0
        self.after_post = None

    def handle(self, request):
        if request.method == "GET":
            return httpx.Response(200, json=self.result())
        body = json.loads(request.content)
        self.calls.append((request.url.path, body))
        key = body["idempotency_key"]
        if key in self.receipts:
            previous, response = self.receipts[key]
            assert previous == body, "Same key changed its wire payload"
        else:
            if "expected_version" in body:
                assert body["expected_version"] == self.version
            self.version += 1
            operation = None
            path = request.url.path
            if path.endswith("/executions") or path.endswith("/operations"):
                steps = (
                    body["operation"]["spec"]["steps"]
                    if "operation" in body
                    else body["spec"]["steps"]
                )
                operation = {"operation_id": str(uuid4())}
                self.operations.append(
                    {
                        **operation,
                        "operation_number": len(self.operations) + 1,
                        "result": {"status": "SUCCEEDED"},
                        "steps": [
                            {
                                "step_id": str(uuid4()),
                                "sequence": step["sequence"],
                                "lineage": step["lineage"],
                                "result": {"status": "SUCCEEDED"},
                            }
                            for step in steps
                        ],
                    }
                )
                self.status = (
                    "SUCCEEDED"
                    if body.get("lifecycle", {}).get("operation_mode")
                    == "SINGLE"
                    else "WAITING_FOR_OPERATION"
                )
            elif path.endswith("/finalize"):
                self.status = "SUCCEEDED"
            elif path.endswith("/cancel"):
                self.status = "CANCELLED"
            response = (
                {"artifact_id": str(uuid4())}
                if path.endswith("/artifacts")
                else {
                    "execution_id": str(self.execution_id),
                    "operation": operation,
                    "state": {"status": self.status, "version": self.version},
                }
            )
            self.receipts[key] = (body, response)
        if self.after_post:
            self.after_post()
        if self.lose_responses:
            self.lose_responses -= 1
            raise httpx.ReadTimeout(
                "Accepted but response lost", request=request
            )
        return httpx.Response(200, json=response)

    def result(self):
        return {
            "execution": {
                "execution_id": str(self.execution_id),
                "state": {"status": self.status, "version": self.version},
            },
            "operations": self.operations,
        }


@asynccontextmanager
async def effect_harness(tmp_path, *, mode=ExecutionMode.MULTI):
    engine = create_engine(os.environ["TEST_DATABASE_URL"])
    sessions = create_session_factory(engine)
    repository = AgentRepository(sessions)
    task = turn()
    await repository.create_task(
        task_id=UUID(task.active_task_id),
        input_message_id=UUID(task.current_input_message_id),
        user_id=task.user_id,
        project_id=task.project_id,
        session_id=task.session_id,
        content=task.user_message,
        idempotency_key=f"start:{task.active_task_id}",
    )
    remote = ExecutorHTTP()
    settings = Settings(_env_file=None, executor_shared_storage_root=tmp_path)
    model = FakeListChatModel(responses=["# 최초 결과", "# 다른 결과"])
    async with httpx.AsyncClient(
        base_url="http://executor.test/api/v1",
        transport=httpx.MockTransport(remote.handle),
    ) as http:
        executor = ExecutorClient(
            "http://executor.test/api/v1", timeout_seconds=1, client=http
        )
        service = SessionWorkflowServices(
            settings,
            repository,
            executor,
            ToolRegistry(Path("skills")),
            sessions=sessions,
            model=model,
            embeddings=DeterministicHashEmbeddings(dimensions=1024),
        )
        state = {
            **task.model_dump(mode="json"),
            "execution_mode": mode,
            "runtime_profile": "basic",
            "plan": plan(mode, 2),
        }
        try:
            yield SimpleNamespace(
                service=service,
                repository=repository,
                sessions=sessions,
                remote=remote,
                state=state,
                task=task,
                engine=engine,
                model=model,
            )
        finally:
            await engine.dispose()


async def submitted(harness):
    state = harness.state
    persisted = await harness.service.compile_and_persist_plan(
        state, state["plan"]
    )
    state.update(
        {
            "plan_id": str(persisted.plan_id),
            "plan_revision_id": str(persisted.plan_revision_id),
            "plan_revision_number": persisted.plan_revision_number,
            "plan_public_payload_hash": persisted.public_payload_hash,
        }
    )
    receipt = await harness.service.submit_execution(state)
    state.update(
        {
            "execution_id": str(receipt.execution_id),
            "current_operation_id": str(receipt.operation_id),
        }
    )
    return receipt
