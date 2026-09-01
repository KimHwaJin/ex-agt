from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

from fastapi.testclient import TestClient

from ex_agent.api.app import create_app
from ex_agent.api.container import api_container, current_user
from ex_agent.config import Settings


class Admission:
    def __init__(self) -> None:
        self.commands = []

    async def handle(self, command):
        self.commands.append(command)
        return SimpleNamespace(state="APPLIED", last_error=None)


class Repository:
    def __init__(self, task) -> None:
        self.task = task

    async def get_task(self, task_id):
        return self.task if task_id == self.task.id else None


def test_task_routes_admit_start_and_resume_directly():
    now = datetime.now(UTC)
    task_id, message_id = uuid4(), uuid4()
    task = SimpleNamespace(
        id=task_id,
        input_message_id=message_id,
        user_id="user-1",
        project_id="project-1",
        session_id="session-1",
        user_message="분석해줘",
        status="PLANNING",
        execution_id=None,
        current_interrupt=None,
        terminal_message=None,
        version=1,
        created_at=now,
        updated_at=now,
        created_by="user-1",
        updated_by="user-1",
    )
    admission = Admission()
    container = SimpleNamespace(
        settings=Settings(),
        admission=admission,
        repository=Repository(task),
    )
    app = create_app(Settings(), start_runtime=False)
    app.dependency_overrides[api_container] = lambda: container
    app.dependency_overrides[current_user] = lambda: "user-1"

    with TestClient(app) as client:
        created = client.post(
            "/api/v1/projects/project-1/sessions/session-1/tasks",
            json={
                "task_id": str(task_id),
                "input_message_id": str(message_id),
                "content": "분석해줘",
                "idempotency_key": "create-1",
            },
        )
        task.current_interrupt = {
            "kind": "EXECUTION_MODE",
            "task_id": str(task_id),
            "interrupt_id": "interrupt-1",
        }
        resumed = client.post(
            f"/api/v1/tasks/{task_id}/resume",
            json={
                "idempotency_key": "resume-1",
                "signal": {
                    "type": "EXECUTION_MODE",
                    "mode": "SINGLE",
                },
            },
        )

    assert created.status_code == resumed.status_code == 202
    start, resume = admission.commands
    assert start.kind == "START"
    assert start.turn.session_id == "session-1"
    assert resume.kind == "RESUME"
    assert resume.interrupt_id == "interrupt-1"
    assert resume.turn == start.turn
    assert resume.request_id != start.request_id
