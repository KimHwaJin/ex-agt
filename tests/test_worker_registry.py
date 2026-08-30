from pathlib import Path

import pytest

from ex_agent.config import Settings
from ex_agent.worker import WorkflowWorker

_SKILL_ROOT = Path(__file__).resolve().parents[1] / "skills"


@pytest.mark.asyncio
async def test_worker_loads_registry_from_configured_skill_root() -> None:
    worker = WorkflowWorker(
        Settings(
            agent_skill_root=_SKILL_ROOT,
            worker_metrics_enabled=False,
            worker_instance_id="registry-test",
        )
    )

    try:
        assert worker._consumer == "worker-registry-test"
        assert worker._registry.get_tool("fetch_dataset").skill.name == (
            "data-access"
        )
    finally:
        await worker.close()
