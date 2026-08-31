from typing import Any, cast
from uuid import UUID

import pytest

from ex_agent.application.services import DefaultWorkflowServices
from ex_agent.config import Settings


class FailingEmbeddings:
    async def aembed_query(self, text: str) -> list[float]:
        del text
        raise RuntimeError("embedding backend unavailable")


class EventRepository:
    def __init__(self) -> None:
        self.events: list[tuple[UUID, str, dict[str, Any]]] = []

    async def append_task_event(
        self,
        task_id: UUID,
        event_type: str,
        payload: dict[str, Any],
    ) -> None:
        self.events.append((task_id, event_type, payload))


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", [None, "SINGLE", "MULTI"])
async def test_workflow_search_degrades_to_dynamic_planning(mode) -> None:
    task_id = UUID("6a35ab4f-ca25-4ce3-9cb5-7d51ff65646b")
    repository = EventRepository()
    services = object.__new__(DefaultWorkflowServices)
    services._settings = Settings()
    services._repository = cast(Any, repository)
    services._embeddings = cast(Any, FailingEmbeddings())

    result = await services.search_workflows(
        cast(
            Any,
            {
                "active_task_id": str(task_id),
                "user_message": "매출 추이를 분석해줘",
                **({"execution_mode": mode} if mode else {}),
            },
        )
    )

    assert result == []
    assert repository.events == [
        (
            task_id,
            "workflow.search_degraded",
            {
                "reason": ("RuntimeError: embedding backend unavailable"),
                "fallback": f"DYNAMIC_{mode or 'MULTI'}_PLAN",
            },
        )
    ]
