import pytest

from ex_agent.benchmarks.lifecycle import (
    LifecycleHarness,
    ScenarioKind,
    run_lifecycle_batch,
)
from ex_agent.domain.enums import TaskStatus


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("scenario", "boundaries"),
    [
        ("single_custom", 1),
        ("multi_analysis", 3),
    ],
)
async def test_lifecycle_harness_reaches_success(
    scenario: ScenarioKind,
    boundaries: int,
) -> None:
    harness = LifecycleHarness()

    timing = await harness.run(scenario)

    assert timing.executor_boundaries == boundaries
    assert timing.total_seconds >= timing.planning_seconds
    assert set(harness.services.statuses.values()) == {TaskStatus.SUCCEEDED}


@pytest.mark.asyncio
async def test_lifecycle_batch_runs_concurrently() -> None:
    timings = await run_lifecycle_batch(
        scenario="single_custom",
        requests=4,
        concurrency=2,
        llm_delay_seconds=0.001,
        executor_delay_seconds=0.001,
    )

    assert len(timings) == 4
    assert all(item.total_seconds > 0 for item in timings)
