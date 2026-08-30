from __future__ import annotations

from pathlib import Path
from typing import Any, cast
from uuid import UUID, uuid4

import pytest

from ex_agent.application.promotions import (
    WorkflowPromotionForbiddenError,
    WorkflowPromotionNotEligibleError,
    WorkflowPromotionService,
    bind_workflow_inputs,
)
from ex_agent.config import Settings
from ex_agent.domain.contracts import (
    PlanDraft,
    PlanStepDraft,
    WorkflowPromotionRequest,
    WorkflowPromotionResult,
)
from ex_agent.domain.enums import ExecutionMode, PlanningKind, TaskStatus
from ex_agent.llm.factory import DeterministicHashEmbeddings
from ex_agent.persistence.models import SuccessfulExecutionStep, Task
from ex_agent.persistence.repositories.promotions import PromotionSource
from ex_agent.tools.registry import ToolRegistry

_SKILL_ROOT = Path(__file__).parents[1] / "skills"


class FakeRepository:
    def __init__(self, source: PromotionSource) -> None:
        self.source = source
        self.promote_values: dict[str, Any] | None = None

    async def workflow_promotion_source(
        self,
        task_id: UUID,
    ) -> PromotionSource:
        assert task_id == self.source.task.id
        return self.source

    async def promote_workflow(self, **values: Any) -> WorkflowPromotionResult:
        self.promote_values = values
        return WorkflowPromotionResult(
            workflow_id=uuid4(),
            workflow_version_id=uuid4(),
            version=1,
            created=True,
            public_payload_hash="a" * 64,
        )

    async def existing_workflow_promotion(
        self,
        **values: Any,
    ) -> WorkflowPromotionResult | None:
        del values
        return None


def _registry() -> ToolRegistry:
    registry = ToolRegistry(_SKILL_ROOT)
    registry.load()
    return registry


def _source(
    *,
    owner: str = "owner",
    status: TaskStatus = TaskStatus.SUCCEEDED,
    custom_code: bool = False,
) -> PromotionSource:
    registry = _registry()
    revision_id = uuid4()
    plan_id = uuid4()
    task = Task(
        id=uuid4(),
        input_message_id=uuid4(),
        user_id=owner,
        project_id="project-one",
        session_id="session-one",
        user_message="private original request",
        status=status.value,
        execution_id=uuid4(),
        version=1,
    )
    if custom_code:
        step = PlanStepDraft(
            sequence=0,
            title="Custom",
            purpose="Run custom code",
            planning_kind=PlanningKind.CUSTOM_CODE,
            custom_code="def run():\n    return 1\n\nresult = run()",
            selection_rationale="User requested code",
        )
    else:
        manifest = registry.get_tool("fetch_dataset")
        step = PlanStepDraft(
            sequence=0,
            title="Fetch dataset",
            purpose="Acquire reusable analysis input",
            planning_kind=PlanningKind.TOOL_PLAN,
            skill=manifest.skill,
            tool=manifest.tool,
            parameters={
                "query": "SELECT * FROM private_customer_table",
                "dataset_name": "private-customer-export",
                "output_format": "parquet",
            },
            selection_rationale="The analysis needs source data",
            expected_outputs=["dataset"],
        )
    plan = PlanDraft(
        objective="고객 데이터를 분석한다",
        strategy_summary="데이터를 조회한 뒤 품질을 확인한다.",
        execution_mode=ExecutionMode.MULTI,
        runtime_profile="basic",
        steps=[step],
        expected_artifacts=["analysis report"],
    )
    row = SuccessfulExecutionStep(
        task_id=task.id,
        execution_sequence=0,
        operation_id=uuid4(),
        source_plan_id=plan_id,
        source_plan_revision_id=revision_id,
        registry_snapshot_hash=registry.registry_snapshot_hash(),
        step_payload=step.model_dump(mode="json"),
        step_payload_hash="b" * 64,
    )
    return PromotionSource(
        task=task,
        steps=[row],
        plans_by_revision={revision_id: plan},
    )


def _service(
    source: PromotionSource,
) -> tuple[WorkflowPromotionService, FakeRepository]:
    repository = FakeRepository(source)
    settings = Settings(agent_skill_root=_SKILL_ROOT)
    service = WorkflowPromotionService(
        settings,
        cast(Any, repository),
        _registry(),
        DeterministicHashEmbeddings(1024),
    )
    return service, repository


@pytest.mark.asyncio
async def test_draft_replaces_private_parameters_with_inputs() -> None:
    source = _source()
    service, _repository = _service(source)

    draft = await service.draft(source.task.id, actor_user_id="owner")

    assert draft.eligible is True
    assert set(draft.parameter_inputs) == {
        "step_0_dataset_name",
        "step_0_output_format",
        "step_0_query",
    }
    serialized = draft.model_dump_json()
    assert "private_customer_table" not in serialized
    assert "private-customer-export" not in serialized
    assert draft.steps[0].parameters["query"] == {
        "$workflow_input": "step_0_query"
    }


@pytest.mark.asyncio
async def test_promote_keeps_only_explicit_public_defaults() -> None:
    source = _source()
    service, repository = _service(source)
    request = WorkflowPromotionRequest(
        idempotency_key="promote-one",
        name="재사용 고객 분석",
        description="고객 데이터 조회와 분석을 수행합니다.",
        request_examples=["고객 데이터 분석해줘"],
        tags=["customer", "analysis"],
        public_parameter_defaults={"step_0_output_format": "parquet"},
    )

    await service.promote(
        source.task.id,
        actor_user_id="owner",
        request=request,
    )

    assert repository.promote_values is not None
    plan = repository.promote_values["plan"]
    assert plan.execution_mode is ExecutionMode.SINGLE
    assert plan.steps[0].parameters == {
        "dataset_name": {"$workflow_input": "step_0_dataset_name"},
        "output_format": "parquet",
        "query": {"$workflow_input": "step_0_query"},
    }
    assert set(repository.promote_values["input_contract"]) == {
        "step_0_dataset_name",
        "step_0_query",
    }
    assert (
        "private_customer_table"
        not in repository.promote_values["searchable_text"]
    )
    assert "private-customer-export" not in str(plan.model_dump())
    assert plan.objective == request.name
    assert plan.strategy_summary == request.description


def test_bind_workflow_inputs_resolves_and_validates_values() -> None:
    source = _source()
    service, _repository = _service(source)
    plan, contract = service._template_plan(source, {})

    bound = bind_workflow_inputs(
        plan,
        contract,
        {
            "step_0_dataset_name": "shared-analysis",
            "step_0_output_format": "csv",
            "step_0_query": "SELECT * FROM approved_view",
        },
    )

    assert bound.steps[0].parameters["dataset_name"] == "shared-analysis"
    assert bound.steps[0].parameters["output_format"] == "csv"
    with pytest.raises(ValueError, match="Missing Workflow input"):
        bind_workflow_inputs(plan, contract, {})
    invalid = {
        "step_0_dataset_name": "shared-analysis",
        "step_0_output_format": 3,
        "step_0_query": "SELECT 1",
    }
    with pytest.raises(ValueError, match="must be string"):
        bind_workflow_inputs(plan, contract, invalid)


@pytest.mark.asyncio
async def test_promotion_rejects_custom_code_and_non_owner() -> None:
    custom_source = _source(custom_code=True)
    custom_service, _repository = _service(custom_source)

    with pytest.raises(
        WorkflowPromotionNotEligibleError,
        match="CUSTOM_CODE",
    ):
        await custom_service.draft(
            custom_source.task.id,
            actor_user_id="owner",
        )

    source = _source()
    service, _repository = _service(source)
    with pytest.raises(WorkflowPromotionForbiddenError, match="does not own"):
        await service.draft(source.task.id, actor_user_id="other-user")
