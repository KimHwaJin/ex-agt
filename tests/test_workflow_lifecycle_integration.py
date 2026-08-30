from __future__ import annotations

import asyncio
import os
from hashlib import sha256
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from sqlalchemy import delete, select

from ex_agent.application.promotions import WorkflowPromotionService
from ex_agent.application.workflow_lifecycle import (
    WorkflowLifecycleForbiddenError,
    WorkflowLifecycleService,
)
from ex_agent.config import Settings
from ex_agent.domain.contracts import (
    CompiledStep,
    PlanDraft,
    PlanStepDraft,
    WorkflowPromotionRequest,
    WorkflowStatusRequest,
    WorkflowVersionActivationRequest,
    WorkflowVersionCreateRequest,
    WorkflowVersionReviewRequest,
)
from ex_agent.domain.enums import ExecutionMode, PlanningKind, TaskStatus
from ex_agent.llm.factory import DeterministicHashEmbeddings
from ex_agent.persistence.database import (
    create_engine,
    create_session_factory,
    transaction,
)
from ex_agent.persistence.models import (
    Task,
    Workflow,
    WorkflowLifecycleAction,
    WorkflowPromotion,
    WorkflowVersion,
)
from ex_agent.persistence.repositories.workflow_lifecycle import (
    WorkflowLifecycleConflictError,
)
from ex_agent.persistence.repository import AgentRepository
from ex_agent.tools.registry import ToolRegistry

pytestmark = pytest.mark.skipif(
    "TEST_DATABASE_URL" not in os.environ,
    reason="Compose PostgreSQL is not configured",
)

_SKILL_ROOT = Path(__file__).parents[1] / "skills"


@pytest.mark.postgres
@pytest.mark.asyncio
async def test_workflow_versions_are_reviewed_switched_and_audited() -> None:
    engine = create_engine(os.environ["TEST_DATABASE_URL"])
    sessions = create_session_factory(engine)
    repository = AgentRepository(sessions)
    registry = ToolRegistry(_SKILL_ROOT)
    registry.load()
    settings = Settings(agent_skill_root=_SKILL_ROOT)
    promotions = WorkflowPromotionService(
        settings,
        repository,
        registry,
        DeterministicHashEmbeddings(1024),
    )
    lifecycle = WorkflowLifecycleService(
        settings,
        repository,
        promotions,
    )
    task_ids: list[UUID] = []
    workflow_id: UUID | None = None
    try:
        first_task = await _successful_task(repository, registry, task_ids)
        promoted = await promotions.promote(
            first_task,
            actor_user_id="lifecycle-owner",
            request=WorkflowPromotionRequest(
                idempotency_key=f"promote-{first_task}",
                name=f"Lifecycle Workflow {first_task}",
                description="검토된 데이터 조회 Workflow",
                request_examples=["데이터를 조회해줘"],
                tags=["lifecycle"],
                public_parameter_defaults={"step_0_output_format": "parquet"},
            ),
        )
        workflow_id = promoted.workflow_id
        second_task = await _successful_task(repository, registry, task_ids)
        create_request = WorkflowVersionCreateRequest(
            idempotency_key=f"version-{second_task}",
            source_task_id=second_task,
            request_examples=["최신 데이터로 조회해줘"],
            tags=["lifecycle", "v2"],
            public_parameter_defaults={"step_0_output_format": "csv"},
        )

        created = await lifecycle.create_version(
            workflow_id,
            actor_user_id="lifecycle-owner",
            request=create_request,
        )
        replay = await lifecycle.create_version(
            workflow_id,
            actor_user_id="lifecycle-owner",
            request=create_request,
        )

        assert created.version == 2
        assert created.review_status == "PENDING_REVIEW"
        assert created.version_active is False
        assert created.workflow_version_id is not None
        assert replay.applied is False
        assert replay.workflow_version_id == created.workflow_version_id

        review_request = WorkflowVersionReviewRequest(
            idempotency_key=f"approve-{created.workflow_version_id}",
            decision="APPROVE",
            reason="검증 완료",
        )
        approved = await lifecycle.review_version(
            workflow_id,
            created.workflow_version_id,
            actor_user_id="lifecycle-owner",
            request=review_request,
        )
        approval_replay = await lifecycle.review_version(
            workflow_id,
            created.workflow_version_id,
            actor_user_id="lifecycle-owner",
            request=review_request,
        )
        assert approved.review_status == "APPROVED"
        assert approved.version_active is True
        assert approval_replay.applied is False

        rolled_back = await lifecycle.activate_version(
            workflow_id,
            promoted.workflow_version_id,
            actor_user_id="lifecycle-owner",
            request=WorkflowVersionActivationRequest(
                idempotency_key=f"rollback-{workflow_id}",
                reason="v1으로 운영 롤백",
            ),
        )
        assert rolled_back.workflow_version_id == promoted.workflow_version_id
        assert rolled_back.version_active is True

        deactivated = await lifecycle.update_status(
            workflow_id,
            actor_user_id="lifecycle-owner",
            request=WorkflowStatusRequest(
                idempotency_key=f"deactivate-{workflow_id}",
                status="INACTIVE",
                reason="운영 점검",
            ),
        )
        assert deactivated.workflow_status == "INACTIVE"
        candidates = await repository.workflow_candidates(
            DeterministicHashEmbeddings(1024).embed_query("데이터 조회"),
            limit=100,
        )
        assert all(
            candidate.workflow_version_id
            not in {
                promoted.workflow_version_id,
                created.workflow_version_id,
            }
            for candidate in candidates
        )
        with pytest.raises(LookupError, match="Unknown Workflow version"):
            await repository.workflow_version(promoted.workflow_version_id)

        await lifecycle.update_status(
            workflow_id,
            actor_user_id="lifecycle-owner",
            request=WorkflowStatusRequest(
                idempotency_key=f"reactivate-{workflow_id}",
                status="ACTIVE",
                reason="운영 재개",
            ),
        )
        restored = await repository.workflow_version(
            promoted.workflow_version_id
        )
        assert restored.id == promoted.workflow_version_id
        rejected_version = await lifecycle.create_version(
            workflow_id,
            actor_user_id="lifecycle-owner",
            request=WorkflowVersionCreateRequest(
                idempotency_key=f"rejected-version-{second_task}",
                source_task_id=second_task,
                request_examples=["거절 검증용 요청"],
            ),
        )
        assert rejected_version.workflow_version_id is not None
        rejected = await lifecycle.review_version(
            workflow_id,
            rejected_version.workflow_version_id,
            actor_user_id="lifecycle-owner",
            request=WorkflowVersionReviewRequest(
                idempotency_key=(
                    f"reject-{rejected_version.workflow_version_id}"
                ),
                decision="REJECT",
                reason="운영 기준 미충족",
            ),
        )
        assert rejected.review_status == "REJECTED"
        assert rejected.version_active is False
        with pytest.raises(
            WorkflowLifecycleConflictError,
            match="Only APPROVED",
        ):
            await lifecycle.activate_version(
                workflow_id,
                rejected_version.workflow_version_id,
                actor_user_id="lifecycle-owner",
                request=WorkflowVersionActivationRequest(
                    idempotency_key=(
                        f"activate-rejected-"
                        f"{rejected_version.workflow_version_id}"
                    )
                ),
            )
        concurrent_request = WorkflowVersionCreateRequest(
            idempotency_key=f"concurrent-version-{second_task}",
            source_task_id=second_task,
            request_examples=["동시 멱등 요청"],
        )
        concurrent_results = await asyncio.gather(
            lifecycle.create_version(
                workflow_id,
                actor_user_id="lifecycle-owner",
                request=concurrent_request,
            ),
            lifecycle.create_version(
                workflow_id,
                actor_user_id="lifecycle-owner",
                request=concurrent_request,
            ),
        )
        assert sorted(result.applied for result in concurrent_results) == [
            False,
            True,
        ]
        assert (
            concurrent_results[0].workflow_version_id
            == concurrent_results[1].workflow_version_id
        )
        overview = await lifecycle.overview(
            workflow_id,
            actor_user_id="lifecycle-owner",
        )
        assert overview.latest_version == 4
        assert (
            overview.active_workflow_version_id == promoted.workflow_version_id
        )
        first_versions = await lifecycle.versions(
            workflow_id,
            actor_user_id="lifecycle-owner",
            cursor=None,
            limit=2,
        )
        assert [item.version for item in first_versions.items] == [4, 3]
        assert first_versions.next_cursor is not None
        second_versions = await lifecycle.versions(
            workflow_id,
            actor_user_id="lifecycle-owner",
            cursor=first_versions.next_cursor,
            limit=2,
        )
        assert [item.version for item in second_versions.items] == [2, 1]
        assert second_versions.next_cursor is None
        detail = await lifecycle.version_detail(
            workflow_id,
            created.workflow_version_id,
            actor_user_id="lifecycle-owner",
        )
        assert detail.plan.steps[0].tool is not None
        assert detail.plan.steps[0].tool.name == "fetch_dataset"
        assert detail.steps[0].skill_ref["name"] == "data-access"
        assert detail.steps[0].selection_rationale
        assert "private_source_" not in detail.model_dump_json()

        action_ids: list[UUID] = []
        action_cursor = None
        while True:
            action_page = await lifecycle.actions(
                workflow_id,
                actor_user_id="lifecycle-owner",
                cursor=action_cursor,
                limit=3,
            )
            assert all(
                len(item.request_hash) == 64 for item in action_page.items
            )
            action_ids.extend(item.action_id for item in action_page.items)
            action_cursor = action_page.next_cursor
            if action_cursor is None:
                break
        assert len(action_ids) == 8
        assert len(set(action_ids)) == 8
        with pytest.raises(ValueError, match="pagination cursor"):
            await lifecycle.versions(
                workflow_id,
                actor_user_id="lifecycle-owner",
                cursor="not-a-valid-cursor",
                limit=2,
            )
        with pytest.raises(WorkflowLifecycleForbiddenError):
            await lifecycle.update_status(
                workflow_id,
                actor_user_id="other-user",
                request=WorkflowStatusRequest(
                    idempotency_key=f"forbidden-{workflow_id}",
                    status="INACTIVE",
                ),
            )
        with pytest.raises(WorkflowLifecycleForbiddenError):
            await lifecycle.overview(
                workflow_id,
                actor_user_id="other-user",
            )
        with pytest.raises(
            WorkflowLifecycleConflictError,
            match="payload mismatch",
        ):
            await lifecycle.activate_version(
                workflow_id,
                promoted.workflow_version_id,
                actor_user_id="lifecycle-owner",
                request=WorkflowVersionActivationRequest(
                    idempotency_key=f"rollback-{workflow_id}",
                    reason="다른 요청 내용",
                ),
            )

        async with sessions() as session:
            workflow = await session.get(Workflow, workflow_id)
            versions = (
                await session.scalars(
                    select(WorkflowVersion)
                    .where(WorkflowVersion.workflow_id == workflow_id)
                    .order_by(WorkflowVersion.version)
                )
            ).all()
            actions = (
                await session.scalars(
                    select(WorkflowLifecycleAction).where(
                        WorkflowLifecycleAction.workflow_id == workflow_id
                    )
                )
            ).all()
        assert workflow is not None
        assert workflow.latest_version == 4
        assert workflow.status == "ACTIVE"
        assert [version.active for version in versions] == [
            True,
            False,
            False,
            False,
        ]
        assert [version.review_status for version in versions] == [
            "APPROVED",
            "APPROVED",
            "REJECTED",
            "PENDING_REVIEW",
        ]
        assert len(actions) == 8
    finally:
        async with transaction(sessions) as session:
            if workflow_id is not None:
                await session.execute(
                    delete(WorkflowLifecycleAction).where(
                        WorkflowLifecycleAction.workflow_id == workflow_id
                    )
                )
            if task_ids:
                await session.execute(
                    delete(WorkflowPromotion).where(
                        WorkflowPromotion.task_id.in_(task_ids)
                    )
                )
            if workflow_id is not None:
                await session.execute(
                    delete(Workflow).where(Workflow.id == workflow_id)
                )
            if task_ids:
                await session.execute(
                    delete(Task).where(Task.id.in_(task_ids))
                )
        await engine.dispose()


async def _successful_task(
    repository: AgentRepository,
    registry: ToolRegistry,
    task_ids: list[UUID],
) -> UUID:
    task_id = uuid4()
    task_ids.append(task_id)
    manifest = registry.get_tool("fetch_dataset")
    step = PlanStepDraft(
        sequence=0,
        title="Fetch source data",
        purpose="Acquire data for the analysis",
        planning_kind=PlanningKind.TOOL_PLAN,
        skill=manifest.skill,
        tool=manifest.tool,
        parameters={
            "query": f"SELECT * FROM private_source_{task_id.hex}",
            "dataset_name": f"private-{task_id.hex}",
            "output_format": "parquet",
        },
        selection_rationale="The analysis requires source data",
        expected_outputs=["dataset"],
    )
    plan = PlanDraft(
        objective="데이터를 조회한다",
        strategy_summary="승인된 원천 데이터를 조회한다.",
        execution_mode=ExecutionMode.MULTI,
        steps=[step],
    )
    source = "def fetch_dataset():\n    return {}\n\nresult = fetch_dataset()"
    compiled = CompiledStep(
        sequence=0,
        source=source,
        source_sha256=sha256(source.encode()).hexdigest(),
        skill_name=manifest.skill.name,
        tool_name=manifest.tool.name,
        parameters=step.parameters,
    )
    await repository.create_task(
        task_id=task_id,
        input_message_id=uuid4(),
        user_id="lifecycle-owner",
        project_id="lifecycle-project",
        session_id=f"lifecycle-session-{task_id}",
        content="private lifecycle source request",
        idempotency_key=f"create-{task_id}",
    )
    persisted = await repository.persist_plan(
        task_id,
        plan,
        [(compiled, f"{task_id}/step-0000.py")],
        registry.registry_snapshot_hash(),
        None,
    )
    await repository.bind_execution(
        task_id=task_id,
        execution_id=uuid4(),
        operation_id=uuid4(),
        execution_version=1,
        next_step_sequence=1,
    )
    await repository.record_successful_execution_steps(
        task_id=task_id,
        operation_id=uuid4(),
        plan_id=persisted.plan_id,
        plan_revision_id=persisted.plan_revision_id,
        registry_snapshot_hash=registry.registry_snapshot_hash(),
        start_sequence=0,
        steps=[step],
    )
    await repository.update_status(task_id, TaskStatus.SUCCEEDED)
    return task_id
