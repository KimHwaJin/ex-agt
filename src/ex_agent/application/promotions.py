from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import UUID

from langchain_core.embeddings import Embeddings

from ex_agent.config import Settings
from ex_agent.domain.contracts import (
    PlanDraft,
    PlanStepDraft,
    WorkflowPromotionDraft,
    WorkflowPromotionRequest,
    WorkflowPromotionResult,
)
from ex_agent.domain.enums import ExecutionMode, PlanningKind, TaskStatus
from ex_agent.persistence.repositories.promotions import PromotionSource
from ex_agent.persistence.repository import AgentRepository
from ex_agent.tools.registry import ParameterSpec, ToolRegistry

_INPUT_NAME_PATTERN = re.compile(r"[^a-zA-Z0-9_]+")


class WorkflowPromotionNotEligibleError(RuntimeError):
    pass


class WorkflowPromotionForbiddenError(RuntimeError):
    pass


@dataclass(frozen=True)
class PromotionPolicyDecision:
    allowed: bool
    policy_version: str
    reason: str


@dataclass(frozen=True)
class PreparedWorkflowVersion:
    plan: PlanDraft
    input_contract: dict[str, dict[str, Any]]
    embedding: list[float]
    registry_snapshot_hash: str
    searchable_text: str


class PromotionPolicy(Protocol):
    def evaluate(
        self,
        *,
        actor_user_id: str,
        source: PromotionSource,
    ) -> PromotionPolicyDecision: ...


class AuthenticatedServicePromotionPolicy:
    """V1 policy with a replaceable future role/organization boundary."""

    version = "authenticated-owner-v1"

    def evaluate(
        self,
        *,
        actor_user_id: str,
        source: PromotionSource,
    ) -> PromotionPolicyDecision:
        allowed = bool(actor_user_id) and (
            source.task.user_id == actor_user_id
        )
        return PromotionPolicyDecision(
            allowed=allowed,
            policy_version=self.version,
            reason=(
                "authenticated source Task owner"
                if allowed
                else "actor does not own the source Task"
            ),
        )


class WorkflowPromotionService:
    def __init__(
        self,
        settings: Settings,
        repository: AgentRepository,
        registry: ToolRegistry,
        embeddings: Embeddings,
        *,
        policy: PromotionPolicy | None = None,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._registry = registry
        self._embeddings = embeddings
        self._policy = policy or AuthenticatedServicePromotionPolicy()

    async def draft(
        self,
        task_id: UUID,
        *,
        actor_user_id: str,
    ) -> WorkflowPromotionDraft:
        source = await self.eligible_source(task_id, actor_user_id)
        plan, input_contract = self._template_plan(source, {})
        return WorkflowPromotionDraft(
            task_id=task_id,
            eligible=True,
            suggested_name=plan.objective[:255],
            suggested_description=plan.strategy_summary[:4000],
            suggested_request_examples=[
                f"{plan.objective} 워크플로우를 실행해줘"
            ],
            suggested_tags=sorted(
                {
                    step.skill.name
                    for step in plan.steps
                    if step.skill is not None
                }
            ),
            steps=plan.steps,
            parameter_inputs=input_contract,
        )

    async def promote(
        self,
        task_id: UUID,
        *,
        actor_user_id: str,
        request: WorkflowPromotionRequest,
    ) -> WorkflowPromotionResult:
        request_hash = _request_hash(request)
        existing = await self._repository.existing_workflow_promotion(
            task_id=task_id,
            actor_user_id=actor_user_id,
            idempotency_key=request.idempotency_key,
            request_hash=request_hash,
        )
        if existing is not None:
            return existing
        source = await self.eligible_source(task_id, actor_user_id)
        decision = self._policy.evaluate(
            actor_user_id=actor_user_id,
            source=source,
        )
        prepared = await self.prepare_version(
            source=source,
            name=request.name,
            description=request.description,
            request_examples=request.request_examples,
            tags=request.tags,
            public_parameter_defaults=request.public_parameter_defaults,
        )
        return await self._repository.promote_workflow(
            source=source,
            actor_user_id=actor_user_id,
            idempotency_key=request.idempotency_key,
            request_hash=request_hash,
            name=request.name,
            description=request.description,
            request_examples=request.request_examples,
            tags=request.tags,
            plan=prepared.plan,
            input_contract=prepared.input_contract,
            embedding=prepared.embedding,
            embedding_model=self._settings.agent_embedding_model,
            registry_snapshot_hash=prepared.registry_snapshot_hash,
            policy_version=decision.policy_version,
            searchable_text=prepared.searchable_text,
        )

    async def eligible_source(
        self,
        task_id: UUID,
        actor_user_id: str,
    ) -> PromotionSource:
        source = await self._repository.workflow_promotion_source(task_id)
        decision = self._policy.evaluate(
            actor_user_id=actor_user_id,
            source=source,
        )
        if not decision.allowed:
            raise WorkflowPromotionForbiddenError(decision.reason)
        if source.task.status != TaskStatus.SUCCEEDED.value:
            raise WorkflowPromotionNotEligibleError(
                "Only succeeded Tasks can be promoted"
            )
        if source.task.execution_id is None:
            raise WorkflowPromotionNotEligibleError(
                "Task has no Executor execution lineage"
            )
        if not source.steps:
            raise WorkflowPromotionNotEligibleError(
                "Task has no verified successful execution Steps"
            )
        for row in source.steps:
            step = PlanStepDraft.model_validate(row.step_payload)
            if step.planning_kind is not PlanningKind.TOOL_PLAN:
                raise WorkflowPromotionNotEligibleError(
                    "CUSTOM_CODE Tasks cannot be promoted"
                )
            if step.skill is None or step.tool is None:
                raise WorkflowPromotionNotEligibleError(
                    "Tool lineage is incomplete"
                )
            try:
                manifest = self._registry.get_tool(step.tool.name)
            except KeyError as error:
                raise WorkflowPromotionNotEligibleError(str(error)) from error
            if manifest.skill != step.skill or manifest.tool != step.tool:
                raise WorkflowPromotionNotEligibleError(
                    "Skill/Tool registry lineage is no longer compatible"
                )
            if row.source_plan_revision_id not in source.plans_by_revision:
                raise WorkflowPromotionNotEligibleError(
                    "Source Plan revision lineage is incomplete"
                )
        return source

    async def prepare_version(
        self,
        *,
        source: PromotionSource,
        name: str,
        description: str,
        request_examples: list[str],
        tags: list[str],
        public_parameter_defaults: dict[str, Any],
    ) -> PreparedWorkflowVersion:
        plan, input_contract = self._template_plan(
            source,
            public_parameter_defaults,
        )
        plan = plan.model_copy(
            update={
                "objective": name,
                "strategy_summary": description,
                "assumptions": [],
                "expected_artifacts": [],
            }
        )
        searchable_text = _searchable_text(
            name,
            description,
            request_examples,
            tags,
            plan,
        )
        embedding = await self._embeddings.aembed_query(searchable_text)
        if len(embedding) != self._settings.agent_embedding_dimensions:
            raise ValueError(
                "Embedding dimension does not match the configured "
                "pgvector dimension"
            )
        registry_hashes = sorted(
            {row.registry_snapshot_hash for row in source.steps}
        )
        registry_snapshot_hash = (
            registry_hashes[0]
            if len(registry_hashes) == 1
            else hashlib.sha256(
                "\n".join(registry_hashes).encode()
            ).hexdigest()
        )
        return PreparedWorkflowVersion(
            plan=plan,
            input_contract=input_contract,
            embedding=embedding,
            registry_snapshot_hash=registry_snapshot_hash,
            searchable_text=searchable_text,
        )

    def _template_plan(
        self,
        source: PromotionSource,
        public_defaults: dict[str, Any],
    ) -> tuple[PlanDraft, dict[str, dict[str, Any]]]:
        first = source.steps[0]
        base = source.plans_by_revision[first.source_plan_revision_id]
        expected_input_names: set[str] = set()
        input_contract: dict[str, dict[str, Any]] = {}
        templated_steps: list[PlanStepDraft] = []
        for sequence, row in enumerate(source.steps):
            step = PlanStepDraft.model_validate(row.step_payload)
            if step.tool is None:
                raise WorkflowPromotionNotEligibleError(
                    "Tool lineage is incomplete"
                )
            manifest = self._registry.get_tool(step.tool.name)
            parameters: dict[str, Any] = {}
            for parameter_name in sorted(step.parameters):
                input_name = _input_name(sequence, parameter_name)
                expected_input_names.add(input_name)
                specification = manifest.parameters[parameter_name]
                if input_name in public_defaults:
                    default = public_defaults[input_name]
                    _validate_value(input_name, default, specification)
                    parameters[parameter_name] = default
                    continue
                parameters[parameter_name] = {"$workflow_input": input_name}
                input_contract[input_name] = {
                    "type": specification.type,
                    "required": True,
                    "step_sequence": sequence,
                    "parameter_name": parameter_name,
                }
            templated_steps.append(
                step.model_copy(
                    update={
                        "sequence": sequence,
                        "title": manifest.tool.name,
                        "purpose": manifest.description,
                        "selection_rationale": (manifest.creation_rationale),
                        "parameters": parameters,
                        "expected_outputs": [f"{manifest.tool.name} result"],
                        "validation_criteria": [],
                    }
                )
            )
        unknown_defaults = sorted(set(public_defaults) - expected_input_names)
        if unknown_defaults:
            raise ValueError(
                f"Unknown public parameter defaults: {unknown_defaults}"
            )
        return (
            base.model_copy(
                update={
                    "execution_mode": ExecutionMode.SINGLE,
                    "steps": templated_steps,
                }
            ),
            input_contract,
        )


def bind_workflow_inputs(
    plan: PlanDraft,
    input_contract: dict[str, dict[str, Any]],
    input_values: dict[str, Any],
) -> PlanDraft:
    expected = set(input_contract)
    missing = sorted(expected - set(input_values))
    unknown = sorted(set(input_values) - expected)
    if missing:
        raise ValueError(f"Missing Workflow input values: {missing}")
    if unknown:
        raise ValueError(f"Unknown Workflow input values: {unknown}")
    for name, specification in input_contract.items():
        _validate_value(
            name,
            input_values[name],
            ParameterSpec(
                type=str(specification["type"]),
                required=bool(specification.get("required", True)),
            ),
        )
    steps = []
    for step in plan.steps:
        parameters = {
            name: _resolve_parameter(value, input_values)
            for name, value in step.parameters.items()
        }
        steps.append(step.model_copy(update={"parameters": parameters}))
    return plan.model_copy(
        update={"execution_mode": ExecutionMode.SINGLE, "steps": steps}
    )


def _resolve_parameter(value: Any, input_values: dict[str, Any]) -> Any:
    if isinstance(value, dict) and set(value) == {"$workflow_input"}:
        return input_values[str(value["$workflow_input"])]
    return value


def _input_name(sequence: int, parameter_name: str) -> str:
    normalized = _INPUT_NAME_PATTERN.sub("_", parameter_name).strip("_")
    return f"step_{sequence}_{normalized or 'parameter'}"


def _validate_value(
    name: str,
    value: Any,
    specification: ParameterSpec,
) -> None:
    expected = specification.type
    valid = (
        (expected == "string" and isinstance(value, str))
        or (
            expected == "integer"
            and isinstance(value, int)
            and not isinstance(value, bool)
        )
        or (
            expected == "number"
            and isinstance(value, int | float)
            and not isinstance(value, bool)
        )
        or (expected == "boolean" and isinstance(value, bool))
        or (expected == "array" and isinstance(value, list))
        or (expected == "object" and isinstance(value, dict))
    )
    if not valid:
        raise ValueError(f"Workflow input {name!r} must be {expected}")


def _searchable_text(
    name: str,
    description: str,
    examples: list[str],
    tags: list[str],
    plan: PlanDraft,
) -> str:
    values = [
        name,
        description,
        plan.objective,
        plan.strategy_summary,
        *examples,
        *tags,
    ]
    for step in plan.steps:
        values.extend(
            [
                step.title,
                step.purpose,
                step.selection_rationale,
                *(step.expected_outputs),
                step.skill.name if step.skill else "",
                step.tool.name if step.tool else "",
            ]
        )
    return "\n".join(value.strip() for value in values if value.strip())


def _request_hash(request: WorkflowPromotionRequest) -> str:
    encoded = json.dumps(
        request.model_dump(mode="json"),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


__all__ = [
    "AuthenticatedServicePromotionPolicy",
    "PreparedWorkflowVersion",
    "PromotionPolicy",
    "PromotionPolicyDecision",
    "WorkflowPromotionForbiddenError",
    "WorkflowPromotionNotEligibleError",
    "WorkflowPromotionService",
    "bind_workflow_inputs",
]
