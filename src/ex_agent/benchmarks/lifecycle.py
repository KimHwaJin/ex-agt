from __future__ import annotations

import asyncio
from collections import Counter
from dataclasses import dataclass
from hashlib import sha256
from time import perf_counter
from typing import Any, Literal
from uuid import UUID, uuid4

from langgraph.checkpoint.memory import InMemorySaver
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
from langgraph.types import Command, Interrupt

from ex_agent.domain.contracts import (
    ExecutorBoundarySignal,
    ExecutorReconciliation,
    IntentDecision,
    MultiDecision,
    PersistedPlan,
    PlanDraft,
    PlanStepDraft,
    ReportResult,
    RiskReview,
    SubmissionReceipt,
)
from ex_agent.domain.enums import (
    ExecutionMode,
    ExecutorOutcome,
    Intent,
    MultiAction,
    PlanningKind,
    RiskLevel,
    TaskStatus,
)
from ex_agent.graph.builder import build_workflow_graph
from ex_agent.graph.state import AgentGraphState

ScenarioKind = Literal["single_custom", "multi_analysis"]


@dataclass(frozen=True)
class LifecycleTiming:
    scenario: ScenarioKind
    total_seconds: float
    planning_seconds: float
    approval_to_executor_seconds: float
    executor_resume_seconds: float
    report_seconds: float
    executor_boundaries: int


class FakeLlm:
    """Deterministic stand-in for semantic decisions and generation."""

    def __init__(self, delay_seconds: float = 0) -> None:
        self._delay_seconds = delay_seconds
        self.calls: Counter[str] = Counter()

    async def respond(self, component: str) -> None:
        self.calls[component] += 1
        if self._delay_seconds:
            await asyncio.sleep(self._delay_seconds)


class FakeExecutor:
    """Deterministic stand-in for submit, append, and reconciliation."""

    def __init__(self, delay_seconds: float = 0) -> None:
        self._delay_seconds = delay_seconds
        self.calls: Counter[str] = Counter()

    async def wait(self, operation: str) -> None:
        self.calls[operation] += 1
        if self._delay_seconds:
            await asyncio.sleep(self._delay_seconds)


class LifecycleServices:
    """WorkflowServices implementation backed by Fake LLM and Executor."""

    def __init__(self, llm: FakeLlm, executor: FakeExecutor) -> None:
        self.llm = llm
        self.executor = executor
        self.calls: Counter[str] = Counter()
        self.statuses: dict[str, TaskStatus] = {}
        self.report_started_at: dict[str, float] = {}
        self._scenarios: dict[str, ScenarioKind] = {}
        self._multi_adaptations: Counter[str] = Counter()

    def register(self, task_id: UUID, scenario: ScenarioKind) -> None:
        self._scenarios[str(task_id)] = scenario

    async def update_status(
        self,
        state: AgentGraphState,
        status: TaskStatus,
    ) -> None:
        self.calls["update_status"] += 1
        self.statuses[state["active_task_id"]] = status

    async def classify_intent(
        self,
        state: AgentGraphState,
    ) -> IntentDecision:
        await self.llm.respond("classify_intent")
        scenario = self._scenario(state)
        intent = (
            Intent.CODE_EXECUTION
            if scenario == "single_custom"
            else Intent.DATA_ANALYSIS_EXECUTION
        )
        return IntentDecision(
            intent=intent,
            confidence=1,
            decision_summary="Deterministic benchmark route",
            requires_execution_mode=intent is Intent.CODE_EXECUTION,
        )

    async def answer_question(
        self,
        state: AgentGraphState,
        *,
        data_analysis: bool,
    ) -> str:
        del state, data_analysis
        await self.llm.respond("answer_question")
        return "benchmark answer"

    async def commit_answer(
        self,
        state: AgentGraphState,
        answer: str,
    ) -> None:
        del state, answer
        self.calls["commit_answer"] += 1

    async def review_request_risk(
        self,
        state: AgentGraphState,
    ) -> RiskReview:
        del state
        await self.llm.respond("review_request_risk")
        return _low_risk()

    async def search_workflows(
        self,
        state: AgentGraphState,
    ) -> list[Any]:
        del state
        self.calls["search_workflows"] += 1
        return []

    async def load_selected_workflow(
        self,
        state: AgentGraphState,
    ) -> tuple[PlanDraft, PersistedPlan]:
        raise AssertionError(f"No fixed Workflow expected: {state}")

    async def build_plan(
        self,
        state: AgentGraphState,
    ) -> PlanDraft:
        await self.llm.respond("build_plan")
        scenario = self._scenario(state)
        planning_kind = (
            PlanningKind.CUSTOM_CODE
            if scenario == "single_custom"
            else PlanningKind.TOOL_PLAN
        )
        return PlanDraft(
            objective="Measure the complete Agent lifecycle",
            strategy_summary="Execute one deterministic benchmark step",
            execution_mode=(
                ExecutionMode.SINGLE
                if scenario == "single_custom"
                else ExecutionMode.MULTI
            ),
            steps=[_plan_step(planning_kind, sequence=0)],
        )

    async def compile_and_persist_plan(
        self,
        state: AgentGraphState,
        plan: PlanDraft,
    ) -> PersistedPlan:
        del plan
        self.calls["compile_and_persist_plan"] += 1
        revision = state.get("plan_revision_number", 0) + 1
        digest = sha256(
            f"{state['active_task_id']}:{revision}".encode()
        ).hexdigest()
        return PersistedPlan(
            plan_id=uuid4(),
            plan_revision_id=uuid4(),
            plan_revision_number=revision,
            public_payload_hash=digest,
            compiled_bundle_id=uuid4(),
        )

    async def review_compiled_code_risk(
        self,
        state: AgentGraphState,
    ) -> RiskReview:
        del state
        await self.llm.respond("review_compiled_code_risk")
        return _low_risk()

    async def verify_approval_and_lock(
        self,
        state: AgentGraphState,
    ) -> None:
        if state.get("review_action") != "APPROVE":
            raise AssertionError("Benchmark plan was not approved")
        self.calls["verify_approval_and_lock"] += 1

    async def submit_execution(
        self,
        state: AgentGraphState,
    ) -> SubmissionReceipt:
        del state
        await self.executor.wait("submit_execution")
        return SubmissionReceipt(
            execution_id=uuid4(),
            operation_id=uuid4(),
            execution_version=1,
        )

    async def reconcile_executor(
        self,
        state: AgentGraphState,
        signal: ExecutorBoundarySignal,
    ) -> ExecutorReconciliation:
        del state
        await self.executor.wait("reconcile_executor")
        outcome = (
            ExecutorOutcome.SUCCEEDED
            if signal.event_type == "execution.completed"
            else ExecutorOutcome.OPERATION_SUCCEEDED
        )
        return ExecutorReconciliation(
            outcome=outcome,
            execution_id=signal.execution_id,
            execution_version=signal.event_sequence,
            operation_id=uuid4(),
            result_refs=[f"result://{signal.event_sequence}"],
        )

    async def adapt_multi_plan(
        self,
        state: AgentGraphState,
        reconciliation: ExecutorReconciliation,
    ) -> MultiDecision:
        del reconciliation
        await self.llm.respond("adapt_multi_plan")
        task_id = state["active_task_id"]
        self._multi_adaptations[task_id] += 1
        if self._multi_adaptations[task_id] == 1:
            return MultiDecision(
                action=MultiAction.APPEND_STEP,
                rationale="Inspect one additional deterministic cell",
                next_step=_plan_step(PlanningKind.TOOL_PLAN, sequence=0),
            )
        return MultiDecision(
            action=MultiAction.FINALIZE,
            rationale="The deterministic analysis is complete",
        )

    async def append_operation(
        self,
        state: AgentGraphState,
        decision: MultiDecision,
    ) -> SubmissionReceipt:
        del state, decision
        await self.executor.wait("append_operation")
        return SubmissionReceipt(
            execution_id=uuid4(),
            operation_id=uuid4(),
            execution_version=2,
        )

    async def finalize_execution(self, state: AgentGraphState) -> None:
        del state
        await self.executor.wait("finalize_execution")

    async def cancel_execution(
        self,
        state: AgentGraphState,
        reason: str | None,
    ) -> None:
        del state, reason
        raise AssertionError("Benchmark execution must not be cancelled")

    async def build_report_evidence(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        self.calls["build_report_evidence"] += 1
        self.report_started_at[state["active_task_id"]] = perf_counter()
        return {
            "task_id": state["active_task_id"],
            "result_refs": ["result://benchmark"],
        }

    async def generate_and_materialize_report(
        self,
        state: AgentGraphState,
        evidence: dict[str, Any],
    ) -> ReportResult:
        del state, evidence
        await self.llm.respond("generate_report")
        await self.executor.wait("materialize_report")
        return ReportResult(
            markdown="# Benchmark report\n\nExecution succeeded.",
            artifact_id=uuid4(),
        )

    async def commit_terminal(
        self,
        state: AgentGraphState,
        *,
        status: TaskStatus,
        message: str,
    ) -> None:
        del message
        self.calls["commit_terminal"] += 1
        self.statuses[state["active_task_id"]] = status

    def _scenario(self, state: AgentGraphState) -> ScenarioKind:
        return self._scenarios[state["active_task_id"]]


class LifecycleHarness:
    def __init__(
        self,
        *,
        llm_delay_seconds: float = 0,
        executor_delay_seconds: float = 0,
    ) -> None:
        self.services = LifecycleServices(
            FakeLlm(llm_delay_seconds),
            FakeExecutor(executor_delay_seconds),
        )
        self.graph = build_workflow_graph(
            self.services,
            checkpointer=InMemorySaver(
                serde=JsonPlusSerializer(
                    allowed_msgpack_modules=[
                        ("ex_agent.domain.contracts", "IntentDecision"),
                        ("ex_agent.domain.contracts", "PlanDraft"),
                        ("ex_agent.domain.contracts", "RiskReview"),
                        ("ex_agent.domain.enums", "ExecutionMode"),
                        ("ex_agent.domain.enums", "Intent"),
                        ("ex_agent.domain.enums", "PlanningKind"),
                        ("ex_agent.domain.enums", "RiskLevel"),
                        ("ex_agent.domain.enums", "TaskStatus"),
                    ]
                )
            ),
        )

    async def run(self, scenario: ScenarioKind) -> LifecycleTiming:
        task_id = uuid4()
        self.services.register(task_id, scenario)
        config = {"configurable": {"thread_id": str(task_id)}}
        started_at = perf_counter()
        result = await self.graph.ainvoke(
            _initial_state(task_id),
            config=config,
        )

        if _interrupt(result)["kind"] == "EXECUTION_MODE":
            result = await self.graph.ainvoke(
                Command(
                    resume={
                        "type": "EXECUTION_MODE",
                        "mode": "SINGLE",
                    }
                ),
                config=config,
            )
        review = _interrupt(result)
        if review["kind"] != "PLAN_REVIEW":
            raise AssertionError(f"Expected PLAN_REVIEW, got {review}")
        planning_done_at = perf_counter()

        result = await self.graph.ainvoke(
            Command(resume=_approval(review)),
            config=config,
        )
        executor_wait = _interrupt(result)
        if executor_wait["kind"] != "EXECUTOR_EVENT":
            raise AssertionError(
                f"Expected EXECUTOR_EVENT, got {executor_wait}"
            )
        submitted_at = perf_counter()

        boundary_started_at = perf_counter()
        boundary_count = 0
        if scenario == "single_custom":
            result = await self._resume_executor(
                config,
                executor_wait,
                sequence=1,
                event_type="execution.completed",
            )
            boundary_count = 1
        else:
            for sequence, event_type in (
                (1, "execution.operation_completed"),
                (2, "execution.operation_completed"),
                (3, "execution.completed"),
            ):
                result = await self._resume_executor(
                    config,
                    executor_wait,
                    sequence=sequence,
                    event_type=event_type,
                )
                boundary_count += 1
                if sequence < 3:
                    executor_wait = _interrupt(result)
                    if executor_wait["kind"] != "EXECUTOR_EVENT":
                        raise AssertionError(
                            "MULTI did not return to Executor wait"
                        )
        finished_at = perf_counter()
        if result.get("phase") != TaskStatus.SUCCEEDED:
            raise AssertionError(f"Lifecycle did not succeed: {result}")
        report_started_at = self.services.report_started_at[str(task_id)]
        return LifecycleTiming(
            scenario=scenario,
            total_seconds=finished_at - started_at,
            planning_seconds=planning_done_at - started_at,
            approval_to_executor_seconds=submitted_at - planning_done_at,
            executor_resume_seconds=report_started_at - boundary_started_at,
            report_seconds=finished_at - report_started_at,
            executor_boundaries=boundary_count,
        )

    async def _resume_executor(
        self,
        config: dict[str, Any],
        payload: dict[str, Any],
        *,
        sequence: int,
        event_type: Literal[
            "execution.operation_completed",
            "execution.completed",
        ],
    ) -> dict[str, Any]:
        return await self.graph.ainvoke(
            Command(
                resume={
                    "type": "EXECUTOR_BOUNDARY",
                    "execution_id": payload["execution_id"],
                    "event_id": str(uuid4()),
                    "event_sequence": sequence,
                    "event_type": event_type,
                }
            ),
            config=config,
        )


async def run_lifecycle_batch(
    *,
    scenario: ScenarioKind,
    requests: int,
    concurrency: int,
    llm_delay_seconds: float = 0,
    executor_delay_seconds: float = 0,
) -> list[LifecycleTiming]:
    if requests < 1:
        raise ValueError("requests must be positive")
    if concurrency < 1:
        raise ValueError("concurrency must be positive")
    harness = LifecycleHarness(
        llm_delay_seconds=llm_delay_seconds,
        executor_delay_seconds=executor_delay_seconds,
    )
    semaphore = asyncio.Semaphore(concurrency)

    async def run_one() -> LifecycleTiming:
        async with semaphore:
            return await harness.run(scenario)

    return await asyncio.gather(*(run_one() for _ in range(requests)))


def _initial_state(task_id: UUID) -> AgentGraphState:
    return {
        "user_id": "benchmark-user",
        "project_id": "benchmark-project",
        "session_id": f"benchmark-session-{task_id}",
        "active_task_id": str(task_id),
        "current_input_message_id": str(uuid4()),
        "user_message": "Run the deterministic lifecycle benchmark",
        "runtime_profile": "basic",
    }


def _plan_step(
    planning_kind: PlanningKind,
    *,
    sequence: int,
) -> PlanStepDraft:
    common: dict[str, Any] = {
        "sequence": sequence,
        "title": "Deterministic benchmark step",
        "purpose": "Measure graph orchestration without external variance",
        "planning_kind": planning_kind,
        "selection_rationale": "Fixed benchmark fixture",
        "expected_outputs": ["benchmark result"],
    }
    if planning_kind is PlanningKind.CUSTOM_CODE:
        common["custom_code"] = (
            "def benchmark_step():\n    return 1\n\nresult = benchmark_step()"
        )
    else:
        digest = "0" * 64
        common["skill"] = {
            "name": "descriptive-statistics",
            "version": "1.0.0",
            "content_sha256": digest,
        }
        common["tool"] = {
            "name": "summarize-sample-data",
            "version": "1.0.0",
            "source_sha256": digest,
        }
    return PlanStepDraft.model_validate(common)


def _low_risk() -> RiskReview:
    return RiskReview(
        level=RiskLevel.LOW,
        summary="Deterministic benchmark input is safe",
        recommended_action="ALLOW",
    )


def _interrupt(result: dict[str, Any]) -> dict[str, Any]:
    interrupts = result.get("__interrupt__")
    if not interrupts:
        raise AssertionError(f"Expected graph interrupt: {result}")
    interrupt = interrupts[0]
    raw = interrupt.value if isinstance(interrupt, Interrupt) else interrupt
    if not isinstance(raw, dict):
        raise TypeError("Interrupt payload must be a dictionary")
    return raw


def _approval(review: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "PLAN_REVIEW",
        "decision": "APPROVE",
        "plan_revision_id": review["plan_revision_id"],
        "plan_revision_number": review["plan_revision_number"],
        "public_payload_hash": review["public_payload_hash"],
    }
