from pathlib import Path
from typing import Any, cast

from ex_agent.application.services import (
    _executor_source_path,
    _state_execution_mode,
)
from ex_agent.domain.contracts import ExecutorReconciliation, RiskReview
from ex_agent.domain.enums import (
    ExecutionMode,
    ExecutorOutcome,
    PlanningKind,
    RiskLevel,
)
from ex_agent.graph.routes import route_code_risk, route_reconciliation


def test_execution_mode_is_restored_from_checkpoint_string() -> None:
    state = cast(Any, {"execution_mode": "SINGLE"})

    assert _state_execution_mode(state) is ExecutionMode.SINGLE


def test_executor_source_path_is_relative_to_request_root() -> None:
    shared_root = Path("/workspace/shared")
    source = shared_root / "requests/task-1/1/step-0000.py"

    assert _executor_source_path(source, shared_root) == (
        "task-1/1/step-0000.py"
    )


def test_routes_accept_checkpoint_enum_strings() -> None:
    code_risk_state = cast(
        Any,
        {
            "planning_kind": PlanningKind.FIXED_WORKFLOW.value,
            "code_risk": RiskReview(
                level=RiskLevel.LOW,
                summary="안전한 테스트 코드",
                recommended_action="ALLOW",
            ),
        },
    )
    reconciliation_state = cast(
        Any,
        {
            "execution_mode": ExecutionMode.MULTI.value,
            "executor_reconciliation": ExecutorReconciliation(
                outcome=ExecutorOutcome.OPERATION_SUCCEEDED,
                execution_id="565e15b4-0753-498f-8902-b44a5498d62b",
                execution_version=1,
            ).model_dump(mode="json"),
        },
    )

    assert route_code_risk(code_risk_state) == "verify_approval"
    assert route_reconciliation(reconciliation_state) == "adapt_multi_plan"
