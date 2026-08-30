import ast
from pathlib import Path

from ex_agent.application.ports import (
    ConversationServices,
    ExecutionServices,
    LifecycleServices,
    PlanningServices,
    ReportingServices,
)
from ex_agent.application.services import DefaultWorkflowServices
from ex_agent.graph.nodes import WorkflowNodes
from ex_agent.worker import WorkflowWorker

_PACKAGE_ROOT = Path(__file__).parents[1] / "src" / "ex_agent"
_FORBIDDEN_PACKAGE_EDGES = {
    ("application", "api"),
    ("application", "graph"),
    ("persistence", "tools"),
}


def _package_name(path: Path) -> str:
    relative = path.relative_to(_PACKAGE_ROOT)
    return relative.parts[0].removesuffix(".py")


def _internal_imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(), filename=str(path))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.ImportFrom):
            continue
        module = node.module or ""
        if not module.startswith("ex_agent."):
            continue
        imported.add(module.split(".", 2)[1])
    return imported


def test_package_dependency_direction() -> None:
    violations: list[str] = []
    for path in sorted(_PACKAGE_ROOT.rglob("*.py")):
        source = _package_name(path)
        for target in _internal_imports(path):
            if (source, target) in _FORBIDDEN_PACKAGE_EDGES:
                violations.append(
                    f"{path.relative_to(_PACKAGE_ROOT)}: {source} -> {target}"
                )
            if source == "domain" and target != "domain":
                violations.append(
                    f"{path.relative_to(_PACKAGE_ROOT)}: domain -> {target}"
                )
    assert violations == []


def test_reusable_consumer_has_no_agent_domain_dependency() -> None:
    consumer = _PACKAGE_ROOT / "transport" / "consumer.py"

    assert _internal_imports(consumer) == set()


def test_default_services_implements_every_capability_contract() -> None:
    contracts = (
        ConversationServices,
        ExecutionServices,
        LifecycleServices,
        PlanningServices,
        ReportingServices,
    )
    required = {
        name
        for contract in contracts
        for name, value in vars(contract).items()
        if callable(value) and not name.startswith("_")
    }

    missing = sorted(
        name
        for name in required
        if not callable(getattr(DefaultWorkflowServices, name, None))
    )

    assert missing == []


def test_workflow_node_facade_covers_every_registered_node() -> None:
    builder = _PACKAGE_ROOT / "graph" / "builder.py"
    tree = ast.parse(builder.read_text(), filename=str(builder))
    registered = {
        call.args[0].value
        for call in ast.walk(tree)
        if isinstance(call, ast.Call)
        and isinstance(call.func, ast.Attribute)
        and call.func.attr == "add_node"
        and call.args
        and isinstance(call.args[0], ast.Constant)
        and isinstance(call.args[0].value, str)
    }
    missing = sorted(
        name
        for name in registered
        if not callable(getattr(WorkflowNodes, name, None))
    )

    assert len(registered) == 31
    assert missing == []


def test_worker_facade_preserves_runtime_contract() -> None:
    required = {
        "run",
        "close",
        "_command_consumer",
        "_executor_event_consumer",
        "_ensure_groups",
        "_ensure_group",
        "_next_retry_delay",
        "_metrics_loop",
        "_collect_runtime_metrics",
        "_set_stream_metrics",
        "_outbox_loop",
        "_handle_command",
        "_process_command",
        "_run_graph_command",
        "_run_failure_compensation",
        "_compensate_failed_execution",
        "_commands",
        "_handle_executor_event",
        "_process_executor_event",
        "_persist_executor_event",
        "_executor_events",
    }
    missing = sorted(
        name
        for name in required
        if not callable(getattr(WorkflowWorker, name, None))
    )

    assert missing == []
