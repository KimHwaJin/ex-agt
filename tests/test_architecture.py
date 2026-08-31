import ast
import importlib.util
import shutil
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from ex_agent.application.ports import (
    ConversationServices,
    ExecutionServices,
    LifecycleServices,
    PlanningServices,
    ReportingServices,
)
from ex_agent.application.services import DefaultWorkflowServices
from ex_agent.graph.builder import build_workflow_graph
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
            if target == "dev_chat" and source != "dev_chat":
                violations.append(
                    f"{path.relative_to(_PACKAGE_ROOT)}: {source} -> dev_chat"
                )
            if source == "dev_chat" and target not in {
                "dev_chat",
                "api",
                "domain",
            }:
                violations.append(
                    f"{path.relative_to(_PACKAGE_ROOT)}: dev_chat -> {target}"
                )
    assert violations == []


@pytest.mark.parametrize(
    "filename",
    ["consumer.py", "dlq.py", "stream_maintenance.py"],
)
def test_reusable_redis_modules_have_no_agent_domain_dependency(
    filename: str,
) -> None:
    module = _PACKAGE_ROOT / "transport" / filename

    assert _internal_imports(module) == set()


@pytest.mark.parametrize(
    ("filename", "public_type"),
    [
        ("consumer.py", "RedisStreamConsumer"),
        ("dlq.py", "DeadLetterManager"),
        ("stream_maintenance.py", "SafeStreamTrimmer"),
    ],
)
def test_reusable_consumer_loads_as_a_standalone_file(
    tmp_path: Path,
    filename: str,
    public_type: str,
) -> None:
    source = _PACKAGE_ROOT / "transport" / filename
    standalone = tmp_path / f"standalone_{filename}"
    shutil.copyfile(source, standalone)
    module_name = "standalone_consumer"
    spec = importlib.util.spec_from_file_location(module_name, standalone)

    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.modules.pop(module_name, None)

    assert getattr(module, public_type).__module__ == module_name


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
    # Test the compiled topology, independent of literal vs. table wiring.
    graph = build_workflow_graph(MagicMock()).get_graph()
    registered = set(graph.nodes) - {"__start__", "__end__"}
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
        "shutdown",
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
        "_record_command_failure",
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
