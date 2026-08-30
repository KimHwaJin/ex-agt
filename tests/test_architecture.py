import ast
from pathlib import Path

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
