import ast
import os
import re
import subprocess
import sys
from pathlib import Path

import worker


def test_every_worker_module_imports_without_agent_or_graph(tmp_path):
    code = """
import importlib
import importlib.abc
import pkgutil
import sys

class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {
            'agent', 'api', 'ex_agent', 'langgraph', 'langchain',
            'langchain_core', 'fastapi', 'examples',
        }:
            raise ImportError(fullname)

sys.meta_path.insert(0, Block())
import worker
for info in pkgutil.walk_packages(worker.__path__, 'worker.'):
    importlib.import_module(info.name)
"""
    # Outside the repo, no cwd/PYTHONPATH fallback to source-tree imports.
    env = {
        key: value for key, value in os.environ.items() if key != "PYTHONPATH"
    }
    result = subprocess.run(
        [sys.executable, "-I", "-c", code],
        cwd=tmp_path,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


def test_worker_source_has_no_upward_imports():
    root = Path(worker.__file__).parent
    forbidden = {"agent", "api", "ex_agent", "langgraph", "langchain"}
    for path in root.rglob("*.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            names = []
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                names = [node.module]
            assert not {name.split(".")[0] for name in names} & forbidden, path


def test_formal_modules_are_installed_without_editable_source_fallback():
    import agent.integrations.langgraph_adapter
    import agent.worker_main

    for module in (worker, agent.worker_main):
        assert "site-packages" in Path(module.__file__).parts


def test_legacy_schema_bootstrap_is_not_public():
    from worker.store import Store

    assert not hasattr(Store, "migrate")


def test_worker_is_single_source_and_schema_revision_is_preserved():
    root = Path(__file__).resolve().parents[2]
    assert not (root / "standalone_worker").exists()
    assert not (root / "src/worker/langgraph_adapter.py").exists()
    revision = root / "worker_migrations/versions/0001_worker_tables.py"
    assert 'revision = "ew_0001"' in revision.read_text()


def test_worker_documentation_ships_with_module_and_links_resolve():
    root = Path(__file__).resolve().parents[2]
    module = root / "src/worker"
    installed = Path(worker.__file__).parent
    paths = [module / "README.md", *sorted((module / "docs").glob("*.md"))]
    assert len(paths) == 6
    for path in paths:
        assert (installed / path.relative_to(module)).is_file()
        for target in re.findall(r"\]\(([^()\s]+)\)", path.read_text()):
            if target.startswith(("https://", "http://", "#", "mailto:")):
                continue
            target_path = target.split("#", 1)[0]
            assert (path.parent / target_path).exists(), (path, target)


def test_handoff_guide_lists_every_portable_worker_setting():
    from worker.config import Settings

    root = Path(__file__).resolve().parents[2]
    guide = (root / "src/worker/docs/handoff-guide.md").read_text()
    environment = (root / "deploy/worker/.env.example").read_text()
    example_keys = {
        line.split("=", 1)[0]
        for line in environment.splitlines()
        if line and not line.startswith("#")
    }
    expected_keys = {
        f"EW_{field_name.upper()}" for field_name in Settings.model_fields
    }

    for variable in expected_keys:
        assert f"`{variable}`" in guide
    assert example_keys == expected_keys
