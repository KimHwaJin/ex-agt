"""Only trusted, repository-owned functions are executed in these tests."""

import ast

import pytest
from pydantic import ValidationError

from d_test.agent_service.catalog.compiler import (
    compile_proposal,
    parse_arguments,
    render_cell,
)
from d_test.agent_service.catalog.registry import digest, load_catalog
from d_test.agent_service.domain.code_plans import (
    CatalogProposal,
    GeneratedProposal,
)


def catalog_proposal():
    tools = load_catalog()
    return CatalogProposal.model_validate(
        {
            "steps": [
                {
                    "skill_id": tool.skill_id,
                    "skill_version": tool.version,
                    "tool_id": tool.tool_id,
                    "tool_version": tool.version,
                    "description": tool.description,
                    "reason": "테스트 목적",
                    "expected_result": "예상 출력",
                    "parameters": (
                        {"query": "test", "rows": 50} if index == 0 else {}
                    ),
                    "input_step": None if index == 0 else 1,
                }
                for index, tool in enumerate(tools)
            ]
        }
    )


def test_catalog_compiles_and_trusted_cells_run_without_service_imports():
    proposal = catalog_proposal()
    steps, cells = compile_proposal(proposal)
    namespace = {}
    for step, cell in zip(steps, cells, strict=True):
        assert "code" not in step and "function_source" not in step
        tree = ast.parse(cell["code"])
        assert len(tree.body) == 2
        assert isinstance(tree.body[0], ast.FunctionDef)
        assert isinstance(tree.body[1], ast.Assign)
        assert cell["code_sha256"] == digest(cell["code"])
        assert cell["source_sha256"] == digest(cell["function_source"])
        assert "agent_service" not in cell["code"]
        # Fixed repository sources only; never model-generated code.
        exec(cell["code"], namespace)
    assert len(namespace["step_1"]) == 50
    assert namespace["step_2"]["rows"] == 50
    assert namespace["step_3"]["count"] == 50
    assert namespace["step_4"].startswith("<svg")


@pytest.mark.parametrize(
    "change",
    [
        {"tool_id": "not-real"},
        {"skill_id": "not-real"},
        {"skill_id": "sample-data@0.1.0"},
        {"skill_version": "0.2.0"},
        {"tool_version": "0.2.0"},
        {"input_step": 1},
        {"parameters": {"query": "x", "rows": 0}},
        {"parameters": {"query": "x", "rows": "50"}},
        {"parameters": {"query": "x", "extra": 2}},
        {"parameters": {"query": "x", "seed": float("nan")}},
        {"parameters": {"query": "x", "seed": float("inf")}},
        {"parameters": []},
    ],
)
def test_invalid_catalog_selection_is_rejected(change):
    proposal = catalog_proposal()
    proposal.steps[0] = proposal.steps[0].model_copy(update=change)
    with pytest.raises((ValueError, ValidationError)):
        compile_proposal(proposal)


def test_catalog_rejects_result_type_mismatch():
    proposal = catalog_proposal()
    proposal.steps[2].input_step = 2
    with pytest.raises(ValueError, match="records"):
        compile_proposal(proposal)


def test_literal_arguments_do_not_become_code():
    proposal = catalog_proposal()
    attack = "'); __import__('os').system('echo NEVER'); #"
    proposal.steps[0].parameters = {"query": attack}
    _, cells = compile_proposal(proposal)
    call = ast.parse(cells[0]["code"]).body[1].value
    query = next(kw.value for kw in call.keywords if kw.arg == "query")
    assert isinstance(query, ast.Constant) and query.value == attack


def test_generated_compilation_does_not_load_catalog(monkeypatch):
    def forbidden():
        raise AssertionError("Free code must not load Skill/Tool catalog")

    monkeypatch.setattr(
        "d_test.agent_service.catalog.compiler.load_catalog", forbidden
    )
    proposal = GeneratedProposal.model_validate(
        {
            "steps": [
                {
                    "description": "계산",
                    "reason": "직접 작성 요청",
                    "expected_result": "합계",
                    "parameters": {},
                    "input_step": None,
                    "function_lines": [
                        "def compute():",
                        "    return sum(range(11))",
                    ],
                }
            ]
        }
    )
    steps, cells = compile_proposal(proposal)
    assert "tool_id" not in steps[0]
    assert cells[0]["code"].endswith("step_1 = compute()\n")


def test_raw_duplicate_json_keys_are_rejected():
    with pytest.raises(ValueError, match="Duplicate"):
        parse_arguments('{"query":"x","query":"y"}')


@pytest.mark.parametrize(
    "source",
    [
        "print('side effect')\ndef f():\n    return 1",
        "@dangerous\ndef f():\n    return 1",
        "def f(x=dangerous()):\n    return x",
        "def f(x: dangerous()):\n    return x",
        "async def f():\n    return 1",
        "def f():\n    def nested():\n        pass\n    return 1",
        "def step_1():\n    return 1",
        "def f(required):\n    return required",
    ],
)
def test_generated_structure_and_signature_validation(source):
    with pytest.raises(ValueError):
        render_cell(source, {}, None, 1)


def test_generated_dependency_is_earlier_cell_only():
    with pytest.raises(ValueError, match="earlier"):
        render_cell("def f(data):\n    return data", {}, 2, 2)


def test_input_binds_to_first_parameter_without_rewriting_source():
    source = "def describe(df):\n    return len(df)"
    code = render_cell(source, {}, 1, 2)
    assert code.startswith(source)
    assert code.endswith("step_2 = describe(df=step_1)\n")
    with pytest.raises(ValueError, match="conflicts"):
        render_cell(source, {"df": "not a result"}, 1, 2)
