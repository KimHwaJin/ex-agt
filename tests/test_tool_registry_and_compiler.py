from pathlib import Path

import pytest

from ex_agent.domain.contracts import PlanStepDraft
from ex_agent.domain.enums import PlanningKind
from ex_agent.tools.compiler import SourceCompiler
from ex_agent.tools.registry import ToolRegistry


def _registry() -> ToolRegistry:
    registry = ToolRegistry(Path("skills"))
    registry.load()
    return registry


def test_tool_step_compiles_canonical_source_with_lineage() -> None:
    registry = _registry()
    manifest = registry.get_tool("fetch_dataset")
    step = PlanStepDraft(
        sequence=0,
        title="샘플 데이터 준비",
        purpose="분석 가능한 샘플 데이터를 생성한다.",
        planning_kind=PlanningKind.TOOL_PLAN,
        skill=manifest.skill,
        tool=manifest.tool,
        parameters={
            "query": "SELECT * FROM orders",
            "dataset_name": "orders",
        },
        selection_rationale="실데이터 연결 전 다운로드 경계를 검증한다.",
    )

    compiled = SourceCompiler(registry).compile(step)

    assert "def fetch_dataset(" in compiled.source
    assert "result = fetch_dataset(" in compiled.source
    assert compiled.source.endswith(")\nresult\n")
    assert compiled.skill_name == "data-access"
    assert compiled.tool_name == "fetch_dataset"
    assert len(compiled.source_sha256) == 64


def test_custom_code_requires_one_definition_and_call() -> None:
    registry = _registry()
    compiler = SourceCompiler(registry)
    step = PlanStepDraft(
        sequence=0,
        title="코드 실행",
        purpose="요청 코드를 실행한다.",
        planning_kind=PlanningKind.CUSTOM_CODE,
        custom_code="def run():\n    return 1\n\nresult = run()",
        selection_rationale="사용자가 자유 코드 실행을 요청했다.",
    )
    assert "result = run()" in compiler.compile(step).source

    invalid = step.model_copy(
        update={"custom_code": "print('one')\nprint('two')"}
    )
    with pytest.raises(ValueError, match="exactly one function"):
        compiler.compile(invalid)


@pytest.mark.parametrize(
    "invocation",
    [
        "result = run()\nprint(result)",
        "print(run())",
    ],
)
def test_custom_code_allows_output_around_one_function_call(
    invocation: str,
) -> None:
    step = PlanStepDraft(
        sequence=0,
        title="코드 실행",
        purpose="함수 결과를 출력한다.",
        planning_kind=PlanningKind.CUSTOM_CODE,
        custom_code=f"def run():\n    return 2\n\n{invocation}",
        selection_rationale="함수 호출 결과 확인",
    )

    compiled = SourceCompiler(_registry()).compile(step)

    assert invocation in compiled.source


def test_custom_code_rejects_multiple_defined_function_calls() -> None:
    step = PlanStepDraft(
        sequence=0,
        title="코드 실행",
        purpose="함수를 중복 호출한다.",
        planning_kind=PlanningKind.CUSTOM_CODE,
        custom_code=(
            "def run():\n    return 2\n\nfirst = run()\nsecond = run()"
        ),
        selection_rationale="회귀 테스트",
    )

    with pytest.raises(ValueError, match="invoke the function"):
        SourceCompiler(_registry()).compile(step)


def test_registry_snapshot_changes_are_content_addressed() -> None:
    registry = _registry()
    assert len(registry.list_skills()) == 5
    assert len(registry.registry_snapshot_hash()) == 64


def test_registry_canonicalizes_model_supplied_lineage_metadata() -> None:
    registry = _registry()
    manifest = registry.get_tool("fetch_dataset")
    step = PlanStepDraft(
        sequence=0,
        title="샘플 데이터 준비",
        purpose="분석용 데이터를 생성한다.",
        planning_kind=PlanningKind.TOOL_PLAN,
        skill=manifest.skill.model_copy(update={"version": "invented"}),
        tool=manifest.tool.model_copy(update={"version": "invented"}),
        parameters={
            "query": "SELECT 1",
            "dataset_name": "sample",
        },
        selection_rationale="분석 입력이 필요하다.",
    )

    normalized = registry.canonicalize_step_lineage(step)

    assert normalized.skill == manifest.skill
    assert normalized.tool == manifest.tool
