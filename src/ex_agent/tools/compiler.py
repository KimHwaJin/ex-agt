from __future__ import annotations

import ast
import hashlib
import pprint
from pathlib import Path
from typing import Any

from ex_agent.domain.contracts import CompiledStep, PlanStepDraft
from ex_agent.domain.enums import PlanningKind
from ex_agent.executor.files import materialize_input_file
from ex_agent.tools.registry import ParameterSpec, ToolRegistry


class SourceCompiler:
    def __init__(self, registry: ToolRegistry) -> None:
        self._registry = registry

    def compile(self, step: PlanStepDraft) -> CompiledStep:
        if step.planning_kind is PlanningKind.TOOL_PLAN:
            return self._compile_tool_step(step)
        if step.planning_kind is PlanningKind.CUSTOM_CODE:
            return self._compile_custom_step(step)
        raise ValueError(f"Cannot compile planning kind: {step.planning_kind}")

    def materialize(
        self,
        compiled: CompiledStep,
        root: Path,
        task_id: str,
        revision: int,
    ) -> Path:
        result = materialize_input_file(
            root,
            (f"{task_id}/{revision}/step-{compiled.sequence:04d}.py"),
            compiled.source,
        )
        if result.sha256 != compiled.source_sha256:
            raise ValueError("Materialized source checksum changed")
        return result.absolute_path

    def _compile_tool_step(self, step: PlanStepDraft) -> CompiledStep:
        if step.tool is None or step.skill is None:
            raise ValueError("Tool Step is missing lineage")
        manifest = self._registry.get_tool(step.tool.name)
        if manifest.tool != step.tool or manifest.skill != step.skill:
            raise ValueError("Plan Step lineage does not match the registry")
        _validate_parameters(step.parameters, manifest.parameters)
        arguments = "\n".join(
            f"    {name}={pprint.pformat(value, sort_dicts=True)},"
            for name, value in sorted(step.parameters.items())
        )
        invocation = (
            f"\n\nresult = {manifest.tool.name}(\n{arguments}\n)\nresult\n"
        )
        source = manifest.source.rstrip() + invocation
        return CompiledStep(
            sequence=step.sequence,
            source=source,
            source_sha256=_sha256(source),
            skill_name=manifest.skill.name,
            tool_name=manifest.tool.name,
            parameters=step.parameters,
        )

    def _compile_custom_step(self, step: PlanStepDraft) -> CompiledStep:
        source = (step.custom_code or "").strip() + "\n"
        _validate_custom_source(source)
        return CompiledStep(
            sequence=step.sequence,
            source=source,
            source_sha256=_sha256(source),
            skill_name=None,
            tool_name=None,
            parameters=step.parameters,
        )


def _validate_parameters(
    values: dict[str, Any],
    schema: dict[str, ParameterSpec],
) -> None:
    unknown = sorted(set(values) - set(schema))
    if unknown:
        raise ValueError(f"Unknown Tool parameters: {unknown}")
    missing = sorted(
        name
        for name, parameter in schema.items()
        if parameter.required and name not in values
    )
    if missing:
        raise ValueError(f"Missing required Tool parameters: {missing}")
    for name, value in values.items():
        expected = schema[name].type
        if not _matches_type(value, expected):
            raise ValueError(
                f"Parameter {name!r} must have JSON type {expected}"
            )


def _matches_type(value: Any, expected: str) -> bool:
    if expected == "string":
        return isinstance(value, str)
    if expected == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected == "number":
        return isinstance(value, int | float) and not isinstance(value, bool)
    if expected == "boolean":
        return isinstance(value, bool)
    if expected == "array":
        return isinstance(value, list)
    if expected == "object":
        return isinstance(value, dict)
    return False


def _validate_custom_source(source: str) -> None:
    tree = ast.parse(source)
    functions = [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
    ]
    if len(functions) != 1:
        raise ValueError("Custom cell must define exactly one function")
    function_name = functions[0].name
    invocations = [
        candidate
        for node in tree.body
        if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
        for candidate in ast.walk(node)
        if isinstance(candidate, ast.Call)
        and isinstance(candidate.func, ast.Name)
        and candidate.func.id == function_name
    ]
    if len(invocations) != 1:
        raise ValueError("Custom cell must invoke the function it defines")


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
