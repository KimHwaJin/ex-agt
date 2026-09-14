"""Validate and assemble source as data. No eval/exec or function calls."""

import ast
import json
import keyword

from d_test.agent_service.domain.code_plans import (
    CatalogProposal,
    CatalogStep,
    GeneratedProposal,
)

from .registry import digest, load_catalog


def function_definition(source):
    tree = ast.parse(source)
    if len(tree.body) != 1 or not isinstance(tree.body[0], ast.FunctionDef):
        raise ValueError("A cell must contain one function definition")
    function = tree.body[0]
    if (
        function.decorator_list
        or function.returns
        or function.type_comment
        or function.name.startswith("step_")
        or function.args.posonlyargs
        or function.args.vararg
        or function.args.kwarg
        or any(
            arg.annotation
            for arg in ast.walk(function.args)
            if isinstance(arg, ast.arg)
        )
        or sum(
            isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
            for n in ast.walk(tree)
        )
        != 1
    ):
        raise ValueError("Unsupported function definition structure")
    for default in [*function.args.defaults, *function.args.kw_defaults]:
        if default is not None:
            ast.literal_eval(default)
    # Syntax compilation is validation only; the resulting code is discarded.
    compile(tree, "<planned-function>", "exec")
    return function


def parse_arguments(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("Duplicate JSON argument")
            result[key] = value
        return result

    def invalid_constant(_value):
        raise ValueError("Arguments must use finite JSON values")

    result = json.loads(
        raw, object_pairs_hook=pairs, parse_constant=invalid_constant
    )
    if not isinstance(result, dict):
        raise ValueError("Arguments must be a JSON object")
    # Detect overflowed floating point numbers, including nested values.
    json.dumps(result, allow_nan=False)
    if any(not k.isidentifier() or keyword.iskeyword(k) for k in result):
        raise ValueError("Invalid keyword argument")
    if "data" in result:
        raise ValueError("Use input_step for data, not inline data arguments")
    return result


def render_cell(source, arguments, input_step, index):
    function = function_definition(source)
    arguments = dict(arguments)
    names = {
        arg.arg for arg in [*function.args.args, *function.args.kwonlyargs]
    }
    required = {
        arg.arg
        for arg in function.args.args[
            : len(function.args.args) - len(function.args.defaults)
        ]
    } | {
        arg.arg
        for arg, default in zip(
            function.args.kwonlyargs, function.args.kw_defaults, strict=True
        )
        if default is None
    }
    input_name = input_parameter(function, input_step)
    if input_name in arguments:
        raise ValueError("Input result conflicts with a literal parameter")
    supplied = set(arguments) | ({input_name} if input_name else set())
    if not required <= supplied or not supplied <= names:
        raise ValueError("Function arguments do not match its signature")
    if input_step is not None and not 1 <= input_step < index:
        raise ValueError("Input must reference an earlier cell")
    values = {key: repr(value) for key, value in arguments.items()}
    if input_step:
        values[input_name] = f"step_{input_step}"
    call = ", ".join(f"{key}={values[key]}" for key in sorted(values))
    code = f"{source.rstrip()}\n\nstep_{index} = {function.name}({call})\n"
    compile(code, "<planned-cell>", "exec")
    return code


def input_parameter(function, input_step):
    if input_step is None:
        return None
    if not function.args.args:
        raise ValueError("An input result requires a first function parameter")
    return function.args.args[0].arg


def compile_proposal(proposal):
    catalog = isinstance(proposal, CatalogProposal)
    if not catalog and not isinstance(proposal, GeneratedProposal):
        raise TypeError("Unsupported proposal")
    # Free-code compilation never loads the internal catalog.
    tools = {tool.tool_id: tool for tool in load_catalog()} if catalog else {}
    cells, steps, record_steps = [], [], set()
    for index, step in enumerate(proposal.steps, 1):
        raw = json.dumps(step.parameters, allow_nan=False)
        if len(raw) > 8000:
            raise ValueError("Parameter size limit exceeded")
        arguments = parse_arguments(raw)
        public = step.model_dump(
            include={"description", "reason", "expected_result", "input_step"}
        )
        provenance = {}
        if isinstance(step, CatalogStep):
            tool = tools.get(step.tool_id)
            if tool is None or (
                step.skill_id != tool.skill_id
                or step.skill_version != tool.version
                or step.tool_version != tool.version
            ):
                raise ValueError("Unknown Skill/Tool identity or version")
            if tool.consumes_records:
                if step.input_step not in record_steps:
                    raise ValueError("Tool requires an earlier records result")
            elif step.input_step is not None:
                raise ValueError("Tool does not accept an input result")
            arguments = tool.parameters.model_validate(arguments).model_dump()
            source = tool.source
            provenance = {
                "skill_id": tool.skill_id,
                "skill_version": tool.version,
                "tool_id": tool.tool_id,
                "tool_version": tool.version,
                "tool_description": tool.description,
                "skill_sha256": digest(tool.skill_text),
                "source_sha256": digest(source),
            }
            if tool.produces_records:
                record_steps.add(index)
            internal = {"skill_text": tool.skill_text}
        else:
            source = "\n".join(step.function_lines) + "\n"
            if len(source) > 12000:
                raise ValueError("Function size limit exceeded")
            provenance = {"source_sha256": digest(source)}
            internal = {}
        code = render_cell(source, arguments, step.input_step, index)
        public.update(
            {
                **provenance,
                "parameters": arguments,
                "cell_index": index,
                "input_parameter": input_parameter(
                    function_definition(source), step.input_step
                ),
                "code_sha256": digest(code),
            }
        )
        steps.append(public)
        cells.append(
            {
                **public,
                **internal,
                "function_source": source,
                "code": code,
            }
        )
    return steps, cells
