"""Code validation recovery is bounded and never performs execution."""

from uuid import uuid4

import pytest
from langchain_core.messages import HumanMessage

from d_test.agent_service.domain.code_plans import GeneratedProposal
from d_test.agent_service.graphs.assistant.preparation import preparation_node


class Proposals:
    def __init__(self, sources):
        self.sources = sources
        self.calls = []

    async def ainvoke(self, value, config=None):
        self.calls.append(value)
        proposal = GeneratedProposal.model_validate(
            {
                "steps": [
                    {
                        "description": "계산",
                        "reason": "사용자 요청",
                        "expected_result": "정수",
                        "parameters": {},
                        "input_step": None,
                        "function_lines": self.sources[
                            len(self.calls) - 1
                        ].splitlines(),
                    }
                ]
            }
        )
        return {
            "structured_response": {
                "proposal": proposal,
                "risk": {"warnings": [], "rationale": "테스트"},
            }
        }


def state():
    return {
        "run_id": str(uuid4()),
        "plan_version": 1,
        "messages": [HumanMessage(content="직접 코드 작성")],
        "route": {"intent": "code_task"},
        "plan": {
            "implementation": "generated_code",
            "title": "제목",
            "summary": "요약",
            "steps": [],
        },
    }


async def test_invalid_python_is_corrected_once_without_execution():
    planner = Proposals(
        [
            "import math def compute(): return 1\n    pass",
            "def compute():\n    raise AssertionError('must not execute')",
        ]
    )
    prepare = preparation_node({"generated_code": planner})
    result = await prepare(state(), {})
    assert len(planner.calls) == 2
    assert result["plan"]["code_prepared"] is True
    assert result["prepared_plan"]["generation_attempts"] == 2
    assert len(result["prepared_plan"]["validation_feedback"]) == 1


async def test_invalid_python_stops_after_one_correction():
    planner = Proposals(["bad syntax\npass", "still bad syntax\npass"])
    prepare = preparation_node({"generated_code": planner})
    with pytest.raises(SyntaxError):
        await prepare(state(), {})
    assert len(planner.calls) == 2
