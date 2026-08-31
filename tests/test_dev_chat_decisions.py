from copy import deepcopy
from typing import Any

import pytest

from ex_agent.dev_chat.decisions import decision_signal, signal_key
from ex_agent.dev_chat.presentation import public_value, review_card

TASK_ID = "8191c73b-be76-45f5-81c1-8248de4b4a7e"
PLAN_ID = "3ba81b96-10bf-4254-bd2c-c02e9a71aef7"
WORKFLOW_ID = "5fbf5a91-f372-4fdd-b94e-86c17be119d3"


def snapshot(kind: str, **payload: Any) -> dict[str, Any]:
    return {
        "task_id": TASK_ID,
        "status": "PLANNING",
        "version": 3,
        "execution_id": None,
        "current_interrupt": {"kind": kind, **payload},
    }


def response(
    kind: str, *, action: str = "approve", **arguments: Any
) -> dict[str, Any]:
    decision: dict[str, Any] = {"type": action}
    if action == "edit":
        decision["edited_action"] = {"name": kind, "args": arguments}
    return {"decisions": [decision]}


def plan_payload() -> dict[str, Any]:
    return {
        "plan_revision_id": PLAN_ID,
        "plan_revision_number": 1,
        "public_payload_hash": "a" * 64,
        "risk": {"level": "LOW"},
        "plan": {
            "objective": "샘플 분석",
            "execution_mode": "MULTI",
            "steps": [
                {
                    "sequence": 0,
                    "title": "샘플 다운로드",
                    "purpose": "분석 입력 준비",
                    "skill": {"name": "analysis"},
                    "tool": {"name": "download_sample"},
                    "selection_rationale": "입력 데이터가 필요하기 때문",
                    "parameters": {"rows": 10},
                    "custom_code": "NEVER_SHOW_EXECUTABLE_SOURCE",
                }
            ],
        },
    }


@pytest.mark.parametrize(
    ("action", "arguments", "expected"),
    [
        ("approve", {}, "APPROVE"),
        ("reject", {}, "REJECT"),
        ("edit", {"feedback": "자유 코드로 다시 작성"}, "REVISE"),
    ],
)
def test_plan_review_preserves_immutable_revision(
    action: str, arguments: dict[str, Any], expected: str
) -> None:
    task = snapshot("PLAN_REVIEW", **plan_payload())
    original = deepcopy(task)
    signal = decision_signal(
        response("PLAN_REVIEW", action=action, **arguments),
        task,
        "PLAN_REVIEW",
    )
    assert signal is not None
    assert signal["decision"] == expected
    assert signal["plan_revision_id"] == PLAN_ID
    assert signal["public_payload_hash"] == "a" * 64
    assert signal["plan_revision_number"] == 1
    assert task == original
    assert signal_key(task, signal) == signal_key(
        task, dict(reversed(list(signal.items())))
    )


def test_high_risk_requires_explicit_acknowledgement() -> None:
    task = snapshot("PLAN_REVIEW", **plan_payload())
    task["current_interrupt"]["risk"] = {"level": "HIGH"}
    with pytest.raises(ValueError, match="risk_acknowledged"):
        decision_signal(response("PLAN_REVIEW"), task, "PLAN_REVIEW")
    signal = decision_signal(
        response("PLAN_REVIEW", action="edit", risk_acknowledged=True),
        task,
        "PLAN_REVIEW",
    )
    assert signal and signal["decision"] == "APPROVE"
    assert signal["risk_acknowledged"] is True


def test_workflow_selection_uses_candidate_hash() -> None:
    task = snapshot(
        "WORKFLOW_SELECTION",
        proposal_version=2,
        candidates=[
            {
                "workflow_version_id": WORKFLOW_ID,
                "public_payload_hash": "b" * 64,
                "name": "후보",
                "description": "샘플 분석",
                "plan": plan_payload()["plan"],
            }
        ],
    )
    signal = decision_signal(
        response(
            "WORKFLOW_SELECTION",
            action="edit",
            workflow_version_id=WORKFLOW_ID,
            input_values={"rows": 10},
        ),
        task,
        "WORKFLOW_SELECTION",
    )
    assert signal and signal["public_payload_hash"] == "b" * 64
    assert signal["proposal_version"] == 2
    assert signal["input_values"] == {"rows": 10}
    skipped = decision_signal(
        response("WORKFLOW_SELECTION"), task, "WORKFLOW_SELECTION"
    )
    assert skipped and skipped["workflow_version_id"] is None
    with pytest.raises(ValueError, match="제안된"):
        decision_signal(
            response(
                "WORKFLOW_SELECTION",
                action="edit",
                workflow_version_id=PLAN_ID,
            ),
            task,
            "WORKFLOW_SELECTION",
        )


@pytest.mark.parametrize("mode", ["SINGLE", "MULTI"])
def test_execution_mode(mode: str) -> None:
    kind = "EXECUTION_MODE"
    signal = decision_signal(
        response(kind, action="edit", mode=mode), snapshot(kind), kind
    )
    assert signal == {"type": kind, "mode": mode}


def test_clarification_and_request_risk() -> None:
    kind = "CLARIFICATION"
    task = snapshot(kind, question="기간은?")
    with pytest.raises(ValueError):
        decision_signal(response(kind), task, kind)
    signal = decision_signal(
        response(kind, action="edit", answer="최근 3개월"), task, kind
    )
    assert signal == {"type": kind, "answer": "최근 3개월"}
    kind = "REQUEST_RISK_CONFIRMATION"
    assert decision_signal(response(kind), snapshot(kind), kind) == {
        "type": kind,
        "confirmed": True,
    }
    assert decision_signal(
        response(kind, action="reject"), snapshot(kind), kind
    ) == {"type": kind, "confirmed": False}


def test_observation_refresh_is_not_executor_resume_or_cancel() -> None:
    kind = "OBSERVE_EXECUTION"
    task = snapshot("EXECUTOR_EVENT")
    assert decision_signal(response(kind), task, kind) is None
    task["execution_id"] = WORKFLOW_ID
    assert decision_signal(response(kind), task, kind) is None
    signal = decision_signal(
        response(kind, action="edit", cancel_execution=True, reason="테스트"),
        task,
        kind,
    )
    assert signal == {
        "type": "CANCEL_REQUESTED",
        "task_id": TASK_ID,
        "reason": "테스트",
    }


@pytest.mark.parametrize(
    "raw",
    [
        True,
        {"decisions": []},
        {"decisions": [{"type": "approve"}, {"type": "approve"}]},
        {"decisions": ["approve"]},
        response("DIFFERENT_ACTION", action="edit"),
        response("PLAN_REVIEW", action="edit", public_payload_hash="c" * 64),
    ],
)
def test_invalid_or_tampered_response_is_rejected(raw: Any) -> None:
    with pytest.raises(ValueError):
        decision_signal(
            raw, snapshot("PLAN_REVIEW", **plan_payload()), "PLAN_REVIEW"
        )


def test_public_card_displays_lineage_but_never_source() -> None:
    task = public_value(snapshot("PLAN_REVIEW", **plan_payload()))
    card = review_card(task, "PLAN_REVIEW")
    assert "NEVER_SHOW_EXECUTABLE_SOURCE" not in str(task)
    description = card["action_requests"][0]["description"]
    for text in ("analysis", "download_sample", "입력 데이터", "rows"):
        assert text in description
    assert card["review_configs"][0]["allowed_decisions"] == [
        "approve",
        "edit",
        "reject",
    ]
