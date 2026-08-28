import pytest
from fastapi import HTTPException
from pydantic import TypeAdapter

from ex_agent.api.app import _validate_signal_against_interrupt
from ex_agent.domain.contracts import ResumeSignal, RiskConfirmationDecision


def test_request_risk_confirmation_is_a_resume_signal() -> None:
    signal = TypeAdapter(ResumeSignal).validate_python(
        {
            "type": "REQUEST_RISK_CONFIRMATION",
            "confirmed": True,
        }
    )
    assert isinstance(signal, RiskConfirmationDecision)
    assert signal.confirmed


def test_stale_plan_decision_is_rejected_before_command() -> None:
    interrupt = {
        "kind": "PLAN_REVIEW",
        "plan_revision_id": "revision-2",
        "plan_revision_number": 2,
        "public_payload_hash": "b" * 64,
    }
    signal = {
        "type": "PLAN_REVIEW",
        "plan_revision_id": "revision-1",
        "plan_revision_number": 1,
        "public_payload_hash": "a" * 64,
    }
    with pytest.raises(HTTPException) as captured:
        _validate_signal_against_interrupt(interrupt, signal)
    assert captured.value.status_code == 409
