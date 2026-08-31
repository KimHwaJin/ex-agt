from __future__ import annotations

import hashlib
import json
from typing import Any

from pydantic import TypeAdapter

from ex_agent.dev_chat.presentation import review_card
from ex_agent.domain.contracts import ResumeSignal

_signals = TypeAdapter(ResumeSignal)


def decision_signal(
    raw: Any, task: dict[str, Any], kind: str
) -> dict[str, Any] | None:
    """Translate explicit UI decisions, not natural-language intent."""
    if not isinstance(raw, dict) or not isinstance(raw.get("decisions"), list):
        raise ValueError("Chat UI decisions 응답이 필요합니다.")
    if len(raw["decisions"]) != 1:
        raise ValueError("한 번에 하나의 결정만 제출해주세요.")
    decision = raw["decisions"][0]
    if not isinstance(decision, dict):
        raise ValueError("결정은 객체여야 합니다.")
    card = review_card(task, kind)
    action = decision.get("type")
    if action not in card["review_configs"][0]["allowed_decisions"]:
        raise ValueError("이 단계에서 지원하지 않는 결정입니다.")
    arguments = card["action_requests"][0]["args"]
    if action == "edit":
        edited = decision.get("edited_action")
        if not isinstance(edited, dict) or edited.get("name") != kind:
            raise ValueError("승인 대상 이름을 변경할 수 없습니다.")
        values = edited.get("args")
        if not isinstance(values, dict) or values.keys() - arguments.keys():
            raise ValueError("표시된 입력 필드만 변경할 수 있습니다.")
        arguments = {**arguments, **values}
    payload = task.get("current_interrupt") or {}
    signal: dict[str, Any] = {"type": kind}
    if kind == "PLAN_REVIEW":
        feedback = arguments["feedback"]
        if not isinstance(feedback, str):
            raise ValueError("feedback은 문자열이어야 합니다.")
        signal.update(
            {
                key: payload[key]
                for key in (
                    "plan_revision_id",
                    "plan_revision_number",
                    "public_payload_hash",
                )
            }
        )
        signal.update(
            decision=(
                "REJECT"
                if action == "reject"
                else "REVISE"
                if feedback.strip()
                else "APPROVE"
            ),
            feedback=decision.get("message")
            if action == "reject"
            else feedback,
            risk_acknowledged=arguments["risk_acknowledged"],
        )
        if signal["decision"] == "APPROVE":
            _check_risk(payload.get("risk"), arguments)
    elif kind == "WORKFLOW_SELECTION":
        selected = arguments["workflow_version_id"] or None
        candidate = next(
            (
                item
                for item in payload.get("candidates", [])
                if item["workflow_version_id"] == selected
            ),
            None,
        )
        if selected is not None and candidate is None:
            raise ValueError("현재 제안된 workflow_version_id를 선택해주세요.")
        if candidate:
            _check_risk(candidate.get("risk"), arguments)
        signal.update(
            workflow_version_id=selected,
            proposal_version=payload["proposal_version"],
            public_payload_hash=(
                candidate["public_payload_hash"] if candidate else "0" * 64
            ),
            input_values=arguments["input_values"],
            risk_acknowledged=arguments["risk_acknowledged"],
        )
    elif kind == "EXECUTION_MODE":
        signal["mode"] = arguments["mode"]
    elif kind == "CLARIFICATION":
        signal["answer"] = arguments["answer"]
    elif kind == "REQUEST_RISK_CONFIRMATION":
        signal["confirmed"] = action == "approve"
    elif kind == "OBSERVE_RESPONSE":
        return None
    else:
        if type(arguments["cancel_execution"]) is not bool:
            raise ValueError("cancel_execution은 true/false여야 합니다.")
        if not arguments["cancel_execution"]:
            return None
        if not task.get("execution_id"):
            raise ValueError("아직 Executor 실행이 없어 취소할 수 없습니다.")
        signal = {
            "type": "CANCEL_REQUESTED",
            "task_id": task["task_id"],
            "reason": arguments["reason"] or None,
        }
    return _signals.validate_python(signal).model_dump(mode="json")


def _check_risk(risk: Any, arguments: dict[str, Any]) -> None:
    if not risk:
        return
    if risk.get("level") == "CRITICAL":
        raise ValueError("CRITICAL 위험은 승인할 수 없습니다.")
    if (
        risk.get("level") == "HIGH"
        and arguments.get("risk_acknowledged") is not True
    ):
        raise ValueError(
            "위험에 동의하면 risk_acknowledged=true로 설정하세요."
        )


def signal_key(task: dict[str, Any], signal: dict[str, Any]) -> str:
    encoded = json.dumps(signal, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(encoded.encode()).hexdigest()
    return f"chat-ui:{task['task_id']}:{task['version']}:{digest}"
