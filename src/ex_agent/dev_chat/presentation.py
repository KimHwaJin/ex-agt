from __future__ import annotations

import json
from typing import Any


def public_value(value: Any) -> Any:
    """Never copy executable source into the development chat checkpoint."""
    if isinstance(value, dict):
        return {
            key: public_value(item)
            for key, item in value.items()
            if key not in {"custom_code", "source"}
        }
    if isinstance(value, list):
        return [public_value(item) for item in value]
    return value


def format_plan(plan: dict[str, Any]) -> str:
    lines = [
        str(plan.get("objective", "")),
        str(plan.get("strategy_summary", "")),
        f"실행 모드: {plan.get('execution_mode', '')}",
    ]
    for step in plan.get("steps", []):
        skill = step.get("skill") or {}
        tool = step.get("tool") or {}
        lines.extend(
            [
                f"\n### {step['sequence'] + 1}. {step['title']}",
                str(step.get("purpose", "")),
                f"스킬: {skill.get('name', '없음 — 자유 코드')}",
                f"함수: {tool.get('name', '사용자 요청에 맞춰 생성')}",
                f"선택·생성 이유: {step.get('selection_rationale', '')}",
                "파라미터: "
                + json.dumps(step.get("parameters", {}), ensure_ascii=False),
                "예상 결과: " + ", ".join(step.get("expected_outputs", [])),
            ]
        )
    return "\n\n".join(lines)


def describe_progress(task: dict[str, Any]) -> str:
    """A replaceable status message, never a human decision or a tool call."""
    labels = {
        "ACCEPTED": "요청을 처리하고 있습니다.",
        "CLASSIFYING": "요청의 의도를 확인하고 있습니다.",
        "ANSWERING": "답변을 작성하고 있습니다.",
        "PLANNING": "실행계획을 준비하고 있습니다.",
        "REVISING_PLAN": "요청한 내용을 반영해 계획을 수정하고 있습니다.",
        "WAITING_FOR_APPROVAL": "전달한 결정을 처리하고 있습니다.",
        "QUEUED_FOR_EXECUTION": "코드 실행을 준비하고 있습니다.",
        "EXECUTING": "코드를 실행하고 있습니다.",
        "WAITING_FOR_EXECUTOR_EVENT": "코드 실행 결과를 기다리고 있습니다.",
        "FINALIZING_EXECUTION": "실행 결과를 정리하고 있습니다.",
        "GENERATING_REPORT": "결과 리포트를 작성하고 있습니다.",
        "CANCEL_REQUESTED": (
            "취소를 요청했습니다. Executor 확인 대기 중입니다."
        ),
    }
    content = labels.get(task["status"], "작업을 처리하고 있습니다.")
    content += "\n\n진행 상황과 완료 결과가 자동으로 갱신됩니다."
    if task.get("execution_id"):
        content += f"\n\nExecution ID: `{task['execution_id']}`"
        content += "\n\nUI의 Stop은 화면 관찰만 중단하며 작업 취소가 아닙니다."
    return content


def describe(task: dict[str, Any], kind: str) -> str:
    payload = task.get("current_interrupt") or {}
    heading = ""
    if task.get("execution_id"):
        heading += f"Execution ID: `{task['execution_id']}`\n\n"
    if kind == "PLAN_REVIEW":
        content = format_plan(payload["plan"])
        content += (
            "\n\nApprove: 현재 계획 승인. Reject: 거절. "
            "Edit: feedback에 수정 요청을 입력하면 재계획 후 다시 승인. "
            "HIGH 위험 승인에는 risk_acknowledged=true가 필요합니다."
        )
    elif kind == "WORKFLOW_SELECTION":
        mode = payload.get("dynamic_execution_mode", "MULTI")
        content = (
            f"Approve: 후보를 선택하지 않고 동적 {mode} 계획 생성. "
            "후보를 사용하려면 Edit에서 workflow_version_id와 "
            "input_values를 입력하세요. 선택하면 SINGLE 실행을 승인합니다."
        )
        for candidate in payload.get("candidates", []):
            content += (
                f"\n\n## {candidate['name']}\n\n"
                f"{candidate['description']}\n\n"
                f"workflow_version_id: `{candidate['workflow_version_id']}`"
                "\n\n"
                + format_plan(candidate["plan"])
                + "\n\n입력 계약: "
                + json.dumps(
                    candidate.get("input_contract", {}), ensure_ascii=False
                )
                + "\n\n위험 검토: "
                + json.dumps(candidate.get("risk"), ensure_ascii=False)
            )
    elif kind == "EXECUTION_MODE":
        content = (
            "Approve: SINGLE. MULTI를 선택하려면 Edit에서 mode를 "
            '"MULTI"로 변경하세요.'
        )
    elif kind == "CLARIFICATION":
        content = str(payload.get("question", "추가 정보를 입력해주세요."))
        content += "\n\nEdit에서 answer를 입력하세요."
    elif kind == "REQUEST_RISK_CONFIRMATION":
        content = "위험 검토 결과입니다. Approve: 동의, Reject: 중단."
    elif kind == "OBSERVE_RESPONSE":
        content = (
            "요청을 처리하고 있습니다. "
            "Approve를 누르면 결과를 다시 확인합니다. "
            "이 버튼은 코드 실행 승인이 아니라 상태 새로고침입니다."
        )
    else:
        content = (
            "작업은 기존 Worker에서 계속 진행 중입니다. "
            "Approve를 누르면 다음 관찰 구간 동안 결과를 조회합니다. "
            "UI의 Stop/Resolve/새로고침은 Executor 취소가 아닙니다."
        )
        if task.get("execution_id"):
            content += (
                "\n\n실제 취소: Edit에서 cancel_execution=true로 "
                "설정하세요. 취소 완료는 Executor 확인 후 표시됩니다."
            )
    if payload.get("risk"):
        content += "\n\n위험 검토: " + json.dumps(
            payload["risk"], ensure_ascii=False
        )
    return heading + content


def review_card(
    task: dict[str, Any], kind: str, error: str = ""
) -> dict[str, Any]:
    arguments: dict[str, Any]
    allowed = ["approve", "edit"]
    if kind == "PLAN_REVIEW":
        arguments = {"feedback": "", "risk_acknowledged": False}
        allowed.append("reject")
    elif kind == "WORKFLOW_SELECTION":
        arguments = {
            "workflow_version_id": "",
            "input_values": {},
            "risk_acknowledged": False,
        }
    elif kind == "EXECUTION_MODE":
        arguments = {"mode": "SINGLE"}
    elif kind == "CLARIFICATION":
        arguments = {"answer": ""}
        allowed = ["edit"]
    elif kind == "REQUEST_RISK_CONFIRMATION":
        arguments = {}
        allowed = ["approve", "reject"]
    elif kind == "OBSERVE_RESPONSE":
        # Compatibility for checkpoints paused before automatic observation.
        # New graph runs only create cards for HUMAN_INTERRUPTS.
        arguments = {}
        allowed = ["approve"]
    else:
        arguments = {"cancel_execution": False, "reason": ""}
        allowed = (
            ["approve", "edit"] if task.get("execution_id") else ["approve"]
        )
    description = describe(task, kind)
    if error:
        description = f"입력 오류: {error}\n\n{description}"
    return {
        "action_requests": [
            {
                "name": kind,
                "args": arguments,
                "description": description,
            }
        ],
        "review_configs": [
            {"action_name": kind, "allowed_decisions": allowed}
        ],
    }
