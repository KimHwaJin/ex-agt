"""Checkpointed planning and side-effect-free human approval boundary."""

import json
from uuid import UUID, uuid5

from langchain_core.messages import AIMessage, HumanMessage
from langchain_core.runnables import RunnableConfig
from langgraph.types import interrupt
from pydantic import TypeAdapter

from d_test.agent_service.domain.planning import PlanDraft, RequestRoute
from d_test.agent_service.domain.runs import PlanResponse

from .nodes import answer_id
from .state import WorkflowState

RESPONSE = TypeAdapter(PlanResponse)
UNAVAILABLE = {
    "code": "EXECUTOR_NOT_CONFIGURED",
    "message": "계획은 승인했지만 실행 서비스가 아직 연결되지 않았습니다.",
}


def classification_node(router):
    async def classify(state: WorkflowState, config: RunnableConfig):
        result = await router.ainvoke(
            {"messages": state["messages"]}, config=config
        )
        route = RequestRoute.model_validate(result["structured_response"])
        return {"route": route.model_dump()}

    return classify


def next_after_classification(state: WorkflowState):
    if state["route"]["intent"] in ("analysis_task", "code_task"):
        return "plan"
    return "respond"


def planning_node(planner):
    async def plan(state: WorkflowState, config: RunnableConfig):
        request = HumanMessage(
            content=json.dumps(
                {
                    "route": state["route"],
                    "previous_plan": state.get("plan"),
                    "revision_instruction": state.get("instruction"),
                },
                ensure_ascii=False,
            )
        )
        result = await planner.ainvoke(
            {"messages": [*state["messages"], request]}, config=config
        )
        draft = PlanDraft.model_validate(result["structured_response"])
        version = state.get("plan_version", 0) + 1
        payload = {
            **draft.model_dump(),
            "plan_id": str(uuid5(UUID(state["run_id"]), "execution-plan")),
            "plan_version": version,
            "intent": state["route"]["intent"],
            "classification_reason": state["route"]["reason"],
            "instruction": state.get("instruction"),
            "stage": "draft",
            "executable": False,
            "execution_available": False,
            "notice": "계획 초안: Skill/Tool·실행 서비스 미연결 상태입니다.",
            "response_schema": RESPONSE.json_schema(),
        }
        return {
            "plan": payload,
            "plan_version": version,
            "messages": [
                AIMessage(
                    content=plan_text(payload),
                    id=f"{state['run_id']}:plan:{version}",
                )
            ],
        }

    return plan


def plan_text(plan):
    lines = [plan["title"], plan["summary"], plan["notice"]]
    for index, step in enumerate(plan["steps"], 1):
        lines.append(
            f"{index}. {step['description']}\n"
            f"이유: {step['reason']}\n예상 산출물: {step['expected_result']}"
        )
        if step.get("tool_id"):
            lines.append(
                f"Skill: {step['skill_id']}@{step['skill_version']} / "
                f"Tool: {step['tool_id']}@{step['tool_version']}"
            )
        if "parameters" in step:
            lines.append(
                "파라미터: "
                + json.dumps(step["parameters"], ensure_ascii=False)
            )
    for warning in plan.get("generation_risk", {}).get("warnings", []):
        lines.append(f"경고: {warning}")
    return "\n\n".join(lines)


def review_plan(state: WorkflowState):
    # This node restarts on resume. No model call or external write goes here.
    value = interrupt(state["plan"])
    decision = RESPONSE.validate_python(value["response"])
    receipt = str(UUID(value["review_id"]))
    response = decision.model_dump()
    instruction = response.get("instruction")
    return {
        "decision": decision.decision,
        "instruction": instruction,
        "applied_review_id": receipt,
        "messages": [
            HumanMessage(
                content=(
                    f"계획 v{state['plan_version']} 검토: {decision.decision}"
                    + (f"\n수정 요청: {instruction}" if instruction else "")
                ),
                id=f"{state['run_id']}:review:{receipt}",
            )
        ],
    }


def next_after_review(state: WorkflowState):
    return "plan" if state["decision"] == "modify" else "review_outcome"


def review_outcome(state: WorkflowState):
    rejected = state["decision"] == "reject"
    text = (
        "계획을 거절했습니다. 코드 실행 없이 작업을 종료합니다."
        if rejected
        else "계획을 승인했습니다. 다만 아직 Executor가 연결되지 않아 "
        "실행하지 못했습니다. 실행 결과·노트북·리포트는 생성되지 않았습니다."
    )
    return {
        "answer": text,
        "outcome": "rejected" if rejected else "failed",
        "error": None if rejected else UNAVAILABLE,
        "messages": [AIMessage(content=text, id=answer_id(state["run_id"]))],
    }
