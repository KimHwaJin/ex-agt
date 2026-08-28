from __future__ import annotations

from typing import Any, cast

from langgraph.graph import END, START, StateGraph

from ex_agent.application.ports import WorkflowServices
from ex_agent.graph.nodes import WorkflowNodes
from ex_agent.graph.routes import (
    route_approved_execution,
    route_code_risk,
    route_external_signal,
    route_intent,
    route_multi_action,
    route_plan_review,
    route_reconciliation,
    route_request_risk,
    route_workflow_decision,
    route_workflow_search,
)
from ex_agent.graph.state import AgentGraphState


def build_workflow_graph(
    services: WorkflowServices,
    *,
    checkpointer: Any = None,
) -> Any:
    nodes = WorkflowNodes(services)
    graph = StateGraph(cast(Any, AgentGraphState))

    graph.add_node("hydrate_turn", nodes.hydrate_turn)
    graph.add_node("classify_intent", nodes.classify_intent)
    graph.add_node("clarify_request", nodes.clarify_request)
    graph.add_node("answer_general", nodes.answer_general)
    graph.add_node("answer_data_question", nodes.answer_data_question)
    graph.add_node("commit_answer", nodes.commit_answer)
    graph.add_node("choose_execution_mode", nodes.choose_execution_mode)
    graph.add_node("review_request_risk", nodes.review_request_risk)
    graph.add_node("confirm_request_risk", nodes.confirm_request_risk)
    graph.add_node("search_workflows", nodes.search_workflows)
    graph.add_node("choose_workflow", nodes.choose_workflow)
    graph.add_node("load_selected_workflow", nodes.load_selected_workflow)
    graph.add_node("build_plan", nodes.build_plan)
    graph.add_node(
        "compile_and_persist_plan",
        nodes.compile_and_persist_plan,
    )
    graph.add_node(
        "review_compiled_code_risk",
        nodes.review_compiled_code_risk,
    )
    graph.add_node("review_plan", nodes.review_plan)
    graph.add_node("verify_approval", nodes.verify_approval)
    graph.add_node("submit_execution", nodes.submit_execution)
    graph.add_node("wait_external_signal", nodes.wait_external_signal)
    graph.add_node("reconcile_executor", nodes.reconcile_executor)
    graph.add_node("adapt_multi_plan", nodes.adapt_multi_plan)
    graph.add_node("append_operation", nodes.append_operation)
    graph.add_node("finalize_execution", nodes.finalize_execution)
    graph.add_node("cancel_execution", nodes.cancel_execution)
    graph.add_node("build_report_evidence", nodes.build_report_evidence)
    graph.add_node("generate_report", nodes.generate_report)
    graph.add_node("commit_success", nodes.commit_success)
    graph.add_node("commit_rejected", nodes.commit_rejected)
    graph.add_node("commit_blocked", nodes.commit_blocked)
    graph.add_node("commit_failed", nodes.commit_failed)
    graph.add_node("commit_cancelled", nodes.commit_cancelled)

    graph.add_edge(START, "hydrate_turn")
    graph.add_edge("hydrate_turn", "classify_intent")
    graph.add_conditional_edges("classify_intent", route_intent)
    graph.add_edge("clarify_request", "classify_intent")
    graph.add_edge("answer_general", "commit_answer")
    graph.add_edge("answer_data_question", "commit_answer")
    graph.add_edge("commit_answer", END)
    graph.add_edge("choose_execution_mode", "review_request_risk")
    graph.add_conditional_edges(
        "review_request_risk",
        route_request_risk,
    )
    graph.add_conditional_edges(
        "confirm_request_risk",
        route_request_risk,
    )
    graph.add_conditional_edges("search_workflows", route_workflow_search)
    graph.add_conditional_edges("choose_workflow", route_workflow_decision)
    graph.add_edge("build_plan", "compile_and_persist_plan")
    graph.add_edge(
        "compile_and_persist_plan",
        "review_compiled_code_risk",
    )
    graph.add_conditional_edges(
        "review_compiled_code_risk",
        route_code_risk,
    )
    graph.add_conditional_edges("review_plan", route_plan_review)
    graph.add_edge(
        "load_selected_workflow",
        "review_compiled_code_risk",
    )
    graph.add_conditional_edges(
        "verify_approval",
        route_approved_execution,
    )
    graph.add_edge("submit_execution", "wait_external_signal")
    graph.add_conditional_edges(
        "wait_external_signal",
        route_external_signal,
    )
    graph.add_conditional_edges("reconcile_executor", route_reconciliation)
    graph.add_conditional_edges("adapt_multi_plan", route_multi_action)
    graph.add_edge("append_operation", "wait_external_signal")
    graph.add_edge("finalize_execution", "wait_external_signal")
    graph.add_edge("cancel_execution", "wait_external_signal")
    graph.add_edge("build_report_evidence", "generate_report")
    graph.add_edge("generate_report", "commit_success")
    graph.add_edge("commit_success", END)
    graph.add_edge("commit_rejected", END)
    graph.add_edge("commit_blocked", END)
    graph.add_edge("commit_failed", END)
    graph.add_edge("commit_cancelled", END)

    return graph.compile(checkpointer=checkpointer)
