"""Admission policies only: no graph, network or runtime imports."""

from agent_service.domain.management import DomainError


def initial_state(settings):
    if settings.agent_backend == "demo":
        return {
            "phase": "start",
            "scenario": settings.demo_scenario,
            "revision": 1,
        }
    return {
        "graph_version": "assistant-v2",
        "attempts": 0,
        "model_name": settings.model_name,
        "model_provider": settings.model_provider,
    }


def resumed_state(run, response, pending=None):
    if (
        run["backend"] == "langgraph"
        and run["checkpoint"].get("graph_version") == "assistant-v2"
        and pending
        and pending.get("graph_interrupt_id")
    ):
        return {
            **run["checkpoint"],
            "attempts": 0,
            "resume": {
                "review_id": str(pending["interrupt_id"]),
                "graph_interrupt_id": pending["graph_interrupt_id"],
                "response": response,
            },
        }
    if run["backend"] != "demo":
        raise DomainError(
            "RESUME_NOT_SUPPORTED",
            "현재 그래프의 승인 대기 정보가 유효하지 않습니다.",
            409,
        )
    return {
        **run["checkpoint"],
        "response": response,
        "phase": "review_response",
    }
