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
        "graph_version": "assistant-v1",
        "attempts": 0,
        "model_name": settings.model_name,
        "model_provider": settings.model_provider,
    }


def resumed_state(run, response):
    if run["backend"] != "demo":
        raise DomainError(
            "RESUME_NOT_SUPPORTED",
            "현재 대화 그래프에는 승인 대기 단계가 없습니다.",
            409,
        )
    return {
        **run["checkpoint"],
        "response": response,
        "phase": "review_response",
    }
