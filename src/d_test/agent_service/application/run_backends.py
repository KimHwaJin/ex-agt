"""Admission policies only: no graph, network or runtime imports."""

from d_test.agent_service.domain.management import DomainError


def select_model(settings, requested=None, previous=None):
    if settings.agent_backend != "langgraph":
        if requested is not None:
            raise DomainError(
                "MODEL_SELECTION_UNAVAILABLE", "모델 선택이 불가능합니다.", 422
            )
        return None
    name = requested or previous or settings.model_name
    if name not in settings.selectable_models:
        raise DomainError(
            "MODEL_NOT_AVAILABLE", "사용할 수 없는 모델입니다.", 422
        )
    return name


def initial_state(settings, model_name=None):
    if settings.agent_backend == "demo":
        return {
            "phase": "start",
            "scenario": settings.demo_scenario,
            "revision": 1,
        }
    return {
        "graph_version": "assistant-v3",
        "attempts": 0,
        "model_name": model_name or settings.model_name,
        "model_provider": settings.model_provider,
    }


def resumed_state(run, response, pending=None):
    if (
        run["backend"] == "langgraph"
        and run["checkpoint"].get("graph_version")
        in {"assistant-v2", "assistant-v3"}
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
