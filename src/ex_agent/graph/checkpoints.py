"""Explicit deserialization allowlist shared by both graph hosts."""

from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer


def checkpoint_serializer() -> JsonPlusSerializer:
    return JsonPlusSerializer(
        allowed_msgpack_modules=[
            ("ex_agent.domain.contracts", "IntentDecision"),
            ("ex_agent.domain.contracts", "PlanDraft"),
            ("ex_agent.domain.contracts", "RiskReview"),
            ("ex_agent.domain.contracts", "WorkflowCandidate"),
            ("ex_agent.domain.enums", "ExecutionMode"),
            ("ex_agent.domain.enums", "Intent"),
            ("ex_agent.domain.enums", "PlanningKind"),
            ("ex_agent.domain.enums", "RiskLevel"),
            ("ex_agent.domain.enums", "TaskStatus"),
        ]
    )
