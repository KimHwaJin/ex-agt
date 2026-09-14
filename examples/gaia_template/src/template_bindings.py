"""Replace only this private-auth/protocol boundary during migration."""

from agent_service.domain.management import DomainError
from agent_service.integrations.template_protocol import AuthorizedRequest


async def resolve_request(payload: dict, config: dict) -> AuthorizedRequest:
    """Implement with the internal template's verified principal/context.

    1. Resolve authenticated employee to internal user UUID (not body trust).
    2. Resolve external session to our session, checking owner/project.
    3. If A2A, verify persisted taskId/contextId/owner binding on resume.
    4. Map message or explicit review response to our v1 envelope.
    5. Call decode_v1(mapped_payload, owner=verified_user_uuid).

    main_model_name belongs to envelope.request, with no endpoint override.
    request_id is stable for retries; every new HITL reply gets a new key.
    metadata.thread_id alone does not configure LangGraph persistence.
    """
    raise DomainError(
        "TEMPLATE_IDENTITY_NOT_CONFIGURED",
        "템플릿 인증·세션 매핑을 연결해야 합니다.",
        503,
    )
