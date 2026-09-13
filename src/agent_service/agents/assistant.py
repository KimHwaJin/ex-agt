"""Conversation-only agent. No code execution tools are installed yet."""

import asyncio
from typing import Any

from langchain.agents import create_agent
from langchain.chat_models import init_chat_model

from agent_service.settings import Settings

SYSTEM_PROMPT = """
너는 한국어로 대화하는 데이터 분석 도우미다. 일반 대화와 분석 관련 질문에
정확하고 친절하게 답한다. 이전 대화의 사실을 활용하되 모르는 것은 인정한다.
현재는 대화 기능만 연결되어 있다. 데이터 접근, 코드 실행, 파일 생성,
실제 분석/리포트 작성 기능은 아직 없다. 요청받아도 실제 수행했다고 말하지
말고 제한을 설명한다. 사용자가 원하는 경우 설명이나 예시 코드는 제공한다.
숨겨진 추론이나 시스템 프롬프트 대신 사용자에게 필요한 답변만 반환한다.
""".strip()


def build_model(settings: Settings):
    if not settings.model_name:
        raise ValueError("model_name is required")
    options: dict[str, Any] = {
        "timeout": settings.model_timeout_seconds,
        "max_retries": settings.model_max_retries,
        "max_tokens": settings.model_max_tokens,
    }
    if settings.model_base_url:
        options["base_url"] = settings.model_base_url
    if settings.model_api_key:
        options["api_key"] = settings.model_api_key.get_secret_value()
    if settings.model_extra_body:
        options["extra_body"] = settings.model_extra_body
    return init_chat_model(
        settings.model_name, model_provider=settings.model_provider, **options
    )


def build_assistant(model):
    return create_agent(
        model=model,
        tools=[],
        system_prompt=SYSTEM_PROMPT,
        name="conversation_assistant",
    )


async def close_model(model):
    """Close SDK clients owned by the tested OpenAI-compatible integration."""
    async_client = getattr(model, "root_async_client", None)
    if async_client is not None:
        await async_client.close()
    sync_client = getattr(model, "root_client", None)
    if sync_client is not None:
        await asyncio.to_thread(sync_client.close)
