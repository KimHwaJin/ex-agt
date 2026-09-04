from math import sqrt
from typing import Any

import pytest
from langchain_core.language_models.fake_chat_models import FakeListChatModel

from ex_agent.config import Settings
from ex_agent.llm.factory import (
    DeterministicHashEmbeddings,
    build_chat_model,
    build_embeddings,
)


def test_chat_model_uses_internal_vllm(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}
    fake = FakeListChatModel(responses=["OK"])

    def fake_init_chat_model(**kwargs: Any) -> FakeListChatModel:
        captured.update(kwargs)
        return fake

    monkeypatch.setattr(
        "ex_agent.llm.factory.init_chat_model",
        fake_init_chat_model,
    )

    assert build_chat_model(Settings()) is fake
    assert captured == {
        "model": "qwen38-27b-nvfp4",
        "model_provider": "openai",
        "base_url": "http://model.frodo.com/v1",
        "api_key": "EMPTY",
        "temperature": 0.0,
        "timeout": 120.0,
        "max_retries": 2,
        "max_tokens": 4096,
        "extra_body": {"chat_template_kwargs": {"enable_thinking": False}},
    }


def test_embeddings_default_to_offline_dummy() -> None:
    embeddings = build_embeddings(Settings())

    assert isinstance(embeddings, DeterministicHashEmbeddings)
    assert embeddings.embed_query("같은 요청") == embeddings.embed_query(
        "같은 요청"
    )
    assert len(embeddings.embed_query("매출 분석")) == 1024
    vector = embeddings.embed_query("매출 분석")
    assert sqrt(sum(value * value for value in vector)) == pytest.approx(1)


def test_openai_embeddings_remain_configurable(monkeypatch: Any) -> None:
    captured: dict[str, Any] = {}
    sentinel = object()

    def fake_embeddings(**kwargs: Any) -> object:
        captured.update(kwargs)
        return sentinel

    monkeypatch.setattr(
        "ex_agent.llm.factory.OpenAIEmbeddings",
        fake_embeddings,
    )

    settings = Settings(
        agent_embedding_provider="openai",
        agent_embedding_model="future-embedding-model",
    )
    assert build_embeddings(settings) is sentinel
    assert captured == {
        "model": "future-embedding-model",
        "base_url": "http://model.frodo.com/v1",
        "api_key": "EMPTY",
        "request_timeout": 120.0,
        "max_retries": 2,
    }
