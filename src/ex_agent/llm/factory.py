from __future__ import annotations

import hashlib
import math
import re
import unicodedata

from langchain.chat_models import init_chat_model
from langchain_core.embeddings import Embeddings
from langchain_core.language_models import BaseChatModel
from langchain_openai import OpenAIEmbeddings

from ex_agent.config import Settings

_WORD_PATTERN = re.compile(r"\w+", re.UNICODE)


class DeterministicHashEmbeddings(Embeddings):
    """Offline development embeddings with lexical similarity only."""

    def __init__(self, dimensions: int) -> None:
        if dimensions < 1:
            raise ValueError("Embedding dimensions must be positive")
        self._dimensions = dimensions

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        return [self._embed(text) for text in texts]

    def embed_query(self, text: str) -> list[float]:
        return self._embed(text)

    def _embed(self, text: str) -> list[float]:
        normalized = unicodedata.normalize("NFKC", text).casefold().strip()
        words = _WORD_PATTERN.findall(normalized)
        compact = "".join(words)
        features = [f"word:{word}" for word in words]
        for width in (2, 3):
            features.extend(
                f"char{width}:{compact[index : index + width]}"
                for index in range(max(0, len(compact) - width + 1))
            )
        if not features:
            features = ["<empty>"]

        vector = [0.0] * self._dimensions
        for feature in features:
            digest = hashlib.blake2b(
                feature.encode("utf-8"),
                digest_size=8,
                person=b"ex-agent",
            ).digest()
            index = int.from_bytes(digest[:4], "big") % self._dimensions
            sign = 1.0 if digest[4] & 1 else -1.0
            vector[index] += sign

        norm = math.sqrt(sum(value * value for value in vector))
        return [value / norm for value in vector]


def build_chat_model(settings: Settings) -> BaseChatModel:
    return init_chat_model(
        model=settings.agent_model,
        model_provider=settings.agent_model_provider,
        base_url=settings.agent_model_base_url.rstrip("/"),
        api_key=settings.agent_model_api_key,
        temperature=settings.agent_model_temperature,
        timeout=settings.agent_model_timeout_seconds,
        max_retries=settings.agent_model_max_retries,
        max_tokens=settings.agent_model_max_tokens,
        extra_body={
            "chat_template_kwargs": {
                "enable_thinking": settings.agent_model_enable_thinking,
            }
        },
    )


def build_embeddings(settings: Settings) -> Embeddings:
    if settings.agent_embedding_provider == "dummy":
        return DeterministicHashEmbeddings(settings.agent_embedding_dimensions)
    return OpenAIEmbeddings(
        model=settings.agent_embedding_model,
        base_url=settings.agent_embedding_base_url.rstrip("/"),
        api_key=settings.agent_embedding_api_key,
        request_timeout=settings.agent_model_timeout_seconds,
        max_retries=settings.agent_model_max_retries,
    )
