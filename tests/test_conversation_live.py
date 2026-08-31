"""Opt-in model evaluation. Does not create tasks or invoke Executor."""

import os
from typing import Any, cast

import pytest

from ex_agent.application.capabilities.conversation import (
    ConversationCapability,
)
from ex_agent.config import Settings
from ex_agent.domain.enums import Intent
from ex_agent.llm.factory import build_chat_model

_MODEL_URL = os.getenv("EX_AGENT_TEST_LIVE_MODEL_URL")
pytestmark = [
    pytest.mark.llm,
    pytest.mark.skipif(
        not _MODEL_URL, reason="Live model evaluation disabled"
    ),
]


def capability() -> ConversationCapability:
    settings = Settings(agent_model_base_url=cast(str, _MODEL_URL))
    return ConversationCapability(cast(Any, None), build_chat_model(settings))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("message", "intent"),
    [
        ("ㅎㅇㅎㅇ", Intent.GENERAL_QA),
        ("반가워~", Intent.GENERAL_QA),
        ("오 땡큐 ㅋㅋ", Intent.GENERAL_QA),
        ("너는 무슨 일을 도와줄 수 있어?", Intent.GENERAL_QA),
        ("프랑스의 수도가 어디야?", Intent.GENERAL_QA),
        ("for문이 어떻게 동작하는지 설명해줘", Intent.GENERAL_QA),
        (
            "평균과 중앙값의 차이를 두 문장으로 설명해줘.",
            Intent.DATA_ANALYSIS_QA,
        ),
        ("상관관계가 있으면 인과관계도 있는 거야?", Intent.DATA_ANALYSIS_QA),
        (
            "실행하지 말고 pandas로 평균 구하는 예시 코드만 보여줘",
            Intent.DATA_ANALYSIS_QA,
        ),
        (
            "데이터레이크 매출 데이터를 받아 월별 추이를 분석해줘",
            Intent.DATA_ANALYSIS_EXECUTION,
        ),
        (
            "샘플 데이터를 만들어서 결측치와 요약 통계를 확인해줘",
            Intent.DATA_ANALYSIS_EXECUTION,
        ),
        (
            "지난달 매출 데이터 분석해줄 수 있어?",
            Intent.DATA_ANALYSIS_EXECUTION,
        ),
        ("print(1 + 1)을 실행해줘", Intent.CODE_EXECUTION),
        (
            "SINGLE로 1부터 10까지 합하는 코드를 실행해줘",
            Intent.CODE_EXECUTION,
        ),
    ],
)
async def test_live_intent(message: str, intent: Intent) -> None:
    result = await capability().classify_intent({"user_message": message})
    assert result.intent is intent, result.model_dump()
    assert not result.requires_clarification, result.model_dump()
    assert result.clarification_question is None, result.model_dump()
    assert result.requires_execution_mode == (
        intent is Intent.CODE_EXECUTION
        and result.requested_execution_mode is None
    )


@pytest.mark.parametrize(
    ("message", "mode"),
    [
        ("싱글모드로 샘플 데이터 생성, EDA, 플롯까지 실행해줘", "SINGLE"),
        (
            "분석 계획 전체를 먼저 정하고 그대로 한 번에 실행해줘. "
            "중간 결과를 보고 다음 계획을 바꾸지 마",
            "SINGLE",
        ),
        ("멀티로 결과를 보면서 샘플 데이터를 분석해줘", "MULTI"),
        ("샘플 데이터 만들어서 EDA와 플롯까지 실행해줘", None),
        ("SINGLE과 MULTI의 차이가 뭐야?", None),
        ("SINGLE로 1부터 10까지 더하는 코드를 실행해줘", "SINGLE"),
    ],
)
async def test_live_explicit_execution_mode(message: str, mode: str | None):
    result = await capability().classify_intent({"user_message": message})
    assert result.requested_execution_mode == mode, result.model_dump()


@pytest.mark.asyncio
async def test_live_ambiguous_request_still_asks_for_context() -> None:
    result = await capability().classify_intent({"user_message": "그거 해줘"})
    assert result.requires_clarification, result.model_dump()
    assert result.clarification_question
    assert "SINGLE" not in result.clarification_question
    assert "MULTI" not in result.clarification_question
