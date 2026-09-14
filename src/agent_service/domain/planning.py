"""Validated model outputs for routing and non-executable plan drafts."""

from typing import Literal

from pydantic import Field

from .runs import StrictModel

Intent = Literal[
    "general_question", "analysis_question", "analysis_task", "code_task"
]


class RequestRoute(StrictModel):
    intent: Intent
    reason: str = Field(min_length=1, max_length=1000)


class DraftStep(StrictModel):
    description: str = Field(min_length=1, max_length=1000)
    reason: str = Field(min_length=1, max_length=1000)
    expected_result: str = Field(min_length=1, max_length=1000)


class PlanDraft(StrictModel):
    title: str = Field(min_length=1, max_length=200)
    summary: str = Field(min_length=1, max_length=2000)
    implementation: Literal["catalog", "generated_code"]
    steps: list[DraftStep] = Field(min_length=1, max_length=12)
