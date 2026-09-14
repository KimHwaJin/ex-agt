"""Internal code proposals. Raw source is never an approval-card field."""

from typing import Annotated, Any

from pydantic import Field

from .planning import DraftStep
from .runs import StrictModel


class RiskAssessment(StrictModel):
    warnings: list[Annotated[str, Field(min_length=1, max_length=1000)]] = (
        Field(max_length=8)
    )
    rationale: str = Field(min_length=1, max_length=1000)


class CodeStep(DraftStep):
    parameters: dict[str, Any] = Field(
        max_length=32,
        description="Literal keyword argument object, excluding data",
    )
    input_step: int | None = Field(
        ge=1,
        le=12,
        description="Earlier step result passed to the first argument",
    )


class CatalogStep(CodeStep):
    skill_id: str
    skill_version: str
    tool_id: str
    tool_version: str


class CatalogProposal(StrictModel):
    steps: list[CatalogStep] = Field(min_length=1, max_length=12)


class GeneratedStep(CodeStep):
    function_lines: list[Annotated[str, Field(max_length=500)]] = Field(
        min_length=2,
        max_length=200,
        description="One Python function as lines, preserving indentation",
    )


class GeneratedProposal(StrictModel):
    steps: list[GeneratedStep] = Field(min_length=1, max_length=12)
