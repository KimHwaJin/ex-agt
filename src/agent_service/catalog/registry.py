"""Load packaged text once; do not import or execute function source."""

import ast
import hashlib
from dataclasses import dataclass
from functools import lru_cache
from importlib.resources import files

from pydantic import Field

from agent_service.domain.runs import StrictModel


class FetchParameters(StrictModel):
    query: str = Field(min_length=1, max_length=2000)
    rows: int = Field(default=100, ge=1, le=10000, strict=True)
    seed: int = Field(default=42, ge=0, le=2147483647, strict=True)


class InspectParameters(StrictModel):
    pass


class DescribeParameters(StrictModel):
    column: str = Field(default="revenue", min_length=1, max_length=100)


class PlotParameters(StrictModel):
    category: str = Field(default="category", min_length=1, max_length=100)
    value: str = Field(default="revenue", min_length=1, max_length=100)


def digest(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


@dataclass(frozen=True)
class CatalogTool:
    skill_id: str
    tool_id: str
    description: str
    parameters: type[StrictModel]
    consumes_records: bool
    produces_records: bool
    skill_text: str
    source: str
    version: str = "0.1.0"

    def metadata(self):
        return {
            "skill_id": self.skill_id,
            "skill_version": self.version,
            "tool_id": self.tool_id,
            "tool_version": self.version,
            "description": self.description,
            "parameters_schema": self.parameters.model_json_schema(),
            "input_kind": "records" if self.consumes_records else None,
            "output_kind": "records" if self.produces_records else "result",
            "skill": self.skill_text,
        }


@lru_cache(maxsize=1)
def load_catalog() -> tuple[CatalogTool, ...]:
    root = files("agent_service.catalog").joinpath("assets")
    specs = (
        (
            "sample-data",
            "fetch_sample_data",
            "합성 매출 데이터 준비",
            FetchParameters,
            False,
            True,
        ),
        (
            "data-inspection",
            "inspect_data",
            "행·열·결측치 확인",
            InspectParameters,
            True,
            False,
        ),
        (
            "descriptive-analysis",
            "describe_numeric",
            "숫자 열 기술통계",
            DescribeParameters,
            True,
            False,
        ),
        (
            "visualization",
            "plot_category_totals",
            "범주별 합계 SVG 차트",
            PlotParameters,
            True,
            False,
        ),
    )
    result = []
    for skill, tool, description, parameters, consumes, produces in specs:
        source = root.joinpath(f"{tool}.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        if (
            len(tree.body) != 1
            or not isinstance(tree.body[0], ast.FunctionDef)
            or tree.body[0].name != tool
            or tree.body[0].decorator_list
        ):
            raise ValueError("Catalog source must define exactly one function")
        result.append(
            CatalogTool(
                skill,
                tool,
                description,
                parameters,
                consumes,
                produces,
                root.joinpath(f"{skill}.md").read_text(encoding="utf-8"),
                source,
            )
        )
    return tuple(result)
