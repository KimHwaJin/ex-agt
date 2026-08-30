from __future__ import annotations

import hashlib
import json
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field

from ex_agent.domain.contracts import (
    PlanStepDraft,
    SkillReference,
    ToolReference,
)
from ex_agent.domain.enums import PlanningKind


class ParameterSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: str
    required: bool = False


class ToolManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    skill: SkillReference
    tool: ToolReference
    description: str
    creation_rationale: str
    parameters: dict[str, ParameterSpec]
    source: str


class SkillDocument(BaseModel):
    name: str
    version: str
    description: str
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    content: str
    tools: list[ToolManifest]


class ToolRegistry:
    """Load immutable Skill and Tool definitions from the source tree."""

    def __init__(self, root: Path) -> None:
        self._root = root.resolve()
        self._skills: dict[str, SkillDocument] = {}
        self._tools: dict[str, ToolManifest] = {}

    def load(self) -> None:
        skills: dict[str, SkillDocument] = {}
        tools: dict[str, ToolManifest] = {}
        for manifest_path in sorted(self._root.glob("*/manifest.json")):
            skill_dir = manifest_path.parent.resolve()
            if self._root not in skill_dir.parents:
                raise ValueError("Skill path escaped the registry root")
            raw = json.loads(manifest_path.read_text(encoding="utf-8"))
            skill_meta = raw["skill"]
            skill_content = (skill_dir / "SKILL.md").read_text(
                encoding="utf-8"
            )
            skill_hash = _sha256(skill_content.encode())
            loaded_tools: list[ToolManifest] = []
            for item in raw["tools"]:
                source_path = (skill_dir / item["source"]).resolve()
                if skill_dir not in source_path.parents:
                    raise ValueError("Tool source escaped its Skill directory")
                source = source_path.read_text(encoding="utf-8")
                manifest = ToolManifest(
                    skill=SkillReference(
                        name=skill_meta["name"],
                        version=skill_meta["version"],
                        content_sha256=skill_hash,
                    ),
                    tool=ToolReference(
                        name=item["name"],
                        version=item["version"],
                        source_sha256=_sha256(source.encode()),
                    ),
                    description=item["description"],
                    creation_rationale=item["creation_rationale"],
                    parameters=item["parameters"],
                    source=source,
                )
                if manifest.tool.name in tools:
                    raise ValueError(
                        f"Duplicate Tool name: {manifest.tool.name}"
                    )
                tools[manifest.tool.name] = manifest
                loaded_tools.append(manifest)
            skill = SkillDocument(
                name=skill_meta["name"],
                version=skill_meta["version"],
                description=_frontmatter_value(
                    skill_content,
                    "description",
                ),
                content_sha256=skill_hash,
                content=skill_content,
                tools=loaded_tools,
            )
            if skill.name in skills:
                raise ValueError(f"Duplicate Skill name: {skill.name}")
            skills[skill.name] = skill
        self._skills = skills
        self._tools = tools

    def list_skills(self) -> list[SkillDocument]:
        return list(self._skills.values())

    def get_skill(self, name: str) -> SkillDocument:
        try:
            return self._skills[name]
        except KeyError as error:
            raise KeyError(f"Unknown Skill: {name}") from error

    def get_tool(self, name: str) -> ToolManifest:
        try:
            return self._tools[name]
        except KeyError as error:
            raise KeyError(f"Unknown Tool: {name}") from error

    def canonicalize_step_lineage(
        self,
        step: PlanStepDraft,
    ) -> PlanStepDraft:
        """Replace model-supplied versions and hashes with registry values."""
        if step.planning_kind is not PlanningKind.TOOL_PLAN:
            return step
        if step.skill is None or step.tool is None:
            raise ValueError("Tool Step is missing Skill/Tool lineage")
        manifest = self.get_tool(step.tool.name)
        if manifest.skill.name != step.skill.name:
            raise ValueError("Planner returned mismatched Skill/Tool lineage")
        return step.model_copy(
            update={
                "skill": manifest.skill,
                "tool": manifest.tool,
            }
        )

    def registry_snapshot_hash(self) -> str:
        snapshot = [
            {
                "skill": item.skill.model_dump(mode="json"),
                "tool": item.tool.model_dump(mode="json"),
            }
            for item in sorted(
                self._tools.values(),
                key=lambda manifest: manifest.tool.name,
            )
        ]
        encoded = json.dumps(
            snapshot,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        return _sha256(encoded)


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _frontmatter_value(content: str, key: str) -> str:
    lines = content.splitlines()
    if not lines or lines[0].strip() != "---":
        raise ValueError("SKILL.md must start with YAML frontmatter")
    prefix = f"{key}:"
    for line in lines[1:]:
        if line.strip() == "---":
            break
        if line.startswith(prefix):
            return line.removeprefix(prefix).strip()
    raise ValueError(f"SKILL.md frontmatter is missing {key!r}")
