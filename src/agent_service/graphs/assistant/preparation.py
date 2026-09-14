"""Checkpoint exact code before publishing a code-free approval card."""

import asyncio
import json

from langchain_core.messages import AIMessage, BaseMessage, HumanMessage
from langchain_core.runnables import RunnableConfig

from agent_service.catalog.compiler import compile_proposal
from agent_service.catalog.registry import digest
from agent_service.domain.code_plans import CatalogProposal, GeneratedProposal

from .review import plan_text
from .state import WorkflowState


def preparation_node(planners):
    async def prepare(state: WorkflowState, config: RunnableConfig):
        draft = state["plan"]
        if not draft:
            raise ValueError("Missing plan draft")
        implementation = draft["implementation"]
        # A code task never consults the analysis catalog, even on a bad draft.
        if state["route"]["intent"] == "code_task":
            implementation = "generated_code"
        selected = planners[implementation]
        request = {
            "title": draft["title"],
            "summary": draft["summary"],
            "steps": draft["steps"],
            "revision_instruction": state.get("instruction"),
        }
        # No previous tool-source snapshots or catalog prompts enter free code.
        messages: list[BaseMessage] = [
            m for m in state["messages"] if isinstance(m, HumanMessage)
        ]
        messages.append(
            HumanMessage(content=json.dumps(request, ensure_ascii=False))
        )
        schema = (
            CatalogProposal
            if implementation == "catalog"
            else GeneratedProposal
        )
        feedback = []
        for attempt in range(2):
            result = await selected.ainvoke(
                {"messages": messages}, config=config
            )
            prepared = result["structured_response"]
            proposal = schema.model_validate(prepared["proposal"])
            try:
                steps, cells = await asyncio.to_thread(
                    compile_proposal, proposal
                )
                break
            except (ValueError, SyntaxError) as error:
                if attempt:
                    raise
                # One bounded model correction. This is not an execution retry.
                # Feedback stays internal, and no proposed code is executed.
                detail = (
                    f"Python syntax error on line {error.lineno}"
                    if isinstance(error, SyntaxError)
                    else str(error)[:2000]
                )
                feedback.append(detail)
                messages.extend(
                    [
                        AIMessage(content=proposal.model_dump_json()),
                        HumanMessage(
                            content=(
                                "계획 검증 실패. 원래 요청을 유지하고 "
                                "전체 계획을 한 번 수정하세요. "
                                "function_lines에는 def 하나만 작성하고 "
                                "함수 내부 import, 개행/들여쓰기를 지키세요. "
                                "카탈로그는 제공된 ID/버전/인자만 쓰세요. "
                                f"검증 오류: {detail}"
                            )
                        ),
                    ]
                )
        payload = {
            **draft,
            "steps": steps,
            "implementation": implementation,
            "stage": "code_prepared",
            "code_prepared": True,
            "executable": False,
            "execution_available": False,
            "notice": "셀 코드 준비 완료 · 아직 실행되지 않았습니다. "
            "Executor 연결 및 실행 전 검사가 필요합니다.",
            "generation_risk": prepared["risk"],
        }
        bundle = {
            "plan": payload,
            "cells": cells,
            "schema_version": 1,
            "validation_feedback": feedback,
            "generation_attempts": attempt + 1,
        }
        fingerprint = digest(
            json.dumps(
                bundle,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        )
        payload["plan_sha256"] = fingerprint
        return {
            "plan": payload,
            "prepared_plan": bundle,
            "messages": [
                AIMessage(
                    content=plan_text(payload),
                    id=f"{state['run_id']}:plan:{state['plan_version']}",
                )
            ],
        }

    return prepare
