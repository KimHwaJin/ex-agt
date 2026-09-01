"""Opt-in Agent -> Executor -> Jupyter -> Worker lifecycle validation."""

from __future__ import annotations

import asyncio
import os
from typing import Any, cast
from uuid import uuid4

import httpx
import pytest

from ex_agent.domain.enums import TaskStatus

_API_URL = os.getenv("EX_AGENT_TEST_LIVE_EXECUTION_API_URL")
_EXECUTOR_URL = os.getenv("EX_AGENT_TEST_LIVE_EXECUTOR_URL")
_USER_ID = "live-executor-e2e-user"
_PROJECT_ID = "live-executor-e2e-project"

pytestmark = [
    pytest.mark.llm,
    pytest.mark.executor,
    pytest.mark.skipif(
        not (_API_URL and _EXECUTOR_URL),
        reason="Live Agent execution lifecycle disabled",
    ),
]


async def test_live_single_execution_reaches_reported_notebook() -> None:
    """Exercise direct API admission and Worker-owned event resumption."""

    async with _api_client() as api:
        task_id = await _start_task(
            api,
            (
                "SINGLE 모드 자유 코드 실행 요청입니다. 한 셀에 함수 "
                "정의 하나와 호출 하나를 함께 넣어 Python으로 1부터 "
                "10까지 합을 계산하고 출력해주세요."
            ),
        )
        task, kinds, plan = await _drive_task(api, task_id, "SINGLE")

    assert kinds == ["PLAN_REVIEW"]
    assert len(plan["steps"]) == 1
    _assert_successful_task(task)
    assert "55" in task["terminal_message"]

    result, notebook = await _executor_evidence(task["execution_id"])
    assert {item["type"] for item in result["artifacts"]} >= {
        "NOTEBOOK",
        "REPORT",
    }
    code_cells = _code_cells(notebook)
    assert len(code_cells) == 1
    assert any(
        output.get("text") == "55\n" for output in code_cells[0]["outputs"]
    )
    _assert_report_cell(task, notebook)


async def test_live_multi_analysis_creates_plot_and_single_review() -> None:
    """Exercise tool planning, incremental operations and report delivery."""

    async with _api_client() as api:
        task_id = await _start_task(
            api,
            (
                "워크플로우 후보는 선택하지 않고 MULTI 모드로 분석해주세요. "
                "외부 데이터 쿼리 'SELECT * FROM sample_sales'를 사용해 "
                "샘플 매출 데이터를 생성하고, 데이터 구조와 결측치를 "
                "점검하고, 수치형 요약 통계와 segment별 revenue 평균을 "
                "계산하고, revenue 분포 플롯을 만든 뒤 결과 리포트를 "
                "작성해주세요."
            ),
        )
        task, kinds, plan = await _drive_task(api, task_id, "MULTI")

    assert kinds.count("WORKFLOW_SELECTION") <= 1
    assert kinds.count("PLAN_REVIEW") == 1
    assert set(kinds) <= {"WORKFLOW_SELECTION", "PLAN_REVIEW"}
    assert len(plan["steps"]) == 1
    _assert_successful_task(task)

    result, notebook = await _executor_evidence(task["execution_id"])
    assert len(result["operations"]) >= 2
    assert all(
        operation["result"]["status"] == "SUCCEEDED"
        for operation in result["operations"]
    )
    assert {item["type"] for item in result["artifacts"]} >= {
        "DATASET",
        "NOTEBOOK",
        "PLOT",
        "REPORT",
    }
    assert len(_code_cells(notebook)) >= 4
    _assert_report_cell(task, notebook)


def _api_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=f"{cast(str, _API_URL).rstrip('/')}/api/v1/",
        headers={"X-User-ID": _USER_ID},
        timeout=httpx.Timeout(300),
    )


async def _start_task(api: httpx.AsyncClient, content: str) -> str:
    task_id = str(uuid4())
    session_id = str(uuid4())
    response = await api.post(
        f"projects/{_PROJECT_ID}/sessions/{session_id}/tasks",
        json={
            "task_id": task_id,
            "input_message_id": str(uuid4()),
            "content": content,
            "idempotency_key": f"live-e2e:start:{task_id}",
        },
    )
    response.raise_for_status()
    assert response.status_code == 202
    return task_id


async def _drive_task(
    api: httpx.AsyncClient,
    task_id: str,
    expected_mode: str,
) -> tuple[dict[str, Any], list[str], dict[str, Any]]:
    deadline = asyncio.get_running_loop().time() + 600
    handled: set[str] = set()
    kinds: list[str] = []
    reviewed_plan: dict[str, Any] | None = None
    while asyncio.get_running_loop().time() < deadline:
        response = await api.get(f"tasks/{task_id}")
        response.raise_for_status()
        task = response.json()
        if TaskStatus(task["status"]).is_terminal:
            assert reviewed_plan is not None
            return task, kinds, reviewed_plan

        interrupt = task["current_interrupt"]
        if not interrupt or interrupt["kind"] == "EXECUTOR_EVENT":
            await asyncio.sleep(0.5)
            continue
        interrupt_id = interrupt["interrupt_id"]
        if interrupt_id in handled:
            await asyncio.sleep(0.5)
            continue

        kind = interrupt["kind"]
        kinds.append(kind)
        if kind == "WORKFLOW_SELECTION":
            signal = {
                "type": kind,
                "workflow_version_id": None,
                "proposal_version": interrupt["proposal_version"],
                "public_payload_hash": "0" * 64,
                "input_values": {},
                "risk_acknowledged": False,
            }
        elif kind == "EXECUTION_MODE":
            signal = {"type": kind, "mode": expected_mode}
        elif kind == "PLAN_REVIEW":
            assert reviewed_plan is None, interrupt
            reviewed_plan = interrupt["plan"]
            assert reviewed_plan["execution_mode"] == expected_mode
            signal = {
                "type": kind,
                "decision": "APPROVE",
                "plan_revision_id": interrupt["plan_revision_id"],
                "plan_revision_number": interrupt["plan_revision_number"],
                "public_payload_hash": interrupt["public_payload_hash"],
                "feedback": None,
                "risk_acknowledged": interrupt["risk"]["level"] == "HIGH",
            }
        elif kind == "REQUEST_RISK_CONFIRMATION":
            signal = {"type": kind, "confirmed": True}
        else:
            raise AssertionError(f"Unexpected human interrupt: {interrupt}")

        handled.add(interrupt_id)
        response = await api.post(
            f"tasks/{task_id}/resume",
            json={
                "idempotency_key": f"live-e2e:{kind}:{interrupt_id}",
                "signal": signal,
            },
        )
        response.raise_for_status()
    raise TimeoutError(f"Task did not finish: {task_id}")


async def _executor_evidence(
    execution_id: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    async with httpx.AsyncClient(
        base_url=f"{cast(str, _EXECUTOR_URL).rstrip('/')}/api/v1/",
        timeout=30,
    ) as executor:
        result_response = await executor.get(
            f"executions/{execution_id}/result"
        )
        result_response.raise_for_status()
        result = result_response.json()
        assert result["execution"]["state"]["status"] == "SUCCEEDED"

        notebook_response = await executor.get(
            f"executions/{execution_id}/notebook",
            params={"view": "FULL", "limit": 200},
        )
        notebook_response.raise_for_status()
        notebook = notebook_response.json()
    assert notebook["page"]["has_more"] is False
    return result, notebook


def _assert_successful_task(task: dict[str, Any]) -> None:
    assert task["status"] == TaskStatus.SUCCEEDED
    assert task["current_interrupt"] is None
    assert task["execution_id"]
    assert task["terminal_message"]


def _code_cells(notebook: dict[str, Any]) -> list[dict[str, Any]]:
    return [cell for cell in notebook["cells"] if cell["type"] == "code"]


def _assert_report_cell(
    task: dict[str, Any], notebook: dict[str, Any]
) -> None:
    assert notebook["cells"][-1]["type"] == "markdown"
    assert notebook["cells"][-1]["source"] == task["terminal_message"]
