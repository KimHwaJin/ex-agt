from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import dataclass, field
from pathlib import Path
from time import perf_counter
from typing import Any
from uuid import uuid4

import httpx

_TERMINAL_STATUSES = {"SUCCEEDED", "FAILED", "CANCELLED", "REJECTED"}


@dataclass
class Observation:
    started_at: float
    status_first_seen: dict[str, float] = field(default_factory=dict)
    interrupts: list[dict[str, Any]] = field(default_factory=list)
    plan_steps: list[dict[str, Any]] = field(default_factory=list)
    approval_seconds: float | None = None
    execution_boundaries: int = 0

    def observe_status(self, status: str) -> None:
        self.status_first_seen.setdefault(
            status,
            perf_counter() - self.started_at,
        )


async def _json_request(
    client: httpx.AsyncClient,
    method: str,
    path: str,
    *,
    body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    response = await client.request(method, path, json=body)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict):
        raise TypeError(f"Expected object response from {path}")
    return payload


def _resume_signal(interrupt: dict[str, Any]) -> dict[str, Any] | None:
    kind = interrupt["kind"]
    if kind == "CLARIFICATION":
        return {
            "type": "CLARIFICATION",
            "answer": (
                "샘플 데이터를 생성한 뒤 스키마를 점검하고 종료해 주세요."
            ),
        }
    if kind == "REQUEST_RISK_CONFIRMATION":
        return {"type": "REQUEST_RISK_CONFIRMATION", "confirmed": True}
    if kind == "WORKFLOW_SELECTION":
        return {
            "type": "WORKFLOW_SELECTION",
            "workflow_version_id": None,
            "proposal_version": interrupt["proposal_version"],
            "public_payload_hash": "0" * 64,
            "risk_acknowledged": False,
        }
    if kind == "EXECUTION_MODE":
        return {"type": "EXECUTION_MODE", "mode": "MULTI"}
    if kind == "PLAN_REVIEW":
        return {
            "type": "PLAN_REVIEW",
            "decision": "APPROVE",
            "plan_revision_id": interrupt["plan_revision_id"],
            "plan_revision_number": interrupt["plan_revision_number"],
            "public_payload_hash": interrupt["public_payload_hash"],
            "risk_acknowledged": False,
        }
    return None


def _interrupt_record(
    interrupt: dict[str, Any],
    elapsed: float,
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "kind": interrupt["kind"],
        "elapsed_seconds": elapsed,
    }
    if interrupt["kind"] == "PLAN_REVIEW":
        record["plan_revision_number"] = interrupt["plan_revision_number"]
        record["planning_kind"] = interrupt["plan"]["steps"][0][
            "planning_kind"
        ]
    if interrupt["kind"] == "EXECUTOR_EVENT":
        record["last_event_sequence"] = interrupt.get("last_event_sequence")
    return record


async def _drive_task(
    client: httpx.AsyncClient,
    task_id: str,
    *,
    timeout_seconds: float,
    poll_seconds: float,
    observation: Observation,
) -> dict[str, Any]:
    deadline = perf_counter() + timeout_seconds
    handled: set[str] = set()
    while perf_counter() < deadline:
        task = await _json_request(client, "GET", f"tasks/{task_id}")
        status = str(task["status"])
        observation.observe_status(status)
        interrupt = task.get("current_interrupt")
        if isinstance(interrupt, dict):
            fingerprint = json.dumps(interrupt, sort_keys=True)
            if fingerprint not in handled:
                handled.add(fingerprint)
                elapsed = perf_counter() - observation.started_at
                observation.interrupts.append(
                    _interrupt_record(interrupt, elapsed)
                )
                if interrupt["kind"] == "EXECUTOR_EVENT":
                    observation.execution_boundaries += 1
                signal = _resume_signal(interrupt)
                if signal is not None:
                    if interrupt["kind"] == "PLAN_REVIEW":
                        observation.plan_steps.extend(
                            interrupt["plan"]["steps"]
                        )
                        observation.approval_seconds = elapsed
                    await _json_request(
                        client,
                        "POST",
                        f"tasks/{task_id}/resume",
                        body={
                            "idempotency_key": (
                                f"live-multi-resume-{uuid4()}"
                            ),
                            "signal": signal,
                        },
                    )
        if status in _TERMINAL_STATUSES:
            return task
        await asyncio.sleep(poll_seconds)
    raise TimeoutError(f"Task {task_id} did not finish within the timeout")


def _operation_summary(result: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            "operation_number": operation["operation_number"],
            "status": operation["result"]["status"],
            "steps": [
                {
                    "sequence": step["sequence"],
                    "status": step["result"]["status"],
                    "skill_name": step["lineage"].get("skill_name"),
                    "tool_name": step["lineage"].get("tool_name"),
                    "parameters": step["lineage"].get(
                        "input_parameters",
                        {},
                    ),
                }
                for step in operation["steps"]
            ],
        }
        for operation in result["operations"]
    ]


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    task_id = str(uuid4())
    session_id = f"live-multi-session-{uuid4()}"
    headers = {"X-User-ID": args.user_id}
    started_at = perf_counter()
    observation = Observation(started_at)
    async with (
        httpx.AsyncClient(
            base_url=args.agent_base_url.rstrip("/") + "/api/v1/",
            headers=headers,
            timeout=30,
        ) as agent,
        httpx.AsyncClient(
            base_url=args.executor_base_url.rstrip("/") + "/api/v1/",
            timeout=30,
        ) as executor,
    ):
        accepted = await _json_request(
            agent,
            "POST",
            f"projects/{args.project_id}/sessions/{session_id}/tasks",
            body={
                "task_id": task_id,
                "input_message_id": str(uuid4()),
                "content": args.request,
                "idempotency_key": f"live-multi-create-{task_id}",
            },
        )
        accepted_seconds = perf_counter() - started_at
        terminal = await _drive_task(
            agent,
            task_id,
            timeout_seconds=args.timeout_seconds,
            poll_seconds=args.poll_seconds,
            observation=observation,
        )
        total_seconds = perf_counter() - started_at
        execution_id = terminal.get("execution_id")
        if not execution_id:
            raise RuntimeError(f"Task has no execution_id: {terminal}")
        result = await _json_request(
            executor,
            "GET",
            f"executions/{execution_id}/result",
        )
        notebook = await _json_request(
            executor,
            "GET",
            f"executions/{execution_id}/notebook?view=FULL&limit=200",
        )

    operations = _operation_summary(result)
    code_cells = [cell for cell in notebook["cells"] if cell["type"] == "code"]
    markdown_cells = [
        cell for cell in notebook["cells"] if cell["type"] == "markdown"
    ]
    if terminal["status"] != "SUCCEEDED":
        raise RuntimeError(f"Agent Task failed: {terminal}")
    if result["execution"]["state"]["status"] != "SUCCEEDED":
        raise RuntimeError(f"Executor did not succeed: {result}")
    if len(operations) < 2:
        raise RuntimeError("MULTI E2E must execute at least two Operations")
    if len(code_cells) < 2:
        raise RuntimeError("MULTI E2E notebook must contain two code cells")
    if not markdown_cells:
        raise RuntimeError("Successful E2E must append a Markdown report")

    return {
        "task_id": task_id,
        "execution_id": execution_id,
        "task_status": terminal["status"],
        "executor_status": result["execution"]["state"]["status"],
        "latency_seconds": {
            "accepted": accepted_seconds,
            "plan_ready": observation.approval_seconds,
            "execution_visible": observation.status_first_seen.get(
                "WAITING_FOR_EXECUTOR_EVENT"
            ),
            "report_started": observation.status_first_seen.get(
                "GENERATING_REPORT"
            ),
            "total": total_seconds,
        },
        "status_first_seen_seconds": observation.status_first_seen,
        "interrupts": observation.interrupts,
        "plan_steps": observation.plan_steps,
        "operations": operations,
        "notebook": {
            "total_cells": notebook["page"]["total_count"],
            "code_cells": len(code_cells),
            "markdown_cells": len(markdown_cells),
            "report_preview": markdown_cells[-1]["source"][:500],
        },
        "accepted_response": accepted,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run a real Agent, Executor, Redis, PostgreSQL, and Jupyter "
            "MULTI analysis lifecycle."
        )
    )
    parser.add_argument(
        "--agent-base-url",
        default="http://127.0.0.1:8010",
    )
    parser.add_argument(
        "--executor-base-url",
        default="http://127.0.0.1:8000",
    )
    parser.add_argument("--user-id", default="live-multi-user")
    parser.add_argument("--project-id", default="live-multi-project")
    parser.add_argument("--timeout-seconds", type=float, default=900)
    parser.add_argument("--poll-seconds", type=float, default=0.5)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--request",
        default=(
            "데이터 레이크 담당자가 준 쿼리 `SELECT * FROM sales_sample`로 "
            "샘플 분석 데이터를 생성하고 점검해 주세요. 첫 셀에서는 반드시 "
            "data-access의 fetch_dataset Tool을 사용해 dataset_name을 "
            "`agent_multi_e2e`, output_format을 `csv`, seed를 20260828로 "
            "지정하세요. 첫 셀의 실제 출력에서 생성된 path를 확인한 뒤, "
            "두 번째 셀에서는 반드시 data-inspection의 inspect_dataset "
            "Tool로 그 path의 스키마와 3개 샘플 행을 확인하세요. 두 번째 "
            "결과가 성공하면 추가 분석 없이 실행을 종료하고 한국어 성공 "
            "리포트를 작성하세요."
        ),
    )
    return parser


def main() -> None:
    args = _parser().parse_args()
    result = asyncio.run(_run(args))
    rendered = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output is not None:
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)


if __name__ == "__main__":
    main()
