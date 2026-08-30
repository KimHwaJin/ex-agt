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
    interrupts: list[str] = field(default_factory=list)
    handled_interrupts: set[str] = field(default_factory=set)

    def observe(self, task: dict[str, Any]) -> None:
        status = str(task["status"])
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
            "answer": "요청한 대기 함수를 한 번 실행하고 결과를 알려주세요.",
        }
    if kind == "EXECUTION_MODE":
        return {"type": "EXECUTION_MODE", "mode": "SINGLE"}
    if kind == "REQUEST_RISK_CONFIRMATION":
        return {"type": kind, "confirmed": True}
    if kind == "PLAN_REVIEW":
        return {
            "type": "PLAN_REVIEW",
            "decision": "APPROVE",
            "plan_revision_id": interrupt["plan_revision_id"],
            "plan_revision_number": interrupt["plan_revision_number"],
            "public_payload_hash": interrupt["public_payload_hash"],
            "risk_acknowledged": True,
        }
    return None


async def _resume_new_interrupt(
    client: httpx.AsyncClient,
    task_id: str,
    task: dict[str, Any],
    observation: Observation,
) -> str | None:
    interrupt = task.get("current_interrupt")
    if not isinstance(interrupt, dict):
        return None
    fingerprint = (
        f"{task.get('version')}:{json.dumps(interrupt, sort_keys=True)}"
    )
    if fingerprint in observation.handled_interrupts:
        return str(interrupt["kind"])
    observation.handled_interrupts.add(fingerprint)
    kind = str(interrupt["kind"])
    observation.interrupts.append(kind)
    signal = _resume_signal(interrupt)
    if signal is not None:
        await _json_request(
            client,
            "POST",
            f"tasks/{task_id}/resume",
            body={
                "idempotency_key": f"restart-e2e-resume-{uuid4()}",
                "signal": signal,
            },
        )
    return kind


async def _wait_for_status(
    client: httpx.AsyncClient,
    task_id: str,
    observation: Observation,
    statuses: set[str],
    *,
    timeout_seconds: float,
) -> dict[str, Any]:
    deadline = perf_counter() + timeout_seconds
    while perf_counter() < deadline:
        task = await _json_request(client, "GET", f"tasks/{task_id}")
        observation.observe(task)
        await _resume_new_interrupt(client, task_id, task, observation)
        status = str(task["status"])
        if status in statuses:
            return task
        if status in _TERMINAL_STATUSES:
            raise RuntimeError(f"Task terminated before {statuses}: {task}")
        await asyncio.sleep(0.1)
    raise TimeoutError(f"Task {task_id} did not reach {statuses}")


async def _wait_for_terminal(
    client: httpx.AsyncClient,
    task_id: str,
    observation: Observation,
    *,
    timeout_seconds: float,
) -> dict[str, Any]:
    return await _wait_for_status(
        client,
        task_id,
        observation,
        _TERMINAL_STATUSES,
        timeout_seconds=timeout_seconds,
    )


async def _compose(
    compose_directory: Path,
    *arguments: str,
) -> str:
    process = await asyncio.create_subprocess_exec(
        "docker",
        "compose",
        *arguments,
        cwd=compose_directory,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    output, _ = await process.communicate()
    rendered = output.decode(errors="replace")
    if process.returncode != 0:
        raise RuntimeError(
            f"docker compose {' '.join(arguments)} failed:\n{rendered}"
        )
    return rendered


async def _kill_worker(compose_directory: Path) -> None:
    await _compose(
        compose_directory,
        "kill",
        "--signal",
        "SIGKILL",
        "worker",
    )


async def _start_worker(compose_directory: Path) -> None:
    await _compose(compose_directory, "up", "--detach", "worker")


async def _wait_for_executor_running(
    client: httpx.AsyncClient,
    execution_id: str,
    *,
    timeout_seconds: float,
) -> dict[str, Any]:
    deadline = perf_counter() + timeout_seconds
    while perf_counter() < deadline:
        result = await _json_request(
            client,
            "GET",
            f"executions/{execution_id}/result",
        )
        status = str(result["execution"]["state"]["status"])
        if status in {"RUNNING", "DISPATCHED"}:
            return result
        if status in _TERMINAL_STATUSES:
            raise RuntimeError(
                f"Executor terminated before restart injection: {status}"
            )
        await asyncio.sleep(0.1)
    raise TimeoutError("Executor did not start before fault injection")


async def _assert_session_locked(
    client: httpx.AsyncClient,
    project_id: str,
    session_id: str,
) -> int:
    response = await client.post(
        f"projects/{project_id}/sessions/{session_id}/tasks",
        json={
            "task_id": str(uuid4()),
            "input_message_id": str(uuid4()),
            "content": "잠금 확인용 요청",
            "idempotency_key": f"restart-e2e-locked-{uuid4()}",
        },
    )
    if response.status_code not in {409, 423}:
        raise RuntimeError(
            "Session accepted a new Task while execution was active: "
            f"{response.status_code} {response.text}"
        )
    return response.status_code


async def _create_follow_up(
    client: httpx.AsyncClient,
    project_id: str,
    session_id: str,
) -> str:
    task_id = str(uuid4())
    await _json_request(
        client,
        "POST",
        f"projects/{project_id}/sessions/{session_id}/tasks",
        body={
            "task_id": task_id,
            "input_message_id": str(uuid4()),
            "content": "일반 질의입니다. 숫자 2와 2를 더한 값만 알려주세요.",
            "idempotency_key": f"restart-e2e-follow-up-{task_id}",
        },
    )
    return task_id


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    task_id = str(uuid4())
    session_id = f"restart-e2e-session-{uuid4()}"
    observation = Observation(perf_counter())
    worker_running = True
    headers = {"X-User-ID": args.user_id}
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
        try:
            await _json_request(
                agent,
                "POST",
                f"projects/{args.project_id}/sessions/{session_id}/tasks",
                body={
                    "task_id": task_id,
                    "input_message_id": str(uuid4()),
                    "content": args.request,
                    "idempotency_key": f"restart-e2e-create-{task_id}",
                },
            )
            await _wait_for_status(
                agent,
                task_id,
                observation,
                {"PLANNING"},
                timeout_seconds=args.phase_timeout_seconds,
            )

            await _kill_worker(args.compose_directory)
            worker_running = False
            planning_killed_at = perf_counter()
            await asyncio.sleep(args.planning_downtime_seconds)
            await _start_worker(args.compose_directory)
            worker_running = True
            await _wait_for_status(
                agent,
                task_id,
                observation,
                {"WAITING_FOR_APPROVAL"},
                timeout_seconds=args.recovery_timeout_seconds,
            )
            planning_recovery = perf_counter() - planning_killed_at

            task = await _wait_for_status(
                agent,
                task_id,
                observation,
                {"WAITING_FOR_EXECUTOR_EVENT", "EXECUTING"},
                timeout_seconds=args.phase_timeout_seconds,
            )
            execution_id = task.get("execution_id")
            if not execution_id:
                raise RuntimeError(f"Task omitted execution_id: {task}")
            await _wait_for_executor_running(
                executor,
                str(execution_id),
                timeout_seconds=args.phase_timeout_seconds,
            )

            await _kill_worker(args.compose_directory)
            worker_running = False
            execution_killed_at = perf_counter()
            locked_status = await _assert_session_locked(
                agent,
                args.project_id,
                session_id,
            )
            await asyncio.sleep(args.execution_downtime_seconds)
            await _start_worker(args.compose_directory)
            worker_running = True

            terminal = await _wait_for_terminal(
                agent,
                task_id,
                observation,
                timeout_seconds=args.recovery_timeout_seconds,
            )
            execution_recovery = perf_counter() - execution_killed_at
            if terminal["status"] != "SUCCEEDED":
                raise RuntimeError(f"Recovered Task failed: {terminal}")
            result = await _json_request(
                executor,
                "GET",
                f"executions/{execution_id}/result",
            )
            if result["execution"]["state"]["status"] != "SUCCEEDED":
                raise RuntimeError(f"Recovered Executor failed: {result}")

            follow_up_id = await _create_follow_up(
                agent,
                args.project_id,
                session_id,
            )
            follow_up = await _wait_for_terminal(
                agent,
                follow_up_id,
                Observation(perf_counter()),
                timeout_seconds=args.phase_timeout_seconds,
            )
            if follow_up["status"] != "SUCCEEDED":
                raise RuntimeError(f"Follow-up Task failed: {follow_up}")
        finally:
            if not worker_running:
                await _start_worker(args.compose_directory)

    return {
        "task_id": task_id,
        "execution_id": str(execution_id),
        "task_status": terminal["status"],
        "executor_status": result["execution"]["state"]["status"],
        "planning_restart_recovery_seconds": planning_recovery,
        "execution_restart_recovery_seconds": execution_recovery,
        "locked_probe_status": locked_status,
        "follow_up_task_id": follow_up_id,
        "follow_up_status": follow_up["status"],
        "status_first_seen_seconds": observation.status_first_seen,
        "interrupts": observation.interrupts,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inject two Worker SIGKILL failures into a live Task."
    )
    parser.add_argument(
        "--agent-base-url",
        default="http://127.0.0.1:8010",
    )
    parser.add_argument(
        "--executor-base-url",
        default="http://127.0.0.1:8000",
    )
    parser.add_argument(
        "--compose-directory",
        type=Path,
        default=Path(__file__).resolve().parents[1],
    )
    parser.add_argument("--user-id", default="restart-e2e-user")
    parser.add_argument("--project-id", default="restart-e2e-project")
    parser.add_argument("--planning-downtime-seconds", type=float, default=3)
    parser.add_argument(
        "--execution-downtime-seconds",
        type=float,
        default=20,
    )
    parser.add_argument("--phase-timeout-seconds", type=float, default=180)
    parser.add_argument(
        "--recovery-timeout-seconds",
        type=float,
        default=240,
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--request",
        default=(
            "데이터 분석과 무관한 자유 코드 실행 요청입니다. 정확히 하나의 "
            "함수 wait_and_return을 정의하고, 함수 내부에서 time.sleep(15)를 "
            "호출한 뒤 문자열 'worker-restart-ok'를 반환하세요. 함수를 정확히 "
            "한 번 호출해 반환값을 출력하고 종료해 주세요."
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
