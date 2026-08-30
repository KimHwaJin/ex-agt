from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import asdict, dataclass, field
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


@dataclass
class ScenarioResult:
    service: str
    task_id: str
    execution_id: str
    task_status: str
    executor_status: str
    outage_seconds: float
    recovery_after_restore_seconds: float
    total_recovery_seconds: float
    locked_before_status: int
    locked_during_status: int | None
    task_status_during_outage: str | None
    follow_up_task_id: str
    follow_up_status: str
    executor_binding_count: int
    task_completed_event_count: int
    executor_boundary_event_count: int
    session_locked_after_completion: bool
    redis_pending_after_completion: int
    status_first_seen_seconds: dict[str, float]
    interrupts: list[str]


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
) -> None:
    interrupt = task.get("current_interrupt")
    if not isinstance(interrupt, dict):
        return
    fingerprint = json.dumps(interrupt, sort_keys=True)
    if fingerprint in observation.handled_interrupts:
        return
    observation.handled_interrupts.add(fingerprint)
    kind = str(interrupt["kind"])
    observation.interrupts.append(kind)
    signal = _resume_signal(interrupt)
    if signal is None:
        return
    await _json_request(
        client,
        "POST",
        f"tasks/{task_id}/resume",
        body={
            "idempotency_key": f"dependency-e2e-resume-{uuid4()}",
            "signal": signal,
        },
    )


async def _wait_for_status(
    client: httpx.AsyncClient,
    task_id: str,
    observation: Observation,
    statuses: set[str],
    *,
    timeout_seconds: float,
    tolerate_unavailable: bool = False,
) -> dict[str, Any]:
    deadline = perf_counter() + timeout_seconds
    last_error: Exception | None = None
    while perf_counter() < deadline:
        try:
            task = await _json_request(client, "GET", f"tasks/{task_id}")
            observation.observe(task)
            await _resume_new_interrupt(
                client,
                task_id,
                task,
                observation,
            )
        except (httpx.HTTPError, httpx.TimeoutException) as error:
            if not tolerate_unavailable:
                raise
            last_error = error
            await asyncio.sleep(0.25)
            continue
        status = str(task["status"])
        if status in statuses:
            return task
        if status in _TERMINAL_STATUSES:
            raise RuntimeError(f"Task terminated before {statuses}: {task}")
        await asyncio.sleep(0.1)
    detail = f"; last transport error: {last_error}" if last_error else ""
    raise TimeoutError(f"Task {task_id} did not reach {statuses}{detail}")


async def _compose(
    compose_directory: Path,
    *arguments: str,
    check: bool = True,
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
    rendered = output.decode(errors="replace").strip()
    if check and process.returncode != 0:
        raise RuntimeError(
            f"docker compose {' '.join(arguments)} failed:\n{rendered}"
        )
    return rendered


async def _stop_service(compose_directory: Path, service: str) -> None:
    await _compose(compose_directory, "stop", "--timeout", "5", service)


async def _start_service(compose_directory: Path, service: str) -> None:
    await _compose(compose_directory, "start", service)
    deadline = perf_counter() + 60
    if service == "redis":
        check = ("exec", "-T", "redis", "redis-cli", "ping")
    else:
        check = (
            "exec",
            "-T",
            "postgres",
            "pg_isready",
            "-U",
            "agent",
            "-d",
            "agent",
        )
    while perf_counter() < deadline:
        output = await _compose(
            compose_directory,
            *check,
            check=False,
        )
        if output.endswith("PONG") or "accepting connections" in output:
            return
        await asyncio.sleep(0.25)
    raise TimeoutError(f"Compose service did not become healthy: {service}")


async def _wait_for_executor_running(
    client: httpx.AsyncClient,
    execution_id: str,
    *,
    timeout_seconds: float,
) -> None:
    deadline = perf_counter() + timeout_seconds
    while perf_counter() < deadline:
        result = await _json_request(
            client,
            "GET",
            f"executions/{execution_id}/result",
        )
        status = str(result["execution"]["state"]["status"])
        if status in {"RUNNING", "DISPATCHED"}:
            return
        if status in _TERMINAL_STATUSES:
            raise RuntimeError(
                f"Executor terminated before outage injection: {status}"
            )
        await asyncio.sleep(0.1)
    raise TimeoutError("Executor did not start before outage injection")


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
            "idempotency_key": f"dependency-e2e-locked-{uuid4()}",
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
            "content": "일반 질의입니다. 숫자 3과 4의 합만 알려주세요.",
            "idempotency_key": f"dependency-e2e-follow-up-{task_id}",
        },
    )
    return task_id


async def _database_audit(
    compose_directory: Path,
    task_id: str,
    session_id: str,
) -> tuple[int, int, int, bool]:
    query = (
        "select "
        "(select count(*) from agent_executor_bindings "
        f"where task_id = '{task_id}'), "
        "(select count(*) from agent_task_events "
        f"where task_id = '{task_id}' and event_type = 'task.completed'), "
        "(select count(*) from agent_task_events "
        f"where task_id = '{task_id}' "
        "and event_type = 'executor.boundary_received'), "
        "coalesce((select locked from agent_session_locks "
        f"where session_id = '{session_id}'), false)"
    )
    output = await _compose(
        compose_directory,
        "exec",
        "-T",
        "postgres",
        "psql",
        "-U",
        "agent",
        "-d",
        "agent",
        "-At",
        "-F",
        "|",
        "-c",
        query,
    )
    fields = output.split("|")
    if len(fields) != 4:
        raise RuntimeError(f"Unexpected PostgreSQL audit result: {output}")
    return (
        int(fields[0]),
        int(fields[1]),
        int(fields[2]),
        fields[3] == "t",
    )


async def _redis_pending(compose_directory: Path) -> int:
    output = await _compose(
        compose_directory,
        "exec",
        "-T",
        "redis",
        "redis-cli",
        "XPENDING",
        "executor.events",
        "agent-executor-events-v1",
    )
    first_line = output.splitlines()[0] if output else "0"
    return int(first_line)


async def _wait_for_no_pending(
    compose_directory: Path,
    *,
    timeout_seconds: float,
) -> int:
    deadline = perf_counter() + timeout_seconds
    pending = -1
    while perf_counter() < deadline:
        pending = await _redis_pending(compose_directory)
        if pending == 0:
            return pending
        await asyncio.sleep(0.5)
    raise TimeoutError(f"Redis executor event pending remained at {pending}")


async def _run_scenario(
    args: argparse.Namespace,
    service: str,
) -> ScenarioResult:
    task_id = str(uuid4())
    session_id = f"dependency-e2e-{service}-{uuid4()}"
    observation = Observation(perf_counter())
    service_running = True
    headers = {"X-User-ID": args.user_id}
    async with (
        httpx.AsyncClient(
            base_url=args.agent_base_url.rstrip("/") + "/api/v1/",
            headers=headers,
            timeout=10,
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
                    "idempotency_key": (
                        f"dependency-e2e-create-{service}-{task_id}"
                    ),
                },
            )
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
            locked_before = await _assert_session_locked(
                agent,
                args.project_id,
                session_id,
            )

            outage_started_at = perf_counter()
            await _stop_service(args.compose_directory, service)
            service_running = False
            locked_during: int | None = None
            task_status_during: str | None = None
            if service == "redis":
                await asyncio.sleep(1)
                during = await _json_request(
                    agent,
                    "GET",
                    f"tasks/{task_id}",
                )
                task_status_during = str(during["status"])
                if task_status_during in _TERMINAL_STATUSES:
                    raise RuntimeError(
                        "Agent Task completed while Redis was unavailable"
                    )
                locked_during = await _assert_session_locked(
                    agent,
                    args.project_id,
                    session_id,
                )
            await asyncio.sleep(args.outage_seconds)
            await _start_service(args.compose_directory, service)
            service_running = True
            restored_at = perf_counter()

            terminal = await _wait_for_status(
                agent,
                task_id,
                observation,
                _TERMINAL_STATUSES,
                timeout_seconds=args.recovery_timeout_seconds,
                tolerate_unavailable=True,
            )
            completed_at = perf_counter()
            if terminal["status"] != "SUCCEEDED":
                raise RuntimeError(f"Recovered Task failed: {terminal}")
            result = await _json_request(
                executor,
                "GET",
                f"executions/{execution_id}/result",
            )
            executor_status = result["execution"]["state"]["status"]
            if executor_status != "SUCCEEDED":
                raise RuntimeError(f"Recovered Executor failed: {result}")

            follow_up_id = await _create_follow_up(
                agent,
                args.project_id,
                session_id,
            )
            follow_up = await _wait_for_status(
                agent,
                follow_up_id,
                Observation(perf_counter()),
                _TERMINAL_STATUSES,
                timeout_seconds=args.phase_timeout_seconds,
                tolerate_unavailable=True,
            )
            if follow_up["status"] != "SUCCEEDED":
                raise RuntimeError(f"Follow-up Task failed: {follow_up}")

            (
                binding_count,
                completed_count,
                boundary_count,
                locked,
            ) = await _database_audit(
                args.compose_directory,
                task_id,
                session_id,
            )
            if (binding_count, completed_count, boundary_count, locked) != (
                1,
                1,
                2,
                False,
            ):
                raise RuntimeError(
                    "Recovery audit detected duplicate or leaked state: "
                    f"{binding_count=}, {completed_count=}, "
                    f"{boundary_count=}, {locked=}"
                )
            pending = await _wait_for_no_pending(
                args.compose_directory,
                timeout_seconds=args.recovery_timeout_seconds,
            )
        finally:
            if not service_running:
                await _start_service(args.compose_directory, service)

    return ScenarioResult(
        service=service,
        task_id=task_id,
        execution_id=str(execution_id),
        task_status=str(terminal["status"]),
        executor_status=str(executor_status),
        outage_seconds=restored_at - outage_started_at,
        recovery_after_restore_seconds=completed_at - restored_at,
        total_recovery_seconds=completed_at - outage_started_at,
        locked_before_status=locked_before,
        locked_during_status=locked_during,
        task_status_during_outage=task_status_during,
        follow_up_task_id=follow_up_id,
        follow_up_status=str(follow_up["status"]),
        executor_binding_count=binding_count,
        task_completed_event_count=completed_count,
        executor_boundary_event_count=boundary_count,
        session_locked_after_completion=locked,
        redis_pending_after_completion=pending,
        status_first_seen_seconds=observation.status_first_seen,
        interrupts=observation.interrupts,
    )


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    services = (
        [args.scenario] if args.scenario != "all" else ["redis", "postgres"]
    )
    results = []
    for service in services:
        results.append(asdict(await _run_scenario(args, service)))
    return {"scenarios": results}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Stop Redis/PostgreSQL during a live execution and verify "
            "recovery."
        )
    )
    parser.add_argument(
        "--scenario",
        choices=("redis", "postgres", "all"),
        default="all",
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
    parser.add_argument("--user-id", default="dependency-e2e-user")
    parser.add_argument("--project-id", default="dependency-e2e-project")
    parser.add_argument("--outage-seconds", type=float, default=20)
    parser.add_argument("--phase-timeout-seconds", type=float, default=180)
    parser.add_argument(
        "--recovery-timeout-seconds",
        type=float,
        default=300,
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--request",
        default=(
            "데이터 분석과 무관한 자유 코드 실행 요청입니다. 정확히 하나의 "
            "함수 wait_and_return을 정의하고, 함수 내부에서 time.sleep(15)를 "
            "호출한 뒤 문자열 'dependency-outage-ok'를 반환하세요. 함수를 "
            "정확히 한 번 호출해 반환값을 출력하고 종료해 주세요."
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
