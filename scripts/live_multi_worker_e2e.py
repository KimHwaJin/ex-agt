from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from time import perf_counter
from typing import Any
from uuid import uuid4

import httpx
from live_dependency_outage_e2e import (
    Observation,
    _assert_session_locked,
    _compose,
    _database_audit,
    _json_request,
    _resume_signal,
)
from redis.asyncio import Redis

_TERMINAL_STATUSES = {"SUCCEEDED", "FAILED", "CANCELLED", "REJECTED"}


@dataclass
class TaskResult:
    task_id: str
    session_id: str
    execution_id: str
    task_status: str
    executor_status: str
    executor_binding_count: int
    task_completed_event_count: int
    executor_boundary_event_count: int
    session_locked_after_completion: bool
    max_command_attempt_count: int
    failure_compensation_count: int
    status_first_seen_seconds: dict[str, float]
    interrupts: list[str]


@dataclass
class MultiWorkerResult:
    killed_worker: str
    killed_consumer: str
    recovered_task_id: str
    failover_recovery_seconds: float
    command_worker_instances: int
    executor_event_worker_instances: int
    concurrent_executor_running: bool
    command_pending_after_completion: int
    executor_pending_after_completion: int
    tasks: list[TaskResult]


async def _docker(*arguments: str, check: bool = True) -> str:
    process = await asyncio.create_subprocess_exec(
        "docker",
        *arguments,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    output, _ = await process.communicate()
    rendered = output.decode(errors="replace").strip()
    if check and process.returncode != 0:
        raise RuntimeError(f"docker {' '.join(arguments)} failed:\n{rendered}")
    return rendered


async def _start_extra_worker(
    compose_directory: Path,
    container_name: str,
) -> None:
    await _compose(
        compose_directory,
        "run",
        "--detach",
        "--name",
        container_name,
        "--no-deps",
        "--env",
        "WORKER_METRICS_ENABLED=false",
        "--env",
        f"WORKER_INSTANCE_ID={container_name}",
        "worker",
    )


async def _remove_extra_worker(container_name: str) -> None:
    await _docker("rm", "--force", container_name, check=False)


async def _assert_container_running(container_name: str) -> None:
    running = await _docker(
        "inspect",
        "--format",
        "{{.State.Running}}",
        container_name,
    )
    if running != "true":
        logs = await _docker("logs", container_name, check=False)
        raise RuntimeError(
            f"Worker container is not running: {container_name}\n{logs}"
        )


async def _resume_new_interrupt(
    client: httpx.AsyncClient,
    task_id: str,
    task: dict[str, Any],
    observation: Observation,
    *,
    approve_plan: bool,
) -> None:
    interrupt = task.get("current_interrupt")
    if not isinstance(interrupt, dict):
        return
    if interrupt.get("kind") == "PLAN_REVIEW" and not approve_plan:
        return
    fingerprint = (
        f"{task.get('version')}:{json.dumps(interrupt, sort_keys=True)}"
    )
    if fingerprint in observation.handled_interrupts:
        return
    signal = _resume_signal(interrupt)
    if signal is None:
        return
    observation.handled_interrupts.add(fingerprint)
    observation.interrupts.append(str(interrupt["kind"]))
    await _json_request(
        client,
        "POST",
        f"tasks/{task_id}/resume",
        body={
            "idempotency_key": f"multi-worker-e2e-resume-{uuid4()}",
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
    approve_plan: bool = False,
) -> dict[str, Any]:
    deadline = perf_counter() + timeout_seconds
    while perf_counter() < deadline:
        task = await _json_request(client, "GET", f"tasks/{task_id}")
        observation.observe(task)
        await _resume_new_interrupt(
            client,
            task_id,
            task,
            observation,
            approve_plan=approve_plan,
        )
        status = str(task["status"])
        if status in statuses:
            return task
        if status in _TERMINAL_STATUSES:
            raise RuntimeError(f"Task terminated before {statuses}: {task}")
        await asyncio.sleep(0.1)
    raise TimeoutError(f"Task {task_id} did not reach {statuses}")


async def _pending_owner_for_tasks(
    redis: Redis,
    stream: str,
    group: str,
    task_ids: set[str],
) -> tuple[str, str] | None:
    pending = await redis.xpending_range(
        stream,
        group,
        min="-",
        max="+",
        count=100,
    )
    for item in pending:
        message_id = str(item["message_id"])
        entries = await redis.xrange(
            stream,
            min=message_id,
            max=message_id,
            count=1,
        )
        if not entries:
            continue
        fields = entries[0][1]
        task_id = str(fields.get("task_id", ""))
        if task_id in task_ids:
            return task_id, str(item["consumer"])
    return None


async def _wait_for_active_owner(
    client: httpx.AsyncClient,
    redis: Redis,
    created: list[tuple[str, str, Observation]],
    stream: str,
    group: str,
    *,
    timeout_seconds: float,
) -> tuple[str, str]:
    deadline = perf_counter() + timeout_seconds
    task_ids = {task_id for task_id, _, _ in created}
    while perf_counter() < deadline:
        states: dict[str, dict[str, Any]] = {}
        for task_id, _, observation in created:
            task = await _json_request(client, "GET", f"tasks/{task_id}")
            states[task_id] = task
            observation.observe(task)
            await _resume_new_interrupt(
                client,
                task_id,
                task,
                observation,
                approve_plan=False,
            )
        owner = await _pending_owner_for_tasks(
            redis,
            stream,
            group,
            task_ids,
        )
        if owner is not None:
            task = states[owner[0]]
            if (
                task["status"] in {"CLASSIFYING", "PLANNING"}
                and task.get("current_interrupt") is None
            ):
                return owner
        await asyncio.sleep(0.05)
    raise TimeoutError("No actively processing command owner found")


async def _create_task(
    client: httpx.AsyncClient,
    project_id: str,
    request: str,
) -> tuple[str, str, Observation]:
    task_id = str(uuid4())
    session_id = f"multi-worker-e2e-{uuid4()}"
    observation = Observation(perf_counter())
    await _json_request(
        client,
        "POST",
        f"projects/{project_id}/sessions/{session_id}/tasks",
        body={
            "task_id": task_id,
            "input_message_id": str(uuid4()),
            "content": request,
            "idempotency_key": f"multi-worker-e2e-create-{task_id}",
        },
    )
    return task_id, session_id, observation


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
                f"Executor terminated before running snapshot: {status}"
            )
        await asyncio.sleep(0.1)
    raise TimeoutError(f"Executor did not start: {execution_id}")


async def _executor_status(
    client: httpx.AsyncClient,
    execution_id: str,
) -> str:
    result = await _json_request(
        client,
        "GET",
        f"executions/{execution_id}/result",
    )
    return str(result["execution"]["state"]["status"])


async def _command_audit(
    compose_directory: Path,
    task_id: str,
) -> tuple[int, int]:
    query = (
        "select coalesce(max(attempt_count), 0), "
        "count(*) filter (where command_type = 'FAILURE_COMPENSATION') "
        "from agent_workflow_commands "
        f"where task_id = '{task_id}'"
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
    if len(fields) != 2:
        raise RuntimeError(f"Unexpected command audit result: {output}")
    return int(fields[0]), int(fields[1])


async def _pending_count(redis: Redis, stream: str, group: str) -> int:
    pending: Any = await redis.xpending(stream, group)
    if isinstance(pending, dict):
        return int(pending.get("pending", 0))
    return 0


async def _wait_pending_zero(
    redis: Redis,
    streams: tuple[tuple[str, str], ...],
    *,
    timeout_seconds: float,
) -> tuple[int, ...]:
    deadline = perf_counter() + timeout_seconds
    counts: tuple[int, ...] = ()
    while perf_counter() < deadline:
        counts = tuple(
            [
                await _pending_count(redis, stream, group)
                for stream, group in streams
            ]
        )
        if all(count == 0 for count in counts):
            return counts
        await asyncio.sleep(0.5)
    raise TimeoutError(f"Redis pending messages remained: {counts}")


async def _delete_test_consumers(
    redis: Redis,
    stream: str,
    group: str,
    prefix: str,
) -> None:
    consumers = await redis.xinfo_consumers(stream, group)
    for consumer in consumers:
        name = str(consumer["name"])
        if name.startswith(prefix) and int(consumer["pending"]) == 0:
            await redis.xgroup_delconsumer(stream, group, name)


async def _run(args: argparse.Namespace) -> MultiWorkerResult:
    extra_name = f"ex-agent-worker-e2e-{uuid4().hex[:12]}"
    extra_exists = False
    primary_running = True
    redis = Redis.from_url(args.redis_url, decode_responses=True)
    primary_container_id = await _compose(
        args.compose_directory,
        "ps",
        "--quiet",
        "worker",
    )
    if not primary_container_id:
        raise RuntimeError("Primary Worker container is not running")
    headers = {"X-User-ID": args.user_id}
    try:
        await _start_extra_worker(args.compose_directory, extra_name)
        extra_exists = True
        await asyncio.sleep(2)
        await _assert_container_running(extra_name)

        async with (
            httpx.AsyncClient(
                base_url=args.agent_base_url.rstrip("/") + "/api/v1/",
                headers=headers,
                timeout=30,
            ) as agent,
            httpx.AsyncClient(
                base_url=(args.executor_base_url.rstrip("/") + "/api/v1/"),
                timeout=30,
            ) as executor,
        ):
            created = await asyncio.gather(
                *(
                    _create_task(agent, args.project_id, args.request)
                    for _ in range(args.task_count)
                )
            )
            (
                recovered_task_id,
                killed_consumer,
            ) = await _wait_for_active_owner(
                agent,
                redis,
                created,
                args.command_stream,
                args.command_group,
                timeout_seconds=10,
            )
            killed_at = perf_counter()
            extra_consumer_prefix = f"worker-{extra_name}-command-"
            if killed_consumer.startswith(extra_consumer_prefix):
                killed_worker = "extra"
                await _docker(
                    "kill",
                    "--signal",
                    "SIGKILL",
                    extra_name,
                )
            else:
                killed_worker = "primary"
                await _docker(
                    "kill",
                    "--signal",
                    "SIGKILL",
                    primary_container_id,
                )
                primary_running = False

            approvals = await asyncio.gather(
                *(
                    _wait_for_status(
                        agent,
                        task_id,
                        observation,
                        {"WAITING_FOR_APPROVAL"},
                        timeout_seconds=args.recovery_timeout_seconds,
                    )
                    for task_id, _, observation in created
                )
            )
            recovered_index = [item[0] for item in created].index(
                recovered_task_id
            )
            failover_recovery = perf_counter() - killed_at

            if killed_worker == "extra":
                await _remove_extra_worker(extra_name)
                extra_exists = False
                await _start_extra_worker(
                    args.compose_directory,
                    extra_name,
                )
                extra_exists = True
            else:
                await _compose(
                    args.compose_directory,
                    "up",
                    "--detach",
                    "worker",
                )
                primary_running = True
            await asyncio.sleep(2)
            await _assert_container_running(extra_name)

            await asyncio.gather(
                *(
                    _resume_new_interrupt(
                        agent,
                        task_id,
                        approvals[index],
                        observation,
                        approve_plan=True,
                    )
                    for index, (task_id, _, observation) in enumerate(created)
                )
            )
            executing = await asyncio.gather(
                *(
                    _wait_for_status(
                        agent,
                        task_id,
                        observation,
                        {"WAITING_FOR_EXECUTOR_EVENT", "EXECUTING"},
                        timeout_seconds=args.phase_timeout_seconds,
                        approve_plan=True,
                    )
                    for task_id, _, observation in created
                )
            )
            execution_ids = [
                str(task["execution_id"])
                for task in executing
                if task.get("execution_id")
            ]
            if len(execution_ids) != args.task_count:
                raise RuntimeError("One or more Tasks omitted execution_id")
            if len(set(execution_ids)) != args.task_count:
                raise RuntimeError("Tasks reused an Executor execution_id")
            await asyncio.gather(
                *(
                    _wait_for_executor_running(
                        executor,
                        execution_id,
                        timeout_seconds=args.phase_timeout_seconds,
                    )
                    for execution_id in execution_ids
                )
            )
            running_snapshot = await asyncio.gather(
                *(
                    _executor_status(executor, execution_id)
                    for execution_id in execution_ids
                )
            )
            concurrent_running = all(
                status in {"RUNNING", "DISPATCHED"}
                for status in running_snapshot
            )
            if not concurrent_running:
                raise RuntimeError(
                    f"Executor operations did not overlap: {running_snapshot}"
                )
            locked_statuses = await asyncio.gather(
                *(
                    _assert_session_locked(
                        agent,
                        args.project_id,
                        session_id,
                    )
                    for _, session_id, _ in created
                )
            )
            if any(status not in {409, 423} for status in locked_statuses):
                raise RuntimeError(
                    f"Session lock probe failed: {locked_statuses}"
                )

            terminal = await asyncio.gather(
                *(
                    _wait_for_status(
                        agent,
                        task_id,
                        observation,
                        _TERMINAL_STATUSES,
                        timeout_seconds=args.recovery_timeout_seconds,
                        approve_plan=True,
                    )
                    for task_id, _, observation in created
                )
            )
            if any(task["status"] != "SUCCEEDED" for task in terminal):
                raise RuntimeError(f"One or more Tasks failed: {terminal}")

            task_results: list[TaskResult] = []
            for index, (task_id, session_id, observation) in enumerate(
                created
            ):
                executor_status = await _executor_status(
                    executor,
                    execution_ids[index],
                )
                binding, completed, boundaries, locked = await _database_audit(
                    args.compose_directory,
                    task_id,
                    session_id,
                )
                attempts, compensation = await _command_audit(
                    args.compose_directory,
                    task_id,
                )
                if (binding, completed, boundaries, locked) != (
                    1,
                    1,
                    2,
                    False,
                ):
                    raise RuntimeError(
                        f"Duplicate or leaked state for Task {task_id}: "
                        f"{binding=}, {completed=}, {boundaries=}, {locked=}"
                    )
                if compensation != 0:
                    raise RuntimeError(
                        f"Unexpected failure compensation for Task {task_id}"
                    )
                task_results.append(
                    TaskResult(
                        task_id=task_id,
                        session_id=session_id,
                        execution_id=execution_ids[index],
                        task_status=str(terminal[index]["status"]),
                        executor_status=executor_status,
                        executor_binding_count=binding,
                        task_completed_event_count=completed,
                        executor_boundary_event_count=boundaries,
                        session_locked_after_completion=locked,
                        max_command_attempt_count=attempts,
                        failure_compensation_count=compensation,
                        status_first_seen_seconds=(
                            observation.status_first_seen
                        ),
                        interrupts=observation.interrupts,
                    )
                )
            recovered_attempts = task_results[
                recovered_index
            ].max_command_attempt_count
            if recovered_attempts < 2:
                raise RuntimeError(
                    "Killed command was not reclaimed by another Worker"
                )

            pending_counts = await _wait_pending_zero(
                redis,
                (
                    (args.command_stream, args.command_group),
                    (args.executor_stream, args.executor_group),
                ),
                timeout_seconds=args.recovery_timeout_seconds,
            )
            await _remove_extra_worker(extra_name)
            extra_exists = False
            await _delete_test_consumers(
                redis,
                args.command_stream,
                args.command_group,
                f"worker-{extra_name}-command-",
            )
            await _delete_test_consumers(
                redis,
                args.executor_stream,
                args.executor_group,
                f"worker-{extra_name}-executor-",
            )
    finally:
        if not primary_running:
            await _compose(
                args.compose_directory,
                "up",
                "--detach",
                "worker",
            )
        if extra_exists:
            await _remove_extra_worker(extra_name)
        await redis.aclose()

    return MultiWorkerResult(
        killed_worker=killed_worker,
        killed_consumer=killed_consumer,
        recovered_task_id=recovered_task_id,
        failover_recovery_seconds=failover_recovery,
        command_worker_instances=2,
        executor_event_worker_instances=2,
        concurrent_executor_running=concurrent_running,
        command_pending_after_completion=pending_counts[0],
        executor_pending_after_completion=pending_counts[1],
        tasks=task_results,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run two Agent Workers, kill a command owner, and verify failover."
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
    parser.add_argument(
        "--redis-url",
        default="redis://127.0.0.1:56379/0",
    )
    parser.add_argument(
        "--compose-directory",
        type=Path,
        default=Path(__file__).resolve().parents[1],
    )
    parser.add_argument("--user-id", default="multi-worker-e2e-user")
    parser.add_argument("--project-id", default="multi-worker-e2e-project")
    parser.add_argument("--task-count", type=int, default=2)
    parser.add_argument("--command-stream", default="agent.commands")
    parser.add_argument(
        "--command-group",
        default="agent-workflow-workers-v1",
    )
    parser.add_argument("--executor-stream", default="executor.events")
    parser.add_argument(
        "--executor-group",
        default="agent-executor-events-v1",
    )
    parser.add_argument("--phase-timeout-seconds", type=float, default=240)
    parser.add_argument(
        "--recovery-timeout-seconds",
        type=float,
        default=360,
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--request",
        default=(
            "데이터 분석과 무관한 자유 코드 실행 요청입니다. 정확히 하나의 "
            "함수 wait_and_return을 정의하고, 함수 내부에서 time.sleep(15)를 "
            "호출한 뒤 문자열 'multi-worker-ok'를 반환하세요. 함수를 정확히 "
            "한 번 호출해 반환값을 출력하고 종료해 주세요."
        ),
    )
    return parser


def main() -> None:
    args = _parser().parse_args()
    if args.task_count < 2:
        raise ValueError("task-count must be at least 2")
    result = asdict(asyncio.run(_run(args)))
    rendered = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output is not None:
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)


if __name__ == "__main__":
    main()
