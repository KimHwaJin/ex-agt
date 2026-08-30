from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import asdict, dataclass
from math import ceil
from pathlib import Path
from time import perf_counter
from uuid import uuid4

import httpx
from live_dependency_outage_e2e import (
    Observation,
    _assert_session_locked,
    _compose,
    _database_audit,
    _json_request,
)
from live_multi_worker_e2e import (
    _assert_container_running,
    _command_audit,
    _delete_test_consumers,
    _docker,
    _executor_status,
    _remove_extra_worker,
    _start_extra_worker,
    _wait_for_executor_running,
    _wait_for_status,
    _wait_pending_zero,
)
from redis.asyncio import Redis

_TERMINAL_STATUSES = {"SUCCEEDED", "FAILED", "CANCELLED", "REJECTED"}


@dataclass
class RestartRecord:
    target: str
    started_seconds: float
    duration_seconds: float
    container_id: str


@dataclass
class LongTaskResult:
    task_id: str
    session_id: str
    execution_id: str
    task_status: str
    executor_status: str
    elapsed_seconds: float
    audit_convergence_after_task_seconds: float
    executor_binding_count: int
    task_completed_event_count: int
    executor_boundary_event_count: int
    session_locked_after_completion: bool
    max_command_attempt_count: int
    failure_compensation_count: int
    status_first_seen_seconds: dict[str, float]
    interrupts: list[str]


@dataclass
class ProbeResult:
    task_id: str
    session_id: str
    status: str
    elapsed_seconds: float
    audit_convergence_after_task_seconds: float
    task_completed_event_count: int
    max_command_attempt_count: int
    failure_compensation_count: int
    status_first_seen_seconds: dict[str, float]
    interrupts: list[str]


@dataclass
class SoakResult:
    requested_execution_seconds: float
    total_soak_seconds: float
    restart_count: int
    restarts: list[RestartRecord]
    long_task: LongTaskResult
    probe_count: int
    probe_success_count: int
    probe_latency_p50_seconds: float
    probe_latency_p95_seconds: float
    probe_latency_max_seconds: float
    probes: list[ProbeResult]
    command_pending_after_completion: int
    executor_pending_after_completion: int


async def _create_task(
    client: httpx.AsyncClient,
    project_id: str,
    request: str,
    *,
    label: str,
) -> tuple[str, str, Observation]:
    task_id = str(uuid4())
    session_id = f"soak-e2e-{label}-{uuid4()}"
    observation = Observation(perf_counter())
    await _json_request(
        client,
        "POST",
        f"projects/{project_id}/sessions/{session_id}/tasks",
        body={
            "task_id": task_id,
            "input_message_id": str(uuid4()),
            "content": request,
            "idempotency_key": f"soak-e2e-create-{task_id}",
        },
    )
    return task_id, session_id, observation


async def _container_name(container_id: str) -> str:
    name = await _docker("inspect", "--format", "{{.Name}}", container_id)
    return name.removeprefix("/")


async def _restart_worker(
    args: argparse.Namespace,
    target: str,
    primary_name: str,
    extra_name: str,
    started_at: float,
    state: dict[str, bool],
) -> RestartRecord:
    container_name = primary_name if target == "primary" else extra_name
    restart_started = perf_counter()
    await _docker(
        "stop",
        "--time",
        str(args.stop_timeout_seconds),
        container_name,
    )
    state[f"{target}_running"] = False
    if target == "primary":
        await _docker("rm", container_name)
        await _compose(
            args.compose_directory,
            "up",
            "--detach",
            "worker",
        )
    else:
        await _remove_extra_worker(extra_name)
        state["extra_exists"] = False
        await _start_extra_worker(args.compose_directory, extra_name)
        state["extra_exists"] = True
    state[f"{target}_running"] = True
    await asyncio.sleep(args.worker_ready_delay_seconds)
    await _assert_container_running(container_name)
    container_id = await _docker(
        "inspect",
        "--format",
        "{{.Id}}",
        container_name,
    )
    return RestartRecord(
        target=target,
        started_seconds=restart_started - started_at,
        duration_seconds=perf_counter() - restart_started,
        container_id=container_id,
    )


async def _rolling_restarts(
    args: argparse.Namespace,
    primary_name: str,
    extra_name: str,
    started_at: float,
    stop_event: asyncio.Event,
    state: dict[str, bool],
) -> list[RestartRecord]:
    records: list[RestartRecord] = []
    targets = ("primary", "extra")
    target_index = 0
    while True:
        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=args.restart_interval_seconds,
            )
            return records
        except TimeoutError:
            pass
        target = targets[target_index % len(targets)]
        records.append(
            await _restart_worker(
                args,
                target,
                primary_name,
                extra_name,
                started_at,
                state,
            )
        )
        target_index += 1


async def _run_probe(
    args: argparse.Namespace,
    client: httpx.AsyncClient,
    index: int,
) -> ProbeResult:
    if index:
        await asyncio.sleep(index * args.probe_interval_seconds)
    request = (
        "코드 실행이나 데이터 분석 없이 일반 질의로 답하세요. "
        f"다음 숫자를 그대로 포함해 한 문장으로 응답하세요: {index}"
    )
    task_id, session_id, observation = await _create_task(
        client,
        args.project_id,
        request,
        label=f"probe-{index}",
    )
    terminal = await _wait_for_status(
        client,
        task_id,
        observation,
        _TERMINAL_STATUSES,
        timeout_seconds=args.recovery_timeout_seconds,
        approve_plan=True,
    )
    elapsed = perf_counter() - observation.started_at
    (
        (binding, completed, boundaries, locked),
        audit_convergence,
    ) = await _wait_for_database_audit(
        args.compose_directory,
        task_id,
        session_id,
        expected=(0, 1, 0, False),
        timeout_seconds=args.recovery_timeout_seconds,
    )
    attempts, compensation = await _command_audit(
        args.compose_directory,
        task_id,
    )
    if terminal["status"] != "SUCCEEDED":
        raise RuntimeError(f"General QA probe failed: {terminal}")
    if (binding, completed, boundaries, locked) != (0, 1, 0, False):
        raise RuntimeError(
            f"Unexpected probe state for Task {task_id}: "
            f"{binding=}, {completed=}, {boundaries=}, {locked=}"
        )
    if compensation != 0:
        raise RuntimeError(
            f"Unexpected failure compensation for probe Task {task_id}"
        )
    return ProbeResult(
        task_id=task_id,
        session_id=session_id,
        status=str(terminal["status"]),
        elapsed_seconds=elapsed,
        audit_convergence_after_task_seconds=audit_convergence,
        task_completed_event_count=completed,
        max_command_attempt_count=attempts,
        failure_compensation_count=compensation,
        status_first_seen_seconds=observation.status_first_seen,
        interrupts=observation.interrupts,
    )


async def _wait_for_database_audit(
    compose_directory: Path,
    task_id: str,
    session_id: str,
    *,
    expected: tuple[int, int, int, bool],
    timeout_seconds: float,
) -> tuple[tuple[int, int, int, bool], float]:
    started_at = perf_counter()
    deadline = started_at + timeout_seconds
    actual = (0, 0, 0, False)
    while perf_counter() < deadline:
        actual = await _database_audit(
            compose_directory,
            task_id,
            session_id,
        )
        if actual == expected:
            return actual, perf_counter() - started_at
        await asyncio.sleep(1)
    raise TimeoutError(
        f"Database audit did not converge for Task {task_id}: "
        f"{actual=}, {expected=}"
    )


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    index = max(0, ceil(percentile * len(ordered)) - 1)
    return ordered[index]


async def _run(args: argparse.Namespace) -> SoakResult:
    started_at = perf_counter()
    extra_name = f"ex-agent-worker-soak-{uuid4().hex[:12]}"
    state = {
        "primary_running": True,
        "extra_running": False,
        "extra_exists": False,
    }
    redis = Redis.from_url(args.redis_url, decode_responses=True)
    primary_container_id = await _compose(
        args.compose_directory,
        "ps",
        "--quiet",
        "worker",
    )
    if not primary_container_id:
        raise RuntimeError("Primary Worker container is not running")
    primary_name = await _container_name(primary_container_id)
    stop_event = asyncio.Event()
    rolling_task: asyncio.Task[list[RestartRecord]] | None = None
    headers = {"X-User-ID": args.user_id}
    try:
        await _start_extra_worker(args.compose_directory, extra_name)
        state["extra_exists"] = True
        state["extra_running"] = True
        await asyncio.sleep(args.worker_ready_delay_seconds)
        await _assert_container_running(extra_name)

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
            long_request = args.request.format(
                execution_seconds=args.execution_seconds
            )
            task_id, session_id, observation = await _create_task(
                agent,
                args.project_id,
                long_request,
                label="long",
            )
            executing = await _wait_for_status(
                agent,
                task_id,
                observation,
                {"WAITING_FOR_EXECUTOR_EVENT", "EXECUTING"},
                timeout_seconds=args.phase_timeout_seconds,
                approve_plan=True,
            )
            execution_id = str(executing.get("execution_id", ""))
            if not execution_id:
                raise RuntimeError("Long-running Task omitted execution_id")
            await _wait_for_executor_running(
                executor,
                execution_id,
                timeout_seconds=args.phase_timeout_seconds,
            )
            locked_status = await _assert_session_locked(
                agent,
                args.project_id,
                session_id,
            )
            if locked_status not in {409, 423}:
                raise RuntimeError(
                    f"Session lock probe failed: {locked_status}"
                )

            rolling_task = asyncio.create_task(
                _rolling_restarts(
                    args,
                    primary_name,
                    extra_name,
                    started_at,
                    stop_event,
                    state,
                )
            )
            probe_tasks = [
                asyncio.create_task(_run_probe(args, agent, index))
                for index in range(args.probe_count)
            ]
            try:
                terminal = await _wait_for_status(
                    agent,
                    task_id,
                    observation,
                    _TERMINAL_STATUSES,
                    timeout_seconds=args.recovery_timeout_seconds,
                    approve_plan=True,
                )
                long_task_elapsed = perf_counter() - observation.started_at
                probes = list(await asyncio.gather(*probe_tasks))
            finally:
                stop_event.set()
                for probe_task in probe_tasks:
                    if not probe_task.done():
                        probe_task.cancel()
                await asyncio.gather(*probe_tasks, return_exceptions=True)
            restarts = await rolling_task
            rolling_task = None

            if terminal["status"] != "SUCCEEDED":
                raise RuntimeError(f"Long-running Task failed: {terminal}")
            if len(restarts) < args.minimum_restart_count:
                raise RuntimeError(
                    "Long-running Task completed before the minimum rolling "
                    f"restart count: {len(restarts)}"
                )
            expected_targets = [
                ("primary", "extra")[index % 2]
                for index in range(len(restarts))
            ]
            if [record.target for record in restarts] != expected_targets:
                raise RuntimeError("Rolling restart targets did not alternate")

            executor_status = await _executor_status(executor, execution_id)
            (
                (binding, completed, boundaries, locked),
                audit_convergence,
            ) = await _wait_for_database_audit(
                args.compose_directory,
                task_id,
                session_id,
                expected=(1, 1, 2, False),
                timeout_seconds=args.recovery_timeout_seconds,
            )
            attempts, compensation = await _command_audit(
                args.compose_directory,
                task_id,
            )
            if executor_status != "SUCCEEDED":
                raise RuntimeError(
                    f"Executor did not succeed: {executor_status}"
                )
            if (binding, completed, boundaries, locked) != (
                1,
                1,
                2,
                False,
            ):
                raise RuntimeError(
                    f"Duplicate or leaked long Task state: {binding=}, "
                    f"{completed=}, {boundaries=}, {locked=}"
                )
            if compensation != 0:
                raise RuntimeError(
                    "Unexpected failure compensation for long-running Task"
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
            state["extra_exists"] = False
            state["extra_running"] = False
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
        stop_event.set()
        if rolling_task is not None:
            await asyncio.gather(rolling_task, return_exceptions=True)
        if not state["primary_running"]:
            await _compose(
                args.compose_directory,
                "up",
                "--detach",
                "worker",
            )
        if state["extra_exists"]:
            await _remove_extra_worker(extra_name)
        await redis.aclose()

    latencies = [probe.elapsed_seconds for probe in probes]
    return SoakResult(
        requested_execution_seconds=args.execution_seconds,
        total_soak_seconds=perf_counter() - started_at,
        restart_count=len(restarts),
        restarts=restarts,
        long_task=LongTaskResult(
            task_id=task_id,
            session_id=session_id,
            execution_id=execution_id,
            task_status=str(terminal["status"]),
            executor_status=executor_status,
            elapsed_seconds=long_task_elapsed,
            audit_convergence_after_task_seconds=audit_convergence,
            executor_binding_count=binding,
            task_completed_event_count=completed,
            executor_boundary_event_count=boundaries,
            session_locked_after_completion=locked,
            max_command_attempt_count=attempts,
            failure_compensation_count=compensation,
            status_first_seen_seconds=observation.status_first_seen,
            interrupts=observation.interrupts,
        ),
        probe_count=len(probes),
        probe_success_count=sum(
            probe.status == "SUCCEEDED" for probe in probes
        ),
        probe_latency_p50_seconds=_percentile(latencies, 0.50),
        probe_latency_p95_seconds=_percentile(latencies, 0.95),
        probe_latency_max_seconds=max(latencies),
        probes=probes,
        command_pending_after_completion=pending_counts[0],
        executor_pending_after_completion=pending_counts[1],
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run a long Executor Task while alternating two Worker restarts."
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
    parser.add_argument("--user-id", default="soak-e2e-user")
    parser.add_argument("--project-id", default="soak-e2e-project")
    parser.add_argument("--execution-seconds", type=float, default=300)
    parser.add_argument(
        "--restart-interval-seconds",
        type=float,
        default=60,
    )
    parser.add_argument("--minimum-restart-count", type=int, default=2)
    parser.add_argument("--probe-count", type=int, default=6)
    parser.add_argument("--probe-interval-seconds", type=float, default=30)
    parser.add_argument("--stop-timeout-seconds", type=int, default=10)
    parser.add_argument(
        "--worker-ready-delay-seconds",
        type=float,
        default=2,
    )
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
    parser.add_argument("--phase-timeout-seconds", type=float, default=300)
    parser.add_argument(
        "--recovery-timeout-seconds",
        type=float,
        default=900,
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--request",
        default=(
            "데이터 분석과 무관한 자유 코드 실행 요청입니다. 정확히 하나의 "
            "함수 wait_and_return을 정의하고, 함수 내부에서 "
            "time.sleep({execution_seconds})를 호출한 뒤 문자열 "
            "'rolling-soak-ok'를 반환하세요. 함수를 정확히 한 번 호출해 "
            "반환값을 출력하고 종료해 주세요."
        ),
    )
    return parser


def main() -> None:
    args = _parser().parse_args()
    if args.execution_seconds <= 0:
        raise ValueError("execution-seconds must be positive")
    if args.restart_interval_seconds <= 0:
        raise ValueError("restart-interval-seconds must be positive")
    if args.minimum_restart_count < 2:
        raise ValueError("minimum-restart-count must be at least 2")
    if args.probe_count < 1:
        raise ValueError("probe-count must be at least 1")
    if args.probe_interval_seconds < 0:
        raise ValueError("probe-interval-seconds must not be negative")
    result = asdict(asyncio.run(_run(args)))
    rendered = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output is not None:
        args.output.write_text(rendered + "\n", encoding="utf-8")
    print(rendered)


if __name__ == "__main__":
    main()
