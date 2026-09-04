from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import Any
from uuid import uuid4

ROOT = Path(__file__).resolve().parents[1]
DEPLOY_ROOT = ROOT / "deploy" / "rolling-e2e"
_IDENTIFIER = re.compile(r"^[a-z][a-z0-9_]{0,62}$")
_RUN_ID = re.compile(r"^[a-z0-9]{1,16}$")


@dataclass(frozen=True)
class Environment:
    cluster: str
    context: str
    namespace: str
    database: str
    executor_event_group: str
    suffix: str


async def _command(
    *arguments: str,
    input_text: str | None = None,
    cwd: Path = ROOT,
) -> str:
    process = await asyncio.create_subprocess_exec(
        *arguments,
        cwd=cwd,
        stdin=(asyncio.subprocess.PIPE if input_text is not None else None),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    output, _ = await process.communicate(
        None if input_text is None else input_text.encode()
    )
    rendered = output.decode(errors="replace")
    if process.returncode != 0:
        raise RuntimeError(
            f"{' '.join(arguments)} failed with {process.returncode}:\n"
            f"{rendered}"
        )
    return rendered


def _kind_config(
    shared_directory: Path,
    *,
    host_port: int = 18010,
    node_port: int = 30010,
) -> str:
    host_path = json.dumps(str(shared_directory.resolve()))
    return (
        "kind: Cluster\n"
        "apiVersion: kind.x-k8s.io/v1alpha4\n"
        "nodes:\n"
        "  - role: control-plane\n"
        "    extraMounts:\n"
        f"      - hostPath: {host_path}\n"
        "        containerPath: /workspace/shared\n"
        "    extraPortMappings:\n"
        f"      - containerPort: {node_port}\n"
        f"        hostPort: {host_port}\n"
        "        listenAddress: 127.0.0.1\n"
        "        protocol: TCP\n"
    )


async def _ensure_cluster(
    environment: Environment,
    shared_directory: Path,
    *,
    reuse: bool,
    host_port: int = 18010,
    node_port: int = 30010,
) -> None:
    clusters = set((await _command("kind", "get", "clusters")).split())
    if environment.cluster in clusters:
        if not reuse:
            raise RuntimeError(
                f"kind cluster {environment.cluster!r} already exists; "
                "pass --reuse-cluster only after verifying its shared mount"
            )
        return
    await _command(
        "kind",
        "create",
        "cluster",
        "--name",
        environment.cluster,
        "--config=-",
        input_text=_kind_config(
            shared_directory,
            host_port=host_port,
            node_port=node_port,
        ),
    )


async def _build_and_load_image(environment: Environment) -> None:
    await _command(
        "docker",
        "build",
        "--target",
        "runtime",
        "--tag",
        "ex-agent:rolling-e2e",
        ".",
    )
    await _command(
        "kind",
        "load",
        "docker-image",
        "ex-agent:rolling-e2e",
        "--name",
        environment.cluster,
    )


async def _create_database(
    environment: Environment,
    args: argparse.Namespace,
) -> None:
    if not _IDENTIFIER.fullmatch(environment.database):
        raise ValueError("Invalid generated PostgreSQL database name")
    query = (
        "SELECT count(*) FROM pg_database WHERE datname = "
        f"'{environment.database}'"
    )
    existing = await _command(
        "docker",
        "exec",
        args.postgres_container,
        "psql",
        "-U",
        args.postgres_admin_user,
        "-d",
        args.postgres_admin_database,
        "-Atc",
        query,
    )
    if existing.strip() != "0" and args.reuse_run:
        return
    if existing.strip() != "0":
        raise RuntimeError(
            f"Test database already exists: {environment.database}"
        )
    await _command(
        "docker",
        "exec",
        args.postgres_container,
        "psql",
        "-U",
        args.postgres_admin_user,
        "-d",
        args.postgres_admin_database,
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        (
            f'CREATE DATABASE "{environment.database}" '
            f'OWNER "{args.agent_database_user}"'
        ),
    )
    await _command(
        "docker",
        "exec",
        args.postgres_container,
        "psql",
        "-U",
        args.postgres_admin_user,
        "-d",
        environment.database,
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        "CREATE EXTENSION IF NOT EXISTS vector",
    )


async def _create_executor_group(
    environment: Environment,
    args: argparse.Namespace,
) -> None:
    if args.reuse_run:
        return
    await _command(
        "docker",
        "exec",
        args.redis_container,
        "redis-cli",
        "XGROUP",
        "CREATE",
        args.executor_event_stream,
        environment.executor_event_group,
        "$",
        "MKSTREAM",
    )


async def _kubectl(
    environment: Environment,
    *arguments: str,
    input_text: str | None = None,
) -> str:
    return await _command(
        "kubectl",
        "--context",
        environment.context,
        "--namespace",
        environment.namespace,
        *arguments,
        input_text=input_text,
    )


async def _apply_generated_resource(
    environment: Environment,
    resource_arguments: tuple[str, ...],
    literals: dict[str, str],
) -> None:
    arguments = [
        "create",
        *resource_arguments,
        "--dry-run=client",
        "-o",
        "yaml",
    ]
    for key, value in literals.items():
        arguments.append(f"--from-literal={key}={value}")
    manifest = await _kubectl(environment, *arguments)
    await _kubectl(
        environment,
        "apply",
        "-f",
        "-",
        input_text=manifest,
    )


async def _deploy(
    environment: Environment,
    args: argparse.Namespace,
) -> None:
    if not args.reuse_run:
        await _command(
            "kubectl",
            "--context",
            environment.context,
            "create",
            "namespace",
            environment.namespace,
        )
    prefix = f"rolling-e2e-{environment.suffix}"
    await _apply_generated_resource(
        environment,
        ("configmap", "ex-agent-runtime"),
        {
            "APP_ENV": "rolling-e2e",
            "LOG_LEVEL": "INFO",
            "HOST": "0.0.0.0",
            "PORT": "8010",
            "AGENT_COMMAND_STREAM": f"{prefix}.commands",
            "AGENT_COMMAND_CONSUMER_GROUP": f"{prefix}.commands-v1",
            "AGENT_COMMAND_DEAD_LETTER_STREAM": f"{prefix}.commands.dlq",
            "AGENT_PRODUCT_EVENT_STREAM": f"{prefix}.product-events",
            "AGENT_PRODUCT_EVENT_CHANNEL_PREFIX": f"{prefix}.task-events",
            "EXECUTOR_EVENT_STREAM": args.executor_event_stream,
            "EXECUTOR_EVENT_CONSUMER_GROUP": (
                environment.executor_event_group
            ),
            "EXECUTOR_EVENT_DEAD_LETTER_STREAM": (
                f"{prefix}.executor-events.dlq"
            ),
            "EXECUTOR_WORKER_NAMESPACE": prefix,
            "EXECUTOR_BASE_URL": args.executor_base_url_in_cluster,
            "EXECUTOR_SHARED_STORAGE_ROOT": "/workspace/shared",
            "EXECUTOR_SOURCE_MODE": "PATH",
            "EXECUTOR_RUNTIME_PROFILE": "basic",
            "AGENT_MODEL": args.agent_model,
            "AGENT_MODEL_PROVIDER": "openai",
            "AGENT_MODEL_BASE_URL": args.agent_model_base_url,
            "AGENT_MODEL_ENABLE_THINKING": "false",
            "AGENT_EMBEDDING_PROVIDER": "dummy",
            "AGENT_EMBEDDING_MODEL": "dummy-hash-v1",
            "AGENT_EMBEDDING_DIMENSIONS": "1024",
            "AGENT_SKILL_ROOT": "/app/skills",
            "WORKER_COMMAND_CONCURRENCY": "1",
            "WORKER_EXECUTOR_EVENT_CONCURRENCY": "1",
            "CHECKPOINT_POOL_MIN_SIZE": "1",
            "CHECKPOINT_POOL_MAX_SIZE": "2",
            "COMMAND_BLOCK_MILLISECONDS": "500",
            "COMMAND_CLAIM_IDLE_MILLISECONDS": "6000",
            "EXECUTOR_EVENT_CLAIM_IDLE_MILLISECONDS": "6000",
            "TASK_LOCK_TTL_SECONDS": "30",
            "TASK_LOCK_RENEW_INTERVAL_SECONDS": "5",
            "EXECUTOR_EVENT_LOCK_TTL_SECONDS": "30",
            "EXECUTOR_EVENT_LOCK_RENEW_INTERVAL_SECONDS": "5",
            "WORKER_SHUTDOWN_GRACE_SECONDS": "25",
            "WORKER_METRICS_ENABLED": "true",
            "WORKER_METRICS_HOST": "0.0.0.0",
            "WORKER_METRICS_PORT": "8011",
            "WORKER_METRICS_REFRESH_SECONDS": "2",
            "WORKER_READINESS_STALE_SECONDS": "10",
        },
    )
    database_url = (
        "postgresql://"
        f"{args.agent_database_user}:{args.agent_database_password}"
        f"@host.docker.internal:5432/{environment.database}"
    )
    await _apply_generated_resource(
        environment,
        ("secret", "generic", "ex-agent-runtime"),
        {
            "AGENT_DATABASE_URL": database_url.replace(
                "postgresql://",
                "postgresql+psycopg://",
                1,
            ),
            "AGENT_CHECKPOINT_DATABASE_URL": database_url,
            "AGENT_REDIS_URL": "redis://host.docker.internal:6379/0",
            "AGENT_MODEL_API_KEY": args.agent_model_api_key,
        },
    )
    if args.reuse_run:
        await _kubectl(
            environment,
            "delete",
            "job/ex-agent-migrate",
            "--ignore-not-found=true",
        )
    await _kubectl(
        environment,
        "apply",
        "-f",
        str(DEPLOY_ROOT / "migrate-job.yaml"),
    )
    await _kubectl(
        environment,
        "wait",
        "--for=condition=complete",
        "job/ex-agent-migrate",
        "--timeout=180s",
    )
    await _kubectl(
        environment,
        "apply",
        "-f",
        str(DEPLOY_ROOT / "workloads.yaml"),
    )
    for deployment in ("ex-agent-api", "ex-agent-worker"):
        await _kubectl(
            environment,
            "rollout",
            "status",
            f"deployment/{deployment}",
            "--timeout=300s",
        )


async def _run_recovery_scenario(
    environment: Environment,
    args: argparse.Namespace,
) -> dict[str, Any]:
    with tempfile.TemporaryDirectory(prefix="ex-agent-k8s-e2e-") as path:
        output = Path(path) / "result.json"
        await _command(
            sys.executable,
            str(ROOT / "scripts" / "live_worker_restart_e2e.py"),
            "--worker-control",
            "kubernetes",
            "--agent-base-url",
            args.agent_base_url,
            "--executor-base-url",
            args.executor_base_url,
            "--kube-context",
            environment.context,
            "--kube-namespace",
            environment.namespace,
            "--execution-downtime-seconds",
            "0",
            "--output",
            str(output),
        )
        payload = json.loads(output.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise TypeError("Recovery scenario output must be an object")
    return payload


def _sql_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


async def _database_value(
    environment: Environment,
    args: argparse.Namespace,
    query: str,
) -> str:
    output = await _command(
        "docker",
        "exec",
        args.postgres_container,
        "psql",
        "-U",
        args.agent_database_user,
        "-d",
        environment.database,
        "-Atc",
        query,
    )
    return output.strip()


async def _verify_durable_state(
    environment: Environment,
    args: argparse.Namespace,
    scenario: dict[str, Any],
) -> dict[str, Any]:
    task_id = _sql_literal(scenario["task_id"])
    session_id = _sql_literal(scenario["session_id"])
    execution_id = _sql_literal(scenario["execution_id"])
    task_row = await _database_value(
        environment,
        args,
        (
            "SELECT status || '|' || execution_id::text FROM agent_tasks "
            f"WHERE id={task_id}"
        ),
    )
    agent_binding_count = int(
        await _database_value(
            environment,
            args,
            (
                "SELECT count(*) FROM agent_executor_bindings "
                f"WHERE task_id={task_id} AND execution_id={execution_id}"
            ),
        )
    )
    completion_event_count = int(
        await _database_value(
            environment,
            args,
            (
                "SELECT count(*) FROM agent_task_events "
                f"WHERE task_id={task_id} AND event_type='task.completed'"
            ),
        )
    )
    checkpoint_count = int(
        await _database_value(
            environment,
            args,
            (f"SELECT count(*) FROM checkpoints WHERE thread_id={session_id}"),
        )
    )
    worker_binding = await _database_value(
        environment,
        args,
        (
            "SELECT session_id || '|' || task_id::text || '|' || "
            "last_sequence::text FROM ew_bindings "
            f"WHERE execution_id={execution_id}"
        ),
    )
    lock_count = int(
        await _database_value(
            environment,
            args,
            (
                "SELECT count(*) FROM agent_session_locks "
                f"WHERE session_id={session_id}"
            ),
        )
    )
    deadline = perf_counter() + args.durable_drain_timeout_seconds
    while True:
        pending_output = await _command(
            "docker",
            "exec",
            args.redis_container,
            "redis-cli",
            "XPENDING",
            args.executor_event_stream,
            environment.executor_event_group,
        )
        pending_count = int(pending_output.splitlines()[0])
        incomplete_worker_commands = int(
            await _database_value(
                environment,
                args,
                (
                    "SELECT count(*) FROM ew_commands "
                    "WHERE state NOT IN ('DONE', 'IGNORED')"
                ),
            )
        )
        unsent_worker_outbox = int(
            await _database_value(
                environment,
                args,
                "SELECT count(*) FROM ew_outbox WHERE state != 'SENT'",
            )
        )
        if (
            pending_count == 0
            and incomplete_worker_commands == 0
            and unsent_worker_outbox == 0
        ):
            break
        if perf_counter() >= deadline:
            raise RuntimeError(
                "Worker durable pipeline did not drain before timeout: "
                f"pending={pending_count}, "
                f"commands={incomplete_worker_commands}, "
                f"outbox={unsent_worker_outbox}"
            )
        await asyncio.sleep(0.2)
    expected_task = f"SUCCEEDED|{scenario['execution_id']}"
    expected_binding_prefix = (
        f"{scenario['session_id']}|{scenario['task_id']}|"
    )
    if task_row != expected_task:
        raise RuntimeError(f"Unexpected recovered Task row: {task_row}")
    if agent_binding_count != 1:
        raise RuntimeError(
            "Recovered Task must have exactly one Agent binding"
        )
    if completion_event_count != 1:
        raise RuntimeError("Recovered Task must complete exactly once")
    if checkpoint_count < 1:
        raise RuntimeError("Session-scoped LangGraph checkpoint is missing")
    if not worker_binding.startswith(expected_binding_prefix):
        raise RuntimeError(f"Unexpected Worker binding: {worker_binding}")
    if lock_count != 0:
        raise RuntimeError("Session lock remained after successful completion")
    return {
        "executor_event_pending": pending_count,
        "agent_binding_count": agent_binding_count,
        "task_completed_event_count": completion_event_count,
        "session_checkpoint_count": checkpoint_count,
        "worker_binding": worker_binding,
        "session_lock_count": lock_count,
        "incomplete_worker_commands": incomplete_worker_commands,
        "unsent_worker_outbox": unsent_worker_outbox,
    }


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    if args.reuse_run and (not args.reuse_cluster or not args.run_id):
        raise ValueError(
            "reuse_run requires both reuse_cluster and an explicit run_id"
        )
    suffix = args.run_id or uuid4().hex[:8]
    if not _RUN_ID.fullmatch(suffix):
        raise ValueError(
            "run_id must contain 1-16 lowercase letters or digits"
        )
    environment = Environment(
        cluster=args.cluster,
        context=f"kind-{args.cluster}",
        namespace=f"ex-agent-rolling-e2e-{suffix}",
        database=f"agent_roll_e2e_{suffix}",
        executor_event_group=f"agent-roll-e2e-{suffix}",
        suffix=suffix,
    )
    shared_directory = args.executor_shared_directory.resolve()
    if not shared_directory.is_dir():
        raise FileNotFoundError(
            f"Executor shared directory does not exist: {shared_directory}"
        )
    await _ensure_cluster(
        environment,
        shared_directory,
        reuse=args.reuse_cluster,
    )
    await _build_and_load_image(environment)
    await _create_database(environment, args)
    await _create_executor_group(environment, args)
    await _deploy(environment, args)
    scenario = await _run_recovery_scenario(environment, args)
    durable_state = await _verify_durable_state(environment, args, scenario)
    return {
        "environment": {
            "cluster": environment.cluster,
            "context": environment.context,
            "namespace": environment.namespace,
            "database": environment.database,
            "executor_event_group": environment.executor_event_group,
        },
        "scenario": scenario,
        "durable_state": durable_state,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Provision an isolated kind environment and validate graceful "
            "and forced Worker restart recovery."
        )
    )
    parser.add_argument(
        "--executor-shared-directory",
        type=Path,
        required=True,
    )
    parser.add_argument("--cluster", default="ex-agent-rolling-e2e")
    parser.add_argument("--reuse-cluster", action="store_true")
    parser.add_argument("--reuse-run", action="store_true")
    parser.add_argument("--run-id")
    parser.add_argument("--postgres-container", default="executor-postgres-1")
    parser.add_argument("--postgres-admin-user", default="executor")
    parser.add_argument("--postgres-admin-database", default="executor")
    parser.add_argument("--agent-database-user", default="agent")
    parser.add_argument("--agent-database-password", default="agent")
    parser.add_argument("--redis-container", default="executor-redis-1")
    parser.add_argument("--executor-event-stream", default="executor.events")
    parser.add_argument(
        "--executor-base-url-in-cluster",
        default="http://host.docker.internal:8000/api/v1",
    )
    parser.add_argument(
        "--agent-base-url",
        default="http://127.0.0.1:18010",
    )
    parser.add_argument(
        "--executor-base-url",
        default="http://127.0.0.1:8000",
    )
    parser.add_argument("--agent-model", default="qwen38-27b-nvfp4")
    parser.add_argument(
        "--agent-model-base-url",
        default="http://model.frodo.com/v1",
    )
    parser.add_argument("--agent-model-api-key", default="EMPTY")
    parser.add_argument(
        "--durable-drain-timeout-seconds",
        type=float,
        default=15,
    )
    parser.add_argument("--output", type=Path)
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
