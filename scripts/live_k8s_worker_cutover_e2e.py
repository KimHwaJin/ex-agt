"""Rehearse a drained legacy-to-integrated Worker cutover in kind."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from time import perf_counter
from typing import Any
from uuid import uuid4

import httpx

from scripts.live_k8s_worker_restart_e2e import (
    _RUN_ID,
    Environment,
    _apply_generated_resource,
    _command,
    _create_database,
    _create_executor_group,
    _ensure_cluster,
    _kubectl,
    _verify_durable_state,
)
from scripts.live_worker_restart_e2e import (
    Observation,
    _json_request,
    _resume_new_interrupt,
)

ROOT = Path(__file__).resolve().parents[1]
DEPLOY_ROOT = ROOT / "deploy" / "cutover-e2e"
LEGACY_IMAGE = "ex-agent:cutover-legacy"
TARGET_IMAGE = "ex-agent:cutover-target"
TERMINAL = {"SUCCEEDED", "REJECTED", "FAILED", "CANCELLED"}


async def _archive(ref: str) -> bytes:
    process = await asyncio.create_subprocess_exec(
        "git",
        "archive",
        "--format=tar",
        ref,
        cwd=ROOT,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    output, error = await process.communicate()
    if process.returncode != 0:
        raise RuntimeError(
            f"git archive {ref} failed:\n{error.decode(errors='replace')}"
        )
    return output


async def _validate_legacy_ref(ref: str) -> None:
    await _command("git", "merge-base", "--is-ancestor", ref, "HEAD")
    project = await _command("git", "show", f"{ref}:pyproject.toml")
    legacy = 'ex-agent-worker = "ex_agent.worker_main:run_worker"'
    integrated = 'ex-agent-worker = "agent.worker_main:run_worker"'
    if legacy not in project or integrated in project:
        raise ValueError(
            f"Git ref {ref!r} is not a legacy Worker release boundary"
        )


async def _build_images(
    environment: Environment,
    legacy_ref: str,
) -> None:
    archive = await _archive(legacy_ref)
    process = await asyncio.create_subprocess_exec(
        "docker",
        "build",
        "--target",
        "runtime",
        "--tag",
        LEGACY_IMAGE,
        "-",
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    output, _ = await process.communicate(archive)
    if process.returncode != 0:
        raise RuntimeError(
            "Legacy image build failed:\n" + output.decode(errors="replace")
        )
    await _command(
        "docker",
        "build",
        "--target",
        "runtime",
        "--tag",
        TARGET_IMAGE,
        ".",
    )
    for image in (LEGACY_IMAGE, TARGET_IMAGE):
        await _command(
            "kind",
            "load",
            "docker-image",
            image,
            "--name",
            environment.cluster,
        )


async def _configure(
    environment: Environment,
    args: argparse.Namespace,
) -> None:
    await _command(
        "kubectl",
        "--context",
        environment.context,
        "create",
        "namespace",
        environment.namespace,
    )
    prefix = f"cutover-e2e-{environment.suffix}"
    await _apply_generated_resource(
        environment,
        ("configmap", "ex-agent-runtime"),
        {
            "APP_ENV": "cutover-e2e",
            "LOG_LEVEL": "INFO",
            "HOST": "0.0.0.0",
            "PORT": "8010",
            "AGENT_COMMAND_STREAM": f"{prefix}.commands",
            "AGENT_COMMAND_CONSUMER_GROUP": f"{prefix}.commands-v1",
            "AGENT_COMMAND_DEAD_LETTER_STREAM": (f"{prefix}.commands.dlq"),
            "AGENT_PRODUCT_EVENT_STREAM": f"{prefix}.product-events",
            "AGENT_PRODUCT_EVENT_CHANNEL_PREFIX": (f"{prefix}.task-events"),
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
    await _kubectl(
        environment,
        "apply",
        "-f",
        str(DEPLOY_ROOT / "workloads.yaml"),
    )


def _job_manifest(
    name: str,
    image: str,
    arguments: list[str],
) -> str:
    return json.dumps(
        {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {"name": name},
            "spec": {
                "backoffLimit": 0,
                "template": {
                    "spec": {
                        "restartPolicy": "Never",
                        "automountServiceAccountToken": False,
                        "containers": [
                            {
                                "name": "job",
                                "image": image,
                                "imagePullPolicy": "Never",
                                "args": arguments,
                                "envFrom": [
                                    {
                                        "configMapRef": {
                                            "name": "ex-agent-runtime"
                                        }
                                    },
                                    {
                                        "secretRef": {
                                            "name": "ex-agent-runtime"
                                        }
                                    },
                                ],
                            }
                        ],
                    }
                },
            },
        }
    )


async def _run_job(
    environment: Environment,
    name: str,
    image: str,
    arguments: list[str],
    *,
    timeout_seconds: float = 300,
) -> tuple[bool, str]:
    await _kubectl(
        environment,
        "delete",
        f"job/{name}",
        "--ignore-not-found=true",
        "--wait=true",
    )
    await _kubectl(
        environment,
        "apply",
        "-f",
        "-",
        input_text=_job_manifest(name, image, arguments),
    )
    deadline = perf_counter() + timeout_seconds
    succeeded = False
    while perf_counter() < deadline:
        raw = await _kubectl(environment, "get", f"job/{name}", "-o", "json")
        status = json.loads(raw).get("status", {})
        if status.get("succeeded"):
            succeeded = True
            break
        if status.get("failed"):
            break
        await asyncio.sleep(0.5)
    else:
        raise TimeoutError(f"Job did not finish: {name}")
    logs = await _kubectl(environment, "logs", f"job/{name}")
    return succeeded, logs


async def _migrate_legacy(environment: Environment) -> None:
    succeeded, logs = await _run_job(
        environment,
        "ex-agent-legacy-migrate",
        LEGACY_IMAGE,
        ["uv", "run", "--no-sync", "alembic", "upgrade", "head"],
    )
    if not succeeded:
        raise RuntimeError(f"Legacy migration failed:\n{logs}")


async def _migrate_target(environment: Environment) -> None:
    succeeded, logs = await _run_job(
        environment,
        "ex-agent-target-migrate",
        TARGET_IMAGE,
        ["ex-agent-migrate"],
    )
    if not succeeded:
        raise RuntimeError(f"Target migration failed:\n{logs}")


async def _set_release(
    environment: Environment,
    *,
    image: str,
    legacy: bool,
) -> None:
    await _kubectl(
        environment,
        "set",
        "image",
        "deployment/ex-agent-api",
        f"api={image}",
    )
    await _kubectl(
        environment,
        "set",
        "image",
        "deployment/ex-agent-worker",
        f"worker={image}",
    )
    live = "/healthz" if legacy else "/health/live"
    ready = "/readyz" if legacy else "/health/ready"
    patch = {
        "spec": {
            "template": {
                "spec": {
                    "containers": [
                        {
                            "name": "worker",
                            "startupProbe": {"httpGet": {"path": live}},
                            "livenessProbe": {"httpGet": {"path": live}},
                            "readinessProbe": {"httpGet": {"path": ready}},
                        }
                    ]
                }
            }
        }
    }
    await _kubectl(
        environment,
        "patch",
        "deployment/ex-agent-worker",
        "--type=strategic",
        "-p",
        json.dumps(patch),
    )


async def _scale_release(
    environment: Environment,
    replicas: int,
) -> None:
    for deployment in ("ex-agent-worker", "ex-agent-api"):
        await _kubectl(
            environment,
            "scale",
            f"deployment/{deployment}",
            f"--replicas={replicas}",
        )
        if replicas:
            await _kubectl(
                environment,
                "rollout",
                "status",
                f"deployment/{deployment}",
                "--timeout=300s",
            )


async def _release_pods(environment: Environment) -> list[dict[str, Any]]:
    raw = await _kubectl(
        environment,
        "get",
        "pods",
        "-l",
        "app.kubernetes.io/part-of=ex-agent-cutover-e2e",
        "-o",
        "json",
    )
    payload = json.loads(raw)
    return [
        {
            "name": item["metadata"]["name"],
            "uid": item["metadata"]["uid"],
            "image": item["spec"]["containers"][0]["image"],
            "phase": item.get("status", {}).get("phase"),
            "deleting": bool(item["metadata"].get("deletionTimestamp")),
        }
        for item in payload["items"]
    ]


async def _wait_scaled_down(
    environment: Environment,
    *,
    timeout_seconds: float = 60,
) -> None:
    deadline = perf_counter() + timeout_seconds
    while perf_counter() < deadline:
        if not await _release_pods(environment):
            return
        await asyncio.sleep(0.5)
    raise TimeoutError(
        "Legacy API/Worker Pods did not terminate after scale 0"
    )


async def _preflight(
    environment: Environment,
    *,
    stable_seconds: float,
) -> tuple[bool, dict[str, Any]]:
    succeeded, logs = await _run_job(
        environment,
        "ex-agent-cutover-preflight",
        TARGET_IMAGE,
        [
            "ex-agent-cutover-check",
            "--unsafe-accept-operator-freeze-assertion",
            "--stable-seconds",
            str(stable_seconds),
        ],
        timeout_seconds=60 + stable_seconds,
    )
    payload = json.loads(logs)
    return succeeded and payload.get("ready") is True, payload


async def _wait_for_preflight(
    environment: Environment,
    args: argparse.Namespace,
) -> dict[str, Any]:
    deadline = perf_counter() + args.drain_timeout_seconds
    last: dict[str, Any] = {}
    while perf_counter() < deadline:
        ready, last = await _preflight(
            environment,
            stable_seconds=args.stable_seconds,
        )
        if ready:
            return last
        await asyncio.sleep(1)
    raise RuntimeError(f"Legacy release did not drain: {last}")


async def _run_task(
    args: argparse.Namespace,
    *,
    request: str,
    require_execution: bool,
) -> dict[str, Any]:
    task_id = str(uuid4())
    session_id = f"cutover-e2e-session-{uuid4()}"
    observation = Observation(perf_counter())
    headers = {"X-User-ID": args.user_id}
    async with httpx.AsyncClient(
        base_url=args.agent_base_url.rstrip("/") + "/api/v1/",
        headers=headers,
        timeout=30,
    ) as client:
        await _json_request(
            client,
            "POST",
            f"projects/{args.project_id}/sessions/{session_id}/tasks",
            body={
                "task_id": task_id,
                "input_message_id": str(uuid4()),
                "content": request,
                "idempotency_key": f"cutover-e2e-create-{task_id}",
            },
        )
        deadline = perf_counter() + args.task_timeout_seconds
        terminal: dict[str, Any] | None = None
        while perf_counter() < deadline:
            task = await _json_request(client, "GET", f"tasks/{task_id}")
            observation.observe(task)
            await _resume_new_interrupt(
                client,
                task_id,
                task,
                observation,
            )
            if task["status"] in TERMINAL:
                terminal = task
                break
            await asyncio.sleep(0.1)
        if terminal is None:
            raise TimeoutError(f"Cutover smoke Task timed out: {task_id}")
    if terminal["status"] != "SUCCEEDED":
        raise RuntimeError(f"Cutover smoke Task failed: {terminal}")
    execution_id = terminal.get("execution_id")
    if require_execution and not execution_id:
        raise RuntimeError("Target smoke Task did not create an execution")
    if not require_execution and execution_id:
        raise RuntimeError(
            "Legacy Q&A smoke unexpectedly created an execution"
        )
    return {
        "task_id": task_id,
        "session_id": session_id,
        "status": terminal["status"],
        "execution_id": execution_id,
        "interrupts": observation.interrupts,
    }


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    suffix = args.run_id or uuid4().hex[:8]
    if not _RUN_ID.fullmatch(suffix):
        raise ValueError(
            "run_id must contain 1-16 lowercase letters or digits"
        )
    environment = Environment(
        cluster=args.cluster,
        context=f"kind-{args.cluster}",
        namespace=f"ex-agent-cutover-e2e-{suffix}",
        database=f"agent_cutover_e2e_{suffix}",
        executor_event_group=f"agent-cutover-e2e-{suffix}",
        suffix=suffix,
    )
    shared = args.executor_shared_directory.resolve()
    if not shared.is_dir():
        raise FileNotFoundError(f"Missing shared directory: {shared}")
    await _validate_legacy_ref(args.legacy_ref)
    await _ensure_cluster(
        environment,
        shared,
        reuse=args.reuse_cluster,
        host_port=18011,
        node_port=30011,
    )
    await _build_images(environment, args.legacy_ref)
    await _create_database(environment, args)
    await _create_executor_group(environment, args)
    await _configure(environment, args)
    await _migrate_legacy(environment)
    await _set_release(environment, image=LEGACY_IMAGE, legacy=True)
    await _scale_release(environment, 1)
    legacy_instances = await _release_pods(environment)
    legacy_smoke = await _run_task(
        args,
        request="일반 질의입니다. 숫자 2와 3을 더한 값만 알려주세요.",
        require_execution=False,
    )
    before_stop = await _wait_for_preflight(environment, args)
    await _scale_release(environment, 0)
    await _wait_scaled_down(environment)
    after_stop_ready, after_stop = await _preflight(
        environment,
        stable_seconds=args.stable_seconds,
    )
    if not after_stop_ready:
        raise RuntimeError(f"Drain changed after legacy stop: {after_stop}")
    await _set_release(environment, image=TARGET_IMAGE, legacy=False)
    await _migrate_target(environment)
    await _scale_release(environment, 1)
    target_instances = await _release_pods(environment)
    target_smoke = await _run_task(
        args,
        request=(
            "데이터 분석과 무관한 자유 코드 실행 요청입니다. 정확히 하나의 "
            "함수 wait_and_return을 정의하고 time.sleep(2) 후 문자열 "
            "'worker-cutover-ok'를 반환해 출력하세요."
        ),
        require_execution=True,
    )
    target_durable_state = await _verify_durable_state(
        environment,
        args,
        target_smoke,
    )
    return {
        "environment": {
            "cluster": environment.cluster,
            "context": environment.context,
            "namespace": environment.namespace,
            "database": environment.database,
            "executor_event_group": environment.executor_event_group,
        },
        "legacy_ref": args.legacy_ref,
        "legacy_instances": legacy_instances,
        "legacy_smoke": legacy_smoke,
        "preflight_before_stop": before_stop,
        "legacy_scaled_down_pod_count": 0,
        "preflight_after_stop": after_stop,
        "target_instances": target_instances,
        "target_smoke": target_smoke,
        "target_durable_state": target_durable_state,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Rehearse a zero-overlap Worker cutover in kind",
    )
    parser.add_argument(
        "--executor-shared-directory",
        type=Path,
        required=True,
    )
    parser.add_argument("--legacy-ref", default="391a818")
    parser.add_argument("--cluster", default="ex-agent-cutover-e2e")
    parser.add_argument("--reuse-cluster", action="store_true")
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
    parser.add_argument("--agent-base-url", default="http://127.0.0.1:18011")
    parser.add_argument("--agent-model", default="qwen38-27b-fp8")
    parser.add_argument(
        "--agent-model-base-url",
        default="http://model.frodo.com/v1",
    )
    parser.add_argument("--agent-model-api-key", default="EMPTY")
    parser.add_argument("--user-id", default="cutover-e2e-user")
    parser.add_argument("--project-id", default="cutover-e2e-project")
    parser.add_argument("--stable-seconds", type=float, default=2)
    parser.add_argument("--drain-timeout-seconds", type=float, default=120)
    parser.add_argument("--task-timeout-seconds", type=float, default=300)
    parser.add_argument(
        "--durable-drain-timeout-seconds",
        type=float,
        default=15,
    )
    parser.add_argument("--output", type=Path)
    parser.set_defaults(reuse_run=False)
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
