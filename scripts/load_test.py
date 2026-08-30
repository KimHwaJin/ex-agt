from __future__ import annotations

import argparse
import asyncio
import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import Any
from uuid import uuid4

import httpx


@dataclass(frozen=True)
class RequestResult:
    status_code: int
    duration_seconds: float
    error: str | None = None


async def run_load(args: argparse.Namespace) -> dict[str, Any]:
    limits = httpx.Limits(
        max_connections=args.concurrency,
        max_keepalive_connections=args.concurrency,
    )
    timeout = httpx.Timeout(args.timeout)
    semaphore = asyncio.Semaphore(args.concurrency)
    async with httpx.AsyncClient(
        base_url=args.base_url,
        limits=limits,
        timeout=timeout,
    ) as client:

        async def send(index: int) -> RequestResult:
            async with semaphore:
                task_id = uuid4()
                started_at = perf_counter()
                try:
                    response = await client.post(
                        "/api/v1/projects/"
                        f"{args.project_id}/sessions/"
                        f"{args.session_prefix}-{index}-{task_id}/tasks",
                        headers={"X-User-ID": args.user_id},
                        json={
                            "task_id": str(task_id),
                            "input_message_id": str(uuid4()),
                            "content": args.message,
                            "idempotency_key": f"load-{task_id}",
                        },
                    )
                    duration = perf_counter() - started_at
                    return RequestResult(response.status_code, duration)
                except Exception as error:
                    duration = perf_counter() - started_at
                    return RequestResult(
                        0,
                        duration,
                        f"{type(error).__name__}: {error}",
                    )

        if args.warmup:
            await asyncio.gather(
                *(send(-index) for index in range(args.warmup))
            )
        started_at = perf_counter()
        results = await asyncio.gather(
            *(send(index) for index in range(args.requests))
        )
        elapsed = perf_counter() - started_at

    durations = [result.duration_seconds for result in results]
    statuses = Counter(result.status_code for result in results)
    failures = [result for result in results if result.status_code != 202]
    return {
        "requests": args.requests,
        "concurrency": args.concurrency,
        "elapsed_seconds": round(elapsed, 6),
        "throughput_per_second": round(args.requests / elapsed, 3),
        "latency_seconds": {
            "min": round(min(durations), 6),
            "mean": round(sum(durations) / len(durations), 6),
            "p50": round(_percentile(durations, 0.50), 6),
            "p95": round(_percentile(durations, 0.95), 6),
            "p99": round(_percentile(durations, 0.99), 6),
            "max": round(max(durations), 6),
        },
        "status_counts": dict(sorted(statuses.items())),
        "failure_count": len(failures),
        "sample_errors": [
            result.error for result in failures[:5] if result.error is not None
        ],
    }


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    position = (len(ordered) - 1) * percentile
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = position - lower
    return ordered[lower] + ((ordered[upper] - ordered[lower]) * fraction)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Measure Agent API task-acceptance latency.",
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8010")
    parser.add_argument("--project-id", default="load-test")
    parser.add_argument("--session-prefix", default="load-session")
    parser.add_argument("--user-id", default="load-test-user")
    parser.add_argument("--message", default="1 더하기 1을 계산해줘")
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--warmup", type=int, default=0)
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--output", type=Path)
    return parser


def main() -> None:
    args = _parser().parse_args()
    if args.requests < 1 or args.concurrency < 1:
        raise SystemExit("requests and concurrency must be positive")
    report = asyncio.run(run_load(args))
    rendered = json.dumps(report, ensure_ascii=False, indent=2)
    print(rendered)
    if args.output is not None:
        args.output.write_text(f"{rendered}\n", encoding="utf-8")
    if report["failure_count"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
