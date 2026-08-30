from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import asdict
from statistics import mean
from time import perf_counter
from typing import Any

from ex_agent.benchmarks.lifecycle import run_lifecycle_batch


def _percentile(values: list[float], percentile: float) -> float:
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(len(ordered) * percentile))
    return ordered[index]


def _summary(values: list[float]) -> dict[str, float]:
    return {
        "mean": mean(values),
        "p50": _percentile(values, 0.50),
        "p95": _percentile(values, 0.95),
        "max": max(values),
    }


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    started_at = perf_counter()
    timings = await run_lifecycle_batch(
        scenario=args.scenario,
        requests=args.requests,
        concurrency=args.concurrency,
        llm_delay_seconds=args.llm_delay_ms / 1000,
        executor_delay_seconds=args.executor_delay_ms / 1000,
    )
    wall_seconds = perf_counter() - started_at
    fields = (
        "total_seconds",
        "planning_seconds",
        "approval_to_executor_seconds",
        "executor_resume_seconds",
        "report_seconds",
    )
    return {
        "scenario": args.scenario,
        "requests": args.requests,
        "concurrency": args.concurrency,
        "fake_llm_delay_ms": args.llm_delay_ms,
        "fake_executor_delay_ms": args.executor_delay_ms,
        "wall_seconds": wall_seconds,
        "throughput_per_second": args.requests / wall_seconds,
        "latency_seconds": {
            field: _summary([getattr(item, field) for item in timings])
            for field in fields
        },
        "executor_boundaries": sorted(
            {item.executor_boundaries for item in timings}
        ),
        "samples": [asdict(item) for item in timings]
        if args.include_samples
        else None,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Measure the complete LangGraph lifecycle with deterministic "
            "Fake LLM and Executor boundaries."
        )
    )
    parser.add_argument(
        "--scenario",
        choices=("single_custom", "multi_analysis"),
        default="single_custom",
    )
    parser.add_argument("--requests", type=int, default=20)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--llm-delay-ms", type=float, default=5)
    parser.add_argument("--executor-delay-ms", type=float, default=5)
    parser.add_argument("--include-samples", action="store_true")
    return parser


def main() -> None:
    args = _parser().parse_args()
    result = asyncio.run(_run(args))
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
