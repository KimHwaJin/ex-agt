"""Kubernetes preflight entrypoint for a one-time Worker cutover."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging

from agent.cutover import CutoverProbe
from ex_agent.config import Settings


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Verify that the legacy Worker is fully drained",
    )
    result.add_argument(
        "--admissions-frozen",
        action="store_true",
        help="assert that the BFF no longer admits new tasks",
    )
    result.add_argument(
        "--stable-seconds",
        type=float,
        default=5,
        help="seconds between two identical drain samples",
    )
    return result


async def main(arguments: list[str] | None = None) -> int:
    args = parser().parse_args(arguments)
    settings = Settings()
    probe = CutoverProbe(
        database_url=settings.agent_database_url,
        redis_url=settings.agent_redis_url,
        command_stream=settings.agent_command_stream,
        command_group=settings.agent_command_consumer_group,
        executor_event_stream=settings.executor_event_stream,
        executor_event_group=settings.executor_event_consumer_group,
    )
    try:
        report = await probe.stable_report(
            admissions_frozen=args.admissions_frozen,
            stable_seconds=args.stable_seconds,
        )
    finally:
        await probe.close()
    print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))
    return 0 if report.ready else 2


def run() -> None:
    logging.basicConfig(level=logging.INFO)
    raise SystemExit(asyncio.run(main()))


if __name__ == "__main__":
    run()
