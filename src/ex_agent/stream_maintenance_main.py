from __future__ import annotations

import argparse
import asyncio
import json
import os
from dataclasses import asdict
from typing import Any

from redis.asyncio import Redis

from ex_agent.transport.stream_maintenance import SafeStreamTrimmer


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ex-agent-stream-maintenance",
        description="Plan or atomically apply safe Redis Stream trimming.",
    )
    parser.add_argument(
        "--redis-url",
        default=os.environ.get(
            "AGENT_REDIS_URL",
            "redis://127.0.0.1:56379/0",
        ),
    )
    parser.add_argument(
        "--stream",
        action="append",
        required=True,
        help="Stream to inspect. Repeat the option for multiple streams.",
    )
    parser.add_argument(
        "--retention-seconds",
        type=int,
        default=int(os.environ.get("STREAM_RETENTION_SECONDS", "604800")),
    )
    parser.add_argument(
        "--minimum-retained-entries",
        type=int,
        default=int(os.environ.get("STREAM_MINIMUM_RETAINED_ENTRIES", "1000")),
    )
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser(
        "plan", help="Print boundaries without changing Redis."
    )
    trimming = commands.add_parser(
        "trim",
        help="Recalculate boundaries atomically and trim safe entries.",
    )
    trimming.add_argument(
        "--yes",
        action="store_true",
        help="Confirm the command changes Redis state.",
    )
    return parser


async def _run(arguments: argparse.Namespace) -> int:
    if arguments.command == "trim" and not arguments.yes:
        raise SystemExit("trim requires --yes")
    redis = Redis.from_url(arguments.redis_url, decode_responses=True)
    trimmer = SafeStreamTrimmer(
        redis,
        retention_seconds=arguments.retention_seconds,
        minimum_retained_entries=arguments.minimum_retained_entries,
    )
    try:
        if arguments.command == "plan":
            results = await asyncio.gather(
                *(trimmer.plan(stream) for stream in arguments.stream)
            )
        else:
            results = await asyncio.gather(
                *(trimmer.trim(stream) for stream in arguments.stream)
            )
        _print_json({"results": [asdict(result) for result in results]})
        return 0
    finally:
        await redis.aclose()


def _print_json(value: dict[str, Any]) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, default=str))


def run_cli() -> None:
    raise SystemExit(asyncio.run(_run(_parser().parse_args())))


if __name__ == "__main__":
    run_cli()


__all__ = ["run_cli"]
