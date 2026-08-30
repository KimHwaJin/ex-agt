from __future__ import annotations

import argparse
import asyncio
import json
import os
from dataclasses import asdict
from typing import Any

from redis.asyncio import Redis

from ex_agent.transport.dlq import DeadLetterManager


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="ex-agent-dlq",
        description="Inspect, replay, or discard Redis Stream DLQ entries.",
    )
    parser.add_argument(
        "--redis-url",
        default=os.environ.get("AGENT_REDIS_URL", "redis://127.0.0.1:56379/0"),
    )
    parser.add_argument("--stream", required=True)
    parser.add_argument("--audit-stream")
    parser.add_argument(
        "--marker-ttl-seconds",
        type=int,
        default=int(
            os.environ.get("DLQ_ACTION_MARKER_TTL_SECONDS", "7776000")
        ),
    )
    commands = parser.add_subparsers(dest="command", required=True)

    listing = commands.add_parser("list")
    listing.add_argument("--limit", type=int, default=50)
    listing.add_argument("--after")

    for name in ("replay", "discard"):
        action = commands.add_parser(name)
        action.add_argument("entry_ids", nargs="+")
        action.add_argument("--actor", required=True)
        action.add_argument("--reason", required=True)
        action.add_argument(
            "--yes",
            action="store_true",
            help="Confirm the action changes Redis state.",
        )
    return parser


async def _run(arguments: argparse.Namespace) -> int:
    redis = Redis.from_url(arguments.redis_url, decode_responses=True)
    manager = DeadLetterManager(
        redis,
        arguments.stream,
        audit_stream=arguments.audit_stream,
        marker_ttl_seconds=arguments.marker_ttl_seconds,
    )
    try:
        if arguments.command == "list":
            page = await manager.list_entries(
                limit=arguments.limit,
                after=arguments.after,
            )
            _print_json(
                {
                    "entries": [asdict(entry) for entry in page.entries],
                    "next_cursor": page.next_cursor,
                }
            )
            return 0
        if not arguments.yes:
            raise SystemExit("replay/discard requires --yes")
        if arguments.command == "replay":
            results = await manager.replay_many(
                arguments.entry_ids,
                actor=arguments.actor,
                reason=arguments.reason,
            )
        else:
            results = await manager.discard_many(
                arguments.entry_ids,
                actor=arguments.actor,
                reason=arguments.reason,
            )
        _print_json({"results": [asdict(result) for result in results]})
        return 0
    finally:
        await redis.aclose()


def _print_json(value: dict[str, Any]) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, default=str))


def run_dlq_cli() -> None:
    raise SystemExit(asyncio.run(_run(_parser().parse_args())))


if __name__ == "__main__":
    run_dlq_cli()


__all__ = ["run_dlq_cli"]
