"""Kubernetes preflight entrypoint for a one-time Worker cutover."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os

from agent.cutover import (
    AdmissionFreezeProbe,
    CutoverProbe,
    HttpAdmissionFreezeProbe,
    UnsafeStaticAdmissionFreezeProbe,
)
from ex_agent.config import Settings


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Verify that the legacy Worker is fully drained",
    )
    source = result.add_mutually_exclusive_group()
    source.add_argument(
        "--admission-evidence-url",
        default=os.getenv("BFF_CUTOVER_EVIDENCE_URL"),
        help="trusted BFF endpoint returning the admission freeze receipt",
    )
    source.add_argument(
        "--unsafe-accept-operator-freeze-assertion",
        action="store_true",
        help="skip BFF verification for isolated local rehearsals only",
    )
    result.add_argument(
        "--expected-freeze-id",
        default=os.getenv("CUTOVER_FREEZE_ID"),
        help="deployment correlation ID expected in the BFF receipt",
    )
    result.add_argument(
        "--admission-token-env",
        default="BFF_CUTOVER_BEARER_TOKEN",
        help="environment variable containing the BFF bearer token",
    )
    result.add_argument(
        "--admission-timeout-seconds",
        type=float,
        default=5,
        help="timeout for each BFF evidence request",
    )
    result.add_argument(
        "--stable-seconds",
        type=float,
        default=5,
        help="seconds between two identical drain samples",
    )
    return result


async def main(arguments: list[str] | None = None) -> int:
    argument_parser = parser()
    args = argument_parser.parse_args(arguments)
    settings = Settings()
    try:
        admission_probe = _admission_probe(args)
    except ValueError as error:
        argument_parser.error(str(error))
    probe = CutoverProbe(
        database_url=settings.agent_database_url,
        redis_url=settings.agent_redis_url,
        command_stream=settings.agent_command_stream,
        command_group=settings.agent_command_consumer_group,
        executor_event_stream=settings.executor_event_stream,
        executor_event_group=settings.executor_event_consumer_group,
        admission_probe=admission_probe,
    )
    try:
        report = await probe.stable_report(
            stable_seconds=args.stable_seconds,
        )
    finally:
        await probe.close()
    print(json.dumps(report.to_dict(), ensure_ascii=False, indent=2))
    return 0 if report.ready else 2


def _admission_probe(args: argparse.Namespace) -> AdmissionFreezeProbe:
    if args.unsafe_accept_operator_freeze_assertion:
        if args.admission_evidence_url or args.expected_freeze_id:
            raise ValueError(
                "BFF evidence options cannot be combined with the unsafe "
                "operator assertion"
            )
        return UnsafeStaticAdmissionFreezeProbe()
    if not args.admission_evidence_url:
        raise ValueError("--admission-evidence-url is required")
    if not args.expected_freeze_id:
        raise ValueError("--expected-freeze-id is required")
    token = os.getenv(args.admission_token_env, "")
    if not token:
        raise ValueError(
            f"{args.admission_token_env} must contain the BFF bearer token"
        )
    return HttpAdmissionFreezeProbe(
        url=args.admission_evidence_url,
        expected_freeze_id=args.expected_freeze_id,
        bearer_token=token,
        timeout_seconds=args.admission_timeout_seconds,
    )


def run() -> None:
    logging.basicConfig(level=logging.INFO)
    raise SystemExit(asyncio.run(main()))


if __name__ == "__main__":
    run()
