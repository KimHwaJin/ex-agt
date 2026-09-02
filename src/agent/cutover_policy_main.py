"""Print the approved recovery boundary for a cutover phase."""

from __future__ import annotations

import argparse
import json

from agent.cutover_policy import CutoverPhase, rollback_decision


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Classify rollback safety for a Worker cutover phase",
    )
    result.add_argument(
        "--phase",
        required=True,
        choices=[phase.value for phase in CutoverPhase],
    )
    return result


def main(arguments: list[str] | None = None) -> int:
    args = parser().parse_args(arguments)
    decision = rollback_decision(CutoverPhase(args.phase))
    print(json.dumps(decision.to_dict(), ensure_ascii=False, indent=2))
    return 0


def run() -> None:
    raise SystemExit(main())


if __name__ == "__main__":
    run()
