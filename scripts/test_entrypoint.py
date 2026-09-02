"""Initialize the isolated Agent schema before any container test command."""

from __future__ import annotations

import os
import subprocess
import sys


def main() -> None:
    database_url = os.environ.get("TEST_DATABASE_URL")
    if database_url:
        environment = {**os.environ, "AGENT_DATABASE_URL": database_url}
        subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head"],
            check=True,
            env=environment,
        )
    if len(sys.argv) < 2:
        raise SystemExit("A test command is required")
    os.execvp(sys.argv[1], sys.argv[1:])


if __name__ == "__main__":
    main()
