"""Content-addressed PATH inputs, recreated from saved bytes on retry."""

import hashlib
from pathlib import Path
from typing import Any

from ex_agent.executor.files import materialize_input_file


def input_file(task_id: str, content: str, suffix: str) -> dict[str, str]:
    checksum = hashlib.sha256(content.encode()).hexdigest()
    return {
        "path": f"{task_id}/effects/{checksum}.{suffix}",
        "sha256": checksum,
        "content": content,
    }


def restore_files(root: Path, files: list[dict[str, Any]]) -> None:
    for item in files:
        content = item["content"]
        if hashlib.sha256(content.encode()).hexdigest() != item["sha256"]:
            raise ValueError("Saved Executor input checksum mismatch")
        written = materialize_input_file(root, item["path"], content)
        if written.sha256 != item["sha256"]:
            raise ValueError("Materialized Executor input checksum mismatch")


def capture_files(root: Path, steps: list[dict[str, Any]]) -> list[dict]:
    request_root = (root / "requests").resolve()
    files = []
    for step in steps:
        source = step["payload"]["source"]
        path = (request_root / source["path"]).resolve()
        if not path.is_relative_to(request_root):
            raise ValueError("Executor source escaped requests root")
        raw = path.read_bytes()
        if hashlib.sha256(raw).hexdigest() != source["sha256"]:
            raise ValueError("Approved Executor source checksum mismatch")
        files.append(
            {
                "path": source["path"],
                "sha256": source["sha256"],
                "content": raw.decode("utf-8"),
            }
        )
    return files
