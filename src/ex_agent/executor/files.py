from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from uuid import uuid4


@dataclass(frozen=True)
class MaterializedInput:
    absolute_path: Path
    relative_path: str
    sha256: str


def materialize_input_file(
    root: Path,
    relative_path: str,
    content: str,
) -> MaterializedInput:
    """Atomically write one Executor input file under requests/."""
    request_root = (root.resolve() / "requests").resolve()
    relative = PurePosixPath(relative_path)
    if relative.is_absolute() or ".." in relative.parts:
        raise ValueError("Executor input path must be safe and relative")
    if not relative.parts:
        raise ValueError("Executor input path must not be empty")

    path = (request_root / Path(*relative.parts)).resolve()
    try:
        path.relative_to(request_root)
    except ValueError as error:
        raise ValueError(
            "Executor input path escaped requests root"
        ) from error

    encoded = content.encode("utf-8")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    temporary.write_bytes(encoded)
    temporary.replace(path)
    return MaterializedInput(
        absolute_path=path,
        relative_path=relative.as_posix(),
        sha256=hashlib.sha256(encoded).hexdigest(),
    )
