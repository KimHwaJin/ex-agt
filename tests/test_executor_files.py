import hashlib
from pathlib import Path

import pytest

from ex_agent.executor.files import materialize_input_file


def test_materialize_input_file_writes_under_requests(
    tmp_path: Path,
) -> None:
    content = "print('file based')\n"

    result = materialize_input_file(
        tmp_path,
        "task-1/1/step-0000.py",
        content,
    )

    assert result.absolute_path == (
        tmp_path / "requests/task-1/1/step-0000.py"
    )
    assert result.relative_path == "task-1/1/step-0000.py"
    assert result.absolute_path.read_text(encoding="utf-8") == content
    assert result.sha256 == hashlib.sha256(content.encode()).hexdigest()
    assert not list(result.absolute_path.parent.glob("*.tmp"))


@pytest.mark.parametrize(
    "relative_path",
    ["../escape.py", "/absolute.py", "task/../../escape.py"],
)
def test_materialize_input_file_rejects_unsafe_path(
    tmp_path: Path,
    relative_path: str,
) -> None:
    with pytest.raises(ValueError, match="safe and relative"):
        materialize_input_file(tmp_path, relative_path, "print(1)\n")
