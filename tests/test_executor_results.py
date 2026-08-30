import hashlib
import json
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from ex_agent.executor.contracts import ExecutionResult
from ex_agent.executor.results import validated_result_summaries


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _result_fixture(root: Path) -> tuple[ExecutionResult, Path]:
    execution_id = uuid4()
    operation_id = uuid4()
    step_id = uuid4()
    attempt_id = uuid4()
    manifest_dir = (
        root
        / "executions"
        / str(execution_id)
        / "operations"
        / str(operation_id)
        / "steps"
        / str(step_id)
        / "attempts"
        / str(attempt_id)
        / "1"
    )
    output_path = manifest_dir / "outputs/000000-stream-00.txt"
    output_path.parent.mkdir(parents=True)
    output = b"dataset path: artifacts/datasets/e2e.csv\nrows: 500\n"
    output_path.write_bytes(output)
    manifest = {
        "schema_version": "1.0",
        "complete": True,
        "identity": {
            "execution_id": str(execution_id),
            "operation_id": str(operation_id),
            "step_id": str(step_id),
            "execution_attempt_id": str(attempt_id),
            "fencing_token": 1,
            "sequence": 0,
        },
        "outputs": [
            {
                "ordinal": 0,
                "kind": "STREAM",
                "stream_name": "stdout",
                "representations": [
                    {
                        "media_type": "text/plain",
                        "relative_path": ("outputs/000000-stream-00.txt"),
                        "checksum_sha256": _sha256(output),
                        "size_bytes": len(output),
                        "complete": True,
                    }
                ],
            }
        ],
    }
    manifest_path = manifest_dir / "manifest.json"
    manifest_bytes = json.dumps(
        manifest,
        separators=(",", ":"),
    ).encode()
    manifest_path.write_bytes(manifest_bytes)
    reference = {
        "storage": "SHARED_PV",
        "execution_id": str(execution_id),
        "step_id": str(step_id),
        "attempt_id": str(attempt_id),
        "fencing_token": 1,
        "relative_path": str(manifest_path.relative_to(root)),
        "checksum_sha256": _sha256(manifest_bytes),
        "size_bytes": len(manifest_bytes),
        "complete": True,
    }
    result = ExecutionResult.model_validate(
        {
            "execution": {
                "execution_id": str(execution_id),
                "state": {"status": "WAITING_FOR_OPERATION", "version": 3},
            },
            "operations": [
                {
                    "operation_id": str(operation_id),
                    "operation_number": 1,
                    "result": {
                        "status": "SUCCEEDED",
                        "error_message": None,
                    },
                    "steps": [
                        {
                            "step_id": str(step_id),
                            "sequence": 0,
                            "lineage": {
                                "skill_name": "data-access",
                                "tool_name": "fetch_dataset",
                            },
                            "result": {
                                "status": "SUCCEEDED",
                                "output_summary": {
                                    "output_count": 1,
                                },
                                "result_ref": reference,
                            },
                        }
                    ],
                }
            ],
        }
    )
    return result, manifest_path


@pytest.mark.asyncio
async def test_validated_result_summaries_reads_bounded_text(
    tmp_path: Path,
) -> None:
    result, _ = _result_fixture(tmp_path)

    summaries = await validated_result_summaries(
        result,
        tmp_path,
        max_context_chars=24,
        max_manifest_bytes=10000,
    )

    assert summaries[0]["lineage"]["tool_name"] == "fetch_dataset"
    assert summaries[0]["output_previews"] == [
        {
            "ordinal": 0,
            "kind": "STREAM",
            "stream_name": "stdout",
            "media_type": "text/plain",
            "content": "dataset path: artifacts/",
            "truncated": True,
        }
    ]


@pytest.mark.asyncio
async def test_validated_result_summaries_rejects_checksum_mismatch(
    tmp_path: Path,
) -> None:
    result, manifest_path = _result_fixture(tmp_path)
    manifest_path.write_text("{}", encoding="utf-8")

    with pytest.raises(ValueError, match="size mismatch"):
        await validated_result_summaries(
            result,
            tmp_path,
            max_context_chars=1000,
            max_manifest_bytes=10000,
        )


@pytest.mark.asyncio
async def test_validated_result_summaries_rejects_path_escape(
    tmp_path: Path,
) -> None:
    result, _ = _result_fixture(tmp_path)
    reference = result.operations[0].steps[0].result.result_ref
    assert reference is not None
    reference["relative_path"] = "../manifest.json"

    with pytest.raises(ValueError, match="escaped shared storage"):
        await validated_result_summaries(
            result,
            tmp_path,
            max_context_chars=1000,
            max_manifest_bytes=10000,
        )


def test_result_fixture_has_uuid_identity(tmp_path: Path) -> None:
    result, _ = _result_fixture(tmp_path)

    assert isinstance(result.execution.execution_id, UUID)
