from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path
from typing import Any
from uuid import UUID

from ex_agent.executor.contracts import ExecutionResult, ResultStep

_TEXT_MEDIA_TYPES = {
    "application/json",
    "text/csv",
    "text/html",
    "text/markdown",
    "text/plain",
}
_CHUNK_BYTES = 64 * 1024


async def validated_result_summaries(
    result: ExecutionResult,
    shared_root: Path,
    *,
    max_context_chars: int,
    max_manifest_bytes: int,
) -> list[dict[str, Any]]:
    """Return bounded previews after validating Executor result files."""
    return await asyncio.to_thread(
        _validated_result_summaries,
        result,
        shared_root,
        max_context_chars,
        max_manifest_bytes,
    )


def _validated_result_summaries(
    result: ExecutionResult,
    shared_root: Path,
    max_context_chars: int,
    max_manifest_bytes: int,
) -> list[dict[str, Any]]:
    root = shared_root.resolve()
    remaining = max_context_chars
    summaries: list[dict[str, Any]] = []
    for operation in result.operations:
        for step in operation.steps:
            summary: dict[str, Any] = {
                "operation_number": operation.operation_number,
                "step_id": str(step.step_id),
                "sequence": step.sequence,
                "lineage": step.lineage,
                "status": step.result.status,
                "output_summary": step.result.output_summary,
                "error_message": step.result.error_message,
                "output_previews": [],
            }
            reference = step.result.result_ref
            if reference is not None:
                previews, consumed = _read_result_preview(
                    root,
                    result.execution.execution_id,
                    step,
                    reference,
                    max_chars=remaining,
                    max_manifest_bytes=max_manifest_bytes,
                )
                summary["result_ref"] = {
                    "relative_path": reference.get("relative_path"),
                    "checksum_sha256": reference.get("checksum_sha256"),
                }
                summary["output_previews"] = previews
                remaining -= consumed
            summaries.append(summary)
    return summaries


def _read_result_preview(
    root: Path,
    execution_id: UUID,
    step: ResultStep,
    reference: dict[str, Any],
    *,
    max_chars: int,
    max_manifest_bytes: int,
) -> tuple[list[dict[str, Any]], int]:
    if reference.get("storage") != "SHARED_PV":
        raise ValueError("Executor result_ref storage must be SHARED_PV")
    if str(reference.get("execution_id")) != str(execution_id):
        raise ValueError("Executor result_ref execution identity mismatch")
    if str(reference.get("step_id")) != str(step.step_id):
        raise ValueError("Executor result_ref step identity mismatch")
    if reference.get("complete") is not True:
        raise ValueError("Executor result_ref is incomplete")
    relative_path = _required_string(reference, "relative_path")
    manifest_path = _safe_path(root, relative_path)
    size = _required_int(reference, "size_bytes")
    if size > max_manifest_bytes:
        raise ValueError("Executor result manifest exceeds size limit")
    raw, _ = _read_verified_file(
        manifest_path,
        expected_size=size,
        expected_sha256=_required_string(reference, "checksum_sha256"),
        preview_bytes=max_manifest_bytes,
    )
    manifest = json.loads(raw)
    if not isinstance(manifest, dict):
        raise ValueError("Executor result manifest must be an object")
    _validate_manifest_identity(manifest, execution_id, step, reference)

    previews: list[dict[str, Any]] = []
    consumed = 0
    for output in manifest.get("outputs", []):
        if not isinstance(output, dict):
            raise ValueError("Executor result output must be an object")
        for representation in output.get("representations", []):
            if not isinstance(representation, dict):
                raise ValueError(
                    "Executor result representation must be an object"
                )
            if representation.get("complete") is not True:
                raise ValueError(
                    "Executor result representation is incomplete"
                )
            media_type = str(representation.get("media_type", ""))
            if media_type not in _TEXT_MEDIA_TYPES or consumed >= max_chars:
                continue
            representation_path = _safe_path(
                manifest_path.parent,
                _required_string(representation, "relative_path"),
            )
            allowed_chars = max_chars - consumed
            content, truncated = _read_verified_file(
                representation_path,
                expected_size=_required_int(representation, "size_bytes"),
                expected_sha256=_required_string(
                    representation,
                    "checksum_sha256",
                ),
                preview_bytes=max(allowed_chars * 4, 1),
            )
            text = content.decode("utf-8", errors="replace")
            bounded = text[:allowed_chars]
            is_truncated = truncated or len(text) > len(bounded)
            previews.append(
                {
                    "ordinal": output.get("ordinal"),
                    "kind": output.get("kind"),
                    "stream_name": output.get("stream_name"),
                    "media_type": media_type,
                    "content": bounded,
                    "truncated": is_truncated,
                }
            )
            consumed += len(bounded)
    return previews, consumed


def _validate_manifest_identity(
    manifest: dict[str, Any],
    execution_id: UUID,
    step: ResultStep,
    reference: dict[str, Any],
) -> None:
    if manifest.get("complete") is not True:
        raise ValueError("Executor result manifest is incomplete")
    identity = manifest.get("identity")
    if not isinstance(identity, dict):
        raise ValueError("Executor result manifest identity is missing")
    expected = {
        "execution_id": str(execution_id),
        "step_id": str(step.step_id),
        "execution_attempt_id": str(reference.get("attempt_id")),
        "fencing_token": reference.get("fencing_token"),
    }
    if any(identity.get(key) != value for key, value in expected.items()):
        raise ValueError("Executor result manifest identity mismatch")


def _read_verified_file(
    path: Path,
    *,
    expected_size: int,
    expected_sha256: str,
    preview_bytes: int,
) -> tuple[bytes, bool]:
    if expected_size < 0:
        raise ValueError("Executor result size cannot be negative")
    digest = hashlib.sha256()
    actual_size = 0
    preview = bytearray()
    with path.open("rb") as source:
        while chunk := source.read(_CHUNK_BYTES):
            digest.update(chunk)
            actual_size += len(chunk)
            if len(preview) < preview_bytes:
                preview.extend(chunk[: preview_bytes - len(preview)])
    if actual_size != expected_size:
        raise ValueError("Executor result file size mismatch")
    if digest.hexdigest() != expected_sha256:
        raise ValueError("Executor result file checksum mismatch")
    return bytes(preview), actual_size > len(preview)


def _safe_path(root: Path, relative_path: str) -> Path:
    candidate = Path(relative_path)
    if candidate.is_absolute():
        raise ValueError("Executor result path must be relative")
    resolved = (root / candidate).resolve()
    if resolved != root and root not in resolved.parents:
        raise ValueError("Executor result path escaped shared storage")
    return resolved


def _required_string(value: dict[str, Any], key: str) -> str:
    result = value.get(key)
    if not isinstance(result, str) or not result:
        raise ValueError(f"Executor result_ref {key} must be a string")
    return result


def _required_int(value: dict[str, Any], key: str) -> int:
    result = value.get(key)
    if not isinstance(result, int) or isinstance(result, bool):
        raise ValueError(f"Executor result_ref {key} must be an integer")
    return result
