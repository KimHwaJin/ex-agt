import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest
import yaml

from scripts.live_k8s_worker_cutover_e2e import (
    LEGACY_IMAGE,
    TARGET_IMAGE,
    _job_manifest,
    _wait_scaled_down,
)
from scripts.live_k8s_worker_restart_e2e import _kind_config

ROOT = Path(__file__).resolve().parents[1]


def test_cutover_kind_config_uses_isolated_host_and_node_ports(
    tmp_path: Path,
) -> None:
    payload = yaml.safe_load(
        _kind_config(tmp_path, host_port=18011, node_port=30011)
    )

    assert payload["nodes"][0]["extraPortMappings"] == [
        {
            "containerPort": 30011,
            "hostPort": 18011,
            "listenAddress": "127.0.0.1",
            "protocol": "TCP",
        }
    ]


def test_cutover_workloads_start_at_zero_with_recreate_strategy() -> None:
    documents = list(
        yaml.safe_load_all(
            (ROOT / "deploy" / "cutover-e2e" / "workloads.yaml").read_text(
                encoding="utf-8"
            )
        )
    )
    deployments = {
        item["metadata"]["name"]: item
        for item in documents
        if item["kind"] == "Deployment"
    }

    assert set(deployments) == {"ex-agent-api", "ex-agent-worker"}
    for deployment in deployments.values():
        assert deployment["spec"]["replicas"] == 0
        assert deployment["spec"]["strategy"] == {"type": "Recreate"}
    worker = deployments["ex-agent-worker"]["spec"]["template"]["spec"]
    container = worker["containers"][0]
    assert container["image"] == TARGET_IMAGE
    assert container["readinessProbe"]["httpGet"]["path"] == "/health/ready"


@pytest.mark.parametrize("image", [LEGACY_IMAGE, TARGET_IMAGE])
def test_generated_job_never_pulls_or_retries(image: str) -> None:
    manifest = json.loads(_job_manifest("cutover", image, ["command"]))
    container = manifest["spec"]["template"]["spec"]["containers"][0]

    assert manifest["spec"]["backoffLimit"] == 0
    assert container["imagePullPolicy"] == "Never"
    assert [next(iter(item)) for item in container["envFrom"]] == [
        "configMapRef",
        "secretRef",
    ]


@pytest.mark.asyncio
async def test_cutover_waits_until_every_legacy_pod_is_gone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pods = AsyncMock(
        side_effect=[
            [{"uid": "legacy-api"}, {"uid": "legacy-worker"}],
            [{"uid": "legacy-worker", "deleting": True}],
            [],
        ]
    )
    sleep = AsyncMock()
    monkeypatch.setattr(
        "scripts.live_k8s_worker_cutover_e2e._release_pods",
        pods,
    )
    monkeypatch.setattr(
        "scripts.live_k8s_worker_cutover_e2e.asyncio.sleep",
        sleep,
    )

    await _wait_scaled_down(AsyncMock(), timeout_seconds=1)

    assert pods.await_count == 3
    assert sleep.await_count == 2
