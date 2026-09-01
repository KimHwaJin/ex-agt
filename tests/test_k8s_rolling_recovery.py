import json
from pathlib import Path

import pytest
import yaml

from scripts.live_k8s_worker_restart_e2e import _kind_config
from scripts.live_worker_restart_e2e import KubernetesWorkerRestarter

ROOT = Path(__file__).resolve().parents[1]
DEPLOY_ROOT = ROOT / "deploy" / "rolling-e2e"


def test_kind_config_mounts_executor_storage_and_api_port(
    tmp_path: Path,
) -> None:
    payload = yaml.safe_load(_kind_config(tmp_path))
    node = payload["nodes"][0]

    assert node["extraMounts"] == [
        {
            "hostPath": str(tmp_path),
            "containerPath": "/workspace/shared",
        }
    ]
    assert node["extraPortMappings"] == [
        {
            "containerPort": 30010,
            "hostPort": 18010,
            "listenAddress": "127.0.0.1",
            "protocol": "TCP",
        }
    ]


def test_worker_manifest_has_bounded_graceful_shutdown_contract() -> None:
    documents = list(
        yaml.safe_load_all(
            (DEPLOY_ROOT / "workloads.yaml").read_text(encoding="utf-8")
        )
    )
    deployments = {
        document["metadata"]["name"]: document
        for document in documents
        if document["kind"] == "Deployment"
    }
    worker = deployments["ex-agent-worker"]
    pod = worker["spec"]["template"]["spec"]
    container = pod["containers"][0]

    assert worker["spec"]["replicas"] == 1
    assert worker["spec"]["strategy"]["rollingUpdate"] == {
        "maxUnavailable": 0,
        "maxSurge": 1,
    }
    assert pod["terminationGracePeriodSeconds"] == 40
    assert container["args"] == ["ex-agent-worker"]
    assert container["readinessProbe"]["httpGet"]["path"] == "/health/ready"
    assert container["livenessProbe"]["httpGet"]["path"] == "/health/live"


@pytest.mark.asyncio
async def test_kubernetes_pod_snapshot_requires_all_containers_ready(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    controller = KubernetesWorkerRestarter(
        context="kind-test",
        namespace="test",
        deployment="worker",
        selector="app=worker",
        timeout_seconds=1,
    )
    payload = {
        "items": [
            {
                "metadata": {"name": "worker-a", "uid": "uid-a"},
                "status": {
                    "containerStatuses": [
                        {"ready": True},
                        {"ready": False},
                    ]
                },
            },
            {
                "metadata": {"name": "worker-b", "uid": "uid-b"},
                "status": {"containerStatuses": [{"ready": True}]},
            },
        ]
    }

    async def kubectl(*arguments: str) -> str:
        assert arguments == ("get", "pods", "-l", "app=worker", "-o", "json")
        return json.dumps(payload)

    monkeypatch.setattr(controller, "_kubectl", kubectl)

    pods = await controller._pods()

    assert [(pod.uid, pod.ready) for pod in pods] == [
        ("uid-a", False),
        ("uid-b", True),
    ]
