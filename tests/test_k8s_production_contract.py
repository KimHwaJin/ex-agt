from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
DEPLOY_ROOT = ROOT / "deploy" / "k8s"
IMAGE = "registry.example.com/ex-agent:replace-with-version"


def load(name: str) -> dict[str, Any]:
    return yaml.safe_load((DEPLOY_ROOT / name).read_text(encoding="utf-8"))


def containers(document: dict[str, Any]) -> dict[str, dict[str, Any]]:
    values = document["spec"]["template"]["spec"]["containers"]
    return {value["name"]: value for value in values}


def test_runtime_image_exposes_all_three_process_entrypoints() -> None:
    dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")
    project = (ROOT / "pyproject.toml").read_text(encoding="utf-8")

    assert 'ENTRYPOINT ["/usr/bin/tini", "--"]' in dockerfile
    assert 'CMD ["ex-agent-api"]' in dockerfile
    assert 'ex-agent-api = "ex_agent.main:run_api"' in project
    assert 'ex-agent-worker = "agent.worker_main:run_worker"' in project
    assert 'ex-agent-migrate = "agent.migrate_main:run_migrations"' in project


def test_deployment_runs_api_and_worker_from_one_image_in_one_pod() -> None:
    deployment = load("deployment.yaml.example")
    pod = deployment["spec"]["template"]["spec"]
    values = containers(deployment)

    assert set(values) == {"api-agent", "worker"}
    assert {value["image"] for value in values.values()} == {IMAGE}
    assert all("command" not in value for value in values.values())
    assert values["api-agent"]["args"] == ["ex-agent-api"]
    assert values["worker"]["args"] == ["ex-agent-worker"]
    assert pod["terminationGracePeriodSeconds"] == 40
    assert deployment["spec"]["strategy"]["rollingUpdate"] == {
        "maxUnavailable": 0,
        "maxSurge": 1,
    }


def test_deployment_keeps_health_storage_and_secret_boundaries() -> None:
    values = containers(load("deployment.yaml.example"))
    api = values["api-agent"]
    worker = values["worker"]

    assert api["readinessProbe"]["httpGet"] == {
        "path": "/health/ready",
        "port": "http",
    }
    assert worker["readinessProbe"]["httpGet"] == {
        "path": "/health/ready",
        "port": "worker-health",
    }
    assert api["volumeMounts"] == worker["volumeMounts"]
    assert api["env"][0]["name"] == "BFF_AUTH_HMAC_KEYS_JSON"
    assert worker["env"][0]["name"] == "WORKER_INSTANCE_ID"
    assert worker["env"][0]["valueFrom"]["fieldRef"] == {
        "fieldPath": "metadata.uid"
    }
    assert all(
        variable["name"] != "BFF_AUTH_HMAC_KEYS_JSON"
        for variable in worker["env"]
    )


def test_migration_uses_the_same_image_and_complete_entrypoint() -> None:
    migration = load("migrate-job.yaml.example")
    migrate = containers(migration)["migrate"]

    assert migrate["image"] == IMAGE
    assert "command" not in migrate
    assert migrate["args"] == ["ex-agent-migrate"]
    assert migration["spec"]["template"]["spec"]["restartPolicy"] == ("Never")


def test_service_exposes_only_the_api_port() -> None:
    service = load("service.yaml.example")

    assert service["spec"]["selector"] == {
        "app.kubernetes.io/name": "ex-agent"
    }
    assert service["spec"]["ports"] == [
        {"name": "http", "port": 8010, "targetPort": "http"}
    ]


def test_production_config_requires_signed_bff_and_path_storage() -> None:
    config = load("configmap.yaml.example")["data"]

    assert config["APP_ENV"] == "production"
    assert config["BFF_AUTH_MODE"] == "hmac"
    assert config["EXECUTOR_SOURCE_MODE"] == "PATH"
    assert config["EXECUTOR_SHARED_STORAGE_ROOT"] == "/workspace/shared"
