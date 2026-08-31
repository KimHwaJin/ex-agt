from pathlib import Path
from typing import Any

import yaml

_COMPOSE_FILE = Path(__file__).parents[1] / "docker-compose.yml"


def _services() -> dict[str, Any]:
    return yaml.safe_load(_COMPOSE_FILE.read_text())["services"]


def test_default_compose_does_not_start_private_infrastructure() -> None:
    services = _services()
    default_services = {
        name
        for name, service in services.items()
        if not service.get("profiles")
    }

    assert default_services == {"api", "worker", "migrate"}
    for name in ("postgres", "redis"):
        assert services[name]["profiles"] == ["local-infra"]
    for name in default_services:
        dependencies = services[name].get("depends_on", {})
        assert not {"postgres", "redis"}.intersection(dependencies)


def test_application_connections_are_configurable() -> None:
    services = _services()
    connection_names = (
        "AGENT_DATABASE_URL",
        "AGENT_CHECKPOINT_DATABASE_URL",
        "AGENT_REDIS_URL",
    )

    for name in ("api", "worker", "migrate"):
        for variable in connection_names:
            value = services[name]["environment"][variable]
            assert value.startswith("${" + variable + ":-")
            assert "host.docker.internal" in value


def test_integration_tests_keep_isolated_connections() -> None:
    services = _services()

    for name in ("test", "test-migrate"):
        environment = services[name]["environment"]
        for variable in (
            "AGENT_DATABASE_URL",
            "AGENT_CHECKPOINT_DATABASE_URL",
        ):
            assert "@test-postgres:5432/agent_test" in environment[variable]
        assert environment["AGENT_REDIS_URL"] == "redis://test-redis:6379/0"
