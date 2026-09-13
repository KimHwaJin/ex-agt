"""Read YAML without falling back from production to development."""

import os
from pathlib import Path

import yaml
from pydantic import ValidationError

from agent_service.settings import Settings


def load_settings(base_dir: Path) -> Settings:
    environment = os.environ.get("SERVICE_ENV", "development")
    if environment not in {"development", "production"}:
        raise RuntimeError("SERVICE_ENV must be development or production")
    filename = (
        "config_dev.yaml" if environment == "development" else "config.yaml"
    )
    selected = Path(os.environ.get("SERVICE_CONFIG", str(base_dir / filename)))
    if not selected.is_absolute():
        selected = base_dir / selected
    with selected.open(encoding="utf-8") as source:
        values = yaml.safe_load(source)
    if not isinstance(values, dict):
        raise RuntimeError("Configuration YAML must contain a mapping")
    values = dict(values)
    if values.get("environment", environment) != environment:
        raise RuntimeError("Configuration profile does not match SERVICE_ENV")
    values["environment"] = environment
    for key in ("database_url", "cursor_secret"):
        variable = values.pop(f"{key}_env", None)
        if variable:
            value = os.environ.get(variable)
            if value is not None:
                values[key] = value
    logging_yaml = values.get("logging_yaml")
    if logging_yaml:
        path = Path(logging_yaml)
        if not path.is_absolute():
            values["logging_yaml"] = str(selected.parent / path)
    try:
        return Settings.model_validate(values)
    except ValidationError:
        # ValidationError may contain credentials from the input mapping.
        raise RuntimeError("Invalid management configuration") from None
