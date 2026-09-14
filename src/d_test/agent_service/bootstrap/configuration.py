"""Read one of four explicit runtime profiles without unsafe fallback."""

import os
from pathlib import Path

import yaml
from pydantic import ValidationError

from d_test.agent_service.settings import Settings


def load_settings(base_dir: Path) -> Settings:
    environment = os.environ.get("SERVICE_ENV", "local")
    profiles = {
        "local": "config_local.yaml",
        "dev": "config_dev.yaml",
        "stg": "config_stg.yaml",
        "prd": "config.yaml",
    }
    if environment not in profiles:
        raise RuntimeError("SERVICE_ENV must be local, dev, stg or prd")
    filename = profiles[environment]
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
    for key in (
        "database_url",
        "cursor_secret",
        "checkpoint_database_url",
        "model_name",
        "model_provider",
        "model_base_url",
        "model_api_key",
    ):
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
