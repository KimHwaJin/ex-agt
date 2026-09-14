"""Adapt values already loaded by common.config; no private imports."""

from collections.abc import Mapping
from typing import Any

from pydantic import ValidationError

from d_test.agent_service.settings import Settings


def settings_from_template(values: Mapping[str, Any]) -> Settings:
    """Pass config.AGENT_SERVICE, not the whole private Config singleton.

    The host owns profile/file selection and logging initialization. This
    adapter only validates the already selected AGENT_SERVICE mapping.
    """
    if not isinstance(values, Mapping):
        raise RuntimeError("config.AGENT_SERVICE must be a mapping")
    if "environment" not in values:
        raise RuntimeError("config.AGENT_SERVICE.environment is required")
    if values.get("logging_mode", "preconfigured") != "preconfigured":
        raise RuntimeError("Template logging must already be configured")
    try:
        return Settings.model_validate(
            {
                **values,
                "logging_mode": "preconfigured",
            }
        )
    except ValidationError:
        # Pydantic input_value may contain database passwords or API keys.
        raise RuntimeError("Invalid AGENT_SERVICE settings") from None
