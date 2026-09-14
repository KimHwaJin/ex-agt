"""Adapt values already loaded by common.config; no private imports."""

from collections.abc import Mapping
from typing import Any

from pydantic import ValidationError

from d_test.agent_service.settings import Settings


def settings_from_template(
    values: Mapping[str, Any], *, profile: str
) -> Settings:
    """Pass config.AGENT_SERVICE, not the whole private Config singleton.

    The host owns config.dev.yml/config.stg.yml/config.yml selection and
    logging initialization. Unknown profiles and invalid settings fail closed.
    """
    profiles = {
        "dev": "development",
        "stg": "production",
        "prd": "production",
    }
    if profile not in profiles:
        raise RuntimeError("HCP_ACTIVE_PROFILE must be dev, stg or prd")
    if not isinstance(values, Mapping):
        raise RuntimeError("config.AGENT_SERVICE must be a mapping")
    expected = profiles[profile]
    if values.get("environment", expected) != expected:
        raise RuntimeError("Template profile and agent environment differ")
    if values.get("logging_mode", "preconfigured") != "preconfigured":
        raise RuntimeError("Template logging must already be configured")
    try:
        return Settings.model_validate(
            {
                **values,
                "environment": expected,
                "logging_mode": "preconfigured",
            }
        )
    except ValidationError:
        # Pydantic input_value may contain database passwords or API keys.
        raise RuntimeError("Invalid AGENT_SERVICE settings") from None
