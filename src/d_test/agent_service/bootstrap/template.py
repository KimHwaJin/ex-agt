"""Adapt values already loaded by common.config; no private imports."""

from collections.abc import Mapping
from typing import Any, get_args

from pydantic import ValidationError

from d_test.agent_service.settings import Settings

# Only our fixed validation messages are safe to expose. Arbitrary validator
# messages (e.g. ipaddress errors) can contain the rejected input value.
_RUNTIME_HINTS = {
    "database_url must be a PostgreSQL URI": (
        "DATABASE_URL: postgresql:// 또는 postgres:// 형식이 필요합니다"
    ),
    "cursor_secret must have at least 32 characters": (
        "CURSOR_SECRET: 32자 이상이어야 합니다"
    ),
    "pool_min_size exceeds pool_max_size": (
        "POOL_MIN_SIZE: POOL_MAX_SIZE보다 클 수 없습니다"
    ),
    "bootstrap requires a dedicated checkpoint schema": (
        "CHECKPOINT_SCHEMA: 초기화에는 전용 스키마가 필요합니다"
    ),
    "trusted proxies must be explicitly set": (
        "TRUSTED_PROXY_CIDRS: trusted_header 인증에 필요한 값입니다"
    ),
    "external identity provider is required": (
        "IDENTITY_PROVIDER_FACTORY: external 인증에 필요한 값입니다"
    ),
    "demo agent is forbidden in stg/prd": (
        "AGENT_BACKEND: stg/prd에서는 demo를 사용할 수 없습니다"
    ),
    "development identity is forbidden in stg/prd": (
        "AUTH_MODE: stg/prd에서는 development_header를 사용할 수 없습니다"
    ),
    "stg/prd requires host logging": (
        "LOGGING_MODE: stg/prd에서는 host 또는 preconfigured가 필요합니다"
    ),
    "replace the development cursor in stg/prd": (
        "CURSOR_SECRET: stg/prd에서는 개발 예제 값을 교체해야 합니다"
    ),
    "embedded worker requires an enabled backend": (
        "EMBEDDED_RUN_WORKER: AGENT_BACKEND=disabled와 함께 켤 수 없습니다"
    ),
    "checkpoint database must be PostgreSQL": (
        "CHECKPOINT_DATABASE_URL: PostgreSQL URI 형식이 필요합니다"
    ),
    "langgraph backend requires model_name": (
        "MODEL_NAME: langgraph 실행에 필요한 값입니다"
    ),
    "checkpoint pool needs an extra health slot": (
        "CHECKPOINT_POOL_MAX_SIZE: WORKER_CONCURRENCY보다 커야 합니다"
    ),
    "allowed model names cannot be blank": (
        "ALLOWED_MODEL_NAMES: 빈 모델 이름을 포함할 수 없습니다"
    ),
    "host logging initializer and YAML required": (
        "LOGGING_INITIALIZER / LOGGING_YAML: host 로깅에 필요한 값입니다"
    ),
}
_FIELD_HINTS = {
    "missing": "필수 항목이 없습니다",
    "literal_error": "허용된 선택값인지 확인하세요",
    "string_type": "문자열이 필요합니다",
    "int_parsing": "정수로 변환할 수 없습니다",
    "int_type": "정수가 필요합니다",
    "float_parsing": "숫자로 변환할 수 없습니다",
    "float_type": "숫자가 필요합니다",
    "bool_parsing": "true 또는 false로 설정하세요",
    "bool_type": "true 또는 false로 설정하세요",
    "dict_type": "매핑 형식이 필요합니다",
    "list_type": "목록 형식이 필요합니다",
    "extra_forbidden": "지원하지 않는 설정 항목이 있습니다",
}


def _validation_details(error: ValidationError) -> str:
    details = []
    for issue in error.errors(
        include_input=False, include_context=False, include_url=False
    ):
        location = issue["loc"]
        if location:
            # Unknown keys and nested mapping keys can themselves be secrets.
            field = location[0]
            label = (
                str(field).upper()
                if field in Settings.model_fields
                else "<UNKNOWN_FIELD>"
            )
            code = issue["type"]
            hint = _FIELD_HINTS.get(
                code, "값의 형식 또는 허용 범위를 확인하세요"
            )
            if code == "literal_error" and field in Settings.model_fields:
                choices = get_args(Settings.model_fields[field].annotation)
                if choices and all(
                    isinstance(value, str) for value in choices
                ):
                    hint = "허용값: " + ", ".join(choices)
            details.append(f"{label}: {hint} [{code}]")
        else:
            message = issue["msg"].removeprefix("Value error, ")
            hint = _RUNTIME_HINTS.get(message)
            if hint is None and message.endswith(
                "does not appear to be an IPv4 or IPv6 network"
            ):
                hint = (
                    "TRUSTED_PROXY_CIDRS: 올바른 IPv4/IPv6 CIDR이 필요합니다"
                )
            details.append(hint or "설정 간 검증 실패 [value_error]")
    return "; ".join(dict.fromkeys(details))


def settings_from_template(values: Mapping[str, Any]) -> Settings:
    """Pass config.AGENT_SERVICE, not the whole private Config singleton.

    The host owns profile/file selection and logging initialization. This
    adapter only validates the already selected AGENT_SERVICE mapping.
    Normalize settings keys only; provider payloads keep their original keys.
    """
    if not isinstance(values, Mapping):
        raise RuntimeError("config.AGENT_SERVICE must be a mapping")
    normalized: dict[str, Any] = {}
    for key, value in values.items():
        if not isinstance(key, str) or key.lower() in normalized:
            raise RuntimeError("Invalid or duplicate AGENT_SERVICE key")
        normalized[key.lower()] = value
    values = normalized
    if "environment" not in values:
        raise RuntimeError("config.AGENT_SERVICE.ENVIRONMENT is required")
    if values.get("logging_mode", "preconfigured") != "preconfigured":
        raise RuntimeError("Template logging must already be configured")
    try:
        return Settings.model_validate(
            {
                **values,
                "logging_mode": "preconfigured",
            }
        )
    except ValidationError as error:
        raise RuntimeError(
            f"Invalid AGENT_SERVICE settings: {_validation_details(error)}"
        ) from None
