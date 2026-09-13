"""Only process entrypoints initialize logging; domain code never does."""

import importlib
import logging
from collections.abc import Callable
from pathlib import Path
from typing import cast

from agent_service.settings import Settings


def import_callable(path: str) -> Callable:
    module, separator, name = path.partition(":")
    if not separator:
        raise RuntimeError("Configured callable must use module:function")
    value = getattr(importlib.import_module(module), name)
    if not callable(value):
        raise RuntimeError("Configured object is not callable")
    return cast(Callable, value)


def initialize_logging(settings: Settings) -> None:
    if settings.logging_mode == "host":
        path = Path(settings.logging_yaml or "")
        if not path.is_file():
            raise RuntimeError("Host logging YAML does not exist")
        initializer = import_callable(settings.logging_initializer or "")
        initializer(path)
        return
    # Development only. Existing template handlers are not replaced.
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
