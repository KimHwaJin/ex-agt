"""Dependency entrypoints for HTTP routers."""

from .identity import CurrentUser as CurrentUser
from .identity import EmployeeId as EmployeeId
from .parameters import IdempotencyKey as IdempotencyKey
from .services import Service as Service
