"""Shared pagination and idempotency header validation."""

from typing import Annotated

from fastapi import Header, Query

Limit = Annotated[int, Query(ge=1, le=100)]
Cursor = Annotated[str | None, Query(min_length=1, max_length=4096)]

IdempotencyKey = Annotated[
    str,
    Header(
        alias="Idempotency-Key",
        min_length=1,
        max_length=200,
        pattern=r"^[A-Za-z0-9._:-]+$",
    ),
]

EventCursor = Annotated[
    str | None, Header(alias="Last-Event-ID", max_length=100)
]
