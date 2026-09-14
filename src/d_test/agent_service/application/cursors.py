"""Opaque, signed keyset cursors bound to the owner and query scope."""

import base64
import hashlib
import hmac
import json
from datetime import datetime
from uuid import UUID

from d_test.agent_service.domain.management import DomainError


class CursorCodec:
    def __init__(self, secret: str):
        self.secret = secret.encode()

    def encode(self, scope: str, created_at: datetime, item_id: UUID) -> str:
        payload = json.dumps(
            [1, scope, created_at.isoformat(), str(item_id)],
            separators=(",", ":"),
        ).encode()
        signature = hmac.digest(self.secret, payload, hashlib.sha256)
        return base64.urlsafe_b64encode(signature + payload).decode()

    def decode(
        self, scope: str, cursor: str | None
    ) -> tuple[datetime, UUID] | None:
        if cursor is None:
            return None
        try:
            if len(cursor) > 4096:
                raise ValueError
            raw = base64.b64decode(cursor, altchars=b"-_", validate=True)
            signature, payload = raw[:32], raw[32:]
            expected = hmac.digest(self.secret, payload, hashlib.sha256)
            if not hmac.compare_digest(signature, expected):
                raise ValueError
            version, saved_scope, timestamp, item_id = json.loads(payload)
            if version != 1 or saved_scope != scope:
                raise ValueError
            parsed = datetime.fromisoformat(timestamp)
            if parsed.tzinfo is None:
                raise ValueError
            return parsed, UUID(item_id)
        except (ValueError, TypeError, UnicodeError):
            raise DomainError(
                "INVALID_CURSOR", "유효하지 않은 페이지 커서입니다.", 422
            ) from None
