"""Bound input before JSON parsing; no changes to host template endpoints."""

from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Receive, Scope, Send


class RunBodyLimit:
    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if (
            scope["type"] != "http"
            or scope["method"] != "POST"
            or not scope["path"].endswith("/api/v1/agent/runs")
        ):
            return await self.app(scope, receive, send)
        data = bytearray()
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                return
            chunk = message.get("body", b"")
            if len(data) + len(chunk) > 131072:
                return await self.reject(scope, receive, send, 413)
            data.extend(chunk)
            if not message.get("more_body", False):
                break
        depth, quoted, escaped = 0, False, False
        for char in data:
            if quoted:
                if escaped:
                    escaped = False
                elif char == 92:
                    escaped = True
                elif char == 34:
                    quoted = False
            elif char == 34:
                quoted = True
            elif char in (91, 123):
                depth += 1
                if depth > 32:
                    return await self.reject(scope, receive, send, 422)
            elif char in (93, 125):
                depth -= 1
        delivered = False

        async def buffered():
            nonlocal delivered
            if not delivered:
                delivered = True
                return {
                    "type": "http.request",
                    "body": bytes(data),
                    "more_body": False,
                }
            return await receive()

        await self.app(scope, buffered, send)

    @staticmethod
    async def reject(scope, receive, send, status):
        response = JSONResponse(
            {
                "code": "PAYLOAD_TOO_LARGE"
                if status == 413
                else "JSON_TOO_DEEP",
                "message": "요청 한도: 128 KiB, 중첩 깊이 32.",
                "request_id": scope.get("state", {}).get(
                    "management_request_id"
                ),
            },
            status_code=status,
        )
        await response(scope, receive, send)
