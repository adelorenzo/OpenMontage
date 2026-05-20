"""Bearer-token auth as pure ASGI middleware.

Implemented at the ASGI level (not Starlette's BaseHTTPMiddleware) so it does NOT
buffer responses — important because the MCP streamable-HTTP transport streams
long-lived SSE responses that must not be held in memory.

`/healthz` is open (liveness probe); everything else (the /mcp control plane and
the /artifacts data plane) requires `Authorization: Bearer <token>`.
"""

from __future__ import annotations

import hmac

from starlette.types import ASGIApp, Message, Receive, Scope, Send


def _unauthorized_body(detail: str) -> bytes:
    import json

    return json.dumps({"error": "unauthorized", "detail": detail}).encode()


class BearerAuthMiddleware:
    def __init__(self, app: ASGIApp, token: str, open_paths: set[str] | None = None) -> None:
        self.app = app
        self.token = token
        self.open_paths = open_paths or {"/healthz"}

    def _is_open(self, path: str) -> bool:
        return path in self.open_paths

    def _extract_bearer(self, scope: Scope) -> str | None:
        for key, value in scope.get("headers", []):
            if key.lower() == b"authorization":
                raw = value.decode("latin-1")
                if raw.lower().startswith("bearer "):
                    return raw[7:].strip()
                return None
        return None

    async def _send_401(self, send: Send, detail: str) -> None:
        body = _unauthorized_body(detail)
        await send(
            {
                "type": "http.response.start",
                "status": 401,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode()),
                    (b"www-authenticate", b"Bearer"),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        if self._is_open(path):
            await self.app(scope, receive, send)
            return

        if not self.token:
            await self._send_401(send, "server has no OPENMONTAGE_API_TOKEN configured")
            return

        presented = self._extract_bearer(scope)
        if presented is None:
            await self._send_401(send, "missing or malformed Authorization: Bearer header")
            return

        if not hmac.compare_digest(presented, self.token):
            await self._send_401(send, "invalid token")
            return

        await self.app(scope, receive, send)
