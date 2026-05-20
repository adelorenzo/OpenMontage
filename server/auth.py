"""Bearer-token auth as pure ASGI middleware.

Implemented at the ASGI level (not Starlette's BaseHTTPMiddleware) so it does NOT
buffer responses — important because the MCP streamable-HTTP transport streams
long-lived SSE responses that must not be held in memory.

`/healthz` is open (liveness probe); everything else (the /mcp control plane and
the /artifacts data plane) requires `Authorization: Bearer <token>`.
"""

from __future__ import annotations

import hashlib
import hmac
import time

from starlette.types import ASGIApp, Message, Receive, Scope, Send


def _unauthorized_body(detail: str) -> bytes:
    import json

    return json.dumps({"error": "unauthorized", "detail": detail}).encode()


# ---- signed artifact URLs ----
# get_artifacts returns time-limited HMAC-signed links so a remote client can
# download a file with NO Authorization header (mcp-remote only auths MCP calls,
# not raw GETs). The bearer header still works too.

def sign_artifact(token: str, job_id: str, rel: str, exp: int) -> str:
    msg = f"{job_id}\n{rel}\n{exp}".encode()
    return hmac.new(token.encode(), msg, hashlib.sha256).hexdigest()


def verify_artifact(token: str, job_id: str, rel: str, exp, sig) -> bool:
    if not sig:
        return False
    try:
        exp_i = int(exp)
    except (TypeError, ValueError):
        return False
    if exp_i < int(time.time()):
        return False
    return hmac.compare_digest(sign_artifact(token, job_id, rel, exp_i), str(sig))


class BearerAuthMiddleware:
    def __init__(
        self,
        app: ASGIApp,
        token: str,
        open_paths: set[str] | None = None,
        open_prefixes: tuple[str, ...] | None = None,
    ) -> None:
        self.app = app
        self.token = token
        self.open_paths = open_paths or {"/healthz"}
        # Prefixes whose auth is enforced by the route handler (e.g. /artifacts,
        # which accepts a bearer header OR a signed URL), so the global middleware
        # lets them through.
        self.open_prefixes = tuple(open_prefixes or ())

    def _is_open(self, path: str) -> bool:
        if path in self.open_paths:
            return True
        return bool(self.open_prefixes) and path.startswith(self.open_prefixes)

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
