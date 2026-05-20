"""ASGI application: MCP control plane + HTTP data plane + auth.

Layout (see docs/REMOTE_API.md):
    /mcp              -> FastMCP streamable-HTTP transport (the 6 control tools)
    /artifacts/...    -> file download with HTTP range support
    /healthz          -> liveness (open, no auth)
All paths except /healthz require `Authorization: Bearer <OPENMONTAGE_API_TOKEN>`.

Run:  uvicorn server.app:app --host 0.0.0.0 --port 8787   (or `make serve`)
"""

from __future__ import annotations

import contextlib
import logging

from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import FileResponse, JSONResponse
from starlette.routing import Mount, Route

from server.auth import BearerAuthMiddleware
from server.config import get_settings
from server.jobs import get_store
from server.mcp_server import mcp

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("openmontage.server")

settings = get_settings()
store = get_store()


@contextlib.asynccontextmanager
async def lifespan(app: Starlette):
    if not settings.auth_configured:
        log.warning("OPENMONTAGE_API_TOKEN is not set — all authed routes will return 401.")
    try:
        from tools.tool_registry import registry

        registry.discover()
    except Exception as exc:
        log.warning("registry discover failed at startup: %s", exc)
    # The MCP session manager must run for the streamable-HTTP app to serve.
    async with mcp.session_manager.run():
        await store.start()
        try:
            yield
        finally:
            await store.stop()


async def healthz(request: Request) -> JSONResponse:
    return JSONResponse(
        {
            "status": "ok",
            "backend": settings.agent_backend,
            "model": settings.agent_model,
            "max_concurrent_jobs": settings.max_concurrent_jobs,
            "auth_configured": settings.auth_configured,
            "jobs": store.counts(),
        }
    )


async def artifact(request: Request):
    job_id = request.path_params["job_id"]
    rel = request.path_params["path"]
    base = (settings.projects_dir / job_id).resolve()
    target = (base / rel).resolve()
    # Path-traversal guard: target must live under the job's workspace.
    if target != base and base not in target.parents:
        return JSONResponse({"error": "not found"}, status_code=404)
    if not target.is_file():
        return JSONResponse({"error": "not found"}, status_code=404)
    return FileResponse(str(target))  # Starlette FileResponse handles HTTP Range


routes = [
    Route("/healthz", healthz, methods=["GET"]),
    Route("/artifacts/{job_id}/{path:path}", artifact, methods=["GET", "HEAD"]),
    Mount("/mcp", app=mcp.streamable_http_app()),
]

middleware = [
    Middleware(BearerAuthMiddleware, token=settings.api_token, open_paths={"/healthz"}),
]

app = Starlette(routes=routes, lifespan=lifespan, middleware=middleware)


def main() -> None:
    import uvicorn

    uvicorn.run(app, host=settings.host, port=settings.port)


if __name__ == "__main__":
    main()
