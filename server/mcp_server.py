"""MCP control plane — the 6 tools Hermes calls.

Thin wrappers over JobStore and the OpenMontage tool registry. No creative logic
lives here; the intelligence is in the skills the headless agent reads. Mounted
at /mcp by server/app.py via FastMCP's streamable-HTTP transport.
"""

from __future__ import annotations

import base64
import logging
import mimetypes
from typing import Optional

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

from server.config import get_settings
from server.jobs import get_store

log = logging.getLogger("openmontage.server.mcp")
settings = get_settings()

# FastMCP auto-enables DNS-rebinding protection when bound to localhost, which
# rejects proxied Host headers (e.g. om.dr74.net) with "Invalid Host header" (421).
# This server is gated by a bearer token and runs behind a trusted reverse proxy /
# private network, so host validation is off by default. Operators who want it can
# set OPENMONTAGE_ALLOWED_HOSTS to an explicit comma-separated allow-list.
if settings.allowed_hosts:
    _transport_security = TransportSecuritySettings(
        enable_dns_rebinding_protection=True,
        allowed_hosts=list(settings.allowed_hosts),
        allowed_origins=[f"https://{h}" for h in settings.allowed_hosts]
        + [f"http://{h}" for h in settings.allowed_hosts],
    )
else:
    _transport_security = TransportSecuritySettings(enable_dns_rebinding_protection=False)

mcp = FastMCP(
    "OpenMontage",
    stateless_http=True,
    json_response=True,
    transport_security=_transport_security,
)
# Serve the MCP endpoint at the mount root (so the URL is /mcp, not /mcp/mcp).
mcp.settings.streamable_http_path = "/"


def _registry():
    from tools.tool_registry import registry

    return registry


@mcp.tool()
async def list_capabilities() -> dict:
    """What this OpenMontage box can produce right now.

    Returns the same human-ready capability rollup the local preflight uses
    (configured/total per capability family, composition runtimes, setup offers),
    plus the agent backend/model this server runs."""
    reg = _registry()
    try:
        reg.discover()
    except Exception as exc:
        log.warning("registry discover failed: %s", exc)
    try:
        summary = reg.provider_menu_summary()
    except Exception as exc:
        return {"error": f"could not read provider menu: {exc}"}
    return {
        "capabilities": summary,
        "agent_backend": settings.agent_backend,
        "agent_model": settings.agent_model,
    }


@mcp.tool()
async def submit_video_job(
    brief: str,
    pipeline: Optional[str] = None,
    approval_mode: str = "interactive",
    constraints: Optional[dict] = None,
    budget_usd: Optional[float] = None,
) -> dict:
    """Submit a video-production job to run on this box.

    brief: natural-language description of the video to make (required).
    pipeline: optional pipeline name (e.g. 'animated-explainer'); omit to let the agent choose.
    approval_mode: 'interactive' (pause at creative gates for your approval) or
        'autonomous' (run start-to-finish without pausing).
    constraints: optional dict (duration, platform, aspect_ratio, style, ...).
    budget_usd: optional soft cost cap.

    Returns {job_id, status}. Poll get_job_status; respond to gates with
    respond_to_checkpoint; fetch outputs with get_artifacts."""
    if not brief or not brief.strip():
        return {"error": "brief is required"}
    if approval_mode not in ("interactive", "autonomous"):
        return {"error": "approval_mode must be 'interactive' or 'autonomous'"}
    store = get_store()
    rec = store.create_job(
        brief=brief.strip(),
        pipeline=pipeline,
        approval_mode=approval_mode,
        constraints=constraints or {},
        budget_usd=budget_usd,
    )
    try:
        store.enqueue_start(rec.job_id)
    except RuntimeError as exc:
        return {"error": str(exc)}
    return {"job_id": rec.job_id, "status": rec.status}


@mcp.tool()
async def get_job_status(job_id: str) -> dict:
    """Current status of a job: status, stage, awaiting_human, pending_checkpoint,
    cost_usd, error, artifact_count, has_final_render."""
    s = get_store().status(job_id)
    if s is None:
        return {"error": f"unknown job_id {job_id!r}"}
    return s


@mcp.tool()
async def respond_to_checkpoint(job_id: str, decision: str, notes: Optional[str] = None) -> dict:
    """Respond to a job that is awaiting_human at a creative gate.

    decision: 'approve' (proceed), 'revise' (rework current artifact per notes,
        then re-present), or 'abort' (stop the job). notes: optional guidance.
    Resumes the agent's exact session. Returns {job_id, status}."""
    store = get_store()
    try:
        rec = store.respond(job_id, decision, notes)
    except KeyError:
        return {"error": f"unknown job_id {job_id!r}"}
    except (ValueError, RuntimeError) as exc:
        return {"error": str(exc)}
    return {"job_id": rec.job_id, "status": rec.status}


@mcp.tool()
async def get_artifacts(job_id: str) -> dict:
    """List a job's output files. Each entry: {path, kind, bytes, url}, where `url`
    is a signed, header-free download link. To get the bytes WITHOUT making an HTTP
    request, call fetch_artifact (returns base64 over MCP)."""
    store = get_store()
    if store.status(job_id) is None:
        return {"error": f"unknown job_id {job_id!r}"}
    return {"job_id": job_id, "artifacts": store.list_artifacts(job_id)}


_MAX_CHUNK_BYTES = 8_000_000  # hard cap per fetch_artifact call


@mcp.tool()
async def fetch_artifact(job_id: str, path: str, offset: int = 0, max_bytes: int = 4_000_000) -> dict:
    """Download an artifact's bytes directly over MCP (base64) — for clients that
    cannot make HTTP GETs. Returns the chunk starting at `offset`; for files larger
    than the per-call cap, loop using the returned `next_offset` until `eof` is true,
    concatenating the decoded chunks. If your client CAN fetch HTTP, prefer the signed
    `url` from get_artifacts instead (no base64/context overhead).

    `path` is the artifact's `path` field from get_artifacts (e.g. 'renders/final.mp4').
    Returns: {job_id, path, filename, mime_type, size_bytes, offset, length, eof,
    next_offset, encoding:'base64', content_base64}."""
    store = get_store()
    p = store.artifact_path(job_id, path)
    if p is None:
        return {"error": f"artifact not found: {job_id}/{path}"}
    try:
        size = p.stat().st_size
        cap = max(1, min(int(max_bytes), _MAX_CHUNK_BYTES))
        off = max(0, int(offset))
        with open(p, "rb") as f:
            f.seek(off)
            chunk = f.read(cap)
    except OSError as exc:
        return {"error": f"could not read artifact: {exc}"}
    eof = off + len(chunk) >= size
    return {
        "job_id": job_id,
        "path": path,
        "filename": p.name,
        "mime_type": mimetypes.guess_type(p.name)[0] or "application/octet-stream",
        "size_bytes": size,
        "offset": off,
        "length": len(chunk),
        "eof": eof,
        "next_offset": None if eof else off + len(chunk),
        "encoding": "base64",
        "content_base64": base64.b64encode(chunk).decode("ascii"),
    }


@mcp.tool()
async def cancel_job(job_id: str) -> dict:
    """Cancel a job. Cancels immediately if queued or awaiting_human; if running,
    terminates the agent process (cli backend) so the worker frees and the queue
    keeps moving. Returns {job_id, status}."""
    store = get_store()
    try:
        rec = store.cancel(job_id)
    except KeyError:
        return {"error": f"unknown job_id {job_id!r}"}
    return {"job_id": rec.job_id, "status": rec.status}


@mcp.tool()
async def list_jobs(status: Optional[str] = None, limit: int = 50) -> dict:
    """List jobs on this server (newest first) so you can enumerate or recover job_ids.

    status: optional filter — queued | running | awaiting_human | completed | failed |
        canceled | interrupted. limit: max results (default 50, cap 500).
    Each entry: {job_id, status, stage, approval_mode, created_at, updated_at}."""
    store = get_store()
    jobs = store.list_jobs()
    if status:
        jobs = [j for j in jobs if j.get("status") == status]
    limit = max(1, min(int(limit), 500))
    return {"count": len(jobs), "jobs": jobs[:limit]}
