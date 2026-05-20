"""Environment-driven settings for the remote API server.

All configuration comes from environment variables (loaded from .env if present),
so the same code runs in dev and on the GPU box without edits. See the table in
docs/REMOTE_API.md §9 for the full list.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

# Repo root = parent of the server/ package.
_DEFAULT_REPO_ROOT = Path(__file__).resolve().parent.parent

# Load .env from the repo root once, if python-dotenv is available. We never
# overwrite values already present in the real environment.
try:  # pragma: no cover - trivial
    from dotenv import load_dotenv

    load_dotenv(_DEFAULT_REPO_ROOT / ".env", override=False)
except Exception:  # python-dotenv missing or .env unreadable — env vars still work
    pass


@dataclass(frozen=True)
class Settings:
    api_token: str
    host: str
    port: int
    public_base_url: str
    agent_backend: str            # "sdk" | "cli" | "dry_run"
    agent_model: str
    agent_permission_mode: str    # SDK/CLI permission mode for unattended runs
    agent_max_turns: int
    max_concurrent_jobs: int
    repo_root: Path
    projects_dir: Path            # asset/artifact/render workspaces
    pipelines_dir: Path           # checkpoint workspaces
    anthropic_api_key: str | None
    allowed_hosts: tuple[str, ...]  # MCP Host allow-list; empty = DNS-rebinding protection off

    @property
    def auth_configured(self) -> bool:
        return bool(self.api_token)


def _int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    repo_root = Path(os.environ.get("OPENMONTAGE_REPO_ROOT", str(_DEFAULT_REPO_ROOT))).resolve()
    port = _int("OPENMONTAGE_PORT", 8787)
    public_base = os.environ.get("PUBLIC_BASE_URL", f"http://localhost:{port}").rstrip("/")
    raw_hosts = os.environ.get("OPENMONTAGE_ALLOWED_HOSTS", "").strip()
    allowed_hosts = tuple(h.strip() for h in raw_hosts.split(",") if h.strip()) if raw_hosts else ()
    return Settings(
        api_token=os.environ.get("OPENMONTAGE_API_TOKEN", ""),
        host=os.environ.get("OPENMONTAGE_HOST", "0.0.0.0"),
        port=port,
        public_base_url=public_base,
        agent_backend=os.environ.get("AGENT_BACKEND", "sdk").strip().lower(),
        agent_model=os.environ.get("AGENT_MODEL", "claude-opus-4-7").strip(),
        agent_permission_mode=os.environ.get("AGENT_PERMISSION_MODE", "bypassPermissions").strip(),
        agent_max_turns=_int("AGENT_MAX_TURNS", 200),
        max_concurrent_jobs=max(1, _int("MAX_CONCURRENT_JOBS", 1)),
        repo_root=repo_root,
        projects_dir=repo_root / "projects",
        pipelines_dir=repo_root / "pipelines",
        anthropic_api_key=os.environ.get("ANTHROPIC_API_KEY") or None,
        allowed_hosts=allowed_hosts,
    )
