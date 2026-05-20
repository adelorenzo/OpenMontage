"""Job store, async queue, and worker.

A "job" is one delegated video-production request. Its durable record is
`projects/<job_id>/job.json`; the in-memory queue is rebuilt from disk on
startup. The worker honors MAX_CONCURRENT_JOBS (GPU serialization) and drives
each job through the headless agent runner, interpreting the result against the
pipeline's checkpoints (which live under `pipelines/<job_id>/`).

Status machine (see docs/REMOTE_API.md §3):
    queued -> running -> {completed | failed | canceled | awaiting_human}
    awaiting_human --respond--> running -> ...
On restart, jobs left `running` become `interrupted`.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote

from server import agent_runner
from server.auth import sign_artifact
from server.config import Settings, get_settings

log = logging.getLogger("openmontage.server.jobs")

STATUS_QUEUED = "queued"
STATUS_RUNNING = "running"
STATUS_AWAITING = "awaiting_human"
STATUS_COMPLETED = "completed"
STATUS_FAILED = "failed"
STATUS_CANCELED = "canceled"
STATUS_INTERRUPTED = "interrupted"

_TERMINAL = {STATUS_COMPLETED, STATUS_FAILED, STATUS_CANCELED}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _to_epoch(ts: Optional[str]) -> Optional[float]:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts).timestamp()
    except Exception:
        return None


def slugify(text: str, maxlen: int = 40) -> str:
    text = (text or "").lower()
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    text = re.sub(r"-{2,}", "-", text)
    return text[:maxlen].strip("-") or "job"


def new_job_id(brief: str) -> str:
    return f"{slugify(brief)}-{uuid.uuid4().hex[:8]}"


def _artifact_kind(relpath: str) -> str:
    parts = relpath.split("/")
    top = parts[0]
    if top == "renders":
        return "render"
    if top == "assets":
        sub = parts[1] if len(parts) > 1 else ""
        return {"images": "image", "video": "video", "audio": "audio", "music": "music"}.get(sub, "asset")
    if top == "artifacts":
        return "artifact"
    ext = Path(relpath).suffix.lower()
    return {
        ".mp4": "video", ".mov": "video", ".webm": "video",
        ".png": "image", ".jpg": "image", ".jpeg": "image",
        ".mp3": "audio", ".wav": "audio",
        ".srt": "subtitle", ".json": "artifact",
    }.get(ext, "file")


@dataclass
class JobRecord:
    job_id: str
    brief: str
    pipeline: Optional[str]
    approval_mode: str
    constraints: dict
    budget_usd: Optional[float]
    status: str
    stage: Optional[str] = None
    session_id: Optional[str] = None
    started_at: Optional[str] = None
    cost_usd: float = 0.0
    error: Optional[str] = None
    pending_checkpoint: Optional[dict] = None
    created_at: str = field(default_factory=_now)
    updated_at: str = field(default_factory=_now)
    history: list = field(default_factory=list)

    def touch(self, status: Optional[str] = None, note: Optional[str] = None) -> None:
        if status:
            self.status = status
        self.updated_at = _now()
        self.history.append({"ts": self.updated_at, "status": self.status, "note": note})

    def to_dict(self) -> dict:
        return {
            "job_id": self.job_id,
            "brief": self.brief,
            "pipeline": self.pipeline,
            "approval_mode": self.approval_mode,
            "constraints": self.constraints,
            "budget_usd": self.budget_usd,
            "status": self.status,
            "stage": self.stage,
            "session_id": self.session_id,
            "started_at": self.started_at,
            "cost_usd": self.cost_usd,
            "error": self.error,
            "pending_checkpoint": self.pending_checkpoint,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "history": self.history,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "JobRecord":
        return cls(
            job_id=data["job_id"],
            brief=data.get("brief", ""),
            pipeline=data.get("pipeline"),
            approval_mode=data.get("approval_mode", "interactive"),
            constraints=data.get("constraints") or {},
            budget_usd=data.get("budget_usd"),
            status=data.get("status", STATUS_QUEUED),
            stage=data.get("stage"),
            session_id=data.get("session_id"),
            started_at=data.get("started_at"),
            cost_usd=data.get("cost_usd", 0.0),
            error=data.get("error"),
            pending_checkpoint=data.get("pending_checkpoint"),
            created_at=data.get("created_at", _now()),
            updated_at=data.get("updated_at", _now()),
            history=data.get("history") or [],
        )


class JobStore:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self._jobs: dict[str, JobRecord] = {}
        self._pending_resume: dict[str, tuple[str, Optional[str]]] = {}
        self._queue: Optional[asyncio.Queue] = None
        self._workers: list[asyncio.Task] = []
        self._watchdog_task: Optional[asyncio.Task] = None
        self._started = False

    # ---- paths / persistence ----
    def _job_dir(self, job_id: str) -> Path:
        return self.settings.projects_dir / job_id

    def _job_file(self, job_id: str) -> Path:
        return self._job_dir(job_id) / "job.json"

    def _persist(self, rec: JobRecord) -> None:
        self._job_dir(rec.job_id).mkdir(parents=True, exist_ok=True)
        with open(self._job_file(rec.job_id), "w") as f:
            json.dump(rec.to_dict(), f, indent=2)

    def _save(self, rec: JobRecord) -> None:
        rec.updated_at = _now()
        self._jobs[rec.job_id] = rec
        self._persist(rec)

    @staticmethod
    def _read_json(path: Path) -> Optional[dict]:
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            return None

    # ---- lifecycle ----
    async def start(self) -> None:
        if self._started:
            return
        self._queue = asyncio.Queue()
        self._scan_disk()
        for _ in range(self.settings.max_concurrent_jobs):
            self._workers.append(asyncio.create_task(self._worker()))
        if self.settings.job_inactivity_timeout or self.settings.job_max_runtime:
            self._watchdog_task = asyncio.create_task(self._watchdog())
        self._started = True
        log.info(
            "JobStore started: %d worker(s), backend=%s, watchdog=%s",
            len(self._workers),
            self.settings.agent_backend,
            "on" if self._watchdog_task else "off",
        )

    async def stop(self) -> None:
        if self._watchdog_task:
            self._watchdog_task.cancel()
            try:
                await self._watchdog_task
            except (asyncio.CancelledError, Exception):
                pass
            self._watchdog_task = None
        for w in self._workers:
            w.cancel()
        for w in self._workers:
            try:
                await w
            except (asyncio.CancelledError, Exception):
                pass
        self._workers.clear()
        self._started = False

    def _scan_disk(self) -> None:
        pdir = self.settings.projects_dir
        if not pdir.exists():
            return
        for jf in sorted(pdir.glob("*/job.json")):
            data = self._read_json(jf)
            if not data or "job_id" not in data:
                continue
            try:
                rec = JobRecord.from_dict(data)
            except Exception:
                continue
            if rec.status == STATUS_RUNNING:
                rec.touch(STATUS_INTERRUPTED, "marked interrupted after server restart")
                self._persist(rec)
            self._jobs[rec.job_id] = rec
            if rec.status == STATUS_QUEUED and self._queue is not None:
                self._queue.put_nowait({"action": "start", "job_id": rec.job_id})

    # ---- queue producers ----
    def create_job(
        self,
        brief: str,
        pipeline: Optional[str],
        approval_mode: str,
        constraints: dict,
        budget_usd: Optional[float],
    ) -> JobRecord:
        job_id = new_job_id(brief)
        while job_id in self._jobs or self._job_file(job_id).exists():
            job_id = new_job_id(brief)
        rec = JobRecord(
            job_id=job_id,
            brief=brief,
            pipeline=pipeline,
            approval_mode=approval_mode,
            constraints=constraints,
            budget_usd=budget_usd,
            status=STATUS_QUEUED,
        )
        rec.history.append({"ts": rec.created_at, "status": STATUS_QUEUED, "note": "created"})
        self._save(rec)
        return rec

    def enqueue_start(self, job_id: str) -> None:
        if self._queue is None:
            raise RuntimeError("JobStore not started")
        self._queue.put_nowait({"action": "start", "job_id": job_id})

    def respond(self, job_id: str, decision: str, notes: Optional[str] = None) -> JobRecord:
        rec = self._jobs.get(job_id)
        if rec is None:
            raise KeyError(job_id)
        if decision not in ("approve", "revise", "abort"):
            raise ValueError("decision must be 'approve', 'revise', or 'abort'")
        if rec.status != STATUS_AWAITING:
            raise ValueError(f"job {job_id} is {rec.status!r}, not awaiting_human")
        if decision == "abort":
            rec.pending_checkpoint = None
            rec.touch(STATUS_CANCELED, "operator aborted at checkpoint")
            self._save(rec)
            return rec
        if self._queue is None:
            raise RuntimeError("JobStore not started")
        self._pending_resume[job_id] = (decision, notes)
        rec.pending_checkpoint = None
        rec.touch(STATUS_RUNNING, f"resume requested: {decision}")
        self._save(rec)
        self._queue.put_nowait({"action": "resume", "job_id": job_id})
        return rec

    def cancel(self, job_id: str) -> JobRecord:
        rec = self._jobs.get(job_id)
        if rec is None:
            raise KeyError(job_id)
        if rec.status in (STATUS_QUEUED, STATUS_AWAITING):
            rec.pending_checkpoint = None
            rec.touch(STATUS_CANCELED, "canceled by operator")
            self._save(rec)
        elif rec.status == STATUS_RUNNING:
            rec.touch(STATUS_CANCELED, "canceled by operator (terminating agent)")
            self._save(rec)
            # Kill the agent subprocess so the worker unblocks instead of staying
            # stuck on an orphan (which would head-of-line-block the whole queue).
            try:
                agent_runner.terminate(rec.job_id)
            except Exception:
                log.exception("failed to terminate agent for %s", rec.job_id)
        return rec

    # ---- worker ----
    async def _worker(self) -> None:
        assert self._queue is not None
        while True:
            item = await self._queue.get()
            try:
                await self._run(item)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # never let the worker die
                log.exception("job run crashed: %s", exc)
                rec = self._jobs.get(item.get("job_id", ""))
                if rec is not None:
                    rec.error = f"worker error: {exc}"
                    rec.touch(STATUS_FAILED, "worker exception")
                    self._save(rec)
            finally:
                self._queue.task_done()

    async def _run(self, item: dict) -> None:
        job_id = item["job_id"]
        action = item["action"]
        rec = self._jobs.get(job_id)
        if rec is None:
            return
        if rec.status == STATUS_CANCELED:
            return  # canceled before the worker picked it up
        rec.started_at = _now()
        rec.touch(STATUS_RUNNING, f"{action} agent run")
        self._save(rec)

        if action == "start":
            outcome = await agent_runner.start(rec, self.settings)
        else:
            decision, notes = self._pending_resume.pop(job_id, ("approve", None))
            outcome = await agent_runner.resume(rec, decision, notes, self.settings)

        self._apply_outcome(rec, outcome)

    def _apply_outcome(self, rec: JobRecord, outcome: "agent_runner.RunOutcome") -> None:
        if outcome.session_id:
            rec.session_id = outcome.session_id
        rec.cost_usd = round(rec.cost_usd + (outcome.cost_usd or 0.0), 6)

        # Already finalized by cancel or the watchdog (agent terminated) — don't resurrect it.
        if rec.status in _TERMINAL:
            self._save(rec)
            return

        # dry_run (and only dry_run) overrides checkpoint inspection.
        if outcome.explicit_status:
            rec.stage = outcome.stage or rec.stage
            rec.pending_checkpoint = outcome.pending
            if outcome.explicit_status == STATUS_FAILED:
                rec.error = outcome.error or outcome.summary
            rec.touch(outcome.explicit_status, outcome.summary)
            self._save(rec)
            return

        prog = self.inspect_progress(rec.job_id)
        rec.stage = prog["stage"] or rec.stage

        if prog["completed"]:
            rec.pending_checkpoint = None
            rec.touch(STATUS_COMPLETED, "final render present")
            self._save(rec)
            return
        if prog["awaiting"]:
            rec.pending_checkpoint = prog["pending"]
            rec.touch(STATUS_AWAITING, f"awaiting human at {prog['stage']}")
            self._save(rec)
            return
        if outcome.turn_result == "error":
            rec.error = outcome.error or outcome.summary or "agent run errored"
            rec.touch(STATUS_FAILED, "agent error")
            self._save(rec)
            return
        # Turn ended cleanly but no terminal/awaiting checkpoint was written.
        if rec.approval_mode == "interactive":
            rec.pending_checkpoint = prog["pending"] or {
                "stage": rec.stage,
                "summary": "agent ended its turn; awaiting operator response",
            }
            rec.touch(STATUS_AWAITING, "agent ended turn; awaiting operator")
            self._save(rec)
        else:
            rec.error = "agent finished without producing a final render"
            rec.touch(STATUS_FAILED, "no render produced")
            self._save(rec)

    # ---- watchdog ----
    def _last_activity_ts(self, job_id: str) -> float:
        """Newest mtime across the job's workspace + checkpoints (its 'heartbeat')."""
        newest = 0.0
        for d in (self.settings.projects_dir / job_id, self.settings.pipelines_dir / job_id):
            if not d.exists():
                continue
            for p in d.rglob("*"):
                try:
                    m = p.stat().st_mtime
                except OSError:
                    continue
                if m > newest:
                    newest = m
        return newest

    def _timeout_job(self, rec: JobRecord, reason: str) -> None:
        rec.error = reason
        rec.touch(STATUS_FAILED, reason)
        self._save(rec)
        try:
            agent_runner.terminate(rec.job_id)  # free the worker; _apply_outcome won't resurrect it
        except Exception:
            log.exception("watchdog failed to terminate %s", rec.job_id)

    async def _watchdog(self) -> None:
        interval = self.settings.watchdog_interval
        inactivity = self.settings.job_inactivity_timeout
        max_runtime = self.settings.job_max_runtime
        while True:
            try:
                await asyncio.sleep(interval)
                now = time.time()
                for rec in list(self._jobs.values()):
                    if rec.status != STATUS_RUNNING:
                        continue
                    started = _to_epoch(rec.started_at) or _to_epoch(rec.updated_at) or now
                    last = max(self._last_activity_ts(rec.job_id), started)
                    if max_runtime and (now - started) > max_runtime:
                        log.warning("watchdog: job %s exceeded max runtime %ds", rec.job_id, max_runtime)
                        self._timeout_job(rec, f"killed by watchdog: exceeded max runtime {max_runtime}s")
                    elif inactivity and (now - last) > inactivity:
                        idle = int(now - last)
                        log.warning("watchdog: job %s idle %ds (limit %ds)", rec.job_id, idle, inactivity)
                        self._timeout_job(rec, f"killed by watchdog: no progress for {idle}s (limit {inactivity}s)")
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("watchdog tick error")

    # ---- introspection ----
    def inspect_progress(self, job_id: str) -> dict:
        """Read pipeline checkpoints + render output to derive live progress."""
        pdir = self.settings.pipelines_dir / job_id
        final = self.settings.projects_dir / job_id / "renders" / "final.mp4"
        stage = None
        awaiting = False
        pending = None
        completed = final.exists()

        if pdir.exists():
            checkpoints = sorted(
                pdir.glob("checkpoint_*.json"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            for path in checkpoints:
                data = self._read_json(path)
                if not data:
                    continue
                if stage is None:
                    stage = data.get("stage")
                cp_status = data.get("status")
                if cp_status == "awaiting_human" and not awaiting:
                    awaiting = True
                    stage = data.get("stage")
                    pending = {
                        "stage": data.get("stage"),
                        "artifact_keys": list((data.get("artifacts") or {}).keys()),
                        "timestamp": data.get("timestamp"),
                    }
                if data.get("stage") in ("compose", "publish") and cp_status == "completed":
                    completed = True

        if completed:
            awaiting = False  # a finished render outranks a stale gate
        return {"stage": stage, "awaiting": awaiting, "pending": pending, "completed": completed}

    def _artifact_url(self, job_id: str, rel: str) -> str:
        base = f"{self.settings.public_base_url}/artifacts/{job_id}/{quote(rel, safe='/')}"
        token = self.settings.api_token
        if not token:
            return base  # auth disabled — plain URL
        exp = int(time.time()) + self.settings.artifact_url_ttl
        sig = sign_artifact(token, job_id, rel, exp)
        return f"{base}?exp={exp}&sig={sig}"

    def list_artifacts(self, job_id: str) -> list[dict]:
        base = self._job_dir(job_id)
        out: list[dict] = []
        if not base.exists():
            return out
        for path in sorted(base.rglob("*")):
            if not path.is_file() or path.name == "job.json":
                continue
            rel = path.relative_to(base).as_posix()
            out.append(
                {
                    "path": rel,
                    "kind": _artifact_kind(rel),
                    "bytes": path.stat().st_size,
                    "url": self._artifact_url(job_id, rel),
                }
            )
        return out

    def status(self, job_id: str) -> Optional[dict]:
        rec = self._jobs.get(job_id)
        if rec is None:
            return None
        prog = self.inspect_progress(job_id)
        arts = self.list_artifacts(job_id)
        idle_seconds = None
        if rec.status == STATUS_RUNNING:
            la = self._last_activity_ts(job_id)
            if la > 0:
                idle_seconds = int(time.time() - la)
        return {
            "job_id": rec.job_id,
            "status": rec.status,
            "stage": rec.stage or prog["stage"],
            "awaiting_human": rec.status == STATUS_AWAITING,
            "pending_checkpoint": rec.pending_checkpoint,
            "approval_mode": rec.approval_mode,
            "pipeline": rec.pipeline,
            "cost_usd": rec.cost_usd,
            "error": rec.error,
            "created_at": rec.created_at,
            "updated_at": rec.updated_at,
            "started_at": rec.started_at,
            "idle_seconds": idle_seconds,
            "artifact_count": len(arts),
            "has_final_render": any(a["kind"] == "render" for a in arts),
        }

    def list_jobs(self) -> list[dict]:
        return [
            {
                "job_id": r.job_id,
                "status": r.status,
                "stage": r.stage,
                "approval_mode": r.approval_mode,
                "created_at": r.created_at,
                "updated_at": r.updated_at,
            }
            for r in sorted(self._jobs.values(), key=lambda r: r.created_at, reverse=True)
        ]

    def counts(self) -> dict:
        out: dict[str, int] = {}
        for rec in self._jobs.values():
            out[rec.status] = out.get(rec.status, 0) + 1
        return out


_store: Optional[JobStore] = None


def get_store() -> JobStore:
    global _store
    if _store is None:
        _store = JobStore(get_settings())
    return _store
