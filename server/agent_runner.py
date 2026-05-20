"""Headless agent runner — the substance of the remote API.

Because OpenMontage has no Python orchestrator (the pipeline IS an agent loop),
"running a job" means running a headless Claude agent in the repo, exactly as an
interactive Claude Code session would, but triggered by an API job. This module
abstracts that behind start()/resume() with three interchangeable backends:

  - sdk      : claude-agent-sdk (needs ANTHROPIC_API_KEY)
  - cli      : `claude -p ... --output-format json` subprocess (reuses the box's
               existing `claude` login; good when you can't set an API key)
  - dry_run  : no LLM; simulates stage progression for tests / wiring checks

The runner only reports the raw turn result + session id + cost. The JobStore
decides the job's status by reading the pipeline checkpoints — except for
dry_run, which sets an explicit status to exercise the full state machine.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
from dataclasses import dataclass
from shutil import which
from typing import TYPE_CHECKING, Optional

from server.config import Settings

if TYPE_CHECKING:
    from server.jobs import JobRecord

log = logging.getLogger("openmontage.server.agent_runner")

_RESULT_SUBTYPES = {"success", "error", "error_max_turns", "error_during_execution"}

# Appended to the Claude Code system prompt (sdk) / passed via --append-system-prompt (cli).
# General behavior for the remote runner; per-job details go in the user prompt.
SYSTEM_APPEND = """\
You are running headless inside OpenMontage's remote job runner. A remote operator
(not a human at this terminal) triggered this job over an API and cannot chat with
you turn-by-turn. Follow AGENT_GUIDE.md exactly — every production goes through a
pipeline. Two hard rules specific to the remote runner:

1. Write ALL outputs for this job under projects/<job_id>/ and write stage
   checkpoints under pipelines/<job_id>/ using project_id == <job_id> (given below).
   The final deliverable MUST be projects/<job_id>/renders/final.mp4.
2. Honor the approval_mode given in the job:
   - "autonomous": do NOT stop to ask the operator. At each human-approval gate,
     make the best decision yourself, record it in the checkpoint, and proceed to a
     finished render.
   - "interactive": run up to the next human-approval gate (idea/script/scene_plan),
     write that stage's checkpoint with status "awaiting_human" and its canonical
     artifact, then STOP and end your turn. Do not proceed past the gate. The
     operator will review and send a decision that resumes this exact session.
"""


@dataclass
class RunOutcome:
    turn_result: str                       # "success" | "error" | "interrupted"
    session_id: Optional[str] = None
    cost_usd: float = 0.0
    summary: str = ""
    error: Optional[str] = None
    explicit_status: Optional[str] = None  # dry_run only — overrides checkpoint inspection
    stage: Optional[str] = None
    pending: Optional[dict] = None


# Live cli subprocesses by job_id, so cancel_job can terminate them and free the worker.
_running: dict[str, asyncio.subprocess.Process] = {}


def terminate(job_id: str) -> bool:
    """Kill the running agent subprocess (whole process group) for a job.

    Called by cancel_job so a cancel actually stops the agent and unblocks the
    worker, instead of leaving an orphan that head-of-line-blocks the queue.
    Returns True if a live process was signalled. (cli backend only — the sdk
    backend has no tracked subprocess and cancel stays best-effort there.)"""
    proc = _running.get(job_id)
    if proc is None or proc.returncode is not None:
        return False
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        return True
    except ProcessLookupError:
        return False
    except Exception:
        try:
            proc.kill()
            return True
        except Exception:
            return False


# ---- prompt construction ----

def build_start_prompt(record: "JobRecord") -> str:
    pipeline = record.pipeline or "(choose the best-fit pipeline from pipeline_defs/ and tell me which)"
    constraints = json.dumps(record.constraints, indent=2) if record.constraints else "(none)"
    budget = f"${record.budget_usd:.2f}" if record.budget_usd is not None else "(no explicit cap — be economical)"
    return f"""\
New OpenMontage video job.

job_id: {record.job_id}
project_id (use for projects/ and pipelines/ dirs): {record.job_id}
approval_mode: {record.approval_mode}
pipeline: {pipeline}
budget: {budget}
constraints:
{constraints}

BRIEF:
{record.brief}

Start by reading AGENT_GUIDE.md, then run preflight, select/confirm the pipeline,
and proceed per the approval_mode rules in your system instructions. Remember the
final deliverable is projects/{record.job_id}/renders/final.mp4.
"""


def build_resume_prompt(record: "JobRecord", decision: str, notes: Optional[str]) -> str:
    stage = (record.pending_checkpoint or {}).get("stage", "the current")
    notes_block = notes.strip() if notes else "(no additional notes)"
    guidance = {
        "approve": "The operator APPROVED. Proceed to the next stage.",
        "revise": "The operator requested REVISIONS. Revise the current artifact per the notes, "
                  "then present it again at the same gate (awaiting_human) for another review.",
    }.get(decision, "Proceed.")
    return f"""\
Operator response for job {record.job_id} at the {stage} checkpoint.

decision: {decision}
notes: {notes_block}

{guidance}
Continue the pipeline following the same approval_mode and output-path rules.
"""


# ---- public API ----

async def start(record: "JobRecord", settings: Settings) -> RunOutcome:
    return await _dispatch(record, build_start_prompt(record), settings, is_resume=False)


async def resume(record: "JobRecord", decision: str, notes: Optional[str], settings: Settings) -> RunOutcome:
    return await _dispatch(record, build_resume_prompt(record, decision, notes), settings, is_resume=True)


async def _dispatch(record: "JobRecord", prompt: str, settings: Settings, is_resume: bool) -> RunOutcome:
    backend = settings.agent_backend
    if backend == "dry_run":
        return await _dry_run(record, settings, is_resume)
    resume_session = record.session_id if is_resume else None
    if backend == "cli":
        return await _cli_run(record.job_id, prompt, settings, resume_session)
    if backend == "sdk":
        return await _sdk_run(prompt, settings, resume_session)
    return RunOutcome("error", error=f"unknown AGENT_BACKEND={backend!r} (use sdk|cli|dry_run)")


# ---- backends ----

async def _sdk_run(prompt: str, settings: Settings, resume_session: Optional[str]) -> RunOutcome:
    try:
        from claude_agent_sdk import query, ClaudeAgentOptions
    except Exception as exc:  # not installed
        return RunOutcome("error", error=f"claude-agent-sdk not installed: {exc}")

    kwargs: dict = dict(
        cwd=str(settings.repo_root),
        permission_mode=settings.agent_permission_mode,
        allowed_tools=["Bash", "Read", "Edit", "Write", "Glob", "Grep"],
        max_turns=settings.agent_max_turns,
        model=settings.agent_model,
    )
    if resume_session:
        kwargs["resume"] = resume_session

    # Prefer appending to the Claude Code preset so CLAUDE.md/AGENT_GUIDE behavior
    # is preserved; fall back to a plain system prompt on older SDKs.
    try:
        options = ClaudeAgentOptions(
            system_prompt={"type": "preset", "preset": "claude_code", "append": SYSTEM_APPEND},
            **kwargs,
        )
    except Exception:
        options = ClaudeAgentOptions(system_prompt=SYSTEM_APPEND, **kwargs)

    session_id: Optional[str] = None
    cost = 0.0
    subtype: Optional[str] = None
    result_text = ""
    try:
        async for msg in query(prompt=prompt, options=options):
            data = getattr(msg, "data", None)
            if isinstance(data, dict) and data.get("session_id") and not session_id:
                session_id = data["session_id"]
            sid = getattr(msg, "session_id", None)
            if sid and not session_id:
                session_id = sid
            if getattr(msg, "total_cost_usd", None) is not None or getattr(msg, "subtype", None) in _RESULT_SUBTYPES:
                subtype = getattr(msg, "subtype", subtype)
                c = getattr(msg, "total_cost_usd", None)
                if c is not None:
                    cost = float(c)
                r = getattr(msg, "result", None)
                if r:
                    result_text = str(r)
    except Exception as exc:
        return RunOutcome("error", session_id=session_id, error=f"agent SDK run failed: {exc}")

    turn = "success" if subtype in (None, "success") else "error"
    return RunOutcome(
        turn,
        session_id=session_id,
        cost_usd=cost,
        summary=result_text[:2000],
        error=None if turn == "success" else (result_text or subtype or "agent error"),
    )


def _parse_cli_json(out: bytes) -> Optional[dict]:
    text = out.decode("utf-8", "replace").strip()
    if not text:
        return None
    try:
        obj = json.loads(text)
        return obj if isinstance(obj, dict) else None
    except Exception:
        pass
    # Tolerate trailing/leading noise: try the last JSON object line.
    for line in reversed(text.splitlines()):
        line = line.strip()
        if line.startswith("{") and line.endswith("}"):
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    return obj
            except Exception:
                continue
    return None


async def _cli_run(job_id: str, prompt: str, settings: Settings, resume_session: Optional[str]) -> RunOutcome:
    claude = which("claude") or "claude"
    cmd = [claude, "-p", prompt, "--output-format", "json", "--model", settings.agent_model]
    if settings.agent_permission_mode in ("bypassPermissions", "bypass"):
        cmd.append("--dangerously-skip-permissions")
    else:
        cmd += ["--permission-mode", settings.agent_permission_mode]
    cmd += ["--append-system-prompt", SYSTEM_APPEND]
    if resume_session:
        cmd += ["--resume", resume_session]

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=str(settings.repo_root),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,  # own process group → cancel_job can killpg the whole tree
        )
    except Exception as exc:
        return RunOutcome("error", error=f"failed to launch claude CLI: {exc}")
    _running[job_id] = proc
    try:
        out, err = await proc.communicate()
    finally:
        _running.pop(job_id, None)

    data = _parse_cli_json(out)
    if data is None:
        detail = err.decode("utf-8", "replace")[:500] if err else out.decode("utf-8", "replace")[:500]
        return RunOutcome("error", error=f"claude CLI returned no parseable JSON (rc={proc.returncode}): {detail}")

    subtype = data.get("subtype")
    is_error = bool(data.get("is_error", False))
    session_id = data.get("session_id")
    cost = float(data.get("total_cost_usd") or 0.0)
    result_text = str(data.get("result") or "")
    turn = "success" if (subtype == "success" or (subtype is None and not is_error)) else "error"
    return RunOutcome(
        turn,
        session_id=session_id,
        cost_usd=cost,
        summary=result_text[:2000],
        error=None if turn == "success" else (result_text or subtype or "cli error"),
    )


async def _dry_run(record: "JobRecord", settings: Settings, is_resume: bool) -> RunOutcome:
    """No-LLM simulation. Interactive jobs pause once at an 'idea' gate, then
    complete on resume; autonomous jobs complete immediately. Writes a stub
    render so the artifact/download path is exercised end-to-end."""
    proj = settings.projects_dir / record.job_id
    (proj / "artifacts").mkdir(parents=True, exist_ok=True)

    if record.approval_mode == "interactive" and not is_resume:
        (proj / "artifacts" / "brief.json").write_text(
            json.dumps({"job_id": record.job_id, "brief": record.brief, "dry_run": True}, indent=2)
        )
        return RunOutcome(
            "success",
            session_id=f"dry-{record.job_id}",
            cost_usd=0.0,
            summary="[dry_run] reached idea checkpoint; awaiting operator approval",
            explicit_status="awaiting_human",
            stage="idea",
            pending={"stage": "idea", "summary": "dry-run brief ready for review", "artifact_keys": ["brief"]},
        )

    (proj / "renders").mkdir(parents=True, exist_ok=True)
    final = proj / "renders" / "final.mp4"
    if not final.exists():
        final.write_bytes(b"\x00\x00\x00\x18ftypmp42")  # minimal MP4 header stub
    (proj / "artifacts" / "render_report.json").write_text(
        json.dumps({"job_id": record.job_id, "output": "renders/final.mp4", "dry_run": True}, indent=2)
    )
    return RunOutcome(
        "success",
        session_id=record.session_id or f"dry-{record.job_id}",
        cost_usd=0.0,
        summary="[dry_run] produced stub final.mp4 + render_report",
        explicit_status="completed",
        stage="compose",
    )
