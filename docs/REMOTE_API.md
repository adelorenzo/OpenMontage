# OpenMontage Remote API — MCP Control Plane + HTTP Data Plane

**Status:** design + reference implementation (`server/`)
**Audience:** operators wiring a remote AI agent (e.g. "Hermes") to drive OpenMontage on a GPU box.

---

## 1. Problem & constraints

A remote AI agent ("Hermes") runs on a machine **without** the GPU power this box has. It wants to use
OpenMontage to produce videos here and retrieve the results. Hermes is **MCP-native** (it can consume an
MCP server directly).

Two facts about OpenMontage shape the whole design:

1. **OpenMontage has no Python orchestrator.** The production logic — pipeline selection, stage sequencing,
   review, checkpoints, cost governance — lives in markdown skills + YAML manifests that *an agent reads and
   performs* (see `AGENT_GUIDE.md`, `PROJECT_CONTEXT.md`). The Python `tools/` are dumb capability units
   (`tool.execute(dict) -> ToolResult`). There is nothing to "call" that produces a video; a video is the
   product of an **agent loop**.

2. Therefore "let Hermes use OpenMontage" means one of two things:
   - **Expose tools** and let Hermes *be* the OpenMontage agent (read all skills, drive the state machine
     remotely, tool-by-tool). Chatty, ships the instruction layer off-box, loses checkpoint/cost governance.
   - **Delegate jobs**: Hermes sends intent + approvals; a *second agent loop runs on the GPU box* and
     performs the pipeline locally, returning artifacts. Keeps intelligence, GPU, and governance here.

We chose **full-job delegation**. The consequence worth stating plainly:

> Delegating the full job means **running a headless Claude agent on the GPU box** — it does what an
> interactive Claude Code session does, but triggered by an API job instead of a human chat. The MCP/HTTP
> layer is a thin shell; **the headless agent runner is the substance.**

### Transport decision

| Constraint | Implication |
|---|---|
| Renders are **long-running** (GPU gen + Remotion/HyperFrames take minutes) | async submit → poll/respond, never a synchronous call |
| Artifacts are **large binaries** (tens–hundreds of MB MP4) | must move over HTTP with range support; JSON-RPC can't carry them |
| **Cross-machine** with auth/TLS | bearer token + TLS via reverse proxy |
| Consumer is an **MCP-native agent** | MCP gives zero-glue discovery + job control |

So it is **not** "MCP *vs* FastAPI." Remote MCP requires the streamable-HTTP transport, and we need HTTP for
the bytes regardless. The answer is **MCP mounted on an ASGI app**:

> **MCP for the doorbell, HTTP for the delivery truck.**

---

## 2. Topology

```
  HERMES BOX (no GPU)                     GPU BOX (this machine)
 ┌──────────────────┐                   ┌───────────────────────────────────────┐
 │ Hermes agent      │── MCP /mcp ─────▶ │  uvicorn → Starlette ASGI app          │
 │ (MCP client)      │   (bearer, HTTPS) │   ├─ Mount("/mcp")  FastMCP control     │
 │                   │                   │   │     plane: 6 tools                  │
 │                   │◀── /artifacts ────│   ├─ /artifacts/{job}/{path} (range)    │
 │                   │   (bearer, range) │   └─ /healthz                           │
 └──────────────────┘                   │                                         │
                                         │  AuthMiddleware (bearer on /mcp+/artifacts)
                                         │  JobStore ──▶ async queue ──▶ Worker     │
                                         │   (job.json   (GPU concurrency = 1)      │
                                         │    on disk)        │                     │
                                         │                    ▼                     │
                                         │              AgentRunner                 │
                                         │            (Claude Agent SDK             │
                                         │             query/resume, cwd=repo)      │
                                         │                    │                     │
                                         │     projects/<job_id>/{artifacts,assets, │
                                         │       renders}  +  job.json  +  checkpoints
                                         └───────────────────────────────────────┘
```

The job record reuses what OpenMontage already has: a project workspace
(`projects/<job_id>/{artifacts,assets,renders}`) plus stage checkpoints
(`schemas/checkpoints/checkpoint.schema.json`) **is** a job record with status and outputs. We add one small
`projects/<job_id>/job.json` for queue/session state.

---

## 3. Job lifecycle

```
 submit ─▶ queued ─▶ running ─┬─▶ completed
                              ├─▶ failed
                              ├─▶ canceled
                              └─▶ awaiting_human ──(respond_to_checkpoint)──▶ running ─┐
                                        ▲                                              │
                                        └──────────────────────────────────────────────┘
```

1. **submit** — Hermes calls `submit_video_job`. Server creates `projects/<job_id>/`, writes `job.json`
   (`status=queued`), enqueues, returns `{job_id, status}`.
2. **running** — the worker (one GPU job at a time) starts an agent session: `cwd` = repo root, the default
   Claude Code system prompt **appended** with a remote-job preamble that (a) points the agent at
   `AGENT_GUIDE.md`, (b) hands it the brief + `approval_mode`, (c) tells it to write all outputs under
   `projects/<job_id>/`. The runner captures the SDK `session_id` into `job.json`.
3. **approval gates** — `AGENT_GUIDE.md` mandates human approval at creative stages (`idea`, `script`,
   `scene_plan`). With no human in the room, **Hermes is the approver**:
   - `approval_mode="autonomous"` → the preamble tells the agent to self-approve gates and run to completion.
     Fast; gives up the human governance the project is built around.
   - `approval_mode="interactive"` (default) → the agent runs to the first gate, presents the canonical
     artifact, and ends its turn. The runner detects turn-end + a checkpoint written `awaiting_human`, sets
     the job to `awaiting_human`, and records a `pending_checkpoint` (stage + artifact pointer).
4. **respond** — Hermes polls `get_job_status`, sees `awaiting_human` + the stage, downloads the artifact via
   `get_artifacts`, then calls `respond_to_checkpoint(job_id, decision, notes)`. The server **resumes the same
   agent session** (`resume=session_id`) feeding the decision as the next user turn. Loop 3–4 until done.
5. **completed** — agent writes `renders/final.mp4` + `render_report`; status `completed`. Hermes calls
   `get_artifacts` and downloads over `/artifacts`.

On process restart, jobs left `running` are marked `interrupted` (the agent subprocess died with the
process). v1 does not auto-resume interrupted jobs; Hermes can resubmit. This is a known durability limit
(§8).

---

## 4. MCP control plane (the 7 tools)

Mounted at `/mcp` via `FastMCP(streamable_http_path="/")`. All are thin wrappers over `JobStore` and the tool
registry — they hold no creative logic.

| Tool | Input | Returns | Notes |
|---|---|---|---|
| `list_capabilities` | — | `provider_menu_summary()` + `composition_runtimes` | read-only; what's configured on this box |
| `submit_video_job` | `brief:str`, `pipeline?:str`, `approval_mode?:"interactive"\|"autonomous"`, `constraints?:obj`, `budget_usd?:num` | `{job_id, status}` | creates workspace, enqueues |
| `get_job_status` | `job_id:str` | `{status, stage, awaiting_human, pending_checkpoint?, cost_usd, error?, artifacts[]}` | poll this |
| `respond_to_checkpoint` | `job_id:str`, `decision:"approve"\|"revise"\|"abort"`, `notes?:str` | `{job_id, status}` | resumes the agent session |
| `get_artifacts` | `job_id:str` | `{artifacts:[{path,kind,bytes,url}]}` | each `url` is a **signed, header-free** download link (time-limited) on the HTTP data plane |
| `cancel_job` | `job_id:str` | `{job_id, status}` | cancels queued/awaiting; **terminates the running agent** (cli backend) so the worker frees and the queue keeps moving |
| `list_jobs` | `status?:str`, `limit?:int` | `{count, jobs[]}` | enumerate/recover jobs (newest first), optionally filtered by status |

`list_capabilities` is backed directly by `registry.provider_menu_summary()` — the same human-ready rollup the
preflight uses — so Hermes sees exactly what a local operator would, with **zero extra glue**.

`get_artifacts` returns absolute URLs of the form `{PUBLIC_BASE_URL}/artifacts/{job_id}/{relpath}`. Bytes
never traverse the MCP/JSON-RPC channel.

---

## 5. HTTP data plane

`GET /artifacts/{job_id}/{path:path}`

- Streams files from `projects/<job_id>/` via Starlette `FileResponse` (HTTP range requests supported
  out of the box → resumable / seekable downloads of large MP4s).
- **Path-traversal guard:** the resolved real path must stay within the resolved `projects/<job_id>/`
  directory, else `404`.
- Auth: a **bearer header OR a signed URL**. `get_artifacts` returns time-limited HMAC-signed links
  (`?exp=&sig=`, signed with the API token, TTL `OPENMONTAGE_ARTIFACT_URL_TTL`), so a remote client
  such as mcp-remote — which only authenticates MCP calls, not raw file GETs — can download with **no
  Authorization header**. The bearer header still works for direct `curl`/tooling.

`GET /healthz` → `{status, jobs:{...counts}, runtimes:{...}}`, **no auth** (liveness probe).

---

## 6. Auth & transport security

- Single bearer token in `OPENMONTAGE_API_TOKEN`. `AuthMiddleware` requires
  `Authorization: Bearer <token>` on `/mcp` and `/artifacts`; `/healthz` is open. Comparison is
  constant-time (`hmac.compare_digest`).
- The app speaks plain HTTP; **terminate TLS at a reverse proxy** (Caddy/nginx/Traefik) or run uvicorn with
  `--ssl-keyfile/--ssl-certfile`. Never expose it tokenless on a public interface.
- **Threat note — this is remote code execution by design.** A `submit_video_job` brief is a prompt that
  drives an autonomous, tool-using agent (Bash included) on the GPU box. Treat job intake as untrusted:
  require the token, keep the box network-restricted to Hermes, and consider running the worker under a
  constrained user / container. The agent's `allowed_tools` and `permission_mode` are the blast-radius
  controls (§7).

---

## 7. Agent runner

`server/agent_runner.py` exposes a backend-agnostic interface:

```python
async def start(job, prompt) -> RunOutcome       # new session for a job
async def resume(job, prompt) -> RunOutcome      # continue job.session_id
# RunOutcome = {status: completed|awaiting_human|failed, session_id, cost_usd, summary}
```

### Backends (`AGENT_BACKEND`)

| Backend | How | Auth | When |
|---|---|---|---|
| `sdk` (default) | `claude_agent_sdk.query(prompt, ClaudeAgentOptions(cwd=repo, allowed_tools=[...], system_prompt=<append>, permission_mode="acceptEdits", model=…, resume=session_id))` | `ANTHROPIC_API_KEY` (API billing — the SDK does **not** support subscription login) | production |
| `cli` | subprocess `claude -p <prompt> --output-format stream-json [--resume <id>]` | reuses the box's existing `claude` login (may be a subscription) | when you can't/won't set an API key; documented, not the default |
| `dry_run` | no LLM; simulates stage progression and writes a stub `render_report` + placeholder render | none | tests / wiring verification |

- **Session capture:** the SDK emits `SystemMessage(subtype="init")` with `data["session_id"]`; we persist it
  to `job.json`. Resume with `ClaudeAgentOptions(resume=session_id, ...)`.
- **Turn-end / pause detection:** the run iterator ends on a `ResultMessage`. We then read the job's latest
  checkpoint: if a stage checkpoint is `awaiting_human`, the job becomes `awaiting_human`; if `render_report`
  exists / status is success, `completed`; otherwise `failed`.
- **Tool surface:** the agent gets the standard file/shell tools (`Bash, Read, Edit, Write, Glob, Grep`) so it
  can run the OpenMontage tools exactly as an interactive session does. `permission_mode` defaults to
  `acceptEdits` (autonomous file ops); tighten per deployment.
- **Cost:** `ResultMessage.total_cost_usd` is the orchestration (LLM) cost; per-provider generation cost is
  tracked inside the pipeline by `tools/cost_tracker.py`. Both land in `job.json`.

### Why a second agent, not a Python pipeline?

Because the pipeline *is* an agent (constraint #1). Re-implementing orchestration in Python would fork the
project's intelligence away from the skills. The runner deliberately reuses `AGENT_GUIDE.md` as the contract,
so the remote path and the interactive path stay identical.

---

## 8. Concurrency, durability, limits (v1)

- **GPU serialization:** the worker honors `MAX_CONCURRENT_JOBS` (default **1**). One heavy job at a time;
  the rest wait in the queue. `cost_tracker` governs budget inside each run.
- **Durability:** `job.json` on disk is the source of truth; the in-memory queue is rebuilt from disk on
  startup. The async queue itself is in-process — a crash loses *queued-but-not-started* ordering (jobs are
  rescanned and re-queued) and marks *running* jobs `interrupted`. For stronger guarantees, back the queue
  with Redis/RQ or a DB (future).
- **Single tenant / single box.** No multi-node fan-out, no auth beyond one shared token, no per-user quotas.
- **Interrupted jobs are not auto-resumed** (the SDK session may still exist on disk under
  `~/.claude/projects/`, but v1 does not reattach automatically).

---

## 9. Configuration (env)

| Var | Default | Meaning |
|---|---|---|
| `OPENMONTAGE_API_TOKEN` | — (**required**) | bearer token for `/mcp` + `/artifacts` |
| `OPENMONTAGE_HOST` | `0.0.0.0` | bind host |
| `OPENMONTAGE_PORT` | `8787` | bind port |
| `PUBLIC_BASE_URL` | `http://localhost:8787` | base used to build artifact URLs (set to the public HTTPS URL) |
| `AGENT_BACKEND` | `sdk` | `sdk` \| `cli` \| `dry_run` |
| `AGENT_MODEL` | `claude-opus-4-7` | model id for the runner |
| `MAX_CONCURRENT_JOBS` | `1` | GPU serialization |
| `ANTHROPIC_API_KEY` | — | required when `AGENT_BACKEND=sdk` |
| `OPENMONTAGE_REPO_ROOT` | repo dir | working directory handed to the agent |
| `OPENMONTAGE_ALLOWED_HOSTS` | _(unset)_ | comma-sep `Host` allow-list for MCP DNS-rebinding protection. Unset = protection **off** (correct behind a reverse proxy / Tailscale, where the bearer token is the gate). If set, a proxied `Host` not on the list is rejected with `421 Invalid Host header`. |
| `OPENMONTAGE_ARTIFACT_URL_TTL` | `86400` | seconds a signed `get_artifacts` download URL stays valid |
| `OPENMONTAGE_JOB_INACTIVITY_TIMEOUT` | `1800` | watchdog kills + fails a running job with no filesystem progress for this long (0 = off) |
| `OPENMONTAGE_JOB_MAX_RUNTIME` | `0` | absolute per-job runtime cap in seconds (0 = off) |

---

## 10. Running it

```bash
pip install -r requirements.txt -r requirements-server.txt   # adds claude-agent-sdk, uvicorn, mcp
export OPENMONTAGE_API_TOKEN=$(openssl rand -hex 32)
export ANTHROPIC_API_KEY=sk-ant-...        # for AGENT_BACKEND=sdk
make serve                                  # or: uvicorn server.app:app --host 0.0.0.0 --port 8787
```

Hermes connects its MCP client to `https://<gpu-box>/mcp` with header
`Authorization: Bearer $OPENMONTAGE_API_TOKEN`, calls `list_capabilities` to confirm reach, then
`submit_video_job`.

### Smoke test without an API key

```bash
AGENT_BACKEND=dry_run OPENMONTAGE_API_TOKEN=test make serve
# submit a job over MCP; it walks queued→running→completed and writes a stub render_report.
```

---

## 11. File map

| File | Role |
|---|---|
| `server/config.py` | env-driven settings |
| `server/auth.py` | bearer-token middleware + constant-time check |
| `server/jobs.py` | `JobStore`, job records (`job.json`), async queue + worker, status machine |
| `server/agent_runner.py` | `sdk` / `cli` / `dry_run` backends; session capture + resume |
| `server/mcp_server.py` | `FastMCP` with the 6 control-plane tools |
| `server/app.py` | Starlette app: mount `/mcp`, `/artifacts`, `/healthz`, auth middleware, lifespan |
| `requirements-server.txt` | server-only deps |

---

## 12. Explicitly out of scope (v1)

- The fine-grained "expose every tool over MCP" alternative (Hermes orchestrates). Possible later by surfacing
  `registry` tools as MCP tools, but it forfeits on-box governance.
- Multi-node / horizontal scale, multi-tenant auth, per-user quotas, billing.
- Webhooks/push to Hermes (poll-only in v1; a `job.updated` webhook is a clean future add).
- Auto-resume of interrupted in-flight jobs.
