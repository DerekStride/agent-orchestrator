# agent-orchestrator

`agent-orchestrator` drives one dependency-scoped SQ task graph through dedicated Git worktrees and OMP worker sessions. SQ remains the task source of truth; a durable local ledger correlates Git, Herdr, Agent ID, and AgentMail state without merging branches, deleting worktrees, or silently retrying failed work.

## Install

Requirements: a Rust toolchain plus the local `sq`, `git`, `herdr`, `agent-id`, and `agent-mail` CLIs available on `PATH`.

```sh
cargo install --path . --locked
agent-orchestrator prime
```

`prime`, top-level help, and `finish --help` are static and run without any integration installed.

## Integrations and overrides

| Integration | Purpose | Executable/session override |
|---|---|---|
| SQ | Reads the canonical JSONL queue; persists task claims and run ownership | `AGENT_ORCHESTRATOR_SQ`; queue precedence is `--queue`, `SQ_QUEUE_PATH`, nearest ancestor `.sift/issues.jsonl`, then `<repo>/.sift/issues.jsonl` |
| Git | Resolves bases, checks ancestry, and validates reported commits | `AGENT_ORCHESTRATOR_GIT` |
| Herdr | Checks the server, creates worktrees, starts OMP workers, prompts them, and reports lifecycle state | `AGENT_ORCHESTRATOR_HERDR`; session via `HERDR_SESSION` (default `default`) |
| Agent ID | Identifies the orchestrator and finds exactly one live OMP identity whose canonical `cwd` is the worker worktree | `AGENT_ORCHESTRATOR_AGENT_ID` |
| AgentMail | Delivers handoffs and receives correlated terminal reports | `AGENT_ORCHESTRATOR_AGENT_MAIL` |

An unavailable executable or Herdr session is a terminal command error. The orchestrator does not substitute an integration or infer state from a different source. The SQ graph is validated before Herdr or Agent ID runs, and a closed root completes without runtime integrations.

## Run one scoped graph

```sh
agent-orchestrator finish ROOT_TASK_ID \
  --queue /absolute/path/issues.jsonl \
  --repo /absolute/path/repository \
  --state-dir /absolute/path/orchestrator-state \
  --worktree-root /absolute/path/worktrees
```

`ROOT_TASK_ID` is required. The run contains only that task and its transitive `blocked_by` dependencies; unrelated queue items are never claimed. The orchestrator rejects missing tasks, missing blockers, duplicate IDs, dependency cycles, plan drift, foreign run ownership, and unowned `in_progress` work before dispatching more work.

`--once` performs one reconciliation/dispatch pass and returns `active` or `complete`. Without it, the command polls every `--poll-seconds` (default `5`). `--lease-seconds` is an inactivity lease (default `900`): each successful Agent ID and Herdr observation renews it; transient observation failures remain recorded and become terminal only after the lease expires.

## Worktrees, branches, and queue access

Each claimed task receives:

- branch `agent-orchestrator/<root-task-id>/<task-id>`;
- worktree `<worktree-root>/<repository-name>.<task-id>`;
- an absolute `.sift/issues.jsonl` symlink to the canonical queue;
- an AgentMail handoff containing task/run IDs, canonical queue, worktree, branch, dependencies, acceptance criteria, and validation commands.

The default worktree root is the repository parent. A task without blockers starts at the repository's current `HEAD`. Existing branches from closed blockers are used as stack bases; closed blockers without an orchestration branch are treated as already integrated into `HEAD`. With multiple existing blocker branches, exactly one must descend from all others; otherwise the orchestrator refuses an ambiguous convergence. This stacking makes the final root branch include its dependency commits without an orchestrator-side merge.

Existing branches, worktrees, or untracked queue paths are never replaced. If Git checked out the canonical tracked `.sift/issues.jsonl` path into a worker, the orchestrator marks that path `skip-worktree` before replacing the copy with the canonical absolute symlink. Re-running queue-link setup is idempotent. `finish` does not merge worker branches or remove worker worktrees.

## Durable ledger and reconciliation

The default state directory is `<repo-parent>/.agent-orchestrator/<repository-name>`; `--state-dir` overrides it. `<root-task-id>.json` records the ULID run ID, created/updated Unix timestamps, immutable plan snapshot, canonical paths, orchestrator identity, and every task's branch, worktree, Herdr handles, worker identity, handoff receipt, rolling lease, heartbeat, last observation error, report message ID, and complete report body. `<root-task-id>.lock` prevents concurrent orchestration of the same root.

A retained ledger must match the requested root, queue, repository, worktree root, orchestrator identity, and current SQ plan. Runtime-only SQ changes—status, timestamps, and matching run ownership—do not count as plan drift. Interrupted provisioning, reset/reopened tasks, replaced identities, missing worktrees, wrong branches, blocked workers, settled workers without reports, and expired observation leases stop the run. Temporary Agent ID or Herdr lookup failures leave the runtime active and are retried by the normal poll loop until the lease expires. Terminal blocked/failed reports remain terminal on later invocations; they are never retried implicitly.

## Strict completion contract

Before closing its SQ task, a worker must produce and validate its reported commit or durable artifact. It then updates only its assigned SQ task and sends a JSON-only AgentMail report to the handoff's orchestrator with subject:

```text
agent-orchestrator report <task-id> <run-id>
```

The body schema is:

```json
{"task_id":"<task-id>","run_id":"<run-id>","status":"completed|blocked|failed","commit":"<commit-or-null>","artifact":"<artifact-or-null>","evidence":["<command>: <result>"],"summary":"<blocker-or-failure-or-null>"}
```

Completion requires all of the following:

1. `status` is `completed` and the SQ task is already `closed`.
2. `commit` names a commit reachable from the assigned branch, or `artifact` names an existing absolute path or worktree-relative path.
3. `evidence` contains at least one non-empty validation result.
4. The message sender, subject, task ID, and run ID match the retained worker and handoff.

A dependent task is not dispatched merely because its blocker is closed in SQ. If the blocker belongs to the current run, its report and deliverable must first be validated and recorded as completed in the ledger. This prevents a worker that closes SQ before committing from creating a downstream branch at the blocker's old tip.

`blocked` and `failed` require a non-empty `summary` and must not disagree with a closed SQ task. Pane/session exit alone is never completion. A malformed, duplicate, mismatched, or unverifiable report stops reconciliation and remains unread until corrected.
