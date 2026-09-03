# agent-orchestrator

Finish a scoped SQ task tree with supervised coding agents.

SQ is the desired state. Observed state is SQ status, Herdr panes, Agent ID identities, AgentMail
reports, Git worktrees, and branches. `finish` reconciles the two for one explicitly named root task
and stops visibly whenever it cannot prove progress.

## Install

```bash
cargo install --path .
agent-orchestrator prime
```

## Local assumptions

| Requirement | Used for | Failure mode |
|---|---|---|
| `herdr` in `PATH`, live session | worktree workspaces, panes, OMP lifecycle | clear top-level error before any task is claimed |
| `agent-id` with OMP registration | orchestrator and worker identity | error when the identity is missing or not OMP-registered |
| `agent-mail` | handoffs and structured worker reports | error when send/scan/read fails |
| `sq` | claiming tasks with namespaced ownership metadata | error when SQ refuses the edit |
| `git` repository at `--repo` | worktree and branch provisioning | error when the path is not a repository |

Override any executable for testing with `AGENT_ORCHESTRATOR_HERDR`, `AGENT_ORCHESTRATOR_AGENT_ID`,
`AGENT_ORCHESTRATOR_AGENT_MAIL`, `AGENT_ORCHESTRATOR_SQ`, and `AGENT_ORCHESTRATOR_GIT`.
Set `HERDR_SESSION` to target a named Herdr session; it defaults to `default`.

## Usage

```bash
agent-orchestrator finish ROOT_TASK_ID \
  --queue .sift/issues.jsonl \
  --repo . \
  --state-dir .agent-orchestrator \
  --once
```

Only the dependency closure of `ROOT_TASK_ID` is touched; unrelated queue work is never claimed.
Each ready task receives a branch `agent-orchestrator/<root>/<task>`, a Git worktree beside the
repository, a symlink to the canonical queue, a Herdr workspace and OMP pane, and an AgentMail
handoff naming the orchestrator, worker, task, queue, worktree, branch, dependencies, acceptance
criteria, and validation expectations. A dependent task is based on its completed blocker branch;
ambiguous convergence is a visible failure.

## Completion contract

A task closes only when SQ reports `closed` **and** a matching AgentMail report arrives with a commit
or artifact plus non-empty evidence. Pane exit, idle, or `done` is not completion. Blocked, failed,
lost, drifted, and already-owned states stop the run with a non-zero exit.

The runtime ledger in `--state-dir` records task → run → agent/session → pane → worktree → branch →
message IDs → lease and heartbeat, and is locked so two orchestrators cannot reconcile one root.

## Checks

```bash
cargo fmt --all -- --check
cargo test --all --locked
```
