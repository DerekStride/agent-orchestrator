pub const MANUAL: &str = r#"# agent-orchestrator

`agent-orchestrator finish ROOT_TASK_ID` coordinates one bounded SQ task graph. It does not take ownership of unrelated queue items.

## Scoped SQ workflow

1. Treat the canonical SQ JSONL queue as the source of truth. Select `ROOT_TASK_ID` and only the work explicitly assigned beneath that orchestration run.
2. Record ownership, dependencies, and task state in SQ before dispatch. Each worker updates only its assigned task; ownership or dependency changes return to the orchestrator for a decision.
3. Give every worker a dedicated worktree, branch, starting reference, queue path, acceptance criteria, and validation commands. Never merge branches or delete worktrees as an implicit side effect of finishing a task.
4. Keep orchestration state under `--state-dir`; place worktrees under `--worktree-root`. `--queue` and `--repo` select the canonical queue and repository explicitly.
5. Poll at `--poll-seconds`, bound a worker attempt with `--lease-seconds`, or use `--once` for one dispatch/diagnosis pass.

## External integrations

- SQ stores durable task state and dependency readiness.
- Git stores branches, commits, and worktrees containing the deliverable.
- Herdr starts and observes worker sessions.
- Agent ID provides stable orchestrator and worker identities.
- AgentMail carries handoffs, evidence, decisions, blockers, and terminal reports.

`prime`, top-level help, and `finish --help` are static. They do not invoke or discover Herdr, `agent-id`, `agent-mail`, `sq`, or `git`.

## Completion contract

A worker updates its assigned SQ task first, then sends the orchestrator the requested terminal report. `completed` requires a commit or durable artifact plus non-empty command/result validation evidence. `blocked` and `failed` name the concrete blocker or failure. Session or pane exit alone is not completion. Do not silently retry failed work, take a sibling task, merge a branch, or delete a worktree.

The terminal AgentMail body is JSON only and identifies the task, orchestration run, status, commit or artifact, evidence, and summary. The orchestrator accepts completion only when that report agrees with SQ and the referenced deliverable exists.

## One-pass diagnosis

When a lease expires, a worker exits, or a report is missing, diagnose once before acting: inspect the scoped SQ task, AgentMail report/receipt, referenced commit or artifact, worktree, Agent ID lifecycle, and Herdr session. Reconcile those signals into `completed`, `blocked`, `failed`, or a still-active lease. Never infer completion from process exit and never start a silent replacement attempt; report the evidence and make the next dispatch decision explicit.
"#;
