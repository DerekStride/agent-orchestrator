use anyhow::Result;

use crate::cli::PrimeArgs;

const MANUAL: &str = r#"# Agent orchestrator workflow

`agent-orchestrator finish ROOT_TASK_ID` supervises only the dependency closure rooted at `ROOT_TASK_ID`. The SQ queue is the desired-state contract; no second task manifest is used.

## Before running

1. Run `sq prime`, `agent-id prime`, and `agent-mail prime`.
2. Build the complete SQ dependency graph, including acceptance criteria and optional validation checks.
3. Start inside a Git repository and a live Herdr session. Install the OMP integration so Herdr can report lifecycle state.
4. Keep one canonical `.sift/issues.jsonl`; task worktrees receive a symlink to it.

## Reconcile a scoped plan

```bash
agent-orchestrator finish ROOT_TASK_ID --queue .sift/issues.jsonl
```

The explicit root is mandatory. Tasks outside its dependency closure are never claimed. The reconciler validates missing blockers, cycles, ownership, and plan drift before provisioning ready work.

Each worker receives a dedicated branch, worktree, Herdr workspace and pane, automatically registered Agent ID identity, and an AgentMail handoff. The handoff names the orchestrator and worker, task ID, canonical queue, dependencies, acceptance criteria, worktree, branch, and expected validation evidence.

## Completion contract

SQ status remains authoritative. A pane exit is not completion. A task closes only after its SQ state and a structured AgentMail report agree and the report contains a commit or artifact plus concrete evidence. Failed, blocked, lost, or ambiguous runs stop visibly; the reconciler does not steal ownership, retry workers, merge branches, or delete worktrees.

The runtime ledger records each task, run, agent/session, pane, worktree, branch, handoff/report message IDs, lease, and heartbeat. Re-run the same scoped command to reconcile retained handles safely.

## One-pass diagnosis

Use `--once` to perform one pass and print the current outcome without waiting:

```bash
agent-orchestrator finish ROOT_TASK_ID --once
```

Missing executables, invalid SQ graphs, unavailable Herdr sessions, plan drift, and branch convergence conflicts are top-level errors. Fix the stated condition; do not edit the ledger to suppress it.

## Exact command syntax

```bash
agent-orchestrator finish --help
agent-orchestrator prime
```
"#;

pub fn execute(_args: &PrimeArgs) -> Result<()> {
    print!("{MANUAL}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prime_contains_the_agent_workflow() {
        assert!(MANUAL.contains("finish ROOT_TASK_ID"));
        assert!(MANUAL.contains("SQ status remains authoritative"));
        assert!(MANUAL.contains("AgentMail handoff"));
        assert!(MANUAL.contains("Herdr workspace and pane"));
        assert!(MANUAL.contains("runtime ledger"));
    }
}
