use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "agent-orchestrator",
    version,
    about = "Coordinate scoped coding-agent work through SQ and AgentMail",
    disable_help_subcommand = true
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Print the agent-facing orchestration manual.
    Prime,

    /// Drive one root SQ task to a reported terminal state.
    Finish(FinishArgs),
}

#[derive(Debug, Args)]
pub struct FinishArgs {
    /// Root SQ task whose scoped orchestration run should finish.
    #[arg(value_name = "ROOT_TASK_ID")]
    pub root_task_id: String,

    /// Canonical SQ JSONL queue shared by the orchestrator and workers.
    #[arg(long, env = "SQ_QUEUE_PATH", value_name = "PATH")]
    pub queue: Option<PathBuf>,
    /// OMP role or model passed to every worker at launch.
    #[arg(long, value_name = "MODEL")]
    pub model: Option<String>,

    /// Git repository containing the root task's work.
    #[arg(long, value_name = "PATH")]
    pub repo: Option<PathBuf>,

    /// Directory for durable orchestration run state.
    #[arg(long, value_name = "PATH")]
    pub state_dir: Option<PathBuf>,

    /// Parent directory in which worker worktrees are created.
    #[arg(long, value_name = "PATH")]
    pub worktree_root: Option<PathBuf>,

    /// Diagnose and dispatch once instead of polling until completion.
    #[arg(long)]
    pub once: bool,

    /// Delay between orchestration polling passes.
    #[arg(long, value_name = "SECONDS", default_value_t = 5)]
    pub poll_seconds: u64,

    /// Seconds without a successful worker observation before its lease expires.
    #[arg(long, value_name = "SECONDS", default_value_t = 900)]
    pub lease_seconds: u64,
}
