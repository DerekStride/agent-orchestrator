use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "agent-orchestrator",
    version,
    about = "Finish an SQ task tree with supervised coding agents",
    long_about = "agent-orchestrator reconciles an explicitly scoped SQ dependency tree with Git worktrees, Herdr panes, Agent ID identities, and AgentMail evidence."
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Reconcile one SQ task tree until it completes or needs intervention
    Finish(FinishArgs),
    /// Output the agent-facing orchestration workflow
    Prime(PrimeArgs),
}

#[derive(Debug, Args)]
pub struct FinishArgs {
    /// Root task whose dependency closure defines the only allowed scope
    #[arg(value_name = "ROOT_TASK_ID")]
    pub root_task_id: String,

    /// Canonical SQ JSONL queue
    #[arg(
        long,
        value_name = "PATH",
        env = "SQ_QUEUE_PATH",
        default_value = ".sift/issues.jsonl"
    )]
    pub queue: PathBuf,

    /// Git repository containing the work to perform
    #[arg(long, value_name = "PATH", default_value = ".")]
    pub repo: PathBuf,

    /// Directory for the runtime ledger
    #[arg(long, value_name = "PATH", default_value = ".agent-orchestrator")]
    pub state_dir: PathBuf,

    /// Parent directory for task worktrees; defaults beside the repository
    #[arg(long, value_name = "PATH")]
    pub worktree_root: Option<PathBuf>,

    /// Perform one reconciliation pass instead of waiting for workers
    #[arg(long)]
    pub once: bool,

    /// Seconds between reconciliation passes
    #[arg(long, value_name = "SECONDS", default_value_t = 5)]
    pub poll_seconds: u64,

    /// Seconds without a worker heartbeat before it is considered lost
    #[arg(long, value_name = "SECONDS", default_value_t = 900)]
    pub lease_seconds: u64,
}

#[derive(Debug, Args)]
#[command(about = "Output the agent-facing orchestration workflow")]
pub struct PrimeArgs {}
