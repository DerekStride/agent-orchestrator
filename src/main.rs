mod cli;
mod finish;
mod herdr;
mod identity;
mod mail;
mod prime;
mod runtime;
mod sq;
mod worktree;

use anyhow::Result;
use clap::Parser;

fn main() {
    if let Err(error) = run() {
        eprintln!("agent-orchestrator: {error:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = cli::Cli::parse();

    match cli.command {
        cli::Commands::Finish(args) => finish::execute(&args),
        cli::Commands::Prime(args) => prime::execute(&args),
    }
}
