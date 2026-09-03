use std::process::ExitCode;

use agent_orchestrator::{
    cli::{Cli, Command, FinishArgs},
    finish::{execute, FinishOptions},
};
use clap::Parser;

fn main() -> ExitCode {
    match Cli::parse().command {
        Command::Prime => {
            print!("{}", agent_orchestrator::prime::MANUAL);
            ExitCode::SUCCESS
        }
        Command::Finish(args) => finish(args),
    }
}

fn finish(args: FinishArgs) -> ExitCode {
    let repo = match args.repo {
        Some(repo) => repo,
        None => match std::env::current_dir() {
            Ok(current_dir) => current_dir,
            Err(error) => {
                eprintln!("error: cannot determine current directory: {error}");
                return ExitCode::from(2);
            }
        },
    };
    let options = FinishOptions {
        root_task_id: args.root_task_id,
        queue: args.queue,
        repo,
        state_dir: args.state_dir,
        worktree_root: args.worktree_root,
        once: args.once,
        poll_seconds: args.poll_seconds,
        lease_seconds: args.lease_seconds,
    };
    match execute(options) {
        Ok(output) => match serde_json::to_string(&output) {
            Ok(json) => {
                println!("{json}");
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("error: cannot serialize finish result: {error}");
                ExitCode::from(2)
            }
        },
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::from(2)
        }
    }
}
