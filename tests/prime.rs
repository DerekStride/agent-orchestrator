use std::process::{Command, Output};

fn run_without_integrations(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_agent-orchestrator"))
        .args(args)
        .env_clear()
        .output()
        .expect("agent-orchestrator binary should run")
}

#[test]
fn prime_and_help_are_static_and_finish_requires_root_task_id() {
    let prime = run_without_integrations(&["prime"]);
    assert!(prime.status.success());
    let manual = String::from_utf8(prime.stdout).expect("prime output should be UTF-8");
    for expected in [
        "Scoped SQ workflow",
        "External integrations",
        "Completion contract",
        "One-pass diagnosis",
    ] {
        assert!(manual.contains(expected), "prime should explain {expected}");
    }

    let help = run_without_integrations(&["--help"]);
    assert!(help.status.success());

    let finish_help = run_without_integrations(&["finish", "--help"]);
    assert!(finish_help.status.success());
    let finish_help = String::from_utf8(finish_help.stdout).expect("finish help should be UTF-8");
    for expected in [
        "ROOT_TASK_ID",
        "--queue",
        "--repo",
        "--state-dir",
        "--worktree-root",
        "--once",
        "--poll-seconds",
        "--lease-seconds",
    ] {
        assert!(
            finish_help.contains(expected),
            "finish help should expose {expected}"
        );
    }

    let missing_root = run_without_integrations(&["finish"]);
    assert!(!missing_root.status.success());
    let error = String::from_utf8(missing_root.stderr).expect("clap error should be UTF-8");
    assert!(error.contains("ROOT_TASK_ID"));
}
