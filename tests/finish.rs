#![cfg(unix)]

use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

use agent_orchestrator::sq::{Queue, TaskStatus};
use serde_json::{json, Value};

static NEXT_TEST_DIR: AtomicU64 = AtomicU64::new(0);

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let sequence = NEXT_TEST_DIR.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "agent-orchestrator-finish-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn finish_closed_root_is_complete_without_runtime_integrations() {
    let directory = TestDir::new();
    let repo = directory.path().join("repo");
    fs::create_dir(&repo).unwrap();
    let queue = directory.path().join("issues.jsonl");
    write_queue(&queue, "closed");
    let state = directory.path().join("state");
    let worktrees = directory.path().join("worktrees");

    let output = Command::new(env!("CARGO_BIN_EXE_agent-orchestrator"))
        .args(["finish", "root", "--queue"])
        .arg(&queue)
        .arg("--repo")
        .arg(&repo)
        .arg("--state-dir")
        .arg(&state)
        .arg("--worktree-root")
        .arg(&worktrees)
        .env_clear()
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["status"], "complete");
    assert_eq!(result["root_task_id"], "root");
    assert_eq!(result["runtimes"], json!([]));
    assert!(result["run_id"].is_null());
}

#[test]
fn finish_rejects_zero_poll_and_lease_intervals_as_operational_errors() {
    for option in ["--poll-seconds", "--lease-seconds"] {
        let output = Command::new(env!("CARGO_BIN_EXE_agent-orchestrator"))
            .args(["finish", "root", option, "0"])
            .env_clear()
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1));
        assert!(String::from_utf8_lossy(&output.stderr)
            .contains(&format!("{option} must be greater than zero")));
    }
}

#[test]
fn finish_refuses_unowned_in_progress_before_runtime_preflight() {
    let directory = TestDir::new();
    let repo = directory.path().join("repo");
    fs::create_dir(&repo).unwrap();
    let queue = directory.path().join("issues.jsonl");
    write_queue(&queue, "in_progress");
    let marker = directory.path().join("herdr-ran");
    let herdr = directory.path().join("fake-herdr");
    write_executable(
        &herdr,
        &format!("#!/bin/sh\ntouch {}\n", shell_quote(&marker)),
    );

    let output = Command::new(env!("CARGO_BIN_EXE_agent-orchestrator"))
        .args(["finish", "root", "--queue"])
        .arg(&queue)
        .arg("--repo")
        .arg(&repo)
        .arg("--state-dir")
        .arg(directory.path().join("state"))
        .arg("--worktree-root")
        .arg(directory.path().join("worktrees"))
        .env("AGENT_ORCHESTRATOR_HERDR", &herdr)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(!marker.exists());
    assert!(String::from_utf8_lossy(&output.stderr)
        .contains("in_progress without agent_orchestrator.run_id ownership"));
}

#[test]
fn finish_once_provisions_and_persists_a_complete_active_runtime() {
    let directory = TestDir::new();
    let repo = directory.path().join("repo");
    init_repo(&repo);
    let queue = directory.path().join("issues.jsonl");
    write_queue(&queue, "pending");
    let state = directory.path().join("state");
    let worktrees = directory.path().join("worktrees");
    fs::create_dir(&worktrees).unwrap();
    let worktree = worktrees.join("repo.root");

    let herdr = directory.path().join("fake-herdr");
    write_executable(
        &herdr,
        "#!/bin/sh\nset -eu\nif [ \"$3\" = status ]; then\n  printf '%s\\n' '{\"running\":true,\"compatible\":true}'\nelif [ \"$3\" = worktree ]; then\n  git -C \"$6\" worktree add -b \"$8\" \"${12}\" \"${10}\" >/dev/null 2>&1\n  printf '%s\\n' '{\"result\":{\"type\":\"worktree_created\",\"workspace\":{\"workspace_id\":\"w1\"},\"root_pane\":{\"pane_id\":\"w1:p1\"}}}'\nelif [ \"$3\" = agent ]; then\n  case \"$4\" in\n    start) kind=agent_started; status=idle ;;\n    prompt) kind=agent_prompted; status=idle ;;\n    get) kind=agent_info; status=working ;;\n  esac\n  printf '{\"result\":{\"type\":\"%s\",\"agent\":{\"name\":\"%s\",\"workspace_id\":\"w1\",\"pane_id\":\"w1:p1\",\"agent_status\":\"%s\"}}}\\n' \"$kind\" \"$5\" \"$status\"\nfi\n",
    );

    let agent_id = directory.path().join("fake-agent-id");
    let orchestrator = json!({
        "session_id": "orchestrator-session",
        "name": "Orchestrator Agent",
        "slug": "orchestrator-agent",
        "cwd": fs::canonicalize(&repo).unwrap(),
        "state": {"value": "working"},
        "extensions": {"omp": {"state": "working"}},
    });
    let worker = json!({
        "session_id": "worker-session",
        "name": "Worker Agent",
        "slug": "worker-agent",
        "cwd": worktree,
        "state": {"value": "working"},
        "extensions": {"omp": {"state": "working"}},
    });
    write_executable(
        &agent_id,
        &format!(
            "#!/bin/sh\nset -eu\ncase \"$1\" in\n  current) printf '%s\\n' '{}' ;;\n  discover) printf '%s\\n' '[{}]' ;;\nesac\n",
            orchestrator, worker
        ),
    );

    let agent_mail = directory.path().join("fake-agent-mail");
    write_executable(
        &agent_mail,
        "#!/bin/sh\nset -eu\ncommand=$1\nshift\nif [ \"$command\" = send ]; then\n  to= from= subject=\n  while [ \"$#\" -gt 0 ]; do\n    case \"$1\" in\n      --to) to=$2; shift 2 ;;\n      --from) from=$2; shift 2 ;;\n      --subject) subject=$2; shift 2 ;;\n      --reply-to|--body) shift 2 ;;\n      --json) shift ;;\n      *) shift ;;\n    esac\n  done\n  printf '{\"id\":\"handoff-1\",\"recipient\":\"%s\",\"sender\":\"%s\",\"subject\":\"%s\",\"state\":\"delivered\"}\\n' \"$to\" \"$from\" \"$subject\"\nelse\n  printf '%s\\n' '[]'\nfi\n",
    );

    let run_finish = || {
        Command::new(env!("CARGO_BIN_EXE_agent-orchestrator"))
            .args(["finish", "root", "--once", "--queue"])
            .arg(&queue)
            .arg("--repo")
            .arg(&repo)
            .arg("--state-dir")
            .arg(&state)
            .arg("--worktree-root")
            .arg(&worktrees)
            .env("AGENT_ORCHESTRATOR_HERDR", &herdr)
            .env("AGENT_ORCHESTRATOR_AGENT_ID", &agent_id)
            .env("AGENT_ORCHESTRATOR_AGENT_MAIL", &agent_mail)
            .env("HERDR_SESSION", "finish-test")
            .output()
            .unwrap()
    };
    let output = run_finish();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["status"], "active");
    assert_eq!(result["runtimes"][0]["task_id"], "root");
    assert_eq!(result["runtimes"][0]["stage"], "active");
    assert_eq!(
        result["runtimes"][0]["branch"],
        "agent-orchestrator/root/root"
    );

    let run_id = result["run_id"].as_str().unwrap();
    let claimed = Queue::read(&queue).unwrap().scope("root").unwrap();
    let task = claimed.task("root").unwrap();
    assert_eq!(task.stored_status(), TaskStatus::InProgress);
    assert_eq!(task.owner_run_id(), Some(run_id));
    assert_eq!(
        fs::read_link(worktree.join(".sift/issues.jsonl")).unwrap(),
        fs::canonicalize(&queue).unwrap()
    );

    let mut ledger: Value =
        serde_json::from_slice(&fs::read(state.join("root.json")).unwrap()).unwrap();
    let runtime = &ledger["runtimes"]["root"];
    for field in [
        "workspace_id",
        "pane_id",
        "worker_name",
        "worker_identity",
        "handoff_receipt",
        "lease_expires_at_unix",
        "heartbeat_at_unix",
    ] {
        assert!(!runtime[field].is_null(), "ledger must retain {field}");
    }

    let reconciled = run_finish();
    assert!(
        reconciled.status.success(),
        "{}",
        String::from_utf8_lossy(&reconciled.stderr)
    );
    let reconciled: Value = serde_json::from_slice(&reconciled.stdout).unwrap();
    assert_eq!(reconciled["status"], "active");
    assert_eq!(reconciled["run_id"], result["run_id"]);
    assert_eq!(reconciled["runtimes"][0]["stage"], "active");

    ledger["runtimes"]["root"]["stage"] = json!("claimed");
    fs::write(
        state.join("root.json"),
        serde_json::to_vec_pretty(&ledger).unwrap(),
    )
    .unwrap();
    let interrupted = run_finish();
    assert!(!interrupted.status.success());
    assert!(String::from_utf8_lossy(&interrupted.stderr)
        .contains("interrupted provisioning at stage `Claimed`; refusing a silent retry"));
}

fn write_queue(path: &Path, status: &str) {
    fs::write(
        path,
        format!(
            "{}\n",
            json!({
                "id": "root",
                "title": "Implement root",
                "description": "Root task",
                "status": status,
                "sources": [],
                "metadata": {
                    "acceptance_criteria": ["Root works"],
                    "checks": ["cargo test"],
                },
                "created_at": "2026-09-03T00:00:00Z",
                "updated_at": "2026-09-03T00:00:00Z",
                "blocked_by": [],
            })
        ),
    )
    .unwrap();
}

fn init_repo(repo: &Path) {
    fs::create_dir(repo).unwrap();
    git(repo, &["init", "-b", "main"]);
    git(repo, &["config", "user.email", "test@example.com"]);
    git(repo, &["config", "user.name", "Test"]);
    fs::write(repo.join("README"), "base\n").unwrap();
    git(repo, &["add", "README"]);
    git(repo, &["commit", "-m", "base"]);
}

fn git(repo: &Path, args: &[&str]) {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn write_executable(path: &Path, contents: &str) {
    fs::write(path, contents).unwrap();
    let mut permissions = fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).unwrap();
}

fn shell_quote(path: &Path) -> String {
    format!("'{}'", path.to_string_lossy().replace('\'', "'\\''"))
}
