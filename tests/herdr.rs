#![cfg(unix)]

use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

use agent_orchestrator::herdr::{
    reconcile_worker, worker_name, AgentStatus, HerdrClient, WorkerDisposition, WorkerStop,
    WorktreeSpec,
};
use serde_json::json;

static NEXT_TEST_DIR: AtomicU64 = AtomicU64::new(0);

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let sequence = NEXT_TEST_DIR.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "agent-orchestrator-herdr-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("test directory should be created");
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
fn herdr_finish_validates_graph_before_preflight() {
    let directory = TestDir::new();
    let queue = directory.path().join("issues.jsonl");
    let marker = directory.path().join("herdr-ran");
    let herdr = directory.path().join("fake-herdr");
    fs::write(
        &queue,
        format!(
            "{}\n",
            json!({
                "id": "root",
                "title": "root",
                "description": "root",
                "status": "pending",
                "sources": [],
                "metadata": {},
                "blocked_by": ["absent"]
            })
        ),
    )
    .unwrap();
    write_executable(
        &herdr,
        &format!("#!/bin/sh\ntouch {}\n", shell_quote(&marker)),
    );

    let output = Command::new(env!("CARGO_BIN_EXE_agent-orchestrator"))
        .args(["finish", "root", "--queue"])
        .arg(&queue)
        .env("AGENT_ORCHESTRATOR_HERDR", &herdr)
        .env("HERDR_SESSION", "orchestrator-test")
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(!marker.exists(), "Herdr must not run before SQ validates");
    assert!(String::from_utf8(output.stderr)
        .unwrap()
        .contains("declares missing blocker `absent`"));
}

#[test]
fn herdr_finish_reports_missing_executable_and_unavailable_session() {
    let directory = TestDir::new();
    let queue = write_valid_queue(&directory);
    let missing = directory.path().join("missing-herdr");

    let output = Command::new(env!("CARGO_BIN_EXE_agent-orchestrator"))
        .args(["finish", "root", "--queue"])
        .arg(&queue)
        .env("AGENT_ORCHESTRATOR_HERDR", &missing)
        .env("HERDR_SESSION", "orchestrator-test")
        .output()
        .unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(!output.status.success());
    assert!(stderr.contains("cannot start Herdr executable"));
    assert!(stderr.contains("orchestrator-test"));

    let fake = directory.path().join("fake-herdr");
    write_executable(
        &fake,
        "#!/bin/sh\nprintf '%s\\n' '{\"status\":\"not_running\",\"running\":false,\"compatible\":null}'\n",
    );
    let output = Command::new(env!("CARGO_BIN_EXE_agent-orchestrator"))
        .args(["finish", "root", "--queue"])
        .arg(&queue)
        .env("AGENT_ORCHESTRATOR_HERDR", &fake)
        .env("HERDR_SESSION", "orchestrator-test")
        .output()
        .unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(!output.status.success());
    assert!(stderr.contains("Herdr session `orchestrator-test` is unavailable"));
    assert!(stderr.contains("server is not running"));
}

#[test]
fn herdr_creates_worktree_and_retains_returned_handles() {
    let directory = TestDir::new();
    let arguments = directory.path().join("arguments");
    let fake = directory.path().join("fake-herdr");
    write_executable(
        &fake,
        &format!(
            "#!/bin/sh\nprintf '%s\\n' \"$@\" > {}\nprintf '%s\\n' '{}'\n",
            shell_quote(&arguments),
            json!({
                "id": "request-1",
                "result": {
                    "type": "worktree_created",
                    "workspace": {"workspace_id": "w7"},
                    "root_pane": {"pane_id": "w7:p9"}
                }
            })
        ),
    );

    let repo = directory.path().join("repo");
    let path = directory.path().join("repo.task");
    let client = HerdrClient::with_executable(&fake, "orchestrator-test");
    let workspace = client
        .create_worktree(WorktreeSpec {
            repo: &repo,
            branch: "agent-orchestrator/root/task",
            base: "base-branch",
            path: &path,
            task_label: "task: Implement parser",
        })
        .unwrap();

    assert_eq!(workspace.workspace_id, "w7");
    assert_eq!(workspace.pane_id, "w7:p9");
    assert_eq!(
        read_arguments(&arguments),
        vec![
            "--session".to_owned(),
            "orchestrator-test".to_owned(),
            "worktree".to_owned(),
            "create".to_owned(),
            "--cwd".to_owned(),
            repo.to_string_lossy().into_owned(),
            "--branch".to_owned(),
            "agent-orchestrator/root/task".to_owned(),
            "--base".to_owned(),
            "base-branch".to_owned(),
            "--path".to_owned(),
            path.to_string_lossy().into_owned(),
            "--label".to_owned(),
            "task: Implement parser".to_owned(),
            "--no-focus".to_owned(),
        ]
    );
}

#[test]
fn herdr_starts_named_omp_and_prompts_only_with_handoff_receipt() {
    let directory = TestDir::new();
    let arguments = directory.path().join("arguments");
    let fake = directory.path().join("fake-herdr");
    write_executable(
        &fake,
        &format!(
            "#!/bin/sh\nprintf '%s\\n' \"$@\" > {}\ncase \"$4\" in\n  start) result=agent_started; status=idle ;;\n  prompt) result=agent_prompted; status=working ;;\n  get) result=agent_info; status=unknown ;;\nesac\nprintf '{{\"id\":\"request\",\"result\":{{\"type\":\"%s\",\"agent\":{{\"name\":\"%s\",\"workspace_id\":\"w7\",\"pane_id\":\"w7:p9\",\"agent_status\":\"%s\"}}}}}}\\n' \"$result\" \"$5\" \"$status\"\n",
            shell_quote(&arguments)
        ),
    );

    let client = HerdrClient::with_executable(&fake, "orchestrator-test");
    let workspace = agent_orchestrator::herdr::WorkerWorkspace {
        workspace_id: "w7".to_owned(),
        pane_id: "w7:p9".to_owned(),
    };
    let worker = client
        .start_omp(&workspace, "run-123", "Task/With Spaces")
        .unwrap();
    assert_eq!(worker.name, worker_name("run-123", "Task/With Spaces"));
    assert!(worker.name.len() <= 32);
    assert_valid_herdr_name(&worker.name);
    let start_arguments = read_arguments(&arguments);
    assert_eq!(
        &start_arguments[3..],
        ["start", &worker.name, "--kind", "omp", "--pane", "w7:p9"]
    );

    fs::remove_file(&arguments).unwrap();
    let error = client
        .prompt_after_handoff(&worker, "  ", "Execute the handoff")
        .unwrap_err();
    assert!(error
        .to_string()
        .contains("durable AgentMail handoff receipt"));
    assert!(
        !arguments.exists(),
        "a missing receipt must prevent prompting"
    );

    client
        .prompt_after_handoff(&worker, "01MESSAGE", "Execute the handoff")
        .unwrap();
    assert_eq!(client.status(&worker).unwrap(), AgentStatus::Unknown);
}

#[test]
fn herdr_worker_names_and_lifecycle_are_deterministic_and_non_terminal_by_default() {
    let name = worker_name("run-123", "123/Very Long Task With Punctuation!!!");
    assert_eq!(
        name,
        worker_name("run-123", "123/Very Long Task With Punctuation!!!")
    );
    assert_ne!(
        name,
        worker_name("run-456", "123/Very Long Task With Punctuation!!!")
    );
    assert!(name.len() <= 32);
    assert_valid_herdr_name(&name);

    assert_eq!(
        reconcile_worker(AgentStatus::Working, false),
        WorkerDisposition::Active
    );
    assert_eq!(
        reconcile_worker(AgentStatus::Unknown, false),
        WorkerDisposition::Active
    );
    assert_eq!(
        reconcile_worker(AgentStatus::Idle, false),
        WorkerDisposition::Stop(WorkerStop::SettledWithoutReport(AgentStatus::Idle))
    );
    assert_eq!(
        reconcile_worker(AgentStatus::Done, false),
        WorkerDisposition::Stop(WorkerStop::SettledWithoutReport(AgentStatus::Done))
    );
    assert_eq!(
        reconcile_worker(AgentStatus::Blocked, false),
        WorkerDisposition::Stop(WorkerStop::Blocked)
    );
    assert_eq!(
        reconcile_worker(AgentStatus::Done, true),
        WorkerDisposition::ReportPresent
    );
}

fn write_valid_queue(directory: &TestDir) -> PathBuf {
    let path = directory.path().join("issues.jsonl");
    fs::write(
        &path,
        format!(
            "{}\n",
            json!({
                "id": "root",
                "title": "root",
                "description": "root",
                "status": "pending",
                "sources": [],
                "metadata": {},
                "blocked_by": []
            })
        ),
    )
    .unwrap();
    path
}

fn write_executable(path: &Path, contents: &str) {
    fs::write(path, contents).unwrap();
    let mut permissions = fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).unwrap();
}

fn read_arguments(path: &Path) -> Vec<String> {
    fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect()
}

fn assert_valid_herdr_name(name: &str) {
    let mut bytes = name.bytes();
    assert!(matches!(bytes.next(), Some(b'a'..=b'z')));
    assert!(bytes.all(|byte| matches!(byte, b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_')));
}

fn shell_quote(path: &Path) -> String {
    format!("'{}'", path.to_string_lossy().replace('\'', "'\\''"))
}
