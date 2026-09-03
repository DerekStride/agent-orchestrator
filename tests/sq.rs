use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

use agent_orchestrator::sq::{Error, Queue, SqClient, TaskStatus};
use serde_json::{json, Value};

static NEXT_TEST_DIR: AtomicU64 = AtomicU64::new(0);

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let sequence = NEXT_TEST_DIR.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "agent-orchestrator-sq-{}-{sequence}",
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

fn task(id: &str, status: &str, blocked_by: &[&str], metadata: Value) -> Value {
    json!({
        "id": id,
        "title": format!("{id} task"),
        "description": format!("Implement {id}"),
        "status": status,
        "sources": [],
        "metadata": metadata,
        "created_at": "2026-09-03T00:00:00Z",
        "updated_at": "2026-09-03T00:00:00Z",
        "blocked_by": blocked_by,
    })
}

fn write_queue(directory: &TestDir, tasks: &[Value]) -> PathBuf {
    let path = directory.path().join("issues.jsonl");
    let mut contents = tasks
        .iter()
        .map(Value::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    contents.push('\n');
    fs::write(&path, contents).expect("queue fixture should be written");
    path
}

#[test]
fn sq_scope_contains_only_transitive_blockers_and_models_readiness() {
    let directory = TestDir::new();
    let queue = write_queue(
        &directory,
        &[
            task("leaf", "closed", &[], json!({})),
            task("ready", "pending", &["leaf"], json!({})),
            task("running", "in_progress", &[], json!({})),
            task("held", "blocked", &[], json!({})),
            task("root", "pending", &["ready", "running", "held"], json!({})),
            task("unrelated", "pending", &[], json!({})),
        ],
    );

    let plan = Queue::read(queue)
        .expect("queue should load")
        .scope("root")
        .expect("root scope should validate");

    assert_eq!(
        plan.task_ids().collect::<Vec<_>>(),
        ["held", "leaf", "ready", "root", "running"]
    );
    assert!(plan.task("unrelated").is_none());
    assert_eq!(plan.status("leaf").unwrap(), TaskStatus::Closed);
    assert_eq!(plan.status("ready").unwrap(), TaskStatus::Pending);
    assert_eq!(plan.status("running").unwrap(), TaskStatus::InProgress);
    assert_eq!(plan.status("held").unwrap(), TaskStatus::Blocked);
    assert_eq!(plan.status("root").unwrap(), TaskStatus::Blocked);
    assert_eq!(plan.ready_task_ids().collect::<Vec<_>>(), ["ready"]);
}

#[test]
fn sq_graph_errors_identify_missing_and_duplicate_tasks_and_cycles() {
    let directory = TestDir::new();
    let queue = write_queue(&directory, &[task("root", "pending", &[], json!({}))]);
    let error = Queue::read(&queue).unwrap().scope("absent").unwrap_err();
    assert!(matches!(error, Error::MissingRoot { .. }));
    assert_eq!(error.to_string(), "SQ root task `absent` is missing");

    write_queue(
        &directory,
        &[task("root", "pending", &["absent"], json!({}))],
    );
    let error = Queue::read(&queue).unwrap().scope("root").unwrap_err();
    assert!(matches!(error, Error::MissingBlocker { .. }));
    assert_eq!(
        error.to_string(),
        "SQ task `root` declares missing blocker `absent`"
    );

    write_queue(
        &directory,
        &[
            task("same", "pending", &[], json!({})),
            task("same", "closed", &[], json!({})),
        ],
    );
    let error = Queue::read(&queue).unwrap_err();
    assert!(matches!(error, Error::DuplicateTask { .. }));
    assert_eq!(
        error.to_string(),
        "SQ queue contains duplicate task ID `same`"
    );

    write_queue(
        &directory,
        &[
            task("root", "pending", &["middle"], json!({})),
            task("middle", "pending", &["root"], json!({})),
        ],
    );
    let error = Queue::read(&queue).unwrap().scope("root").unwrap_err();
    assert!(matches!(error, Error::DependencyCycle { .. }));
    assert_eq!(
        error.to_string(),
        "SQ dependency cycle: root -> middle -> root"
    );
}

#[test]
fn sq_snapshot_ignores_runtime_ownership_but_detects_plan_drift() {
    let directory = TestDir::new();
    let baseline = task("root", "pending", &[], json!({"role": "implementation"}));
    let queue = write_queue(&directory, std::slice::from_ref(&baseline));
    let client = SqClient::with_executable(&queue, "unused-sq");
    let plan = client.plan("root").expect("baseline plan should load");

    let mut claimed = baseline.clone();
    claimed["status"] = json!("in_progress");
    claimed["updated_at"] = json!("2026-09-03T01:00:00Z");
    claimed["metadata"]["agent_orchestrator"] = json!({"run_id": "run-a"});
    write_queue(&directory, std::slice::from_ref(&claimed));
    client
        .validated_plan("root", "run-a", Some(plan.snapshot()))
        .expect("status, timestamps, and run ownership are runtime state");

    let mut drifted = claimed.clone();
    drifted["title"] = json!("changed plan");
    write_queue(&directory, std::slice::from_ref(&drifted));
    let error = client
        .validated_plan("root", "run-a", Some(plan.snapshot()))
        .unwrap_err();
    assert!(matches!(error, Error::PlanDrift { .. }));
    assert_eq!(error.to_string(), "SQ plan drift for root task `root`");

    claimed["metadata"]["agent_orchestrator"] = json!({"run_id": "run-b"});
    write_queue(&directory, std::slice::from_ref(&claimed));
    let error = client
        .validated_plan("root", "run-a", Some(plan.snapshot()))
        .unwrap_err();
    assert!(matches!(error, Error::ForeignOwnership { .. }));
    assert!(error
        .to_string()
        .contains("foreign agent_orchestrator.run_id `run-b`"));
}

#[test]
fn sq_closed_foreign_ownership_is_historical() {
    let directory = TestDir::new();
    let queue = write_queue(
        &directory,
        &[
            task(
                "legacy",
                "closed",
                &[],
                json!({"agent_orchestrator": {"run_id": "older-run"}}),
            ),
            task("root", "pending", &["legacy"], json!({})),
        ],
    );

    let plan = SqClient::with_executable(&queue, "unused-sq")
        .validated_plan("root", "current-run", None)
        .expect("closed ownership is provenance, not an active claim");

    assert_eq!(plan.status("legacy").unwrap(), TaskStatus::Closed);
    assert_eq!(plan.ready_task_ids().collect::<Vec<_>>(), ["root"]);
}

#[test]
fn sq_rejects_unowned_in_progress_tasks() {
    let directory = TestDir::new();
    let queue = write_queue(&directory, &[task("root", "in_progress", &[], json!({}))]);

    let error = SqClient::with_executable(&queue, "unused-sq")
        .validated_plan("root", "current-run", None)
        .unwrap_err();

    assert!(matches!(error, Error::UnownedInProgress { .. }));
    assert_eq!(
        error.to_string(),
        "SQ task `root` is in_progress without agent_orchestrator.run_id ownership"
    );
}

#[cfg(unix)]
#[test]
fn sq_claim_uses_configured_executable_and_verifies_persisted_state() {
    use std::os::unix::fs::PermissionsExt;

    let directory = TestDir::new();
    let baseline = task("root", "pending", &[], json!({"role": "implementation"}));
    let queue = write_queue(&directory, std::slice::from_ref(&baseline));
    let arguments = directory.path().join("arguments");
    let script = directory.path().join("fake-sq");

    let mut claimed = baseline;
    claimed["status"] = json!("in_progress");
    claimed["updated_at"] = json!("2026-09-03T01:00:00Z");
    claimed["metadata"]["agent_orchestrator"] = json!({"run_id": "run-a"});
    write_fake_sq(&script, &arguments, &queue, &claimed);
    let mut permissions = fs::metadata(&script).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&script, permissions).unwrap();

    let client = SqClient::with_executable(&queue, &script);
    let plan = client
        .validated_plan("root", "run-a", None)
        .expect("unowned root should be claimable");
    let claimed_plan = client
        .claim(&plan, "root", "run-a")
        .expect("claim should be persisted and verified");

    assert_eq!(
        claimed_plan.task("root").unwrap().stored_status(),
        TaskStatus::InProgress
    );
    assert_eq!(
        claimed_plan.task("root").unwrap().owner_run_id(),
        Some("run-a")
    );
    assert_eq!(
        fs::read_to_string(arguments)
            .unwrap()
            .lines()
            .collect::<Vec<_>>(),
        [
            "edit",
            "root",
            "--queue",
            queue.to_str().unwrap(),
            "--set-status",
            "in_progress",
            "--merge-metadata",
            "{\"agent_orchestrator\":{\"run_id\":\"run-a\"}}",
            "--json",
        ]
    );
}

#[cfg(unix)]
#[test]
fn sq_claim_rejects_success_without_both_persisted_values() {
    use std::os::unix::fs::PermissionsExt;

    let directory = TestDir::new();
    let baseline = task("root", "pending", &[], json!({"role": "implementation"}));
    let queue = write_queue(&directory, std::slice::from_ref(&baseline));
    let script = directory.path().join("fake-sq");
    let arguments = directory.path().join("arguments");

    let mut incomplete = baseline;
    incomplete["status"] = json!("in_progress");
    write_fake_sq(&script, &arguments, &queue, &incomplete);
    let mut permissions = fs::metadata(&script).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&script, permissions).unwrap();

    let client = SqClient::with_executable(&queue, &script);
    let plan = client.plan("root").unwrap();
    let error = client.claim(&plan, "root", "run-a").unwrap_err();
    assert!(matches!(error, Error::ClaimNotPersisted { .. }));
    assert!(error.to_string().contains("run <missing>"));
}

#[test]
fn sq_finish_reports_graph_error_before_runtime_preflight() {
    let directory = TestDir::new();
    let queue = write_queue(
        &directory,
        &[task("root", "pending", &["absent"], json!({}))],
    );

    let output = Command::new(env!("CARGO_BIN_EXE_agent-orchestrator"))
        .args(["finish", "root", "--queue"])
        .arg(queue)
        .output()
        .expect("agent-orchestrator should run");

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).expect("stderr should be UTF-8");
    assert!(stderr.contains("declares missing blocker `absent`"));
    assert!(!stderr.contains("provided by the orchestration runtime"));
}

#[cfg(unix)]
fn write_fake_sq(script: &Path, arguments: &Path, queue: &Path, replacement: &Value) {
    let body = format!(
        "#!/bin/sh\nprintf '%s\\n' \"$@\" > {}\ncat > {} <<'AGENT_ORCHESTRATOR_QUEUE'\n{}\nAGENT_ORCHESTRATOR_QUEUE\nprintf '%s\\n' '{{}}'\n",
        shell_quote(arguments),
        shell_quote(queue),
        replacement
    );
    fs::write(script, body).expect("fake SQ should be written");
}

#[cfg(unix)]
fn shell_quote(path: &Path) -> String {
    format!("'{}'", path.to_string_lossy().replace('\'', "'\\''"))
}
