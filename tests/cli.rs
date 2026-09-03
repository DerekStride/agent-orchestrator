use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;

use assert_cmd::Command;
use predicates::prelude::*;
use tempfile::TempDir;

struct Fixture {
    root: TempDir,
    repo: PathBuf,
    queue: PathBuf,
    state: PathBuf,
    ledger: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let repo = root.path().join("project");
        fs::create_dir_all(repo.join(".sift")).unwrap();
        git(&repo, ["init", "-q", "."]);
        git(&repo, ["config", "user.email", "fixture@example.com"]);
        git(&repo, ["config", "user.name", "Fixture"]);
        fs::write(repo.join("README.md"), "seed\n").unwrap();
        git(&repo, ["add", "README.md"]);
        git(&repo, ["commit", "-qm", "seed"]);

        let state = root.path().join("state");
        fs::create_dir_all(&state).unwrap();

        Self {
            queue: repo.join(".sift/issues.jsonl"),
            ledger: root.path().join("ledger"),
            repo,
            state,
            root,
        }
    }

    fn add_task(&self, id: &str, title: &str, blocked_by: &[&str], status: &str) {
        let blockers = serde_json::to_string(blocked_by).unwrap();
        let line = format!(
            r#"{{"id":"{id}","title":"{title}","description":"Do {title}","status":"{status}","sources":[],"metadata":{{}},"blocked_by":{blockers},"created_at":"2026-01-01T00:00:00.000Z","updated_at":"2026-01-01T00:00:00.000Z"}}"#
        );
        let mut queue = fs::read_to_string(&self.queue).unwrap_or_default();
        queue.push_str(&line);
        queue.push('\n');
        fs::write(&self.queue, queue).unwrap();
    }

    fn set_status(&self, id: &str, status: &str) {
        let queue = fs::read_to_string(&self.queue)
            .unwrap()
            .lines()
            .map(|line| {
                let mut task: serde_json::Value = serde_json::from_str(line).unwrap();
                if task["id"] == id {
                    task["status"] = serde_json::Value::String(status.to_owned());
                }
                serde_json::to_string(&task).unwrap()
            })
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(&self.queue, format!("{queue}\n")).unwrap();
    }

    fn register_worker(&self, slug: &str, worktree: &str) {
        let path = self.root.path().join(worktree);
        let workers = format!(
            r#"[{{"version":1,"session_id":"{slug}-session","name":"Worker {slug}","slug":"{slug}","cwd":"{}","extensions":{{"omp":{{"data":{{}}}}}}}}]"#,
            path.display()
        );
        fs::write(self.state.join("workers.json"), workers).unwrap();
    }

    fn set_agent_state(&self, state: &str) {
        fs::write(self.state.join("agent_state"), state).unwrap();
    }

    fn finish(&self, root_task_id: &str) -> Command {
        let mut command = Command::cargo_bin("agent-orchestrator").unwrap();
        command
            .env("FIXTURE_LOG", self.root.path().join("calls.log"))
            .env("FIXTURE_STATE", &self.state)
            .env("AGENT_ORCHESTRATOR_HERDR", fixture_bin("herdr"))
            .env("AGENT_ORCHESTRATOR_AGENT_ID", fixture_bin("agent-id"))
            .env("AGENT_ORCHESTRATOR_AGENT_MAIL", fixture_bin("agent-mail"))
            .env("AGENT_ORCHESTRATOR_SQ", fixture_bin("sq"))
            .args([
                "finish",
                root_task_id,
                "--queue",
                self.queue.to_str().unwrap(),
                "--repo",
                self.repo.to_str().unwrap(),
                "--state-dir",
                self.ledger.to_str().unwrap(),
                "--once",
            ]);
        command
    }

    fn branches(&self) -> String {
        let output = StdCommand::new("git")
            .arg("-C")
            .arg(&self.repo)
            .args(["branch", "--format=%(refname:short)"])
            .output()
            .unwrap();
        String::from_utf8(output.stdout).unwrap()
    }
}

fn git<const N: usize>(repo: &Path, args: [&str; N]) {
    let status = StdCommand::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .status()
        .unwrap();
    assert!(status.success(), "git {args:?} failed");
}

fn fixture_bin(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

#[test]
fn prime_documents_the_scoped_workflow() {
    Command::cargo_bin("agent-orchestrator")
        .unwrap()
        .arg("prime")
        .assert()
        .success()
        .stdout(predicate::str::contains("finish ROOT_TASK_ID"))
        .stdout(predicate::str::contains("SQ status remains authoritative"));
}

#[test]
fn finish_requires_an_explicit_root_task() {
    Command::cargo_bin("agent-orchestrator")
        .unwrap()
        .arg("finish")
        .assert()
        .failure()
        .stderr(predicate::str::contains("ROOT_TASK_ID"));
}

#[test]
fn closed_root_finishes_without_provisioning_workers() {
    let fixture = Fixture::new();
    fixture.add_task("root", "Root", &[], "closed");

    fixture
        .finish("root")
        .assert()
        .success()
        .stdout(predicate::str::contains(r#""status": "complete""#));

    assert!(!fixture.root.path().join("calls.log").exists());
    assert!(fixture
        .branches()
        .trim()
        .lines()
        .all(|branch| branch != "agent-orchestrator/root/root"));
}

#[test]
fn missing_herdr_is_a_clear_error_after_queue_validation() {
    let fixture = Fixture::new();
    fixture.add_task("root", "Root", &[], "pending");

    fixture
        .finish("root")
        .env(
            "AGENT_ORCHESTRATOR_HERDR",
            fixture.root.path().join("absent-herdr"),
        )
        .assert()
        .failure()
        .stderr(predicate::str::contains("Herdr is required for `finish`"));

    fixture
        .finish("missing-root")
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "SQ root task missing-root does not exist",
        ));
}

#[test]
fn dependency_errors_stop_before_any_worker_is_provisioned() {
    let cycle = Fixture::new();
    cycle.add_task("root", "Root", &["child"], "pending");
    cycle.add_task("child", "Child", &["root"], "pending");
    cycle
        .finish("root")
        .assert()
        .failure()
        .stderr(predicate::str::contains("SQ dependency cycle"));

    let missing = Fixture::new();
    missing.add_task("root", "Root", &["absent"], "pending");
    missing
        .finish("root")
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "references missing blocker absent",
        ));
}

#[test]
fn in_progress_task_without_a_runtime_entry_is_never_stolen() {
    let fixture = Fixture::new();
    fixture.add_task("root", "Root", &["child"], "pending");
    fixture.add_task("child", "Child", &[], "in_progress");

    fixture
        .finish("root")
        .assert()
        .failure()
        .stderr(predicate::str::contains("refusing to steal it"));
}

#[test]
fn ready_task_is_provisioned_with_worktree_branch_and_handoff() {
    let fixture = Fixture::new();
    fixture.add_task("root", "Root", &["child"], "pending");
    fixture.add_task("child", "Child", &[], "pending");
    fixture.register_worker("worker", "project.child");

    fixture
        .finish("root")
        .env("HERDR_SESSION", "integration")
        .assert()
        .success()
        .stdout(predicate::str::contains(r#""status": "active""#))
        .stdout(predicate::str::contains(r#""child""#));

    assert!(fs::read_to_string(fixture.root.path().join("calls.log"))
        .unwrap()
        .contains("herdr --session integration worktree create"));

    assert!(fixture.branches().contains("agent-orchestrator/root/child"));
    assert_eq!(
        fs::read_link(fixture.root.path().join("project.child/.sift/issues.jsonl")).unwrap(),
        fixture.queue.canonicalize().unwrap()
    );

    let handoff = fs::read_to_string(fixture.state.join("handoff-body")).unwrap();
    assert!(handoff.contains("Task ID: child"));
    assert!(handoff.contains("Branch: agent-orchestrator/root/child"));
    assert!(handoff.contains("Worker: Worker worker (worker)"));
    assert!(handoff.contains("agent-orchestrator report child"));

    let ledger: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(fixture.ledger.join("root.json")).unwrap())
            .unwrap();
    let run = &ledger["task_runs"]["child"];
    assert_eq!(run["state"], "working");
    assert_eq!(run["herdr"]["pane_id"], "w1:p1");
    assert_eq!(run["handoff_message_id"], "MSG-HANDOFF");
    assert!(run["lease_expires_at"].as_str().is_some());
}

#[test]
fn interrupted_provisioning_run_is_never_retried_silently() {
    let fixture = Fixture::new();
    fixture.add_task("root", "Root", &["child"], "pending");
    fixture.add_task("child", "Child", &[], "pending");
    fixture.register_worker("worker", "project.child");
    fixture.finish("root").assert().success();

    // Simulate a finish process killed between worktree creation and the handoff.
    let path = fixture.ledger.join("root.json");
    let mut ledger: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
    ledger["task_runs"]["child"]["state"] = serde_json::json!("provisioning");
    fs::write(&path, serde_json::to_string_pretty(&ledger).unwrap()).unwrap();

    fixture
        .finish("root")
        .assert()
        .failure()
        .stderr(predicate::str::contains("refusing to retry it silently"));

    let ledger: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(&path).unwrap()).unwrap();
    assert_eq!(ledger["task_runs"]["child"]["state"], "failed");
    assert!(ledger["task_runs"]["child"]["detail"]
        .as_str()
        .unwrap()
        .contains("stopped during provisioning"));
}

#[test]
fn settled_pane_without_a_report_is_not_completion() {
    let fixture = Fixture::new();
    fixture.add_task("root", "Root", &["child"], "pending");
    fixture.add_task("child", "Child", &[], "pending");
    fixture.register_worker("worker", "project.child");
    fixture.finish("root").assert().success();

    fixture.set_agent_state("idle");
    fixture
        .finish("root")
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "settled without a structured AgentMail report",
        ));
}

#[test]
fn herdr_lookup_failure_stops_without_silent_retry() {
    let fixture = Fixture::new();
    fixture.add_task("root", "Root", &["child"], "pending");
    fixture.add_task("child", "Child", &[], "pending");
    fixture.register_worker("worker", "project.child");
    fixture.finish("root").assert().success();

    fixture.set_agent_state("mystery");
    fixture
        .finish("root")
        .assert()
        .failure()
        .stderr(predicate::str::contains("unsupported agent state mystery"));
}

#[test]
fn closed_blocker_waits_for_validated_report_before_unlocking() {
    let fixture = Fixture::new();
    fixture.add_task("root", "Root", &["child"], "pending");
    fixture.add_task("child", "Child", &[], "pending");
    fixture.register_worker("worker", "project.child");
    fixture.finish("root").assert().success();

    fixture.set_status("child", "closed");
    fixture
        .finish("root")
        .assert()
        .success()
        .stdout(predicate::str::contains(r#""launched": []"#))
        .stdout(predicate::str::contains(r#""waiting":"#))
        .stdout(predicate::str::contains(r#""child""#));

    let ledger: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(fixture.ledger.join("root.json")).unwrap())
            .unwrap();
    assert!(!ledger["task_runs"]
        .as_object()
        .unwrap()
        .contains_key("root"));
}

#[test]
fn validated_report_closes_the_run_and_unlocks_the_dependent_task() {
    let fixture = Fixture::new();
    fixture.add_task("root", "Root", &["child"], "pending");
    fixture.add_task("child", "Child", &[], "pending");
    fixture.register_worker("worker", "project.child");
    fixture.finish("root").assert().success();

    let ledger: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(fixture.ledger.join("root.json")).unwrap())
            .unwrap();
    let run_id = ledger["run_id"].as_str().unwrap().to_owned();

    fs::write(
        fixture.state.join("report-header.json"),
        format!(
            r#"[{{"mailbox":"inbox","id":"MSG-REPORT","sender":"worker","subject":"agent-orchestrator report child {run_id}"}}]"#
        ),
    )
    .unwrap();
    fs::write(
        fixture.state.join("report-message"),
        format!(
            "From: worker\nSubject: agent-orchestrator report child {run_id}\n\n{{\"task_id\":\"child\",\"run_id\":\"{run_id}\",\"status\":\"completed\",\"commit\":\"abc123\",\"evidence\":[\"cargo test: passed\"]}}\n"
        ),
    )
    .unwrap();

    // SQ status must agree with the report before the orchestrator accepts it.
    fixture
        .finish("root")
        .assert()
        .failure()
        .stderr(predicate::str::contains("but SQ status is"));

    fixture.set_status("child", "closed");
    fixture.register_worker("worker", "project.root");

    fixture
        .finish("root")
        .assert()
        .success()
        .stdout(predicate::str::contains(r#""root""#));

    let ledger: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(fixture.ledger.join("root.json")).unwrap())
            .unwrap();
    assert_eq!(ledger["task_runs"]["child"]["state"], "completed");
    assert_eq!(
        ledger["task_runs"]["child"]["report_message_id"],
        "MSG-REPORT"
    );
    assert_eq!(ledger["task_runs"]["root"]["state"], "working");
    assert_eq!(
        ledger["task_runs"]["root"]["base"],
        "agent-orchestrator/root/child"
    );
}

#[test]
fn plan_drift_stops_a_resumed_run() {
    let fixture = Fixture::new();
    fixture.add_task("root", "Root", &["child"], "pending");
    fixture.add_task("child", "Child", &[], "pending");
    fixture.register_worker("worker", "project.child");
    fixture.finish("root").assert().success();

    let queue = fs::read_to_string(&fixture.queue).unwrap().replace(
        r#""description":"Do Child""#,
        r#""description":"Renegotiated""#,
    );
    fs::write(&fixture.queue, queue).unwrap();

    fixture
        .finish("root")
        .assert()
        .failure()
        .stderr(predicate::str::contains("SQ plan drift detected"));
}
