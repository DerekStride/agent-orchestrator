#![cfg(unix)]

use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::atomic::{AtomicU64, Ordering},
};

use agent_orchestrator::sq::{Queue, TaskStatus};
use serde_json::{json, Value};

static NEXT_TEST_DIR: AtomicU64 = AtomicU64::new(0);
const FIXTURE_BIN: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/bin");

struct Scenario {
    directory: PathBuf,
    repo: PathBuf,
    queue: PathBuf,
    state: PathBuf,
    worktrees: PathBuf,
    fixture_state: PathBuf,
}

impl Scenario {
    fn new(tasks: &[Value]) -> Self {
        let sequence = NEXT_TEST_DIR.fetch_add(1, Ordering::Relaxed);
        let directory = std::env::temp_dir().join(format!(
            "agent-orchestrator-workflow-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&directory).unwrap();
        let repo = directory.join("repo");
        init_repo(&repo);
        let queue = directory.join("issues.jsonl");
        write_queue(&queue, tasks);
        let state = directory.join("state");
        let worktrees = directory.join("worktrees");
        let fixture_state = directory.join("fixture-state");
        fs::create_dir(&worktrees).unwrap();
        fs::create_dir(&fixture_state).unwrap();
        Self {
            directory,
            repo,
            queue,
            state,
            worktrees,
            fixture_state,
        }
    }

    fn finish(&self) -> Output {
        self.finish_with(&[])
    }

    fn finish_with(&self, overrides: &[(&str, &Path)]) -> Output {
        let mut command = Command::new(env!("CARGO_BIN_EXE_agent-orchestrator"));
        command
            .args(["finish", "root", "--once", "--queue"])
            .arg(&self.queue)
            .arg("--repo")
            .arg(&self.repo)
            .arg("--state-dir")
            .arg(&self.state)
            .arg("--worktree-root")
            .arg(&self.worktrees)
            .env("AGENT_ORCHESTRATOR_SQ", fixture("sq"))
            .env("AGENT_ORCHESTRATOR_GIT", "git")
            .env("AGENT_ORCHESTRATOR_HERDR", fixture("herdr"))
            .env("AGENT_ORCHESTRATOR_AGENT_ID", fixture("agent-id"))
            .env("AGENT_ORCHESTRATOR_AGENT_MAIL", fixture("agent-mail"))
            .env("HERDR_SESSION", "workflow-test")
            .env("AGENT_ORCHESTRATOR_FIXTURE_STATE", &self.fixture_state)
            .env("AGENT_ORCHESTRATOR_FIXTURE_REPO", &self.repo)
            .env("AGENT_ORCHESTRATOR_FIXTURE_WORKTREES", &self.worktrees);
        for (name, value) in overrides {
            command.env(name, value);
        }
        command.output().unwrap()
    }

    fn write_claim_template(&self, task_id: &str, mut tasks: Vec<Value>) {
        let claimed = tasks
            .iter_mut()
            .find(|task| task["id"] == task_id)
            .expect("claim template task should exist");
        claimed["status"] = json!("in_progress");
        claimed["metadata"]["agent_orchestrator"] = json!({"run_id": "__RUN_ID__"});
        write_queue(
            &self.fixture_state.join(format!("sq-{task_id}.jsonl")),
            &tasks,
        );
    }

    fn set_report(&self, task_id: &str, status: &str, value: &str) {
        fs::write(self.fixture_state.join("report-task"), task_id).unwrap();
        fs::write(self.fixture_state.join("report-status"), status).unwrap();
        let field = if status == "completed" {
            "report-commit"
        } else {
            "report-summary"
        };
        fs::write(self.fixture_state.join(field), value).unwrap();
    }

    fn worktree(&self, task_id: &str) -> PathBuf {
        self.worktrees.join(format!("repo.{task_id}"))
    }

    fn ledger(&self) -> Value {
        serde_json::from_slice(&fs::read(self.state.join("root.json")).unwrap()).unwrap()
    }
}

impl Drop for Scenario {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.directory);
    }
}

#[test]
fn workflow_claims_hands_off_stacks_and_completes_dependency_branches() {
    let leaf = task("leaf", "pending", &[]);
    let root = task("root", "pending", &["leaf"]);
    let scenario = Scenario::new(&[leaf.clone(), root.clone()]);
    scenario.write_claim_template("leaf", vec![leaf.clone(), root.clone()]);

    let first = scenario.finish();
    let first = success_json(first);
    assert_eq!(first["status"], "active");
    assert_eq!(first["runtimes"][0]["task_id"], "leaf");
    assert_eq!(first["runtimes"][0]["stage"], "active");
    let run_id = first["run_id"].as_str().unwrap();
    assert_claimed(&scenario.queue, "root", "leaf", run_id);
    assert_eq!(
        fs::read_link(scenario.worktree("leaf").join(".sift/issues.jsonl")).unwrap(),
        fs::canonicalize(&scenario.queue).unwrap()
    );
    let leaf_handoff = fs::read_to_string(scenario.fixture_state.join("handoff-leaf")).unwrap();
    for expected in [
        &format!(
            "Canonical SQ queue: {}",
            fs::canonicalize(&scenario.queue).unwrap().display()
        ),
        &format!(
            "Worktree: {}",
            fs::canonicalize(scenario.worktree("leaf"))
                .unwrap()
                .display()
        ),
        "Branch: agent-orchestrator/root/leaf",
        "Dependencies: none",
        "Acceptance criteria:",
        "leaf acceptance",
        "Validation expectations:",
        "check leaf",
    ] {
        assert!(leaf_handoff.contains(expected), "missing `{expected}`");
    }

    let leaf_commit = commit_work(&scenario.worktree("leaf"), "leaf.txt", "leaf\n", "leaf");
    let leaf_closed = owned(task("leaf", "closed", &[]), run_id);
    let root_pending = task("root", "pending", &["leaf"]);
    write_queue(
        &scenario.queue,
        &[leaf_closed.clone(), root_pending.clone()],
    );
    scenario.write_claim_template("root", vec![leaf_closed.clone(), root_pending]);
    scenario.set_report("leaf", "completed", &leaf_commit);

    let second = scenario.finish();
    let second = success_json(second);
    assert_eq!(second["status"], "active");
    assert_eq!(second["runtimes"][0]["stage"], "completed");
    assert_eq!(second["runtimes"][1]["task_id"], "root");
    assert_eq!(second["runtimes"][1]["stage"], "active");
    assert_claimed(&scenario.queue, "root", "root", run_id);
    assert_eq!(
        git_output(&scenario.worktree("root"), &["rev-parse", "HEAD"]),
        leaf_commit
    );
    assert_eq!(
        git_output(&scenario.worktree("root"), &["branch", "--show-current"]),
        "agent-orchestrator/root/root"
    );
    let root_handoff = fs::read_to_string(scenario.fixture_state.join("handoff-root")).unwrap();
    assert!(root_handoff.contains("Dependencies: leaf"));
    assert!(root_handoff.contains("Branch: agent-orchestrator/root/root"));
    assert!(root_handoff.contains("Update only task root"));

    let root_commit = commit_work(&scenario.worktree("root"), "root.txt", "root\n", "root");
    write_queue(
        &scenario.queue,
        &[
            leaf_closed,
            owned(task("root", "closed", &["leaf"]), run_id),
        ],
    );
    scenario.set_report("root", "completed", &root_commit);

    let complete = success_json(scenario.finish());
    assert_eq!(complete["status"], "complete");
    assert_eq!(complete["run_id"], run_id);
    assert!(complete["runtimes"]
        .as_array()
        .unwrap()
        .iter()
        .all(|runtime| runtime["stage"] == "completed"));
    let ledger = scenario.ledger();
    assert_eq!(ledger["runtimes"]["leaf"]["report"]["status"], "completed");
    assert_eq!(ledger["runtimes"]["root"]["report"]["status"], "completed");
    assert_eq!(
        ledger["runtimes"]["root"]["report"]["evidence"],
        json!(["fixture scenario: passed"])
    );
}

#[test]
fn workflow_rejects_foreign_ownership_report_disagreement_and_plan_drift() {
    let foreign = owned(task("root", "pending", &[]), "foreign-run");
    let foreign_scenario = Scenario::new(&[foreign]);
    let error = failure(foreign_scenario.finish());
    assert!(error.contains("foreign agent_orchestrator.run_id `foreign-run`"));
    assert!(!foreign_scenario.worktree("root").exists());

    let disagreement = provisioned_root();
    let branch_head = git_output(&disagreement.worktree("root"), &["rev-parse", "HEAD"]);
    disagreement.set_report("root", "completed", &branch_head);
    let error = failure(disagreement.finish());
    assert!(error.contains("worker reported task `root` Completed but SQ status is `in_progress`"));
    assert_eq!(disagreement.ledger()["runtimes"]["root"]["stage"], "active");

    let drift = provisioned_root();
    let mut changed = Queue::read(&drift.queue)
        .unwrap()
        .task("root")
        .unwrap()
        .raw()
        .clone();
    changed["title"] = json!("changed after dispatch");
    write_queue(&drift.queue, &[changed]);
    let error = failure(drift.finish());
    assert!(error.contains("SQ plan drift for root task `root`"));
    assert_eq!(drift.ledger()["runtimes"]["root"]["stage"], "active");
}

#[test]
fn workflow_names_each_unavailable_integration_and_configured_executable() {
    for (integration, variable, expected) in [
        (
            "herdr",
            "AGENT_ORCHESTRATOR_HERDR",
            "cannot start Herdr executable",
        ),
        (
            "agent-id",
            "AGENT_ORCHESTRATOR_AGENT_ID",
            "cannot start Agent ID executable",
        ),
        (
            "git",
            "AGENT_ORCHESTRATOR_GIT",
            "cannot start Git executable",
        ),
        (
            "sq",
            "AGENT_ORCHESTRATOR_SQ",
            "cannot start configured SQ executable",
        ),
        (
            "agent-mail",
            "AGENT_ORCHESTRATOR_AGENT_MAIL",
            "cannot start AgentMail executable",
        ),
    ] {
        let root = task("root", "pending", &[]);
        let scenario = Scenario::new(std::slice::from_ref(&root));
        scenario.write_claim_template("root", vec![root]);
        let missing = scenario.directory.join(format!("missing-{integration}"));
        let error = failure(scenario.finish_with(&[(variable, &missing)]));
        assert!(
            error.contains(expected),
            "{integration} failure was not explicit: {error}"
        );
        assert!(error.contains(&missing.to_string_lossy().to_string()));
    }
}

#[test]
fn workflow_stops_on_blocked_failed_and_lost_workers_without_retry() {
    for status in ["blocked", "failed"] {
        let scenario = provisioned_root();
        scenario.set_report("root", status, &format!("fixture {status}"));
        let error = failure(scenario.finish());
        let title = if status == "blocked" {
            "Blocked"
        } else {
            "Failed"
        };
        assert!(error.contains(&format!("worker reported task `root` {title}")));
        assert!(error.contains(&format!("fixture {status}")));
        assert_eq!(scenario.ledger()["runtimes"]["root"]["stage"], status);

        let retained = failure(scenario.finish());
        assert!(retained.contains(&format!(
            "retained worker report for task `root` is {title}; refusing a silent retry"
        )));
    }

    let blocked = provisioned_root();
    fs::write(blocked.fixture_state.join("herdr-agent-status"), "blocked").unwrap();
    let error = failure(blocked.finish());
    assert!(error.contains("worker for task `root` is blocked without a matching AgentMail report"));
    assert_eq!(blocked.ledger()["runtimes"]["root"]["stage"], "active");

    let lost = provisioned_root();
    fs::write(lost.fixture_state.join("identity-missing"), "").unwrap();
    let transient = success_json(lost.finish());
    assert_eq!(transient["status"], "active");
    let mut ledger = lost.ledger();
    assert!(ledger["runtimes"]["root"]["last_observation_error"]
        .as_str()
        .unwrap()
        .contains("found none"));
    ledger["runtimes"]["root"]["lease_expires_at_unix"] = json!(0);
    fs::write(
        lost.state.join("root.json"),
        serde_json::to_vec_pretty(&ledger).unwrap(),
    )
    .unwrap();
    let error = failure(lost.finish());
    assert!(error.contains("worker lease for task `root` expired"));
    assert_eq!(lost.ledger()["runtimes"]["root"]["stage"], "active");
}

fn provisioned_root() -> Scenario {
    let root = task("root", "pending", &[]);
    let scenario = Scenario::new(std::slice::from_ref(&root));
    scenario.write_claim_template("root", vec![root]);
    let output = success_json(scenario.finish());
    assert_eq!(output["status"], "active");
    assert_eq!(output["runtimes"][0]["stage"], "active");
    scenario
}

fn fixture(name: &str) -> PathBuf {
    Path::new(FIXTURE_BIN).join(name)
}

fn task(id: &str, status: &str, blocked_by: &[&str]) -> Value {
    json!({
        "id": id,
        "title": format!("Implement {id}"),
        "description": format!("Complete {id}"),
        "status": status,
        "sources": [],
        "metadata": {
            "acceptance_criteria": [format!("{id} acceptance")],
            "checks": [format!("check {id}")],
        },
        "blocked_by": blocked_by,
    })
}

fn owned(mut task: Value, run_id: &str) -> Value {
    task["metadata"]["agent_orchestrator"] = json!({"run_id": run_id});
    task
}

fn write_queue(path: &Path, tasks: &[Value]) {
    let contents = tasks
        .iter()
        .map(Value::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(path, format!("{contents}\n")).unwrap();
}

fn assert_claimed(queue: &Path, root: &str, task_id: &str, run_id: &str) {
    let plan = Queue::read(queue).unwrap().scope(root).unwrap();
    let task = plan.task(task_id).unwrap();
    assert_eq!(task.stored_status(), TaskStatus::InProgress);
    assert_eq!(task.owner_run_id(), Some(run_id));
}

fn success_json(output: Output) -> Value {
    assert!(
        output.status.success(),
        "finish failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn failure(output: Output) -> String {
    assert!(
        !output.status.success(),
        "finish unexpectedly succeeded: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    String::from_utf8(output.stderr).unwrap()
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

fn commit_work(worktree: &Path, file: &str, contents: &str, message: &str) -> String {
    fs::write(worktree.join(file), contents).unwrap();
    git(worktree, &["add", file]);
    git(worktree, &["commit", "-m", message]);
    git_output(worktree, &["rev-parse", "HEAD"])
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

fn git_output(repo: &Path, args: &[&str]) -> String {
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
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}
