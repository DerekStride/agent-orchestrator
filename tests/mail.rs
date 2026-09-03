#![cfg(unix)]

use std::{
    fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use agent_orchestrator::{
    identity::Identity,
    mail::{
        handoff_body, reconcile_report, AgentMailClient, Error, Handoff, ReportDisposition,
        ReportStatus, WorkerReport,
    },
    sq::TaskStatus,
};
use serde_json::{json, Map};

static NEXT_TEST_DIR: AtomicU64 = AtomicU64::new(0);

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let sequence = NEXT_TEST_DIR.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "agent-orchestrator-mail-{}-{sequence}",
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
fn mail_handoff_contains_every_execution_and_ownership_boundary() {
    let orchestrator = identity("Orchestrator Agent", "orchestrator", "/repo");
    let worker = identity("Worker Agent", "worker", "/repo.task");
    let dependencies = vec!["dep-a".to_owned(), "dep-b".to_owned()];
    let acceptance = vec!["observable behavior".to_owned()];
    let checks = vec!["cargo test --locked focused".to_owned()];
    let handoff = Handoff {
        run_id: "run-1",
        task_id: "task-1",
        title: "Implement task one",
        description: "Build the first task without changing unrelated work.",
        queue: Path::new("/repo/issues.jsonl"),
        worktree: Path::new("/repo.task"),
        branch: "agent-orchestrator/root/task-1",
        dependencies: &dependencies,
        acceptance_criteria: &acceptance,
        validation_checks: &checks,
        orchestrator: &orchestrator,
        worker: &worker,
    };
    let body = handoff_body(&handoff);

    for expected in [
        "Orchestrator: Orchestrator Agent (orchestrator)",
        "Worker: Worker Agent (worker)",
        "Run ID: run-1",
        "Task ID: task-1",
        "Task: Implement task one",
        "Description: Build the first task without changing unrelated work.",
        "Canonical SQ queue: /repo/issues.jsonl",
        "Worktree: /repo.task",
        "Branch: agent-orchestrator/root/task-1",
        "Dependencies: dep-a, dep-b",
        "observable behavior",
        "cargo test --locked focused",
        "Update only task task-1",
        "first produce and validate the reported commit or durable artifact",
        "agent-orchestrator report task-1 run-1",
        "a JSON-only body",
    ] {
        assert!(body.contains(expected), "missing `{expected}`");
    }
}

#[test]
fn mail_send_handoff_uses_expected_envelope_and_validates_receipt() {
    let directory = TestDir::new();
    let script = directory.path().join("fake-agent-mail");
    let arguments = directory.path().join("arguments");
    fs::write(
        &script,
        format!(
            "#!/bin/sh\nprintf '%s\\0' \"$@\" > {}\nprintf '%s\\n' '{}'\n",
            shell_quote(&arguments),
            json!({
                "id": "MSG-HANDOFF",
                "recipient": "worker",
                "sender": "orchestrator",
                "subject": "agent-orchestrator handoff task-1 run-1",
                "state": "delivered"
            })
        ),
    )
    .unwrap();
    make_executable(&script);
    let orchestrator = identity("Orchestrator Agent", "orchestrator", "/repo");
    let worker = identity("Worker Agent", "worker", "/repo.task");
    let handoff = Handoff {
        run_id: "run-1",
        task_id: "task-1",
        title: "Implement task one",
        description: "Build the first task.",
        queue: Path::new("/repo/issues.jsonl"),
        worktree: Path::new("/repo.task"),
        branch: "agent-orchestrator/root/task-1",
        dependencies: &[],
        acceptance_criteria: &["works".to_owned()],
        validation_checks: &["cargo test".to_owned()],
        orchestrator: &orchestrator,
        worker: &worker,
    };

    let receipt = AgentMailClient::with_executable(&script)
        .send_handoff(&handoff)
        .unwrap();
    assert_eq!(receipt.id, "MSG-HANDOFF");
    let raw = fs::read(&arguments).unwrap();
    let args = raw
        .split(|byte| *byte == 0)
        .filter(|value| !value.is_empty())
        .map(|value| String::from_utf8(value.to_vec()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(args[0], "send");
    assert!(args.windows(2).any(|pair| pair == ["--to", "worker"]));
    assert!(args
        .windows(2)
        .any(|pair| pair == ["--from", "orchestrator"]));
    assert!(args.windows(2).any(|pair| {
        pair[0] == "--subject" && pair[1] == "agent-orchestrator handoff task-1 run-1"
    }));
    assert!(args
        .windows(2)
        .any(|pair| pair[0] == "--body" && pair[1].contains("Update only task task-1")));
}

#[test]
fn mail_report_requires_expected_unread_envelope_and_closed_sq() {
    let directory = TestDir::new();
    let script = directory.path().join("fake-agent-mail");
    let state = directory.path().join("state");
    let arguments = directory.path().join("arguments");
    fs::create_dir(&state).unwrap();
    fs::write(
        state.join("headers"),
        json!([
            {
                "id": "OTHER",
                "sender": "someone-else",
                "subject": "agent-orchestrator report task-1 run-1"
            },
            {
                "id": "MSG-REPORT",
                "sender": "Worker Agent",
                "subject": "agent-orchestrator report task-1 run-1"
            }
        ])
        .to_string(),
    )
    .unwrap();
    fs::write(
        state.join("message"),
        "From: worker\nMessage-ID: MSG-REPORT\nSubject: agent-orchestrator report task-1 run-1\n\n{\"task_id\":\"task-1\",\"run_id\":\"run-1\",\"status\":\"completed\",\"commit\":\"abc123\",\"artifact\":null,\"evidence\":[\"cargo test: passed\"],\"summary\":null}\n",
    )
    .unwrap();
    fs::write(
        &script,
        format!(
            "#!/bin/sh\nprintf '%s\\n' \"$@\" > {}\ncase \"$1\" in\n  scan) cat {} ;;\n  read) cat {} ;;\nesac\n",
            shell_quote(&arguments),
            shell_quote(&state.join("headers")),
            shell_quote(&state.join("message"))
        ),
    )
    .unwrap();
    make_executable(&script);
    let orchestrator = identity("Orchestrator Agent", "orchestrator", "/repo");
    let worker = identity("Worker Agent", "worker", "/repo.task");

    let (message_id, report) = AgentMailClient::with_executable(&script)
        .scan_report(&orchestrator, &worker, "task-1", "run-1")
        .unwrap()
        .expect("the expected unread report should match");
    assert_eq!(message_id, "MSG-REPORT");
    assert_eq!(report.status, ReportStatus::Completed);
    assert_eq!(
        reconcile_report(&TaskStatus::Closed, &report).unwrap(),
        ReportDisposition::Completed
    );
    let error = reconcile_report(&TaskStatus::InProgress, &report).unwrap_err();
    assert!(matches!(error, Error::SqStatusMismatch { .. }));
    assert_eq!(
        fs::read_to_string(&arguments).unwrap(),
        "read\nMSG-REPORT\n--peek\n"
    );
}

#[test]
fn mail_report_validation_rejects_incomplete_terminal_reports() {
    let report = |status, commit, evidence, summary| WorkerReport {
        task_id: "task-1".to_owned(),
        run_id: "run-1".to_owned(),
        status,
        commit,
        artifact: None,
        evidence,
        summary,
    };

    assert!(matches!(
        report(ReportStatus::Completed, None, Vec::new(), None).validate("task-1", "run-1"),
        Err(Error::CompletedWithoutDeliverable)
    ));
    assert!(matches!(
        report(
            ReportStatus::Completed,
            Some("abc123".to_owned()),
            vec![" ".to_owned()],
            None
        )
        .validate("task-1", "run-1"),
        Err(Error::CompletedWithoutEvidence)
    ));
    for status in [ReportStatus::Blocked, ReportStatus::Failed] {
        assert!(matches!(
            report(status, None, Vec::new(), Some(" ".to_owned())).validate("task-1", "run-1"),
            Err(Error::TerminalReportWithoutSummary { .. })
        ));
    }
    assert!(matches!(
        report(
            ReportStatus::Completed,
            Some("abc123".to_owned()),
            vec!["cargo test: passed".to_owned()],
            None
        )
        .validate("other-task", "run-1"),
        Err(Error::ReportCorrelation { .. })
    ));
}

fn identity(name: &str, slug: &str, cwd: &str) -> Identity {
    Identity {
        session_id: format!("{slug}-session"),
        name: name.to_owned(),
        slug: slug.to_owned(),
        cwd: PathBuf::from(cwd),
        state: None,
        extensions: Map::from_iter([("omp".to_owned(), json!({}))]),
    }
}

fn make_executable(path: &Path) {
    let mut permissions = fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).unwrap();
}

fn shell_quote(path: &Path) -> String {
    format!("'{}'", path.to_string_lossy().replace('\'', "'\\''"))
}
