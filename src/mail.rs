use std::ffi::{OsStr, OsString};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};

use crate::identity::Identity;
use crate::sq::TaskStatus;

#[derive(Clone, Debug)]
pub struct AgentMail {
    executable: PathBuf,
}

#[derive(Clone, Debug)]
pub struct Handoff<'a> {
    pub run_id: &'a str,
    pub task_id: &'a str,
    pub queue: &'a Path,
    pub worktree: &'a Path,
    pub branch: &'a str,
    pub dependencies: &'a [String],
    pub acceptance_criteria: &'a [String],
    pub validation_checks: &'a [String],
    pub orchestrator: &'a Identity,
    pub worker: &'a Identity,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct MailReceipt {
    pub id: String,
    pub recipient: String,
    pub sender: String,
    #[serde(default)]
    pub subject: String,
    #[serde(default)]
    pub timestamp: String,
    #[serde(default)]
    pub mailbox: String,
    #[serde(default)]
    pub state: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReportStatus {
    Completed,
    Blocked,
    Failed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReportDisposition {
    Completed,
    Blocked,
    Failed,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkerReport {
    pub task_id: String,
    pub run_id: String,
    pub status: ReportStatus,
    #[serde(default)]
    pub commit: Option<String>,
    #[serde(default)]
    pub artifact: Option<String>,
    #[serde(default)]
    pub evidence: Vec<String>,
    #[serde(default)]
    pub summary: Option<String>,
}

impl WorkerReport {
    pub fn validate(&self, expected_task_id: &str, expected_run_id: &str) -> Result<()> {
        if self.task_id != expected_task_id || self.run_id != expected_run_id {
            bail!(
                "worker report belongs to task {} run {}, expected task {} run {}",
                self.task_id,
                self.run_id,
                expected_task_id,
                expected_run_id
            );
        }

        match self.status {
            ReportStatus::Completed => {
                let has_deliverable = self
                    .commit
                    .as_deref()
                    .is_some_and(|value| !value.trim().is_empty())
                    || self
                        .artifact
                        .as_deref()
                        .is_some_and(|value| !value.trim().is_empty());
                if !has_deliverable {
                    bail!("completed worker report must include a commit or artifact");
                }
                if self.evidence.iter().all(|value| value.trim().is_empty()) {
                    bail!("completed worker report must include validation evidence");
                }
            }
            ReportStatus::Blocked | ReportStatus::Failed => {
                if self
                    .summary
                    .as_deref()
                    .is_none_or(|value| value.trim().is_empty())
                {
                    bail!("blocked or failed worker report must include a summary");
                }
            }
        }
        Ok(())
    }
}

pub fn reconcile_report(
    task_status: &TaskStatus,
    report: &WorkerReport,
) -> Result<ReportDisposition> {
    match (&report.status, task_status) {
        (ReportStatus::Completed, TaskStatus::Closed) => Ok(ReportDisposition::Completed),
        (ReportStatus::Completed, status) => bail!(
            "worker reported task {} completed but SQ status is {status:?}",
            report.task_id
        ),
        (ReportStatus::Blocked, TaskStatus::Closed)
        | (ReportStatus::Failed, TaskStatus::Closed) => bail!(
            "worker reported task {} {:?} after SQ was closed",
            report.task_id,
            report.status
        ),
        (ReportStatus::Blocked, _) => Ok(ReportDisposition::Blocked),
        (ReportStatus::Failed, _) => Ok(ReportDisposition::Failed),
    }
}

#[derive(Clone, Debug, Deserialize)]
struct MailHeader {
    id: String,
    sender: String,
    subject: String,
}

impl AgentMail {
    pub fn from_environment() -> Self {
        Self {
            executable: std::env::var_os("AGENT_ORCHESTRATOR_AGENT_MAIL")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("agent-mail")),
        }
    }

    pub fn send_handoff(&self, handoff: &Handoff<'_>) -> Result<MailReceipt> {
        let subject = handoff_subject(handoff.task_id, handoff.run_id);
        let body = handoff_body(handoff);
        self.run_json([
            OsStr::new("send"),
            OsStr::new("--to"),
            OsStr::new(&handoff.worker.slug),
            OsStr::new("--from"),
            OsStr::new(&handoff.orchestrator.slug),
            OsStr::new("--reply-to"),
            OsStr::new(&handoff.orchestrator.slug),
            OsStr::new("--subject"),
            OsStr::new(&subject),
            OsStr::new("--body"),
            OsStr::new(&body),
            OsStr::new("--json"),
        ])
    }

    pub fn scan_report(
        &self,
        orchestrator: &Identity,
        worker: &Identity,
        task_id: &str,
        run_id: &str,
    ) -> Result<Option<(String, WorkerReport)>> {
        let headers: Vec<MailHeader> = self.run_json([
            OsStr::new("scan"),
            OsStr::new("--to"),
            OsStr::new(&orchestrator.slug),
            OsStr::new("--json"),
        ])?;
        let expected_subject = report_subject(task_id, run_id);
        let matching = headers
            .into_iter()
            .filter(|header| {
                (header.sender == worker.slug || header.sender == worker.name)
                    && header.subject == expected_subject
            })
            .collect::<Vec<_>>();

        match matching.as_slice() {
            [] => Ok(None),
            [header] => {
                let text = self.run_text([
                    OsStr::new("read"),
                    OsStr::new(&header.id),
                    OsStr::new("--peek"),
                ])?;
                let report: WorkerReport =
                    serde_json::from_str(message_body(&text)).with_context(|| {
                        format!(
                            "AgentMail report {} body is not valid report JSON",
                            header.id
                        )
                    })?;
                report.validate(task_id, run_id)?;
                Ok(Some((header.id.clone(), report)))
            }
            _ => bail!("multiple unread AgentMail reports match task {task_id} run {run_id}"),
        }
    }

    pub fn mark_read(&self, message_id: &str) -> Result<()> {
        self.run_text([OsStr::new("read"), OsStr::new(message_id)])?;
        Ok(())
    }

    fn run_json<T, I, S>(&self, args: I) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let output = self.run(args)?;
        serde_json::from_slice(&output.stdout).with_context(|| {
            format!(
                "AgentMail returned invalid JSON: {}",
                String::from_utf8_lossy(&output.stdout).trim()
            )
        })
    }

    fn run_text<I, S>(&self, args: I) -> Result<String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let output = self.run(args)?;
        String::from_utf8(output.stdout).context("AgentMail returned non-UTF-8 output")
    }

    fn run<I, S>(&self, args: I) -> Result<Output>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let args = args
            .into_iter()
            .map(|arg| arg.as_ref().to_os_string())
            .collect::<Vec<OsString>>();
        let output = Command::new(&self.executable).args(&args).output();
        let output = match output {
            Ok(output) => output,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                bail!(
                    "AgentMail executable {} is not available",
                    self.executable.display()
                )
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("failed to run {}", self.executable.display()));
            }
        };

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
            bail!(
                "AgentMail command failed: {}",
                if stderr.is_empty() {
                    format!("exit status {}", output.status)
                } else {
                    stderr
                }
            );
        }
        Ok(output)
    }
}

pub fn handoff_subject(task_id: &str, run_id: &str) -> String {
    format!("agent-orchestrator handoff {task_id} {run_id}")
}

pub fn report_subject(task_id: &str, run_id: &str) -> String {
    format!("agent-orchestrator report {task_id} {run_id}")
}

fn handoff_body(handoff: &Handoff<'_>) -> String {
    let dependencies = list_or_none(handoff.dependencies);
    let acceptance = numbered(handoff.acceptance_criteria);
    let checks = numbered(handoff.validation_checks);
    format!(
        "You are the worker for an agent-orchestrator run.\n\n\
Orchestrator: {} ({})\n\
Worker: {} ({})\n\
Run ID: {}\n\
Task ID: {}\n\
Canonical SQ queue: {}\n\
Worktree: {}\n\
Branch: {}\n\
Dependencies: {}\n\n\
Acceptance criteria:\n{}\n\n\
Validation expectations:\n{}\n\n\
Update only task {} in the canonical SQ queue. Do not take ownership of other tasks, merge branches, delete worktrees, or silently retry failed work. Send decisions and blockers to {} with AgentMail.\n\n\
When finished, first update SQ. Then send an AgentMail message to {} with subject `{}` and a JSON-only body matching:\n\
{{\"task_id\":\"{}\",\"run_id\":\"{}\",\"status\":\"completed|blocked|failed\",\"commit\":\"COMMIT_OR_NULL\",\"artifact\":\"ARTIFACT_OR_NULL\",\"evidence\":[\"COMMAND: RESULT\"],\"summary\":\"BLOCKER_OR_FAILURE_OR_NULL\"}}\n\
A completed report requires a commit or artifact and non-empty validation evidence. Pane exit alone is not completion.\n",
        handoff.orchestrator.name,
        handoff.orchestrator.slug,
        handoff.worker.name,
        handoff.worker.slug,
        handoff.run_id,
        handoff.task_id,
        handoff.queue.display(),
        handoff.worktree.display(),
        handoff.branch,
        dependencies,
        acceptance,
        checks,
        handoff.task_id,
        handoff.orchestrator.slug,
        handoff.orchestrator.slug,
        report_subject(handoff.task_id, handoff.run_id),
        handoff.task_id,
        handoff.run_id,
    )
}

fn list_or_none(values: &[String]) -> String {
    if values.is_empty() {
        "none".to_owned()
    } else {
        values.join(", ")
    }
}

fn numbered(values: &[String]) -> String {
    if values.is_empty() {
        "1. None declared; report the focused command or scenario you used.".to_owned()
    } else {
        values
            .iter()
            .enumerate()
            .map(|(index, value)| format!("{}. {value}", index + 1))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

fn message_body(message: &str) -> &str {
    message
        .split_once("\r\n\r\n")
        .or_else(|| message.split_once("\n\n"))
        .map(|(_, body)| body.trim())
        .unwrap_or_else(|| message.trim())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use serde_json::{json, Map};

    use super::*;

    fn identity(name: &str, slug: &str) -> Identity {
        Identity {
            session_id: format!("{slug}-session"),
            name: name.to_owned(),
            slug: slug.to_owned(),
            cwd: Some(PathBuf::from("/tmp/worktree")),
            state: None,
            extensions: Map::from_iter([("omp".to_owned(), json!({}))]),
        }
    }

    #[test]
    fn handoff_contains_every_execution_boundary() {
        let orchestrator = identity("Orchestrator", "orchestrator");
        let worker = identity("Worker", "worker");
        let dependencies = vec!["dep".to_owned()];
        let acceptance = vec!["observable behavior".to_owned()];
        let checks = vec!["cargo test focused".to_owned()];
        let handoff = Handoff {
            run_id: "run",
            task_id: "task",
            queue: Path::new("/repo/.sift/issues.jsonl"),
            worktree: Path::new("/repo.task"),
            branch: "agent/task",
            dependencies: &dependencies,
            acceptance_criteria: &acceptance,
            validation_checks: &checks,
            orchestrator: &orchestrator,
            worker: &worker,
        };
        let body = handoff_body(&handoff);

        for expected in [
            "Orchestrator: Orchestrator (orchestrator)",
            "Worker: Worker (worker)",
            "Task ID: task",
            "Canonical SQ queue: /repo/.sift/issues.jsonl",
            "Worktree: /repo.task",
            "Branch: agent/task",
            "Dependencies: dep",
            "observable behavior",
            "cargo test focused",
        ] {
            assert!(body.contains(expected), "missing {expected}");
        }
    }

    #[test]
    fn completion_report_requires_deliverable_and_evidence() {
        let mut report = WorkerReport {
            task_id: "task".to_owned(),
            run_id: "run".to_owned(),
            status: ReportStatus::Completed,
            commit: None,
            artifact: None,
            evidence: Vec::new(),
            summary: None,
        };
        assert!(report.validate("task", "run").is_err());
        report.artifact = Some("artifact://result".to_owned());
        report.evidence.push("cargo test: passed".to_owned());
        report.validate("task", "run").unwrap();
    }

    #[test]
    fn completion_requires_matching_closed_sq_transition() {
        let report = WorkerReport {
            task_id: "task".to_owned(),
            run_id: "run".to_owned(),
            status: ReportStatus::Completed,
            commit: Some("abc123".to_owned()),
            artifact: None,
            evidence: vec!["cargo test: passed".to_owned()],
            summary: None,
        };

        assert_eq!(
            reconcile_report(&TaskStatus::Closed, &report).unwrap(),
            ReportDisposition::Completed
        );
        assert!(reconcile_report(&TaskStatus::InProgress, &report).is_err());
    }

    #[test]
    fn parses_json_body_after_mail_headers() {
        let message = "From: worker\nSubject: report\n\n{\"task_id\":\"task\"}";
        assert_eq!(message_body(message), r#"{"task_id":"task"}"#);
    }
}
