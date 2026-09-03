use std::{
    env,
    ffi::{OsStr, OsString},
    fmt, io,
    path::Path,
    process::{Command, ExitStatus, Output},
    string::FromUtf8Error,
};

use serde::{Deserialize, Serialize};

use crate::{identity::Identity, sq::TaskStatus};

pub const AGENT_MAIL_EXECUTABLE_ENV: &str = "AGENT_ORCHESTRATOR_AGENT_MAIL";

pub type Result<T, E = Error> = std::result::Result<T, E>;

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
    pub subject: String,
    #[serde(default)]
    pub timestamp: String,
    #[serde(default)]
    pub mailbox: String,
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
            return Err(Error::ReportCorrelation {
                task_id: self.task_id.clone(),
                run_id: self.run_id.clone(),
                expected_task_id: expected_task_id.to_owned(),
                expected_run_id: expected_run_id.to_owned(),
            });
        }

        match self.status {
            ReportStatus::Completed => {
                let has_deliverable = nonempty(&self.commit) || nonempty(&self.artifact);
                if !has_deliverable {
                    return Err(Error::CompletedWithoutDeliverable);
                }
                if !self.evidence.iter().any(|value| !value.trim().is_empty()) {
                    return Err(Error::CompletedWithoutEvidence);
                }
            }
            ReportStatus::Blocked | ReportStatus::Failed => {
                if !nonempty(&self.summary) {
                    return Err(Error::TerminalReportWithoutSummary {
                        status: self.status.clone(),
                    });
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
        (ReportStatus::Completed, status) => Err(Error::SqStatusMismatch {
            task_id: report.task_id.clone(),
            report_status: report.status.clone(),
            sq_status: *status,
        }),
        (ReportStatus::Blocked, TaskStatus::Closed)
        | (ReportStatus::Failed, TaskStatus::Closed) => Err(Error::SqStatusMismatch {
            task_id: report.task_id.clone(),
            report_status: report.status.clone(),
            sq_status: TaskStatus::Closed,
        }),
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

#[derive(Clone, Debug)]
pub struct AgentMailClient {
    executable: OsString,
}

impl AgentMailClient {
    pub fn configured() -> Self {
        let executable = env::var_os(AGENT_MAIL_EXECUTABLE_ENV)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| OsString::from("agent-mail"));
        Self::with_executable(executable)
    }

    pub fn with_executable(executable: impl Into<OsString>) -> Self {
        Self {
            executable: executable.into(),
        }
    }

    pub fn executable(&self) -> &OsStr {
        &self.executable
    }

    pub fn send_handoff(&self, handoff: &Handoff<'_>) -> Result<MailReceipt> {
        let subject = handoff_subject(handoff.task_id, handoff.run_id);
        let body = handoff_body(handoff);
        let receipt: MailReceipt = self.invoke_json(
            "sending the worker handoff",
            [
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
            ],
        )?;
        validate_receipt(&receipt, handoff, &subject)?;
        Ok(receipt)
    }

    pub fn scan_report(
        &self,
        orchestrator: &Identity,
        worker: &Identity,
        task_id: &str,
        run_id: &str,
    ) -> Result<Option<(String, WorkerReport)>> {
        let headers: Vec<MailHeader> = self.invoke_json(
            "scanning unread worker reports",
            [
                OsStr::new("scan"),
                OsStr::new("--to"),
                OsStr::new(&orchestrator.slug),
                OsStr::new("--json"),
            ],
        )?;
        let expected_subject = report_subject(task_id, run_id);
        let mut matching = headers
            .into_iter()
            .filter(|header| {
                sender_matches(&header.sender, worker) && header.subject == expected_subject
            })
            .collect::<Vec<_>>();

        match matching.len() {
            0 => Ok(None),
            1 => {
                let header = matching.pop().expect("the match count was checked");
                if header.id.trim().is_empty() {
                    return Err(Error::InvalidMailHeader {
                        reason: "matching report has an empty message ID".to_owned(),
                    });
                }
                let text = self.invoke_text(
                    "reading the unread worker report",
                    [
                        OsStr::new("read"),
                        OsStr::new(&header.id),
                        OsStr::new("--peek"),
                    ],
                )?;
                validate_message_headers(&text, &header.id, worker, &expected_subject)?;
                let report: WorkerReport =
                    serde_json::from_str(message_body(&text)).map_err(|source| {
                        Error::InvalidReportJson {
                            message_id: header.id.clone(),
                            source,
                        }
                    })?;
                report.validate(task_id, run_id)?;
                Ok(Some((header.id, report)))
            }
            count => Err(Error::MultipleReports {
                task_id: task_id.to_owned(),
                run_id: run_id.to_owned(),
                count,
            }),
        }
    }

    pub fn mark_read(&self, message_id: &str) -> Result<()> {
        if message_id.trim().is_empty() {
            return Err(Error::InvalidMailHeader {
                reason: "cannot mark an empty message ID as read".to_owned(),
            });
        }
        self.invoke_text(
            "marking the worker report read",
            [OsStr::new("read"), OsStr::new(message_id)],
        )?;
        Ok(())
    }

    fn invoke_json<T, I, S>(&self, operation: &'static str, args: I) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let output = self.invoke(operation, args)?;
        serde_json::from_slice(&output.stdout).map_err(|source| Error::InvalidJson {
            operation,
            output: String::from_utf8_lossy(&output.stdout).trim().to_owned(),
            source,
        })
    }

    fn invoke_text<I, S>(&self, operation: &'static str, args: I) -> Result<String>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let output = self.invoke(operation, args)?;
        String::from_utf8(output.stdout).map_err(|source| Error::InvalidUtf8 { operation, source })
    }

    fn invoke<I, S>(&self, operation: &'static str, args: I) -> Result<Output>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let output = Command::new(&self.executable)
            .args(args)
            .output()
            .map_err(|source| Error::Start {
                executable: self.executable.clone(),
                operation,
                source,
            })?;
        if output.status.success() {
            Ok(output)
        } else {
            Err(Error::CommandFailed {
                operation,
                status: output.status,
                detail: failure_detail(&output),
            })
        }
    }
}

pub fn handoff_subject(task_id: &str, run_id: &str) -> String {
    format!("agent-orchestrator handoff {task_id} {run_id}")
}

pub fn report_subject(task_id: &str, run_id: &str) -> String {
    format!("agent-orchestrator report {task_id} {run_id}")
}

pub fn handoff_body(handoff: &Handoff<'_>) -> String {
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
A completed report requires a commit or artifact and non-empty validation evidence. Blocked or failed reports require a non-empty summary. Pane exit alone is not completion.\n",
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

fn validate_receipt(receipt: &MailReceipt, handoff: &Handoff<'_>, subject: &str) -> Result<()> {
    if receipt.id.trim().is_empty() {
        return Err(Error::InvalidReceipt {
            reason: "missing non-empty `id`".to_owned(),
        });
    }
    if receipt.recipient != handoff.worker.slug {
        return Err(Error::InvalidReceipt {
            reason: format!(
                "recipient `{}`, expected `{}`",
                receipt.recipient, handoff.worker.slug
            ),
        });
    }
    if !sender_matches(&receipt.sender, handoff.orchestrator) {
        return Err(Error::InvalidReceipt {
            reason: format!(
                "sender `{}`, expected `{}`",
                receipt.sender, handoff.orchestrator.slug
            ),
        });
    }
    if receipt.subject != subject {
        return Err(Error::InvalidReceipt {
            reason: format!("subject `{}`, expected `{subject}`", receipt.subject),
        });
    }
    if receipt.state != "delivered" {
        return Err(Error::InvalidReceipt {
            reason: format!("state `{}`, expected `delivered`", receipt.state),
        });
    }
    Ok(())
}

fn validate_message_headers(
    message: &str,
    expected_id: &str,
    worker: &Identity,
    expected_subject: &str,
) -> Result<()> {
    let from = message_header(message, "From").ok_or_else(|| Error::InvalidMailHeader {
        reason: "message is missing `From`".to_owned(),
    })?;
    if !sender_matches(from, worker) {
        return Err(Error::InvalidMailHeader {
            reason: format!("sender `{from}`, expected `{}`", worker.slug),
        });
    }
    let subject = message_header(message, "Subject").ok_or_else(|| Error::InvalidMailHeader {
        reason: "message is missing `Subject`".to_owned(),
    })?;
    if subject != expected_subject {
        return Err(Error::InvalidMailHeader {
            reason: format!("subject `{subject}`, expected `{expected_subject}`"),
        });
    }
    if let Some(message_id) = message_header(message, "Message-ID") {
        if message_id != expected_id {
            return Err(Error::InvalidMailHeader {
                reason: format!("message ID `{message_id}`, expected `{expected_id}`"),
            });
        }
    }
    Ok(())
}

fn sender_matches(sender: &str, identity: &Identity) -> bool {
    sender == identity.slug || sender == identity.name
}

fn nonempty(value: &Option<String>) -> bool {
    value
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
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

fn message_header<'a>(message: &'a str, name: &str) -> Option<&'a str> {
    message
        .lines()
        .take_while(|line| !line.trim().is_empty())
        .find_map(|line| {
            let (field, value) = line.split_once(':')?;
            field
                .eq_ignore_ascii_case(name)
                .then_some(value.trim())
                .filter(|value| !value.is_empty())
        })
}

fn message_body(message: &str) -> &str {
    message
        .split_once("\r\n\r\n")
        .or_else(|| message.split_once("\n\n"))
        .map(|(_, body)| body.trim())
        .unwrap_or_else(|| message.trim())
}

fn failure_detail(output: &Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
    if stderr.is_empty() {
        format!("process exited with {}", output.status)
    } else {
        stderr
    }
}

#[derive(Debug)]
pub enum Error {
    Start {
        executable: OsString,
        operation: &'static str,
        source: io::Error,
    },
    CommandFailed {
        operation: &'static str,
        status: ExitStatus,
        detail: String,
    },
    InvalidJson {
        operation: &'static str,
        output: String,
        source: serde_json::Error,
    },
    InvalidUtf8 {
        operation: &'static str,
        source: FromUtf8Error,
    },
    InvalidReceipt {
        reason: String,
    },
    InvalidMailHeader {
        reason: String,
    },
    MultipleReports {
        task_id: String,
        run_id: String,
        count: usize,
    },
    InvalidReportJson {
        message_id: String,
        source: serde_json::Error,
    },
    ReportCorrelation {
        task_id: String,
        run_id: String,
        expected_task_id: String,
        expected_run_id: String,
    },
    CompletedWithoutDeliverable,
    CompletedWithoutEvidence,
    TerminalReportWithoutSummary {
        status: ReportStatus,
    },
    SqStatusMismatch {
        task_id: String,
        report_status: ReportStatus,
        sq_status: TaskStatus,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Start {
                executable,
                operation,
                source,
            } => write!(
                formatter,
                "cannot start AgentMail executable `{}` while {operation}: {source}",
                executable.to_string_lossy()
            ),
            Self::CommandFailed {
                operation,
                status,
                detail,
            } => write!(
                formatter,
                "AgentMail failed while {operation} ({status}): {detail}"
            ),
            Self::InvalidJson {
                operation, output, ..
            } => write!(
                formatter,
                "AgentMail returned invalid JSON while {operation}: {output}"
            ),
            Self::InvalidUtf8 { operation, .. } => {
                write!(formatter, "AgentMail returned non-UTF-8 output while {operation}")
            }
            Self::InvalidReceipt { reason } => {
                write!(formatter, "AgentMail returned an invalid handoff receipt: {reason}")
            }
            Self::InvalidMailHeader { reason } => {
                write!(formatter, "AgentMail returned an invalid report envelope: {reason}")
            }
            Self::MultipleReports {
                task_id,
                run_id,
                count,
            } => write!(
                formatter,
                "multiple unread AgentMail reports match task `{task_id}` run `{run_id}`: found {count}"
            ),
            Self::InvalidReportJson { message_id, .. } => write!(
                formatter,
                "AgentMail report `{message_id}` body is not valid report JSON"
            ),
            Self::ReportCorrelation {
                task_id,
                run_id,
                expected_task_id,
                expected_run_id,
            } => write!(
                formatter,
                "worker report belongs to task `{task_id}` run `{run_id}`, expected task `{expected_task_id}` run `{expected_run_id}`"
            ),
            Self::CompletedWithoutDeliverable => {
                formatter.write_str("completed worker report must include a commit or artifact")
            }
            Self::CompletedWithoutEvidence => formatter
                .write_str("completed worker report must include non-empty validation evidence"),
            Self::TerminalReportWithoutSummary { status } => write!(
                formatter,
                "{status:?} worker report must include a non-empty summary"
            ),
            Self::SqStatusMismatch {
                task_id,
                report_status,
                sq_status,
            } => write!(
                formatter,
                "worker reported task `{task_id}` {report_status:?} but SQ status is `{sq_status}`"
            ),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Start { source, .. } => Some(source),
            Self::InvalidJson { source, .. } | Self::InvalidReportJson { source, .. } => {
                Some(source)
            }
            Self::InvalidUtf8 { source, .. } => Some(source),
            _ => None,
        }
    }
}
