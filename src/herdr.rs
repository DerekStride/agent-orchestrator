use std::{
    env,
    ffi::{OsStr, OsString},
    fmt, io,
    path::Path,
    process::{Command, ExitStatus, Output},
};

use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const HERDR_EXECUTABLE_ENV: &str = "AGENT_ORCHESTRATOR_HERDR";
pub const HERDR_SESSION_ENV: &str = "HERDR_SESSION";
const DEFAULT_SESSION: &str = "default";

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone, Copy)]
pub struct WorktreeSpec<'a> {
    pub repo: &'a Path,
    pub branch: &'a str,
    pub base: &'a str,
    pub path: &'a Path,
    pub task_label: &'a str,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerWorkspace {
    pub workspace_id: String,
    pub pane_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Worker {
    pub name: String,
    pub workspace_id: String,
    pub pane_id: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentStatus {
    Working,
    Idle,
    Done,
    Blocked,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerDisposition {
    Active,
    ReportPresent,
    Stop(WorkerStop),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerStop {
    SettledWithoutReport(AgentStatus),
    Blocked,
}

pub fn reconcile_worker(status: AgentStatus, structured_report_present: bool) -> WorkerDisposition {
    if structured_report_present {
        return WorkerDisposition::ReportPresent;
    }

    match status {
        AgentStatus::Working | AgentStatus::Unknown => WorkerDisposition::Active,
        AgentStatus::Idle | AgentStatus::Done => {
            WorkerDisposition::Stop(WorkerStop::SettledWithoutReport(status))
        }
        AgentStatus::Blocked => WorkerDisposition::Stop(WorkerStop::Blocked),
    }
}

pub fn worker_name(run_id: &str, task_id: &str) -> String {
    let mut readable = String::with_capacity(12);
    let mut previous_separator = false;
    for byte in task_id.bytes() {
        let character = match byte {
            b'a'..=b'z' | b'0'..=b'9' => byte as char,
            b'A'..=b'Z' => (byte + (b'a' - b'A')) as char,
            b'-' | b'_' => byte as char,
            _ => '-',
        };
        let separator = matches!(character, '-' | '_');
        if separator && (readable.is_empty() || previous_separator) {
            continue;
        }
        readable.push(character);
        previous_separator = separator;
        if readable.len() == 12 {
            break;
        }
    }
    while readable.ends_with(['-', '_']) {
        readable.pop();
    }
    if readable.is_empty() {
        readable.push_str("task");
    }

    let mut hash = 0xcbf29ce484222325_u64;
    for byte in run_id
        .bytes()
        .chain(std::iter::once(0))
        .chain(task_id.bytes())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }

    format!("ao-{readable}-{hash:016x}")
}

#[derive(Debug, Clone)]
pub struct HerdrClient {
    executable: OsString,
    session: String,
}

impl HerdrClient {
    pub fn configured() -> Self {
        let executable = env::var_os(HERDR_EXECUTABLE_ENV)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| OsString::from("herdr"));
        let session = env::var(HERDR_SESSION_ENV)
            .ok()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| DEFAULT_SESSION.to_owned());
        Self::with_executable(executable, session)
    }

    pub fn with_executable(executable: impl Into<OsString>, session: impl Into<String>) -> Self {
        Self {
            executable: executable.into(),
            session: session.into(),
        }
    }

    pub fn executable(&self) -> &OsStr {
        &self.executable
    }

    pub fn session(&self) -> &str {
        &self.session
    }

    pub fn preflight(&self) -> Result<()> {
        let output = self.invoke("preflighting Herdr", |command| {
            command.args(["status", "server", "--json"]);
        })?;
        if !output.status.success() {
            return Err(Error::SessionUnavailable {
                session: self.session.clone(),
                detail: failure_detail(&output),
            });
        }

        let status: ServerStatus =
            serde_json::from_slice(&output.stdout).map_err(|source| Error::InvalidJson {
                operation: "preflighting Herdr",
                source,
            })?;
        if !status.running {
            return Err(Error::SessionUnavailable {
                session: self.session.clone(),
                detail: "server is not running".to_owned(),
            });
        }
        if status.compatible == Some(false) {
            return Err(Error::SessionUnavailable {
                session: self.session.clone(),
                detail: "server protocol is incompatible".to_owned(),
            });
        }
        Ok(())
    }

    pub fn create_worktree(&self, spec: WorktreeSpec<'_>) -> Result<WorkerWorkspace> {
        let response = self.invoke_json("creating Herdr worktree", |command| {
            command
                .args(["worktree", "create", "--cwd"])
                .arg(spec.repo)
                .args(["--branch", spec.branch, "--base", spec.base, "--path"])
                .arg(spec.path)
                .args(["--label", spec.task_label, "--no-focus"]);
        })?;
        expect_response_type(&response, "creating Herdr worktree", "worktree_created")?;

        Ok(WorkerWorkspace {
            workspace_id: response_string(
                &response,
                "/result/workspace/workspace_id",
                "creating Herdr worktree",
            )?,
            pane_id: response_string(
                &response,
                "/result/root_pane/pane_id",
                "creating Herdr worktree",
            )?,
        })
    }
    pub fn close_workspace(&self, workspace_id: &str) -> Result<()> {
        let response = self.invoke_json("closing Herdr workspace", |command| {
            command.args(["workspace", "close", workspace_id]);
        })?;
        expect_response_type(&response, "closing Herdr workspace", "ok")
    }

    pub fn start_omp(
        &self,
        workspace: &WorkerWorkspace,
        run_id: &str,
        task_id: &str,
        model: Option<&str>,
    ) -> Result<Worker> {
        let name = worker_name(run_id, task_id);
        let response = self.invoke_json("starting OMP worker", |command| {
            command
                .args(["agent", "start", &name, "--kind", "omp", "--pane"])
                .arg(&workspace.pane_id);
            if let Some(model) = model {
                command.args(["--", "--model", model]);
            }
        })?;
        let agent = parse_agent(&response, "starting OMP worker", "agent_started")?;
        validate_agent(
            &agent,
            &name,
            &workspace.workspace_id,
            &workspace.pane_id,
            "starting OMP worker",
        )?;

        Ok(Worker {
            name,
            workspace_id: workspace.workspace_id.clone(),
            pane_id: workspace.pane_id.clone(),
        })
    }

    pub fn prompt_after_handoff(
        &self,
        worker: &Worker,
        handoff_receipt: &str,
        prompt: &str,
    ) -> Result<()> {
        if handoff_receipt.trim().is_empty() {
            return Err(Error::MissingHandoffReceipt);
        }

        let response = self.invoke_json("prompting OMP worker", |command| {
            command.args(["agent", "prompt", &worker.name, prompt]);
        })?;
        let agent = parse_agent(&response, "prompting OMP worker", "agent_prompted")?;
        validate_agent(
            &agent,
            &worker.name,
            &worker.workspace_id,
            &worker.pane_id,
            "prompting OMP worker",
        )?;
        Ok(())
    }

    pub fn status(&self, worker: &Worker) -> Result<AgentStatus> {
        let response = self.invoke_json("reading OMP worker", |command| {
            command.args(["agent", "get", &worker.name]);
        })?;
        let agent = parse_agent(&response, "reading OMP worker", "agent_info")?;
        validate_agent(
            &agent,
            &worker.name,
            &worker.workspace_id,
            &worker.pane_id,
            "reading OMP worker",
        )?;
        Ok(agent.agent_status)
    }

    fn invoke(
        &self,
        operation: &'static str,
        configure: impl FnOnce(&mut Command),
    ) -> Result<Output> {
        let mut command = Command::new(&self.executable);
        command.args(["--session", &self.session]);
        configure(&mut command);
        command.output().map_err(|source| Error::Start {
            executable: self.executable.clone(),
            session: self.session.clone(),
            operation,
            source,
        })
    }

    fn invoke_json(
        &self,
        operation: &'static str,
        configure: impl FnOnce(&mut Command),
    ) -> Result<Value> {
        let output = self.invoke(operation, configure)?;
        if !output.status.success() {
            return Err(Error::CommandFailed {
                operation,
                status: output.status,
                detail: failure_detail(&output),
            });
        }
        serde_json::from_slice(&output.stdout)
            .map_err(|source| Error::InvalidJson { operation, source })
    }
}

#[derive(Debug, Deserialize)]
struct ServerStatus {
    running: bool,
    compatible: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct AgentResponse {
    name: Option<String>,
    workspace_id: String,
    pane_id: String,
    agent_status: AgentStatus,
}

fn parse_agent(
    response: &Value,
    operation: &'static str,
    expected_type: &str,
) -> Result<AgentResponse> {
    expect_response_type(response, operation, expected_type)?;
    serde_json::from_value(response.pointer("/result/agent").cloned().ok_or_else(|| {
        Error::UnexpectedResponse {
            operation,
            detail: "missing `result.agent`".to_owned(),
        }
    })?)
    .map_err(|source| Error::InvalidJson { operation, source })
}

fn validate_agent(
    agent: &AgentResponse,
    expected_name: &str,
    expected_workspace_id: &str,
    expected_pane_id: &str,
    operation: &'static str,
) -> Result<()> {
    if agent.name.as_deref() != Some(expected_name) {
        return Err(Error::UnexpectedResponse {
            operation,
            detail: format!(
                "returned agent name `{}`, expected `{expected_name}`",
                agent.name.as_deref().unwrap_or("<missing>")
            ),
        });
    }
    if agent.workspace_id != expected_workspace_id || agent.pane_id != expected_pane_id {
        return Err(Error::UnexpectedResponse {
            operation,
            detail: format!(
                "returned workspace/pane `{}/{}`, expected `{expected_workspace_id}/{expected_pane_id}`",
                agent.workspace_id, agent.pane_id
            ),
        });
    }
    Ok(())
}

fn expect_response_type(response: &Value, operation: &'static str, expected: &str) -> Result<()> {
    let actual = response
        .pointer("/result/type")
        .and_then(Value::as_str)
        .unwrap_or("<missing>");
    if actual == expected {
        Ok(())
    } else {
        Err(Error::UnexpectedResponse {
            operation,
            detail: format!("returned result type `{actual}`, expected `{expected}`"),
        })
    }
}

fn response_string(response: &Value, pointer: &str, operation: &'static str) -> Result<String> {
    response
        .pointer(pointer)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| Error::UnexpectedResponse {
            operation,
            detail: format!("missing non-empty `{pointer}`"),
        })
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
        session: String,
        operation: &'static str,
        source: io::Error,
    },
    SessionUnavailable {
        session: String,
        detail: String,
    },
    CommandFailed {
        operation: &'static str,
        status: ExitStatus,
        detail: String,
    },
    InvalidJson {
        operation: &'static str,
        source: serde_json::Error,
    },
    UnexpectedResponse {
        operation: &'static str,
        detail: String,
    },
    MissingHandoffReceipt,
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Start {
                executable,
                session,
                operation,
                source,
            } => write!(
                formatter,
                "cannot start Herdr executable `{}` for session `{session}` while {operation}: {source}",
                executable.to_string_lossy()
            ),
            Self::SessionUnavailable { session, detail } => {
                write!(formatter, "Herdr session `{session}` is unavailable: {detail}")
            }
            Self::CommandFailed {
                operation,
                status,
                detail,
            } => write!(formatter, "Herdr failed while {operation} ({status}): {detail}"),
            Self::InvalidJson { operation, source } => {
                write!(formatter, "Herdr returned invalid JSON while {operation}: {source}")
            }
            Self::UnexpectedResponse { operation, detail } => {
                write!(formatter, "Herdr returned an unexpected response while {operation}: {detail}")
            }
            Self::MissingHandoffReceipt => write!(
                formatter,
                "cannot prompt OMP worker before a durable AgentMail handoff receipt exists"
            ),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Start { source, .. } => Some(source),
            Self::InvalidJson { source, .. } => Some(source),
            _ => None,
        }
    }
}
