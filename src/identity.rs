use std::{
    env,
    ffi::{OsStr, OsString},
    fmt, fs, io,
    path::{Path, PathBuf},
    process::{Command, ExitStatus, Output},
};

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

pub const AGENT_ID_EXECUTABLE_ENV: &str = "AGENT_ORCHESTRATOR_AGENT_ID";

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Identity {
    pub session_id: String,
    pub name: String,
    pub slug: String,
    pub cwd: PathBuf,
    #[serde(default)]
    pub state: Option<IdentityState>,
    pub extensions: Map<String, Value>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct IdentityState {
    pub value: String,
}

impl Identity {
    fn validate_omp_registration(&self, operation: &'static str) -> Result<()> {
        for (field, value) in [
            ("session_id", self.session_id.as_str()),
            ("name", self.name.as_str()),
            ("slug", self.slug.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(Error::IncompleteIdentity { operation, field });
            }
        }
        if self.cwd.as_os_str().is_empty() {
            return Err(Error::IncompleteIdentity {
                operation,
                field: "cwd",
            });
        }
        if self.extensions.get("omp").is_none_or(Value::is_null) {
            return Err(Error::MissingOmpRegistration {
                operation,
                identity: self.name.clone(),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct AgentIdClient {
    executable: OsString,
}

impl AgentIdClient {
    pub fn configured() -> Self {
        let executable = env::var_os(AGENT_ID_EXECUTABLE_ENV)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| OsString::from("agent-id"));
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

    pub fn current(&self) -> Result<Identity> {
        let identity: Identity = self.invoke_json(
            "reading the current orchestrator identity",
            [OsStr::new("current"), OsStr::new("--json")],
        )?;
        identity.validate_omp_registration("reading the current orchestrator identity")?;
        Ok(identity)
    }

    pub fn discover_worker(&self, worktree: &Path) -> Result<Identity> {
        let identities: Vec<Identity> = self.invoke_json(
            "discovering the worker identity",
            [
                OsStr::new("discover"),
                OsStr::new("--recent"),
                OsStr::new("24"),
                OsStr::new("--json"),
            ],
        )?;
        let expected =
            fs::canonicalize(worktree).map_err(|source| Error::CanonicalizeWorktree {
                worktree: worktree.to_owned(),
                source,
            })?;
        let mut matches = Vec::new();

        for identity in identities {
            if identity.extensions.get("omp").is_none_or(Value::is_null)
                || identity
                    .state
                    .as_ref()
                    .is_some_and(|state| state.value == "stopped")
            {
                continue;
            }
            if fs::canonicalize(&identity.cwd).is_ok_and(|cwd| cwd == expected) {
                identity.validate_omp_registration("discovering the worker identity")?;
                matches.push(identity);
            }
        }

        match matches.len() {
            0 => Err(Error::WorkerNotFound {
                worktree: worktree.to_owned(),
            }),
            1 => Ok(matches.pop().expect("the match count was checked")),
            _ => Err(Error::MultipleWorkers {
                worktree: worktree.to_owned(),
                slugs: matches.into_iter().map(|identity| identity.slug).collect(),
            }),
        }
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
    IncompleteIdentity {
        operation: &'static str,
        field: &'static str,
    },
    MissingOmpRegistration {
        operation: &'static str,
        identity: String,
    },
    CanonicalizeWorktree {
        worktree: PathBuf,
        source: io::Error,
    },
    WorkerNotFound {
        worktree: PathBuf,
    },
    MultipleWorkers {
        worktree: PathBuf,
        slugs: Vec<String>,
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
                "cannot start Agent ID executable `{}` while {operation}: {source}",
                executable.to_string_lossy()
            ),
            Self::CommandFailed {
                operation,
                status,
                detail,
            } => write!(
                formatter,
                "Agent ID failed while {operation} ({status}): {detail}"
            ),
            Self::InvalidJson {
                operation, output, ..
            } => write!(
                formatter,
                "Agent ID returned invalid identity JSON while {operation}: {output}"
            ),
            Self::IncompleteIdentity { operation, field } => write!(
                formatter,
                "Agent ID returned an incomplete identity while {operation}: missing non-empty `{field}`"
            ),
            Self::MissingOmpRegistration {
                operation,
                identity,
            } => write!(
                formatter,
                "Agent ID identity `{identity}` returned while {operation} has no `extensions.omp` registration"
            ),
            Self::CanonicalizeWorktree { worktree, source } => write!(
                formatter,
                "cannot canonicalize worker worktree `{}` for Agent ID discovery: {source}",
                worktree.display()
            ),
            Self::WorkerNotFound { worktree } => write!(
                formatter,
                "Agent ID did not discover exactly one live OMP worker whose canonical cwd is `{}`: found none",
                worktree.display()
            ),
            Self::MultipleWorkers { worktree, slugs } => write!(
                formatter,
                "Agent ID did not discover exactly one live OMP worker whose canonical cwd is `{}`: found {} ({})",
                worktree.display(),
                slugs.len(),
                slugs.join(", ")
            ),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Start { source, .. } => Some(source),
            Self::CanonicalizeWorktree { source, .. } => Some(source),
            Self::InvalidJson { source, .. } => Some(source),
            _ => None,
        }
    }
}
