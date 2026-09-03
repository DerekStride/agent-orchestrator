use std::ffi::{OsStr, OsString};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

#[derive(Clone, Debug)]
pub struct AgentId {
    executable: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct Identity {
    pub session_id: String,
    pub name: String,
    pub slug: String,
    #[serde(default)]
    pub cwd: Option<PathBuf>,
    #[serde(default)]
    pub state: Option<IdentityState>,
    #[serde(default)]
    pub extensions: Map<String, Value>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct IdentityState {
    pub value: String,
}

impl Identity {
    fn validate_omp_registration(&self) -> Result<()> {
        if self.session_id.trim().is_empty()
            || self.name.trim().is_empty()
            || self.slug.trim().is_empty()
        {
            bail!("Agent ID returned an incomplete identity");
        }
        if !self.extensions.contains_key("omp") {
            bail!(
                "Agent ID identity {} was not automatically registered by OMP",
                self.name
            );
        }
        Ok(())
    }
}

impl AgentId {
    pub fn from_environment() -> Self {
        Self {
            executable: std::env::var_os("AGENT_ORCHESTRATOR_AGENT_ID")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("agent-id")),
        }
    }

    pub fn current(&self) -> Result<Identity> {
        let identity: Identity = self.run_json([OsStr::new("current"), OsStr::new("--json")])?;
        identity.validate_omp_registration()?;
        Ok(identity)
    }

    pub fn discover_worker(&self, worktree: &Path) -> Result<Identity> {
        let identities: Vec<Identity> = self.run_json([
            OsStr::new("discover"),
            OsStr::new("--recent"),
            OsStr::new("24"),
            OsStr::new("--json"),
        ])?;
        let expected = normalized_path(worktree);
        let matches = identities
            .into_iter()
            .filter(|identity| identity.extensions.contains_key("omp"))
            .filter(|identity| {
                identity
                    .cwd
                    .as_deref()
                    .is_some_and(|cwd| normalized_path(cwd) == expected)
            })
            .collect::<Vec<_>>();

        match matches.as_slice() {
            [identity] => {
                identity.validate_omp_registration()?;
                Ok(identity.clone())
            }
            [] => bail!(
                "Agent ID did not discover an OMP worker registered for {}",
                worktree.display()
            ),
            many => bail!(
                "Agent ID discovered multiple OMP workers for {}: {}",
                worktree.display(),
                many.iter()
                    .map(|identity| identity.slug.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }
    }

    fn run_json<T, I, S>(&self, args: I) -> Result<T>
    where
        T: serde::de::DeserializeOwned,
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let args = args
            .into_iter()
            .map(|arg| arg.as_ref().to_os_string())
            .collect::<Vec<OsString>>();
        let output = run(&self.executable, &args)?;
        serde_json::from_slice(&output.stdout).with_context(|| {
            format!(
                "Agent ID returned invalid JSON: {}",
                String::from_utf8_lossy(&output.stdout).trim()
            )
        })
    }
}

fn normalized_path(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

fn run(executable: &Path, args: &[OsString]) -> Result<Output> {
    let output = Command::new(executable).args(args).output();
    let output = match output {
        Ok(output) => output,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            bail!(
                "Agent ID executable {} is not available",
                executable.display()
            )
        }
        Err(error) => {
            return Err(error).with_context(|| format!("failed to run {}", executable.display()));
        }
    };

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        bail!(
            "Agent ID command failed: {}",
            if stderr.is_empty() {
                format!("exit status {}", output.status)
            } else {
                stderr
            }
        );
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_identity_without_omp_registration() {
        let identity = Identity {
            session_id: "session".to_owned(),
            name: "Worker".to_owned(),
            slug: "worker".to_owned(),
            cwd: None,
            state: None,
            extensions: Map::new(),
        };

        assert!(identity.validate_omp_registration().is_err());
    }
}
