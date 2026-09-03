use std::ffi::{OsStr, OsString};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug)]
pub struct Herdr {
    executable: PathBuf,
    session: String,
}

#[derive(Clone, Debug)]
pub struct WorktreeLaunch<'a> {
    pub repo: &'a Path,
    pub branch: &'a str,
    pub base: &'a str,
    pub worktree: &'a Path,
    pub label: &'a str,
    pub task_id: &'a str,
    pub run_id: &'a str,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct HerdrHandles {
    pub session: String,
    pub workspace_id: String,
    pub pane_id: String,
    pub agent_name: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentState {
    Working,
    Blocked,
    Done,
    Idle,
    Unknown,
}

impl Herdr {
    pub fn from_environment() -> Self {
        Self {
            executable: std::env::var_os("AGENT_ORCHESTRATOR_HERDR")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("herdr")),
            session: std::env::var("HERDR_SESSION").unwrap_or_else(|_| "default".to_owned()),
        }
    }

    pub fn preflight(&self) -> Result<()> {
        self.run([OsStr::new("status")]).map(|_| ())
    }

    pub fn create_worktree(&self, launch: &WorktreeLaunch<'_>) -> Result<HerdrHandles> {
        let workspace = self.run_json([
            OsStr::new("worktree"),
            OsStr::new("create"),
            OsStr::new("--cwd"),
            launch.repo.as_os_str(),
            OsStr::new("--branch"),
            OsStr::new(launch.branch),
            OsStr::new("--base"),
            OsStr::new(launch.base),
            OsStr::new("--path"),
            launch.worktree.as_os_str(),
            OsStr::new("--label"),
            OsStr::new(launch.label),
            OsStr::new("--no-focus"),
        ])?;
        let workspace_id = required_string(
            &workspace,
            &["result", "workspace", "workspace_id"],
            "workspace ID",
        )?;
        let pane_id = required_string(
            &workspace,
            &["result", "root_pane", "pane_id"],
            "root pane ID",
        )?;
        let agent_name = worker_name(launch.task_id, launch.run_id);

        self.run_json([
            OsStr::new("agent"),
            OsStr::new("start"),
            OsStr::new(&agent_name),
            OsStr::new("--kind"),
            OsStr::new("omp"),
            OsStr::new("--pane"),
            OsStr::new(&pane_id),
        ])
        .with_context(|| {
            format!(
                "Herdr worktree workspace {workspace_id} and pane {pane_id} were created, but OMP failed to start"
            )
        })?;

        Ok(HerdrHandles {
            session: self.session.clone(),
            workspace_id,
            pane_id,
            agent_name,
        })
    }

    pub fn prompt(&self, handles: &HerdrHandles, prompt: &str) -> Result<()> {
        self.run_json([
            OsStr::new("agent"),
            OsStr::new("prompt"),
            OsStr::new(&handles.agent_name),
            OsStr::new(prompt),
        ])
        .with_context(|| {
            format!(
                "OMP agent {} started in pane {}, but the task handoff prompt failed",
                handles.agent_name, handles.pane_id
            )
        })?;
        Ok(())
    }

    pub fn agent_state(&self, handles: &HerdrHandles) -> Result<AgentState> {
        let response = self.run_json([
            OsStr::new("agent"),
            OsStr::new("get"),
            OsStr::new(&handles.agent_name),
        ])?;
        let state = [
            &["result", "agent", "state"][..],
            &["result", "agent", "status"][..],
            &["result", "state"][..],
        ]
        .into_iter()
        .find_map(|path| string_at(&response, path))
        .ok_or_else(|| anyhow::anyhow!("Herdr agent response omitted lifecycle state"))?;

        match state {
            "working" => Ok(AgentState::Working),
            "blocked" => Ok(AgentState::Blocked),
            "done" => Ok(AgentState::Done),
            "idle" => Ok(AgentState::Idle),
            "unknown" => Ok(AgentState::Unknown),
            other => bail!("Herdr returned unsupported agent state {other}"),
        }
    }

    fn run_json<I, S>(&self, args: I) -> Result<Value>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let output = self.run(args)?;
        serde_json::from_slice(&output.stdout).with_context(|| {
            format!(
                "Herdr returned invalid JSON: {}",
                String::from_utf8_lossy(&output.stdout).trim()
            )
        })
    }

    fn run<I, S>(&self, args: I) -> Result<Output>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let mut command_args = vec![OsString::from("--session"), OsString::from(&self.session)];
        command_args.extend(args.into_iter().map(|arg| arg.as_ref().to_os_string()));
        let output = Command::new(&self.executable).args(&command_args).output();
        let output = match output {
            Ok(output) => output,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                bail!(
                    "Herdr is required for `finish` but executable {} is not available",
                    self.executable.display()
                )
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!(
                        "failed to run {} {}",
                        self.executable.display(),
                        display_args(&command_args)
                    )
                });
            }
        };

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
            bail!(
                "Herdr command `{}` failed: {}",
                display_args(&command_args),
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

fn string_at<'a>(value: &'a Value, path: &[&str]) -> Option<&'a str> {
    path.iter()
        .try_fold(value, |current, segment| current.get(segment))?
        .as_str()
}

fn required_string(value: &Value, path: &[&str], label: &str) -> Result<String> {
    string_at(value, path)
        .map(str::to_owned)
        .ok_or_else(|| anyhow::anyhow!("Herdr response omitted {label}"))
}

fn worker_name(task_id: &str, run_id: &str) -> String {
    let raw = format!(
        "ao-{task_id}-{}",
        run_id.chars().take(8).collect::<String>()
    );
    let mut name = raw
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    name.truncate(32);
    name
}

fn display_args(args: &[OsString]) -> String {
    args.iter()
        .map(|arg| arg.to_string_lossy())
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_names_are_valid_and_bounded() {
        let name = worker_name("Feature/Very.Long.Task", "01ABCDEF0123456789");
        assert!(name.starts_with("ao-feature-very-long-task"));
        assert!(name.len() <= 32);
        assert!(name.chars().all(|character| character.is_ascii_lowercase()
            || character.is_ascii_digit()
            || matches!(character, '-' | '_')));
    }

    #[test]
    fn parses_supported_agent_states() {
        let value = serde_json::json!({"result": {"agent": {"state": "blocked"}}});
        assert_eq!(
            string_at(&value, &["result", "agent", "state"]),
            Some("blocked")
        );
    }
}
