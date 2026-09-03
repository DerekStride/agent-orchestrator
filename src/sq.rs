use std::collections::{BTreeMap, BTreeSet};
use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::{BufRead, BufReader, ErrorKind};
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

const OWNER_METADATA_KEY: &str = "agent_orchestrator";

#[derive(Clone, Debug)]
pub struct Sq {
    executable: PathBuf,
    queue: PathBuf,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    Blocked,
    InProgress,
    Closed,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Task {
    pub id: String,
    pub title: String,
    #[serde(default)]
    pub description: String,
    pub status: TaskStatus,
    #[serde(default)]
    pub sources: Vec<Value>,
    #[serde(default)]
    pub metadata: Map<String, Value>,
    #[serde(default)]
    pub blocked_by: Vec<String>,
}

impl Task {
    pub fn owner_run_id(&self) -> Option<&str> {
        self.metadata
            .get(OWNER_METADATA_KEY)?
            .get("run_id")?
            .as_str()
    }

    pub fn acceptance_criteria(&self) -> Vec<String> {
        let configured = self
            .metadata
            .get("acceptance_criteria")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        if configured.is_empty() {
            vec![self.description.clone()]
        } else {
            configured
        }
    }

    pub fn validation_checks(&self) -> Vec<String> {
        self.metadata
            .get("checks")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default()
    }
}

#[derive(Clone, Debug)]
pub struct TaskGraph {
    tasks: BTreeMap<String, Task>,
}

impl TaskGraph {
    pub fn load(path: &Path) -> Result<Self> {
        let file = File::open(path)
            .with_context(|| format!("failed to open SQ queue {}", path.display()))?;
        let mut tasks = BTreeMap::new();

        for (index, line) in BufReader::new(file).lines().enumerate() {
            let line_number = index + 1;
            let line = line.with_context(|| {
                format!(
                    "failed to read SQ queue {} line {line_number}",
                    path.display()
                )
            })?;
            if line.trim().is_empty() {
                continue;
            }

            let task: Task = serde_json::from_str(&line).with_context(|| {
                format!(
                    "failed to parse SQ queue {} line {line_number}",
                    path.display()
                )
            })?;
            if task.id.trim().is_empty() {
                bail!(
                    "SQ queue {} line {line_number} has an empty task ID",
                    path.display()
                );
            }
            let id = task.id.clone();
            if tasks.insert(id.clone(), task).is_some() {
                bail!(
                    "SQ queue {} contains duplicate task ID {id}",
                    path.display()
                );
            }
        }

        Ok(Self { tasks })
    }

    pub fn scope(&self, root_id: &str) -> Result<TaskScope> {
        if !self.tasks.contains_key(root_id) {
            bail!("SQ root task {root_id} does not exist");
        }

        let mut states = BTreeMap::new();
        let mut path = Vec::new();
        let mut scoped_ids = BTreeSet::new();
        self.visit(root_id, &mut states, &mut path, &mut scoped_ids)?;

        let tasks = scoped_ids
            .into_iter()
            .map(|id| {
                let task = self
                    .tasks
                    .get(&id)
                    .expect("visited task must exist")
                    .clone();
                (id, task)
            })
            .collect();

        Ok(TaskScope {
            root_id: root_id.to_owned(),
            tasks,
        })
    }

    fn visit(
        &self,
        task_id: &str,
        states: &mut BTreeMap<String, VisitState>,
        path: &mut Vec<String>,
        scoped_ids: &mut BTreeSet<String>,
    ) -> Result<()> {
        match states.get(task_id) {
            Some(VisitState::Visited) => return Ok(()),
            Some(VisitState::Visiting) => {
                let start = path.iter().position(|id| id == task_id).unwrap_or(0);
                let mut cycle = path[start..].to_vec();
                cycle.push(task_id.to_owned());
                bail!("SQ dependency cycle: {}", cycle.join(" -> "));
            }
            None => {}
        }

        let task = self.tasks.get(task_id).ok_or_else(|| {
            let owner = path.last().map(String::as_str).unwrap_or("root");
            anyhow::anyhow!("SQ task {owner} references missing blocker {task_id}")
        })?;

        states.insert(task_id.to_owned(), VisitState::Visiting);
        path.push(task_id.to_owned());
        for blocker_id in &task.blocked_by {
            self.visit(blocker_id, states, path, scoped_ids)?;
        }
        path.pop();
        states.insert(task_id.to_owned(), VisitState::Visited);
        scoped_ids.insert(task_id.to_owned());
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
enum VisitState {
    Visiting,
    Visited,
}

#[derive(Clone, Debug)]
pub struct TaskScope {
    root_id: String,
    tasks: BTreeMap<String, Task>,
}

impl TaskScope {
    pub fn root(&self) -> &Task {
        self.tasks
            .get(&self.root_id)
            .expect("scope root must be present")
    }

    pub fn tasks(&self) -> impl Iterator<Item = &Task> {
        self.tasks.values()
    }

    pub fn task(&self, id: &str) -> Option<&Task> {
        self.tasks.get(id)
    }

    pub fn ready_tasks(&self) -> Vec<&Task> {
        self.tasks
            .values()
            .filter(|task| task.status == TaskStatus::Pending)
            .filter(|task| {
                task.blocked_by.iter().all(|id| {
                    self.tasks
                        .get(id)
                        .is_some_and(|blocker| blocker.status == TaskStatus::Closed)
                })
            })
            .collect()
    }

    pub fn validate_ownership(&self, run_id: &str) -> Result<()> {
        for task in self.tasks.values() {
            if let Some(owner) = task.owner_run_id() {
                if owner != run_id {
                    bail!(
                        "SQ task {} is already owned by orchestrator run {owner}",
                        task.id
                    );
                }
            }
        }
        Ok(())
    }

    pub fn snapshot(&self) -> PlanSnapshot {
        let tasks = self
            .tasks
            .values()
            .map(|task| {
                let mut metadata = task.metadata.clone();
                metadata.remove(OWNER_METADATA_KEY);
                TaskPlan {
                    id: task.id.clone(),
                    title: task.title.clone(),
                    description: task.description.clone(),
                    sources: task.sources.clone(),
                    metadata,
                    blocked_by: task.blocked_by.clone(),
                }
            })
            .collect();

        PlanSnapshot {
            root_id: self.root_id.clone(),
            tasks,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PlanSnapshot {
    pub root_id: String,
    pub tasks: Vec<TaskPlan>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TaskPlan {
    pub id: String,
    pub title: String,
    pub description: String,
    pub sources: Vec<Value>,
    pub metadata: Map<String, Value>,
    pub blocked_by: Vec<String>,
}

impl Sq {
    pub fn from_environment(queue: &Path) -> Self {
        Self {
            executable: std::env::var_os("AGENT_ORCHESTRATOR_SQ")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("sq")),
            queue: queue.to_path_buf(),
        }
    }

    pub fn claim(&self, task_id: &str, run_id: &str) -> Result<Task> {
        let metadata = serde_json::json!({
            OWNER_METADATA_KEY: {
                "run_id": run_id,
            }
        })
        .to_string();
        let output = self.run([
            OsStr::new("--queue"),
            self.queue.as_os_str(),
            OsStr::new("edit"),
            OsStr::new(task_id),
            OsStr::new("--set-status"),
            OsStr::new("in_progress"),
            OsStr::new("--merge-metadata"),
            OsStr::new(&metadata),
            OsStr::new("--json"),
        ])?;
        let task: Task = serde_json::from_slice(&output.stdout)
            .context("SQ returned invalid JSON while claiming a task")?;
        if task.status != TaskStatus::InProgress || task.owner_run_id() != Some(run_id) {
            bail!("SQ did not persist ownership for task {task_id}");
        }
        Ok(task)
    }

    fn run<I, S>(&self, args: I) -> Result<Output>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let args = args
            .into_iter()
            .map(|argument| argument.as_ref().to_os_string())
            .collect::<Vec<OsString>>();
        let output = Command::new(&self.executable).args(&args).output();
        let output = match output {
            Ok(output) => output,
            Err(error) if error.kind() == ErrorKind::NotFound => {
                bail!(
                    "SQ executable {} is not available",
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
                "SQ command failed: {}",
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

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    fn graph(lines: &[&str]) -> (tempfile::TempDir, TaskGraph) {
        let directory = tempfile::tempdir().unwrap();
        let queue = directory.path().join("issues.jsonl");
        fs::write(&queue, lines.join("\n")).unwrap();
        let graph = TaskGraph::load(&queue).unwrap();
        (directory, graph)
    }

    #[test]
    fn scope_contains_only_root_and_transitive_blockers() {
        let (_directory, graph) = graph(&[
            r#"{"id":"root","title":"Root","status":"pending","blocked_by":["child"]}"#,
            r#"{"id":"child","title":"Child","status":"closed"}"#,
            r#"{"id":"unrelated","title":"Other","status":"pending"}"#,
        ]);

        let scope = graph.scope("root").unwrap();
        assert_eq!(
            scope
                .tasks()
                .map(|task| task.id.as_str())
                .collect::<Vec<_>>(),
            vec!["child", "root"]
        );
        assert_eq!(
            scope
                .ready_tasks()
                .iter()
                .map(|task| task.id.as_str())
                .collect::<Vec<_>>(),
            vec!["root"]
        );
    }

    #[test]
    fn scope_rejects_missing_blocker_and_cycle() {
        let (_directory, missing) =
            graph(&[r#"{"id":"root","title":"Root","status":"pending","blocked_by":["missing"]}"#]);
        assert!(missing
            .scope("root")
            .unwrap_err()
            .to_string()
            .contains("missing blocker"));

        let (_directory, cyclic) = graph(&[
            r#"{"id":"root","title":"Root","status":"pending","blocked_by":["child"]}"#,
            r#"{"id":"child","title":"Child","status":"pending","blocked_by":["root"]}"#,
        ]);
        assert_eq!(
            cyclic.scope("root").unwrap_err().to_string(),
            "SQ dependency cycle: root -> child -> root"
        );
    }

    #[test]
    fn ownership_is_namespaced_and_plan_snapshot_ignores_it() {
        let (_directory, graph) = graph(&[
            r#"{"id":"root","title":"Root","status":"pending","metadata":{"agent_orchestrator":{"run_id":"run-a"},"checks":["cargo test"]}}"#,
        ]);
        let scope = graph.scope("root").unwrap();

        scope.validate_ownership("run-a").unwrap();
        assert!(scope.validate_ownership("run-b").is_err());
        assert!(scope.snapshot().tasks[0]
            .metadata
            .get(OWNER_METADATA_KEY)
            .is_none());
    }
}
