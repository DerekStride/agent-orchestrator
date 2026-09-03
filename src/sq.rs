use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    ffi::{OsStr, OsString},
    fmt,
    fs::File,
    io::{self, BufRead, BufReader},
    path::{Path, PathBuf},
    process::Command,
};

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

pub const SQ_EXECUTABLE_ENV: &str = "AGENT_ORCHESTRATOR_SQ";
pub const SQ_QUEUE_ENV: &str = "SQ_QUEUE_PATH";
const OWNERSHIP_NAMESPACE: &str = "agent_orchestrator";

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Pending,
    Blocked,
    InProgress,
    Closed,
}

impl TaskStatus {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "blocked" => Some(Self::Blocked),
            "in_progress" => Some(Self::InProgress),
            "closed" => Some(Self::Closed),
            _ => None,
        }
    }
}

impl fmt::Display for TaskStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Pending => "pending",
            Self::Blocked => "blocked",
            Self::InProgress => "in_progress",
            Self::Closed => "closed",
        })
    }
}

#[derive(Debug, Clone)]
pub struct Task {
    id: String,
    title: String,
    description: String,
    stored_status: TaskStatus,
    blocked_by: Vec<String>,
    acceptance_criteria: Vec<String>,
    validation_checks: Vec<String>,
    raw: Value,
}

impl Task {
    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn title(&self) -> &str {
        &self.title
    }
    pub fn description(&self) -> &str {
        &self.description
    }

    pub fn stored_status(&self) -> TaskStatus {
        self.stored_status
    }

    pub fn blocked_by(&self) -> &[String] {
        &self.blocked_by
    }
    pub fn acceptance_criteria(&self) -> &[String] {
        &self.acceptance_criteria
    }

    pub fn validation_checks(&self) -> &[String] {
        &self.validation_checks
    }

    pub fn metadata(&self) -> Option<&Map<String, Value>> {
        self.raw.get("metadata").and_then(Value::as_object)
    }

    pub fn raw(&self) -> &Value {
        &self.raw
    }

    pub fn owner_run_id(&self) -> Option<&str> {
        self.metadata()?
            .get(OWNERSHIP_NAMESPACE)?
            .as_object()?
            .get("run_id")?
            .as_str()
    }

    fn plan_value(&self) -> Value {
        let mut value = self.raw.clone();
        let object = value
            .as_object_mut()
            .expect("task JSON is validated as an object when loaded");

        object.remove("status");
        object.remove("created_at");
        object.remove("updated_at");
        if self.blocked_by.is_empty() {
            object.remove("blocked_by");
        }

        if let Some(Value::Object(mut metadata)) = object.remove("metadata") {
            metadata.remove(OWNERSHIP_NAMESPACE);
            if !metadata.is_empty() {
                object.insert("metadata".to_owned(), Value::Object(metadata));
            }
        }

        value
    }
}

#[derive(Debug, Clone)]
pub struct Queue {
    path: PathBuf,
    tasks: BTreeMap<String, Task>,
}

impl Queue {
    pub fn read(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let file = File::open(&path).map_err(|source| Error::ReadQueue {
            path: path.clone(),
            source,
        })?;
        let mut tasks = BTreeMap::new();

        for (index, line) in BufReader::new(file).lines().enumerate() {
            let line_number = index + 1;
            let line = line.map_err(|source| Error::ReadQueue {
                path: path.clone(),
                source,
            })?;
            if line.trim().is_empty() {
                continue;
            }

            let raw = serde_json::from_str(&line).map_err(|source| Error::InvalidJson {
                path: path.clone(),
                line: line_number,
                source,
            })?;
            let task = parse_task(raw, line_number)?;
            let id = task.id.clone();
            if tasks.insert(id.clone(), task).is_some() {
                return Err(Error::DuplicateTask { task_id: id });
            }
        }

        Ok(Self { path, tasks })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn task(&self, task_id: &str) -> Option<&Task> {
        self.tasks.get(task_id)
    }

    pub fn scope(self, root_task_id: &str) -> Result<ScopedPlan> {
        if !self.tasks.contains_key(root_task_id) {
            return Err(Error::MissingRoot {
                task_id: root_task_id.to_owned(),
            });
        }

        let mut marks = BTreeMap::new();
        let mut stack = Vec::new();
        let mut task_ids = BTreeSet::new();
        visit(&self, root_task_id, &mut marks, &mut stack, &mut task_ids)?;

        let snapshot = PlanSnapshot {
            root_task_id: root_task_id.to_owned(),
            tasks: task_ids
                .iter()
                .map(|task_id| {
                    let task = self
                        .tasks
                        .get(task_id)
                        .expect("scope IDs come from the loaded queue");
                    (task_id.clone(), task.plan_value())
                })
                .collect(),
        };

        Ok(ScopedPlan {
            queue: self,
            root_task_id: root_task_id.to_owned(),
            task_ids,
            snapshot,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanSnapshot {
    root_task_id: String,
    tasks: BTreeMap<String, Value>,
}

impl PlanSnapshot {
    pub fn root_task_id(&self) -> &str {
        &self.root_task_id
    }

    pub fn tasks(&self) -> &BTreeMap<String, Value> {
        &self.tasks
    }
}

#[derive(Debug, Clone)]
pub struct ScopedPlan {
    queue: Queue,
    root_task_id: String,
    task_ids: BTreeSet<String>,
    snapshot: PlanSnapshot,
}

impl ScopedPlan {
    pub fn root_task_id(&self) -> &str {
        &self.root_task_id
    }

    pub fn queue_path(&self) -> &Path {
        self.queue.path()
    }

    pub fn task_ids(&self) -> impl ExactSizeIterator<Item = &str> {
        self.task_ids.iter().map(String::as_str)
    }

    pub fn task(&self, task_id: &str) -> Option<&Task> {
        self.task_ids
            .contains(task_id)
            .then(|| self.queue.task(task_id))
            .flatten()
    }

    pub fn status(&self, task_id: &str) -> Result<TaskStatus> {
        let task = self.task(task_id).ok_or_else(|| Error::TaskOutsideScope {
            task_id: task_id.to_owned(),
            root_task_id: self.root_task_id.clone(),
        })?;
        Ok(self.effective_status(task))
    }

    pub fn ready_task_ids(&self) -> impl Iterator<Item = &str> {
        self.task_ids.iter().filter_map(|task_id| {
            let task = self
                .queue
                .task(task_id)
                .expect("scope IDs come from the loaded queue");
            (self.effective_status(task) == TaskStatus::Pending).then_some(task_id.as_str())
        })
    }

    pub fn snapshot(&self) -> &PlanSnapshot {
        &self.snapshot
    }

    pub fn validate_snapshot(&self, expected: &PlanSnapshot) -> Result<()> {
        if &self.snapshot == expected {
            Ok(())
        } else {
            Err(Error::PlanDrift {
                root_task_id: self.root_task_id.clone(),
            })
        }
    }

    pub fn validate_ownership(&self, run_id: &str) -> Result<()> {
        for task_id in &self.task_ids {
            let task = self
                .queue
                .task(task_id)
                .expect("scope IDs come from the loaded queue");
            if let Some(owner_run_id) = task.owner_run_id() {
                if owner_run_id != run_id {
                    return Err(Error::ForeignOwnership {
                        task_id: task_id.clone(),
                        owner_run_id: owner_run_id.to_owned(),
                        run_id: run_id.to_owned(),
                    });
                }
            }
        }
        Ok(())
    }

    fn effective_status(&self, task: &Task) -> TaskStatus {
        if task.stored_status != TaskStatus::Pending {
            return task.stored_status;
        }

        if task.blocked_by.iter().all(|blocker_id| {
            self.queue
                .task(blocker_id)
                .expect("blockers are validated while building the scope")
                .stored_status
                == TaskStatus::Closed
        }) {
            TaskStatus::Pending
        } else {
            TaskStatus::Blocked
        }
    }
}

#[derive(Debug, Clone)]
pub struct SqClient {
    executable: OsString,
    queue: PathBuf,
}

impl SqClient {
    pub fn configured(queue: impl Into<PathBuf>) -> Self {
        let executable = env::var_os(SQ_EXECUTABLE_ENV)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| OsString::from("sq"));
        Self::with_executable(queue, executable)
    }

    pub fn with_executable(queue: impl Into<PathBuf>, executable: impl Into<OsString>) -> Self {
        Self {
            executable: executable.into(),
            queue: queue.into(),
        }
    }

    pub fn executable(&self) -> &OsStr {
        &self.executable
    }

    pub fn queue_path(&self) -> &Path {
        &self.queue
    }

    pub fn plan(&self, root_task_id: &str) -> Result<ScopedPlan> {
        Queue::read(&self.queue)?.scope(root_task_id)
    }

    pub fn validated_plan(
        &self,
        root_task_id: &str,
        run_id: &str,
        expected: Option<&PlanSnapshot>,
    ) -> Result<ScopedPlan> {
        let plan = self.plan(root_task_id)?;
        if let Some(expected) = expected {
            plan.validate_snapshot(expected)?;
        }
        plan.validate_ownership(run_id)?;
        Ok(plan)
    }

    pub fn claim(&self, plan: &ScopedPlan, task_id: &str, run_id: &str) -> Result<ScopedPlan> {
        let current = self.validated_plan(plan.root_task_id(), run_id, Some(plan.snapshot()))?;
        let status = current.status(task_id)?;
        if status != TaskStatus::Pending {
            return Err(Error::TaskNotReady {
                task_id: task_id.to_owned(),
                status,
            });
        }

        let ownership = json!({OWNERSHIP_NAMESPACE: {"run_id": run_id}}).to_string();
        let output = Command::new(&self.executable)
            .arg("edit")
            .arg(task_id)
            .arg("--queue")
            .arg(&self.queue)
            .arg("--set-status")
            .arg("in_progress")
            .arg("--merge-metadata")
            .arg(ownership)
            .arg("--json")
            .output()
            .map_err(|source| Error::StartSq {
                executable: self.executable.clone(),
                source,
            })?;

        if !output.status.success() {
            return Err(Error::SqFailed {
                executable: self.executable.clone(),
                status: output.status.code(),
                stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
            });
        }

        let claimed = self.validated_plan(plan.root_task_id(), run_id, Some(plan.snapshot()))?;
        let task = claimed
            .task(task_id)
            .expect("the pre-claim validation ensures the task is in scope");
        if task.stored_status() != TaskStatus::InProgress || task.owner_run_id() != Some(run_id) {
            return Err(Error::ClaimNotPersisted {
                task_id: task_id.to_owned(),
                expected_run_id: run_id.to_owned(),
                actual_status: task.stored_status(),
                actual_run_id: task.owner_run_id().map(str::to_owned),
            });
        }

        Ok(claimed)
    }
}

pub fn resolve_queue_path(explicit: Option<&Path>, search_root: &Path) -> PathBuf {
    if let Some(path) = explicit {
        return path.to_owned();
    }
    if let Some(path) = env::var_os(SQ_QUEUE_ENV).filter(|value| !value.is_empty()) {
        return PathBuf::from(path);
    }

    for directory in search_root.ancestors() {
        let candidate = directory.join(".sift/issues.jsonl");
        if candidate.is_file() {
            return candidate;
        }
    }

    search_root.join(".sift/issues.jsonl")
}

#[derive(Debug)]
pub enum Error {
    ReadQueue {
        path: PathBuf,
        source: io::Error,
    },
    InvalidJson {
        path: PathBuf,
        line: usize,
        source: serde_json::Error,
    },
    InvalidTask {
        line: usize,
        reason: String,
    },
    DuplicateTask {
        task_id: String,
    },
    MissingRoot {
        task_id: String,
    },
    MissingBlocker {
        task_id: String,
        blocker_id: String,
    },
    DependencyCycle {
        task_ids: Vec<String>,
    },
    PlanDrift {
        root_task_id: String,
    },
    ForeignOwnership {
        task_id: String,
        owner_run_id: String,
        run_id: String,
    },
    TaskOutsideScope {
        task_id: String,
        root_task_id: String,
    },
    TaskNotReady {
        task_id: String,
        status: TaskStatus,
    },
    StartSq {
        executable: OsString,
        source: io::Error,
    },
    SqFailed {
        executable: OsString,
        status: Option<i32>,
        stderr: String,
    },
    ClaimNotPersisted {
        task_id: String,
        expected_run_id: String,
        actual_status: TaskStatus,
        actual_run_id: Option<String>,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ReadQueue { path, source } => {
                write!(formatter, "cannot read SQ queue `{}`: {source}", path.display())
            }
            Self::InvalidJson { path, line, source } => write!(
                formatter,
                "invalid JSON in SQ queue `{}` at line {line}: {source}",
                path.display()
            ),
            Self::InvalidTask { line, reason } => {
                write!(formatter, "invalid SQ task at line {line}: {reason}")
            }
            Self::DuplicateTask { task_id } => {
                write!(formatter, "SQ queue contains duplicate task ID `{task_id}`")
            }
            Self::MissingRoot { task_id } => {
                write!(formatter, "SQ root task `{task_id}` is missing")
            }
            Self::MissingBlocker {
                task_id,
                blocker_id,
            } => write!(
                formatter,
                "SQ task `{task_id}` declares missing blocker `{blocker_id}`"
            ),
            Self::DependencyCycle { task_ids } => {
                write!(formatter, "SQ dependency cycle: {}", task_ids.join(" -> "))
            }
            Self::PlanDrift { root_task_id } => {
                write!(formatter, "SQ plan drift for root task `{root_task_id}`")
            }
            Self::ForeignOwnership {
                task_id,
                owner_run_id,
                run_id,
            } => write!(
                formatter,
                "SQ task `{task_id}` is owned by foreign agent_orchestrator.run_id `{owner_run_id}` (current run `{run_id}`)"
            ),
            Self::TaskOutsideScope {
                task_id,
                root_task_id,
            } => write!(
                formatter,
                "SQ task `{task_id}` is outside root `{root_task_id}` dependency scope"
            ),
            Self::TaskNotReady { task_id, status } => {
                write!(formatter, "SQ task `{task_id}` is not ready (status: {status})")
            }
            Self::StartSq { executable, source } => write!(
                formatter,
                "cannot start configured SQ executable `{}`: {source}",
                Path::new(executable).display()
            ),
            Self::SqFailed {
                executable,
                status,
                stderr,
            } => write!(
                formatter,
                "configured SQ executable `{}` failed with status {}: {stderr}",
                Path::new(executable).display(),
                status.map_or_else(|| "signal".to_owned(), |code| code.to_string())
            ),
            Self::ClaimNotPersisted {
                task_id,
                expected_run_id,
                actual_status,
                actual_run_id,
            } => write!(
                formatter,
                "SQ did not persist claim for task `{task_id}`: expected status in_progress and run `{expected_run_id}`, got status {actual_status} and run {}",
                actual_run_id.as_deref().unwrap_or("<missing>")
            ),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::ReadQueue { source, .. } | Self::StartSq { source, .. } => Some(source),
            Self::InvalidJson { source, .. } => Some(source),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Visit {
    Visiting,
    Done,
}

fn visit(
    queue: &Queue,
    task_id: &str,
    marks: &mut BTreeMap<String, Visit>,
    stack: &mut Vec<String>,
    task_ids: &mut BTreeSet<String>,
) -> Result<()> {
    match marks.get(task_id) {
        Some(Visit::Done) => return Ok(()),
        Some(Visit::Visiting) => {
            let cycle_start = stack
                .iter()
                .position(|stack_id| stack_id == task_id)
                .expect("a visiting task is present in the DFS stack");
            let mut cycle = stack[cycle_start..].to_vec();
            cycle.push(task_id.to_owned());
            return Err(Error::DependencyCycle { task_ids: cycle });
        }
        None => {}
    }

    marks.insert(task_id.to_owned(), Visit::Visiting);
    stack.push(task_id.to_owned());
    let task = queue
        .task(task_id)
        .expect("the root and every traversed blocker are validated before visiting");

    for blocker_id in &task.blocked_by {
        if queue.task(blocker_id).is_none() {
            return Err(Error::MissingBlocker {
                task_id: task_id.to_owned(),
                blocker_id: blocker_id.clone(),
            });
        }
        visit(queue, blocker_id, marks, stack, task_ids)?;
    }

    let popped = stack.pop();
    debug_assert_eq!(popped.as_deref(), Some(task_id));
    marks.insert(task_id.to_owned(), Visit::Done);
    task_ids.insert(task_id.to_owned());
    Ok(())
}

fn parse_task(raw: Value, line: usize) -> Result<Task> {
    let object = raw.as_object().ok_or_else(|| Error::InvalidTask {
        line,
        reason: "task must be a JSON object".to_owned(),
    })?;
    let id = required_string(object, "id", line)?.to_owned();
    let title = required_string(object, "title", line)?.to_owned();
    let description = required_string(object, "description", line)?.to_owned();

    if id.is_empty() {
        return Err(Error::InvalidTask {
            line,
            reason: "`id` must not be empty".to_owned(),
        });
    }

    let status_text = required_string(object, "status", line)?;
    let stored_status = TaskStatus::parse(status_text).ok_or_else(|| Error::InvalidTask {
        line,
        reason: format!("unsupported status `{status_text}`"),
    })?;

    let blocked_by = match object.get("blocked_by") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(values)) => values
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .filter(|blocker_id| !blocker_id.is_empty())
                    .map(str::to_owned)
                    .ok_or_else(|| Error::InvalidTask {
                        line,
                        reason: "`blocked_by` must contain non-empty strings".to_owned(),
                    })
            })
            .collect::<Result<Vec<_>>>()?,
        Some(_) => {
            return Err(Error::InvalidTask {
                line,
                reason: "`blocked_by` must be an array".to_owned(),
            });
        }
    };

    validate_metadata(object.get("metadata"), line)?;
    let acceptance_criteria =
        metadata_strings(object.get("metadata"), "acceptance_criteria", line)?;
    let validation_checks = metadata_strings(object.get("metadata"), "checks", line)?;

    Ok(Task {
        id,
        title,
        description,
        stored_status,
        blocked_by,
        acceptance_criteria,
        validation_checks,
        raw,
    })
}

fn metadata_strings(metadata: Option<&Value>, key: &str, line: usize) -> Result<Vec<String>> {
    let Some(value) = metadata
        .and_then(Value::as_object)
        .and_then(|metadata| metadata.get(key))
    else {
        return Ok(Vec::new());
    };
    let values = value.as_array().ok_or_else(|| Error::InvalidTask {
        line,
        reason: format!("`metadata.{key}` must be an array"),
    })?;
    values
        .iter()
        .map(|value| {
            value
                .as_str()
                .filter(|value| !value.trim().is_empty())
                .map(str::to_owned)
                .ok_or_else(|| Error::InvalidTask {
                    line,
                    reason: format!("`metadata.{key}` must contain non-empty strings"),
                })
        })
        .collect()
}

fn required_string<'a>(object: &'a Map<String, Value>, key: &str, line: usize) -> Result<&'a str> {
    object
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| Error::InvalidTask {
            line,
            reason: format!("`{key}` must be a string"),
        })
}

fn validate_metadata(metadata: Option<&Value>, line: usize) -> Result<()> {
    let Some(metadata) = metadata else {
        return Ok(());
    };
    if metadata.is_null() {
        return Ok(());
    }
    let metadata = metadata.as_object().ok_or_else(|| Error::InvalidTask {
        line,
        reason: "`metadata` must be an object".to_owned(),
    })?;
    let Some(ownership) = metadata.get(OWNERSHIP_NAMESPACE) else {
        return Ok(());
    };
    if ownership.is_null() {
        return Ok(());
    }
    let ownership = ownership.as_object().ok_or_else(|| Error::InvalidTask {
        line,
        reason: format!("`metadata.{OWNERSHIP_NAMESPACE}` must be an object"),
    })?;
    if ownership
        .get("run_id")
        .is_some_and(|run_id| !run_id.is_string())
    {
        return Err(Error::InvalidTask {
            line,
            reason: format!("`metadata.{OWNERSHIP_NAMESPACE}.run_id` must be a string"),
        });
    }
    Ok(())
}
