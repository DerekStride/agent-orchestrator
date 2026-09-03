use std::{
    collections::BTreeMap,
    fmt, fs,
    fs::{File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
    process,
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

use crate::{
    herdr::{Worker, WorkerWorkspace},
    identity::Identity,
    mail::ReportStatus,
    sq::PlanSnapshot,
    worktree::WorktreePlan,
};

const LEDGER_VERSION: u32 = 1;

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeStage {
    Claimed,
    WorktreeCreated,
    QueueLinked,
    WorkerStarted,
    WorkerIdentified,
    HandoffSent,
    Active,
    ReportReceived,
    Completed,
    Blocked,
    Failed,
}

impl RuntimeStage {
    pub fn is_reconcilable(self) -> bool {
        matches!(
            self,
            Self::Active | Self::ReportReceived | Self::Completed | Self::Blocked | Self::Failed
        )
    }

    pub fn is_complete(self) -> bool {
        self == Self::Completed
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct RuntimeRecord {
    pub task_id: String,
    pub stage: RuntimeStage,
    pub worktree: WorktreePlan,
    pub workspace_id: Option<String>,
    pub pane_id: Option<String>,
    pub worker_name: Option<String>,
    pub worker_identity: Option<Identity>,
    pub handoff_receipt: Option<String>,
    pub report_message_id: Option<String>,
    pub report_status: Option<ReportStatus>,
    pub claimed_at_unix: u64,
    pub lease_started_at_unix: Option<u64>,
    pub lease_expires_at_unix: Option<u64>,
    pub heartbeat_at_unix: Option<u64>,
}

impl RuntimeRecord {
    pub fn claimed(worktree: WorktreePlan, now: u64) -> Self {
        Self {
            task_id: worktree.task_id.clone(),
            stage: RuntimeStage::Claimed,
            worktree,
            workspace_id: None,
            pane_id: None,
            worker_name: None,
            worker_identity: None,
            handoff_receipt: None,
            report_message_id: None,
            report_status: None,
            claimed_at_unix: now,
            lease_started_at_unix: None,
            lease_expires_at_unix: None,
            heartbeat_at_unix: None,
        }
    }

    pub fn record_workspace(&mut self, workspace: &WorkerWorkspace) -> Result<()> {
        self.require_stage(RuntimeStage::Claimed)?;
        self.workspace_id = Some(workspace.workspace_id.clone());
        self.pane_id = Some(workspace.pane_id.clone());
        self.stage = RuntimeStage::WorktreeCreated;
        Ok(())
    }

    pub fn record_queue_link(&mut self) -> Result<()> {
        self.require_stage(RuntimeStage::WorktreeCreated)?;
        self.stage = RuntimeStage::QueueLinked;
        Ok(())
    }

    pub fn record_worker(&mut self, worker: &Worker) -> Result<()> {
        self.require_stage(RuntimeStage::QueueLinked)?;
        if self.workspace_id.as_deref() != Some(&worker.workspace_id)
            || self.pane_id.as_deref() != Some(&worker.pane_id)
        {
            return Err(Error::HandleMismatch {
                task_id: self.task_id.clone(),
                detail: "started worker does not match retained workspace and pane".to_owned(),
            });
        }
        self.worker_name = Some(worker.name.clone());
        self.stage = RuntimeStage::WorkerStarted;
        Ok(())
    }

    pub fn record_identity(&mut self, identity: Identity) -> Result<()> {
        self.require_stage(RuntimeStage::WorkerStarted)?;
        self.worker_identity = Some(identity);
        self.stage = RuntimeStage::WorkerIdentified;
        Ok(())
    }

    pub fn record_handoff(&mut self, receipt: String) -> Result<()> {
        self.require_stage(RuntimeStage::WorkerIdentified)?;
        if receipt.trim().is_empty() {
            return Err(Error::HandleMismatch {
                task_id: self.task_id.clone(),
                detail: "handoff receipt is empty".to_owned(),
            });
        }
        self.handoff_receipt = Some(receipt);
        self.stage = RuntimeStage::HandoffSent;
        Ok(())
    }

    pub fn activate(&mut self, now: u64, lease_seconds: u64) -> Result<()> {
        self.require_stage(RuntimeStage::HandoffSent)?;
        self.lease_started_at_unix = Some(now);
        self.lease_expires_at_unix = Some(now.saturating_add(lease_seconds));
        self.heartbeat_at_unix = Some(now);
        self.stage = RuntimeStage::Active;
        Ok(())
    }

    pub fn heartbeat(&mut self, now: u64) -> Result<()> {
        if self.stage != RuntimeStage::Active {
            return Err(Error::WrongStage {
                task_id: self.task_id.clone(),
                expected: RuntimeStage::Active,
                actual: self.stage,
            });
        }
        self.heartbeat_at_unix = Some(now);
        Ok(())
    }

    pub fn record_report(&mut self, message_id: String, status: ReportStatus) -> Result<()> {
        self.require_stage(RuntimeStage::Active)?;
        if message_id.trim().is_empty() {
            return Err(Error::HandleMismatch {
                task_id: self.task_id.clone(),
                detail: "worker report message ID is empty".to_owned(),
            });
        }
        self.report_message_id = Some(message_id);
        self.report_status = Some(status);
        self.stage = RuntimeStage::ReportReceived;
        Ok(())
    }

    pub fn settle_report(&mut self) -> Result<()> {
        self.require_stage(RuntimeStage::ReportReceived)?;
        self.stage = match self.report_status {
            Some(ReportStatus::Completed) => RuntimeStage::Completed,
            Some(ReportStatus::Blocked) => RuntimeStage::Blocked,
            Some(ReportStatus::Failed) => RuntimeStage::Failed,
            None => {
                return Err(Error::HandleMismatch {
                    task_id: self.task_id.clone(),
                    detail: "report status is missing".to_owned(),
                });
            }
        };
        Ok(())
    }

    pub fn workspace(&self) -> Result<WorkerWorkspace> {
        Ok(WorkerWorkspace {
            workspace_id: self
                .required(&self.workspace_id, "workspace_id")?
                .to_owned(),
            pane_id: self.required(&self.pane_id, "pane_id")?.to_owned(),
        })
    }

    pub fn worker(&self) -> Result<Worker> {
        let workspace = self.workspace()?;
        Ok(Worker {
            name: self.required(&self.worker_name, "worker_name")?.to_owned(),
            workspace_id: workspace.workspace_id,
            pane_id: workspace.pane_id,
        })
    }

    pub fn identity(&self) -> Result<&Identity> {
        self.worker_identity
            .as_ref()
            .ok_or_else(|| Error::MissingHandle {
                task_id: self.task_id.clone(),
                field: "worker_identity",
            })
    }

    pub fn receipt(&self) -> Result<&str> {
        self.required(&self.handoff_receipt, "handoff_receipt")
    }

    pub fn lease_expired(&self, now: u64) -> Result<bool> {
        let deadline = self
            .lease_expires_at_unix
            .ok_or_else(|| Error::MissingHandle {
                task_id: self.task_id.clone(),
                field: "lease_expires_at_unix",
            })?;
        Ok(now >= deadline)
    }

    pub fn validate_reconcilable(&self) -> Result<()> {
        if !self.stage.is_reconcilable() {
            return Err(Error::InterruptedProvisioning {
                task_id: self.task_id.clone(),
                stage: self.stage,
            });
        }
        let expected_report_status = match self.stage {
            RuntimeStage::Completed => Some(ReportStatus::Completed),
            RuntimeStage::Blocked => Some(ReportStatus::Blocked),
            RuntimeStage::Failed => Some(ReportStatus::Failed),
            _ => None,
        };
        if let Some(expected) = expected_report_status {
            if self.report_message_id.is_none() || self.report_status.as_ref() != Some(&expected) {
                return Err(Error::HandleMismatch {
                    task_id: self.task_id.clone(),
                    detail: format!(
                        "{:?} runtime does not retain its matching worker report",
                        self.stage
                    ),
                });
            }
            if self.stage == RuntimeStage::Completed {
                return Ok(());
            }
        }
        self.worker()?;
        self.identity()?;
        self.receipt()?;
        if self.lease_started_at_unix.is_none()
            || self.lease_expires_at_unix.is_none()
            || self.heartbeat_at_unix.is_none()
        {
            return Err(Error::MissingHandle {
                task_id: self.task_id.clone(),
                field: "lease or heartbeat",
            });
        }
        if self.stage == RuntimeStage::ReportReceived
            && (self.report_message_id.is_none() || self.report_status.is_none())
        {
            return Err(Error::MissingHandle {
                task_id: self.task_id.clone(),
                field: "worker report",
            });
        }
        Ok(())
    }

    fn required<'a>(&self, value: &'a Option<String>, field: &'static str) -> Result<&'a str> {
        value
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .ok_or_else(|| Error::MissingHandle {
                task_id: self.task_id.clone(),
                field,
            })
    }

    fn require_stage(&self, expected: RuntimeStage) -> Result<()> {
        if self.stage == expected {
            Ok(())
        } else {
            Err(Error::WrongStage {
                task_id: self.task_id.clone(),
                expected,
                actual: self.stage,
            })
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct RunLedger {
    pub version: u32,
    pub run_id: String,
    pub root_task_id: String,
    pub queue: PathBuf,
    pub repo: PathBuf,
    pub worktree_root: PathBuf,
    pub plan: PlanSnapshot,
    pub orchestrator: Identity,
    pub created_at_unix: u64,
    pub runtimes: BTreeMap<String, RuntimeRecord>,
}

impl RunLedger {
    pub fn new(
        run_id: String,
        root_task_id: String,
        queue: PathBuf,
        repo: PathBuf,
        worktree_root: PathBuf,
        plan: PlanSnapshot,
        orchestrator: Identity,
        now: u64,
    ) -> Self {
        Self {
            version: LEDGER_VERSION,
            run_id,
            root_task_id,
            queue,
            repo,
            worktree_root,
            plan,
            orchestrator,
            created_at_unix: now,
            runtimes: BTreeMap::new(),
        }
    }

    pub fn validate(
        &self,
        root_task_id: &str,
        queue: &Path,
        repo: &Path,
        worktree_root: &Path,
    ) -> Result<()> {
        if self.version != LEDGER_VERSION {
            return Err(Error::UnsupportedVersion {
                actual: self.version,
                expected: LEDGER_VERSION,
            });
        }
        for (field, expected, actual) in [
            ("root_task_id", root_task_id, self.root_task_id.as_str()),
            (
                "queue",
                queue.to_string_lossy().as_ref(),
                self.queue.to_string_lossy().as_ref(),
            ),
            (
                "repo",
                repo.to_string_lossy().as_ref(),
                self.repo.to_string_lossy().as_ref(),
            ),
            (
                "worktree_root",
                worktree_root.to_string_lossy().as_ref(),
                self.worktree_root.to_string_lossy().as_ref(),
            ),
        ] {
            if actual != expected {
                return Err(Error::LedgerMismatch {
                    field,
                    expected: expected.to_owned(),
                    actual: actual.to_owned(),
                });
            }
        }
        for (task_id, runtime) in &self.runtimes {
            if runtime.task_id != *task_id || runtime.worktree.task_id != *task_id {
                return Err(Error::HandleMismatch {
                    task_id: task_id.clone(),
                    detail: "runtime map key and retained task IDs disagree".to_owned(),
                });
            }
            runtime.validate_reconcilable()?;
        }
        Ok(())
    }

    pub fn is_complete(&self) -> bool {
        self.runtimes
            .values()
            .all(|runtime| runtime.stage.is_complete())
    }
}

#[derive(Debug)]
pub struct LedgerStore {
    path: PathBuf,
    _lock: File,
}

impl LedgerStore {
    pub fn acquire(state_dir: &Path, root_task_id: &str) -> Result<Self> {
        validate_state_component(root_task_id)?;
        fs::create_dir_all(state_dir).map_err(|source| Error::CreateStateDirectory {
            path: state_dir.to_owned(),
            source,
        })?;
        let path = state_dir.join(format!("{root_task_id}.json"));
        let lock_path = state_dir.join(format!("{root_task_id}.lock"));
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&lock_path)
            .map_err(|source| Error::OpenLock {
                path: lock_path.clone(),
                source,
            })?;
        lock.try_lock().map_err(|source| Error::LockBusy {
            path: lock_path,
            source: source.into(),
        })?;
        Ok(Self { path, _lock: lock })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn load(&self) -> Result<Option<RunLedger>> {
        match fs::read(&self.path) {
            Ok(contents) => serde_json::from_slice(&contents)
                .map(Some)
                .map_err(|source| Error::InvalidLedger {
                    path: self.path.clone(),
                    source,
                }),
            Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(source) => Err(Error::ReadLedger {
                path: self.path.clone(),
                source,
            }),
        }
    }

    pub fn save(&self, ledger: &RunLedger) -> Result<()> {
        let contents = serde_json::to_vec_pretty(ledger).map_err(Error::SerializeLedger)?;
        let parent = self
            .path
            .parent()
            .expect("a ledger path created from a state directory has a parent");
        let temp = parent.join(format!(
            ".{}.{}.tmp",
            self.path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("ledger"),
            process::id()
        ));
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
            .map_err(|source| Error::WriteLedger {
                path: temp.clone(),
                source,
            })?;
        let result = (|| {
            file.write_all(&contents)?;
            file.write_all(b"\n")?;
            file.sync_all()?;
            fs::rename(&temp, &self.path)?;
            File::open(parent)?.sync_all()?;
            Ok::<_, io::Error>(())
        })();
        if let Err(source) = result {
            let _ = fs::remove_file(&temp);
            return Err(Error::WriteLedger {
                path: self.path.clone(),
                source,
            });
        }
        Ok(())
    }
}

pub fn now_unix() -> Result<u64> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(Error::Clock)
}

pub fn new_run_id(root_task_id: &str) -> Result<String> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(Error::Clock)?;
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in root_task_id
        .bytes()
        .chain(process::id().to_le_bytes())
        .chain(duration.as_nanos().to_le_bytes())
    {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    Ok(format!("run-{hash:016x}"))
}

fn validate_state_component(value: &str) -> Result<()> {
    if value.is_empty() || value == "." || value == ".." || value.contains(['/', '\\']) {
        Err(Error::InvalidStateComponent {
            value: value.to_owned(),
        })
    } else {
        Ok(())
    }
}

#[derive(Debug)]
pub enum Error {
    InvalidStateComponent {
        value: String,
    },
    CreateStateDirectory {
        path: PathBuf,
        source: io::Error,
    },
    OpenLock {
        path: PathBuf,
        source: io::Error,
    },
    LockBusy {
        path: PathBuf,
        source: io::Error,
    },
    ReadLedger {
        path: PathBuf,
        source: io::Error,
    },
    InvalidLedger {
        path: PathBuf,
        source: serde_json::Error,
    },
    SerializeLedger(serde_json::Error),
    WriteLedger {
        path: PathBuf,
        source: io::Error,
    },
    UnsupportedVersion {
        actual: u32,
        expected: u32,
    },
    LedgerMismatch {
        field: &'static str,
        expected: String,
        actual: String,
    },
    InterruptedProvisioning {
        task_id: String,
        stage: RuntimeStage,
    },
    WrongStage {
        task_id: String,
        expected: RuntimeStage,
        actual: RuntimeStage,
    },
    MissingHandle {
        task_id: String,
        field: &'static str,
    },
    HandleMismatch {
        task_id: String,
        detail: String,
    },
    Clock(std::time::SystemTimeError),
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidStateComponent { value } => {
                write!(formatter, "root task ID `{value}` is not safe in a ledger path")
            }
            Self::CreateStateDirectory { path, source } => write!(
                formatter,
                "cannot create runtime state directory `{}`: {source}",
                path.display()
            ),
            Self::OpenLock { path, source } => {
                write!(formatter, "cannot open runtime ledger lock `{}`: {source}", path.display())
            }
            Self::LockBusy { path, source } => write!(
                formatter,
                "runtime ledger `{}` is locked by another finish process: {source}",
                path.display()
            ),
            Self::ReadLedger { path, source } => {
                write!(formatter, "cannot read runtime ledger `{}`: {source}", path.display())
            }
            Self::InvalidLedger { path, source } => write!(
                formatter,
                "runtime ledger `{}` is invalid JSON: {source}",
                path.display()
            ),
            Self::SerializeLedger(source) => {
                write!(formatter, "cannot serialize runtime ledger: {source}")
            }
            Self::WriteLedger { path, source } => {
                write!(formatter, "cannot write runtime ledger `{}`: {source}", path.display())
            }
            Self::UnsupportedVersion { actual, expected } => write!(
                formatter,
                "runtime ledger version `{actual}` is unsupported; expected `{expected}`"
            ),
            Self::LedgerMismatch {
                field,
                expected,
                actual,
            } => write!(
                formatter,
                "runtime ledger {field} `{actual}` does not match requested `{expected}`"
            ),
            Self::InterruptedProvisioning { task_id, stage } => write!(
                formatter,
                "task `{task_id}` has interrupted provisioning at stage `{stage:?}`; refusing a silent retry"
            ),
            Self::WrongStage {
                task_id,
                expected,
                actual,
            } => write!(
                formatter,
                "task `{task_id}` runtime stage is `{actual:?}`, expected `{expected:?}`"
            ),
            Self::MissingHandle { task_id, field } => {
                write!(formatter, "task `{task_id}` runtime is missing retained `{field}`")
            }
            Self::HandleMismatch { task_id, detail } => {
                write!(formatter, "task `{task_id}` retained handles disagree: {detail}")
            }
            Self::Clock(source) => write!(formatter, "system clock is before Unix epoch: {source}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::CreateStateDirectory { source, .. }
            | Self::OpenLock { source, .. }
            | Self::LockBusy { source, .. }
            | Self::ReadLedger { source, .. }
            | Self::WriteLedger { source, .. } => Some(source),
            Self::InvalidLedger { source, .. } | Self::SerializeLedger(source) => Some(source),
            Self::Clock(source) => Some(source),
            _ => None,
        }
    }
}
