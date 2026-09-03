use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use fs2::FileExt;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::herdr::HerdrHandles;
use crate::identity::Identity;
use crate::mail::WorkerReport;
use crate::sq::PlanSnapshot;

const LEDGER_VERSION: u32 = 1;

#[derive(Debug)]
pub struct RuntimeStore {
    ledger_path: PathBuf,
    _lock: File,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RuntimeLedger {
    pub version: u32,
    pub run_id: String,
    pub root_task_id: String,
    pub queue: PathBuf,
    pub repo: PathBuf,
    pub orchestrator: Identity,
    pub plan: PlanSnapshot,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub task_runs: BTreeMap<String, TaskRun>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RunState {
    Provisioning,
    Working,
    Blocked,
    Failed,
    Completed,
    Lost,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TaskRun {
    pub task_id: String,
    pub run_id: String,
    pub state: RunState,
    pub worktree: PathBuf,
    pub branch: String,
    pub base: String,
    #[serde(default)]
    pub worker: Option<Identity>,
    #[serde(default)]
    pub herdr: Option<HerdrHandles>,
    #[serde(default)]
    pub handoff_message_id: Option<String>,
    #[serde(default)]
    pub report_message_id: Option<String>,
    #[serde(default)]
    pub report: Option<WorkerReport>,
    pub heartbeat_at: DateTime<Utc>,
    pub lease_expires_at: DateTime<Utc>,
    #[serde(default)]
    pub detail: Option<String>,
}

impl TaskRun {
    pub fn provisioning(
        task_id: String,
        run_id: String,
        worktree: PathBuf,
        branch: String,
        base: String,
        lease_seconds: u64,
    ) -> Self {
        let now = Utc::now();
        Self {
            task_id,
            run_id,
            state: RunState::Provisioning,
            worktree,
            branch,
            base,
            worker: None,
            herdr: None,
            handoff_message_id: None,
            report_message_id: None,
            report: None,
            heartbeat_at: now,
            lease_expires_at: lease_deadline(now, lease_seconds),
            detail: None,
        }
    }

    pub fn heartbeat(&mut self, lease_seconds: u64) {
        let now = Utc::now();
        self.heartbeat_at = now;
        self.lease_expires_at = lease_deadline(now, lease_seconds);
    }

    pub fn lease_expired(&self) -> bool {
        Utc::now() >= self.lease_expires_at
    }
}

impl RuntimeStore {
    pub fn open(state_dir: &Path, root_task_id: &str) -> Result<Self> {
        fs::create_dir_all(state_dir).with_context(|| {
            format!(
                "failed to create runtime state directory {}",
                state_dir.display()
            )
        })?;
        let name = safe_component(root_task_id);
        let lock_path = state_dir.join(format!("{name}.lock"));
        let ledger_path = state_dir.join(format!("{name}.json"));
        let lock = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&lock_path)
            .with_context(|| format!("failed to open runtime lock {}", lock_path.display()))?;
        FileExt::try_lock_exclusive(&lock).with_context(|| {
            format!("another agent-orchestrator process is reconciling root task {root_task_id}")
        })?;

        Ok(Self {
            ledger_path,
            _lock: lock,
        })
    }

    pub fn has_ledger(&self) -> bool {
        self.ledger_path.exists()
    }

    pub fn load_or_create(
        &self,
        root_task_id: &str,
        queue: &Path,
        repo: &Path,
        orchestrator: &Identity,
        plan: &PlanSnapshot,
    ) -> Result<RuntimeLedger> {
        if self.ledger_path.exists() {
            let bytes = fs::read(&self.ledger_path).with_context(|| {
                format!(
                    "failed to read runtime ledger {}",
                    self.ledger_path.display()
                )
            })?;
            let ledger: RuntimeLedger = serde_json::from_slice(&bytes).with_context(|| {
                format!(
                    "failed to parse runtime ledger {}",
                    self.ledger_path.display()
                )
            })?;
            if ledger.version != LEDGER_VERSION {
                bail!(
                    "runtime ledger {} has unsupported version {}",
                    self.ledger_path.display(),
                    ledger.version
                );
            }
            if ledger.root_task_id != root_task_id || ledger.queue != queue || ledger.repo != repo {
                bail!("runtime ledger scope does not match this finish invocation");
            }
            if ledger.orchestrator.session_id != orchestrator.session_id {
                bail!(
                    "runtime ledger is owned by orchestrator {} ({})",
                    ledger.orchestrator.name,
                    ledger.orchestrator.session_id
                );
            }
            if &ledger.plan != plan {
                bail!(
                    "SQ plan drift detected for root task {root_task_id}; resolve the plan before resuming"
                );
            }
            return Ok(ledger);
        }

        let now = Utc::now();
        let ledger = RuntimeLedger {
            version: LEDGER_VERSION,
            run_id: Ulid::generate().to_string(),
            root_task_id: root_task_id.to_owned(),
            queue: queue.to_path_buf(),
            repo: repo.to_path_buf(),
            orchestrator: orchestrator.clone(),
            plan: plan.clone(),
            created_at: now,
            updated_at: now,
            task_runs: BTreeMap::new(),
        };
        self.save(&ledger)?;
        Ok(ledger)
    }

    pub fn save(&self, ledger: &RuntimeLedger) -> Result<()> {
        let parent = self
            .ledger_path
            .parent()
            .context("runtime ledger path has no parent directory")?;
        let temporary = parent.join(format!(".{}.tmp", Ulid::generate()));
        let mut file = File::create(&temporary)
            .with_context(|| format!("failed to create runtime ledger {}", temporary.display()))?;
        serde_json::to_writer_pretty(&mut file, ledger)
            .context("failed to serialize runtime ledger")?;
        file.write_all(b"\n")
            .context("failed to finish runtime ledger")?;
        file.sync_all().context("failed to sync runtime ledger")?;
        fs::rename(&temporary, &self.ledger_path).with_context(|| {
            format!(
                "failed to replace runtime ledger {}",
                self.ledger_path.display()
            )
        })?;
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.ledger_path
    }
}

fn lease_deadline(now: DateTime<Utc>, lease_seconds: u64) -> DateTime<Utc> {
    let seconds = i64::try_from(lease_seconds).unwrap_or(i64::MAX);
    now.checked_add_signed(Duration::seconds(seconds))
        .unwrap_or(DateTime::<Utc>::MAX_UTC)
}

fn safe_component(value: &str) -> String {
    let value = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                character
            } else {
                '-'
            }
        })
        .collect::<String>();
    if value.is_empty() {
        "root".to_owned()
    } else {
        value
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Map};

    use super::*;

    fn identity(session: &str) -> Identity {
        Identity {
            session_id: session.to_owned(),
            name: "Orchestrator".to_owned(),
            slug: "orchestrator".to_owned(),
            cwd: None,
            state: None,
            extensions: Map::from_iter([("omp".to_owned(), json!({}))]),
        }
    }

    fn plan(title: &str) -> PlanSnapshot {
        serde_json::from_value(json!({
            "root_id": "root",
            "tasks": [{
                "id": "root",
                "title": title,
                "description": "description",
                "sources": [],
                "metadata": {},
                "blocked_by": []
            }]
        }))
        .unwrap()
    }

    #[test]
    fn persisted_ledger_rejects_plan_drift_and_new_owner() {
        let directory = tempfile::tempdir().unwrap();
        let store = RuntimeStore::open(directory.path(), "root").unwrap();
        let queue = Path::new("/repo/.sift/issues.jsonl");
        let repo = Path::new("/repo");
        let first = store
            .load_or_create("root", queue, repo, &identity("one"), &plan("Root"))
            .unwrap();
        assert_eq!(first.root_task_id, "root");
        assert!(store
            .load_or_create("root", queue, repo, &identity("one"), &plan("Changed"))
            .unwrap_err()
            .to_string()
            .contains("plan drift"));
        assert!(store
            .load_or_create("root", queue, repo, &identity("two"), &plan("Root"))
            .unwrap_err()
            .to_string()
            .contains("owned by orchestrator"));
    }
}
