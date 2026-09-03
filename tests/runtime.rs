use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use agent_orchestrator::{
    herdr::{Worker, WorkerWorkspace},
    identity::Identity,
    runtime::{Error, LedgerStore, RunLedger, RuntimeRecord, RuntimeStage},
    sq::Queue,
    worktree::WorktreePlan,
};
use serde_json::{json, Map};

static NEXT_TEST_DIR: AtomicU64 = AtomicU64::new(0);

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let sequence = NEXT_TEST_DIR.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "agent-orchestrator-runtime-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn runtime_ledger_locks_and_persists_every_supervision_handle() {
    let directory = TestDir::new();
    let queue = directory.path().join("issues.jsonl");
    fs::write(
        &queue,
        format!(
            "{}\n",
            json!({
                "id": "root",
                "title": "root task",
                "description": "Implement root",
                "status": "pending",
                "sources": [],
                "metadata": {},
                "blocked_by": [],
            })
        ),
    )
    .unwrap();
    let plan = Queue::read(&queue).unwrap().scope("root").unwrap();
    let repo = directory.path().join("repo");
    let worktree_root = directory.path().join("worktrees");
    fs::create_dir(&repo).unwrap();
    fs::create_dir(&worktree_root).unwrap();

    let mut runtime = RuntimeRecord::claimed(
        WorktreePlan {
            task_id: "root".to_owned(),
            branch: "agent-orchestrator/root/root".to_owned(),
            base: "base-commit".to_owned(),
            path: worktree_root.join("repo.root"),
        },
        10,
    );
    let workspace = WorkerWorkspace {
        workspace_id: "workspace-1".to_owned(),
        pane_id: "workspace-1:pane-1".to_owned(),
    };
    runtime.record_workspace(&workspace).unwrap();
    runtime.record_queue_link().unwrap();
    runtime
        .record_worker(&Worker {
            name: "ao-root-123".to_owned(),
            workspace_id: workspace.workspace_id.clone(),
            pane_id: workspace.pane_id.clone(),
        })
        .unwrap();
    runtime.record_identity(identity(&repo)).unwrap();
    runtime.record_handoff("message-1".to_owned()).unwrap();
    runtime.activate(20, 900).unwrap();
    assert_eq!(runtime.stage, RuntimeStage::Active);
    assert_eq!(runtime.heartbeat_at_unix, Some(20));
    assert_eq!(runtime.lease_expires_at_unix, Some(920));

    let mut ledger = RunLedger::new(
        "run-1".to_owned(),
        "root".to_owned(),
        fs::canonicalize(&queue).unwrap(),
        fs::canonicalize(&repo).unwrap(),
        fs::canonicalize(&worktree_root).unwrap(),
        plan.snapshot().clone(),
        identity(&repo),
        10,
    );
    ledger.runtimes.insert("root".to_owned(), runtime);

    let state = directory.path().join("state");
    let store = LedgerStore::acquire(&state, "root").unwrap();
    let error = LedgerStore::acquire(&state, "root").unwrap_err();
    assert!(matches!(error, Error::LockBusy { .. }));
    store.save(&ledger).unwrap();
    let loaded = store.load().unwrap().unwrap();
    assert_eq!(loaded, ledger);
    loaded
        .validate(
            "root",
            &fs::canonicalize(queue).unwrap(),
            &fs::canonicalize(repo).unwrap(),
            &fs::canonicalize(worktree_root).unwrap(),
        )
        .unwrap();
}

fn identity(cwd: &Path) -> Identity {
    let mut extensions = Map::new();
    extensions.insert("omp".to_owned(), json!({"state": "working"}));
    Identity {
        session_id: "session-1".to_owned(),
        name: "Worker Agent".to_owned(),
        slug: "worker-agent".to_owned(),
        cwd: cwd.to_owned(),
        state: None,
        extensions,
    }
}
