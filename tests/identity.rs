#![cfg(unix)]

use std::{
    fs,
    os::unix::fs::{symlink, PermissionsExt},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use agent_orchestrator::identity::{AgentIdClient, Error};
use serde_json::json;

static NEXT_TEST_DIR: AtomicU64 = AtomicU64::new(0);

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let sequence = NEXT_TEST_DIR.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "agent-orchestrator-identity-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir(&path).expect("test directory should be created");
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
fn identity_current_requires_complete_omp_registration() {
    let directory = TestDir::new();
    let script = directory.path().join("fake-agent-id");
    let arguments = directory.path().join("arguments");
    let cwd = directory.path().join("orchestrator");
    fs::create_dir(&cwd).unwrap();
    write_agent_id(
        &script,
        &arguments,
        &json!({
            "session_id": "session-1",
            "name": "Orchestrator Agent",
            "slug": "orchestrator-agent",
            "cwd": cwd,
            "extensions": {"omp": {"data": {}}}
        }),
    );

    let identity = AgentIdClient::with_executable(&script).current().unwrap();
    assert_eq!(identity.session_id, "session-1");
    assert_eq!(identity.name, "Orchestrator Agent");
    assert_eq!(identity.slug, "orchestrator-agent");
    assert_eq!(identity.cwd, cwd);
    assert!(identity.extensions.contains_key("omp"));
    assert_eq!(fs::read_to_string(&arguments).unwrap(), "current\n--json\n");

    write_agent_id(
        &script,
        &arguments,
        &json!({
            "session_id": "session-1",
            "name": "Orchestrator Agent",
            "slug": "orchestrator-agent",
            "cwd": cwd,
            "extensions": {}
        }),
    );
    let error = AgentIdClient::with_executable(&script)
        .current()
        .unwrap_err();
    assert!(matches!(error, Error::MissingOmpRegistration { .. }));
    assert!(error.to_string().contains("extensions.omp"));

    write_agent_id(
        &script,
        &arguments,
        &json!({
            "session_id": "session-1",
            "name": "Orchestrator Agent",
            "slug": "orchestrator-agent",
            "extensions": {"omp": {}}
        }),
    );
    let error = AgentIdClient::with_executable(&script)
        .current()
        .unwrap_err();
    assert!(matches!(error, Error::InvalidJson { .. }));
    assert!(error.to_string().contains("invalid identity JSON"));
}

#[test]
fn identity_discovery_matches_one_canonical_worktree_cwd() {
    let directory = TestDir::new();
    let script = directory.path().join("fake-agent-id");
    let arguments = directory.path().join("arguments");
    let worktree = directory.path().join("worktree");
    let alias = directory.path().join("worktree-link");
    let other = directory.path().join("other");
    fs::create_dir(&worktree).unwrap();
    fs::create_dir(&other).unwrap();
    symlink(&worktree, &alias).unwrap();
    let identity = |session: &str, slug: &str, cwd: &Path| {
        json!({
            "session_id": session,
            "name": format!("Worker {slug}"),
            "slug": slug,
            "cwd": cwd,
            "extensions": {"omp": {"data": {}}}
        })
    };
    write_agent_id(
        &script,
        &arguments,
        &json!([
            identity("other-session", "other", &other),
            identity("worker-session", "worker", &alias)
        ]),
    );

    let worker = AgentIdClient::with_executable(&script)
        .discover_worker(&worktree)
        .unwrap();
    assert_eq!(worker.slug, "worker");
    assert_eq!(
        fs::read_to_string(&arguments).unwrap(),
        "discover\n--recent\n24\n--json\n"
    );

    write_agent_id(&script, &arguments, &json!([]));
    let error = AgentIdClient::with_executable(&script)
        .discover_worker(&worktree)
        .unwrap_err();
    assert!(matches!(error, Error::WorkerNotFound { .. }));

    write_agent_id(
        &script,
        &arguments,
        &json!([
            identity("worker-1", "worker-one", &worktree),
            identity("worker-2", "worker-two", &alias)
        ]),
    );
    let error = AgentIdClient::with_executable(&script)
        .discover_worker(&worktree)
        .unwrap_err();
    assert!(matches!(error, Error::MultipleWorkers { .. }));
    assert!(error.to_string().contains("worker-one, worker-two"));
}

fn write_agent_id(script: &Path, arguments: &Path, response: &serde_json::Value) {
    fs::write(
        script,
        format!(
            "#!/bin/sh\nprintf '%s\\n' \"$@\" > {}\nprintf '%s\\n' '{}'\n",
            shell_quote(arguments),
            response
        ),
    )
    .unwrap();
    let mut permissions = fs::metadata(script).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(script, permissions).unwrap();
}

fn shell_quote(path: &Path) -> String {
    format!("'{}'", path.to_string_lossy().replace('\'', "'\\''"))
}
