use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

use agent_orchestrator::{
    sq::Queue,
    worktree::{create_queue_link, verify_queue_link, Error, GitClient},
};
use serde_json::json;

static NEXT_TEST_DIR: AtomicU64 = AtomicU64::new(0);

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        let sequence = NEXT_TEST_DIR.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "agent-orchestrator-worktree-{}-{sequence}",
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
fn worktree_uses_unique_descendant_blocker_branch_and_rejects_convergence() {
    let directory = TestDir::new();
    let repo = directory.path().join("repo");
    init_repo(&repo);
    git(&repo, &["branch", "agent-orchestrator/root/a"]);
    git(
        &repo,
        &[
            "checkout",
            "-b",
            "agent-orchestrator/root/b",
            "agent-orchestrator/root/a",
        ],
    );
    fs::write(repo.join("b"), "b\n").unwrap();
    git(&repo, &["add", "b"]);
    git(&repo, &["commit", "-m", "b"]);
    git(&repo, &["checkout", "main"]);
    git(&repo, &["branch", "agent-orchestrator/root/c"]);

    let queue = directory.path().join("issues.jsonl");
    write_queue(
        &queue,
        &[
            task("a", "closed", &[]),
            task("b", "closed", &["a"]),
            task("root", "pending", &["a", "b"]),
        ],
    );
    let plan = Queue::read(&queue).unwrap().scope("root").unwrap();
    let worktree_root = directory.path().join("worktrees");
    fs::create_dir(&worktree_root).unwrap();
    let client = GitClient::with_executable(&repo, "git");
    let planned = client.plan_task(&plan, &worktree_root, "root").unwrap();
    assert_eq!(planned.branch, "agent-orchestrator/root/root");
    assert_eq!(planned.base, "agent-orchestrator/root/b");
    assert_eq!(planned.path, worktree_root.join("repo.root"));

    write_queue(
        &queue,
        &[
            task("a", "closed", &[]),
            task("c", "closed", &[]),
            task("root", "pending", &["a", "c"]),
        ],
    );
    let plan = Queue::read(&queue).unwrap().scope("root").unwrap();
    let error = client.plan_task(&plan, &worktree_root, "root").unwrap_err();
    assert!(matches!(error, Error::AmbiguousBlockerBranches { .. }));
    assert!(error
        .to_string()
        .contains("no unique descendant blocker branch"));
}

#[test]
fn worktree_bases_preclosed_blockers_without_retained_branches_on_head() {
    let directory = TestDir::new();
    let repo = directory.path().join("repo");
    init_repo(&repo);
    let queue = directory.path().join("issues.jsonl");
    write_queue(
        &queue,
        &[
            task("legacy", "closed", &[]),
            task("root", "pending", &["legacy"]),
        ],
    );
    let plan = Queue::read(&queue).unwrap().scope("root").unwrap();
    let worktree_root = directory.path().join("worktrees");
    fs::create_dir(&worktree_root).unwrap();
    let client = GitClient::with_executable(&repo, "git");

    let planned = client.plan_task(&plan, &worktree_root, "root").unwrap();

    assert_eq!(planned.base, git_output(&repo, &["rev-parse", "HEAD"]));
}

#[test]
fn worktree_queue_link_is_absolute_external_and_idempotent() {
    let directory = TestDir::new();
    let worktree = directory.path().join("repo.task");
    fs::create_dir(&worktree).unwrap();
    let queue = directory.path().join("issues.jsonl");
    fs::write(&queue, "{}\n").unwrap();
    let client = GitClient::with_executable(&worktree, "git");

    let link = create_queue_link(&client, &worktree, &queue).unwrap();
    let target = fs::read_link(&link).unwrap();
    assert!(target.is_absolute());
    assert_eq!(target, fs::canonicalize(&queue).unwrap());
    verify_queue_link(&worktree, Some(&queue)).unwrap();

    assert_eq!(create_queue_link(&client, &worktree, &queue).unwrap(), link);
}

#[test]
fn worktree_replaces_a_tracked_queue_copy_without_dirtying_the_index() {
    let directory = TestDir::new();
    let repo = directory.path().join("repo");
    init_repo(&repo);
    fs::create_dir(repo.join(".sift")).unwrap();
    fs::write(repo.join(".sift/issues.jsonl"), "checked-out copy\n").unwrap();
    git(&repo, &["add", "-f", ".sift/issues.jsonl"]);
    git(&repo, &["commit", "-m", "queue"]);
    let worktree = directory.path().join("repo.task");
    let worktree_path = worktree.to_str().unwrap();
    git(&repo, &["worktree", "add", "-b", "task", worktree_path]);
    let queue = directory.path().join("issues.jsonl");
    fs::write(&queue, "canonical queue\n").unwrap();
    let client = GitClient::with_executable(&repo, "git");

    create_queue_link(&client, &worktree, &queue).unwrap();

    assert_eq!(
        fs::read_link(worktree.join(".sift/issues.jsonl")).unwrap(),
        fs::canonicalize(queue).unwrap()
    );
    assert!(git_output(
        &worktree,
        &["status", "--short", "--", ".sift/issues.jsonl"]
    )
    .is_empty());
}

fn init_repo(repo: &Path) {
    fs::create_dir(repo).unwrap();
    git(repo, &["init", "-b", "main"]);
    git(repo, &["config", "user.email", "test@example.com"]);
    git(repo, &["config", "user.name", "Test"]);
    fs::write(repo.join("README"), "base\n").unwrap();
    git(repo, &["add", "README"]);
    git(repo, &["commit", "-m", "base"]);
}

fn git(repo: &Path, args: &[&str]) {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn git_output(repo: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(repo)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "git {args:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap().trim().to_owned()
}

fn task(id: &str, status: &str, blocked_by: &[&str]) -> serde_json::Value {
    json!({
        "id": id,
        "title": format!("{id} task"),
        "description": format!("Implement {id}"),
        "status": status,
        "sources": [],
        "metadata": {},
        "blocked_by": blocked_by,
    })
}

fn write_queue(path: &Path, tasks: &[serde_json::Value]) {
    let contents = tasks
        .iter()
        .map(serde_json::Value::to_string)
        .collect::<Vec<_>>()
        .join("\n");
    fs::write(path, format!("{contents}\n")).unwrap();
}
