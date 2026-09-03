use std::collections::BTreeSet;
use std::ffi::{OsStr, OsString};
use std::fs;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use anyhow::{bail, Context, Result};

use crate::herdr::{Herdr, HerdrHandles, WorktreeLaunch};

#[derive(Clone, Debug)]
pub struct Worktrees {
    git: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorktreePlan {
    pub repo: PathBuf,
    pub path: PathBuf,
    pub branch: String,
    pub base: String,
}

impl Worktrees {
    pub fn from_environment() -> Self {
        Self {
            git: std::env::var_os("AGENT_ORCHESTRATOR_GIT")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("git")),
        }
    }

    pub fn repo_root(&self, requested: &Path) -> Result<PathBuf> {
        let output = self.git_output([
            OsStr::new("-C"),
            requested.as_os_str(),
            OsStr::new("rev-parse"),
            OsStr::new("--show-toplevel"),
        ])?;
        let path = PathBuf::from(String::from_utf8(output.stdout)?.trim());
        path.canonicalize()
            .with_context(|| format!("failed to resolve Git repository {}", path.display()))
    }

    pub fn plan(
        &self,
        repo: &Path,
        root_task_id: &str,
        task_id: &str,
        blocker_branches: &[String],
        worktree_root: Option<&Path>,
    ) -> Result<WorktreePlan> {
        let branch = format!(
            "agent-orchestrator/{}/{}",
            safe_component(root_task_id),
            safe_component(task_id)
        );
        let parent = match worktree_root {
            Some(path) => path.to_path_buf(),
            None => repo
                .parent()
                .context("Git repository has no parent directory")?
                .to_path_buf(),
        };
        let repository_name = repo
            .file_name()
            .and_then(OsStr::to_str)
            .context("Git repository path has no UTF-8 directory name")?;
        let path = parent.join(format!("{repository_name}.{}", safe_component(task_id)));
        let base = self.select_base(repo, blocker_branches)?;

        if fs::symlink_metadata(&path).is_ok() {
            bail!("worktree path already exists: {}", path.display());
        }
        if self.branch_exists(repo, &branch)? {
            bail!("worktree branch already exists without a retained runtime: {branch}");
        }
        self.verify_ref(repo, &base)?;

        Ok(WorktreePlan {
            repo: repo.to_path_buf(),
            path,
            branch,
            base,
        })
    }

    pub fn provision(
        &self,
        plan: &WorktreePlan,
        queue: &Path,
        task_id: &str,
        run_id: &str,
        label: &str,
        herdr: &Herdr,
    ) -> Result<HerdrHandles> {
        let launch = WorktreeLaunch {
            repo: &plan.repo,
            branch: &plan.branch,
            base: &plan.base,
            worktree: &plan.path,
            label,
            task_id,
            run_id,
        };
        let handles = herdr.create_worktree(&launch)?;
        share_queue(queue, &plan.path)?;
        Ok(handles)
    }

    fn select_base(&self, repo: &Path, blocker_branches: &[String]) -> Result<String> {
        let branches = blocker_branches
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        match branches.as_slice() {
            [] => Ok("HEAD".to_owned()),
            [branch] => Ok(branch.clone()),
            _ => {
                let candidates = branches
                    .iter()
                    .filter(|candidate| {
                        branches.iter().all(|other| {
                            other == *candidate
                                || self.is_ancestor(repo, other, candidate).unwrap_or(false)
                        })
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                match candidates.as_slice() {
                    [candidate] => Ok(candidate.clone()),
                    _ => bail!(
                        "ambiguous branch convergence among blockers: {}",
                        branches.join(", ")
                    ),
                }
            }
        }
    }

    fn branch_exists(&self, repo: &Path, branch: &str) -> Result<bool> {
        let reference = format!("refs/heads/{branch}");
        let output = self.git_raw([
            OsStr::new("-C"),
            repo.as_os_str(),
            OsStr::new("show-ref"),
            OsStr::new("--verify"),
            OsStr::new("--quiet"),
            OsStr::new(&reference),
        ])?;
        match output.status.code() {
            Some(0) => Ok(true),
            Some(1) => Ok(false),
            _ => command_failed("git show-ref", &output),
        }
    }

    fn verify_ref(&self, repo: &Path, reference: &str) -> Result<()> {
        let commit = format!("{reference}^{{commit}}");
        self.git_output([
            OsStr::new("-C"),
            repo.as_os_str(),
            OsStr::new("rev-parse"),
            OsStr::new("--verify"),
            OsStr::new(&commit),
        ])?;
        Ok(())
    }

    fn is_ancestor(&self, repo: &Path, ancestor: &str, descendant: &str) -> Result<bool> {
        let output = self.git_raw([
            OsStr::new("-C"),
            repo.as_os_str(),
            OsStr::new("merge-base"),
            OsStr::new("--is-ancestor"),
            OsStr::new(ancestor),
            OsStr::new(descendant),
        ])?;
        match output.status.code() {
            Some(0) => Ok(true),
            Some(1) => Ok(false),
            _ => command_failed("git merge-base --is-ancestor", &output),
        }
    }

    fn git_output<I, S>(&self, args: I) -> Result<Output>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let output = self.git_raw(args)?;
        if output.status.success() {
            Ok(output)
        } else {
            command_failed("git", &output)
        }
    }

    fn git_raw<I, S>(&self, args: I) -> Result<Output>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let args = args
            .into_iter()
            .map(|argument| argument.as_ref().to_os_string())
            .collect::<Vec<OsString>>();
        match Command::new(&self.git).args(&args).output() {
            Ok(output) => Ok(output),
            Err(error) if error.kind() == ErrorKind::NotFound => {
                bail!("Git executable {} is not available", self.git.display())
            }
            Err(error) => {
                Err(error).with_context(|| format!("failed to run {}", self.git.display()))
            }
        }
    }
}

fn command_failed<T>(command: &str, output: &Output) -> Result<T> {
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
    bail!(
        "{command} failed: {}",
        if stderr.is_empty() {
            format!("exit status {}", output.status)
        } else {
            stderr
        }
    )
}

fn share_queue(queue: &Path, worktree: &Path) -> Result<()> {
    let queue = queue
        .canonicalize()
        .with_context(|| format!("failed to resolve canonical SQ queue {}", queue.display()))?;
    let sift = worktree.join(".sift");
    fs::create_dir_all(&sift).with_context(|| format!("failed to create {}", sift.display()))?;
    let link = sift.join("issues.jsonl");
    if fs::symlink_metadata(&link).is_ok() {
        fs::remove_file(&link)
            .with_context(|| format!("failed to replace worktree queue {}", link.display()))?;
    }
    create_symlink(&queue, &link).with_context(|| {
        format!(
            "failed to link worktree queue {} to {}",
            link.display(),
            queue.display()
        )
    })
}

#[cfg(unix)]
fn create_symlink(source: &Path, destination: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(source, destination)
}

#[cfg(windows)]
fn create_symlink(source: &Path, destination: &Path) -> std::io::Result<()> {
    std::os::windows::fs::symlink_file(source, destination)
}

fn safe_component(value: &str) -> String {
    let value = value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_' | '.') {
                character.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>();
    if value.is_empty() {
        "task".to_owned()
    } else {
        value
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn shared_queue_is_an_absolute_symlink() {
        let directory = tempfile::tempdir().unwrap();
        let queue = directory.path().join("issues.jsonl");
        fs::write(&queue, "{}\n").unwrap();
        let worktree = directory.path().join("worktree");
        fs::create_dir(&worktree).unwrap();

        share_queue(&queue, &worktree).unwrap();

        assert_eq!(
            fs::read_link(worktree.join(".sift/issues.jsonl")).unwrap(),
            queue.canonicalize().unwrap()
        );
    }

    fn git<const N: usize>(repo: &Path, args: [&str; N]) {
        let status = Command::new("git")
            .arg("-C")
            .arg(repo)
            .args(args)
            .status()
            .unwrap();
        assert!(status.success(), "git {args:?} failed");
    }

    fn repository() -> tempfile::TempDir {
        let directory = tempfile::tempdir().unwrap();
        let repo = directory.path();
        git(repo, ["init", "-q", "."]);
        git(repo, ["config", "user.email", "fixture@example.com"]);
        git(repo, ["config", "user.name", "Fixture"]);
        git(repo, ["commit", "-q", "--allow-empty", "-m", "seed"]);
        directory
    }

    #[test]
    fn base_selection_stacks_on_the_tip_and_rejects_divergence() {
        let directory = repository();
        let repo = directory.path();
        let worktrees = Worktrees::from_environment();

        git(repo, ["checkout", "-q", "-b", "first"]);
        git(repo, ["commit", "-q", "--allow-empty", "-m", "first"]);
        git(repo, ["checkout", "-q", "-b", "second"]);
        git(repo, ["commit", "-q", "--allow-empty", "-m", "second"]);

        assert_eq!(
            worktrees
                .select_base(repo, &["first".to_owned(), "second".to_owned()])
                .unwrap(),
            "second"
        );
        assert_eq!(worktrees.select_base(repo, &[]).unwrap(), "HEAD");

        git(repo, ["checkout", "-q", "first"]);
        git(repo, ["checkout", "-q", "-b", "sibling"]);
        git(repo, ["commit", "-q", "--allow-empty", "-m", "sibling"]);

        let error = worktrees
            .select_base(repo, &["second".to_owned(), "sibling".to_owned()])
            .unwrap_err()
            .to_string();
        assert!(
            error.contains("ambiguous branch convergence among blockers"),
            "unexpected error: {error}"
        );
    }
}
