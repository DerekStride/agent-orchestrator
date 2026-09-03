use std::{
    env,
    ffi::{OsStr, OsString},
    fmt, fs, io,
    path::{Path, PathBuf},
    process::{Command, ExitStatus, Output},
};

use serde::{Deserialize, Serialize};

use crate::sq::{ScopedPlan, TaskStatus};

pub const GIT_EXECUTABLE_ENV: &str = "AGENT_ORCHESTRATOR_GIT";
const BRANCH_PREFIX: &str = "agent-orchestrator";
const QUEUE_LINK: &str = ".sift/issues.jsonl";

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorktreePlan {
    pub task_id: String,
    pub branch: String,
    pub base: String,
    pub path: PathBuf,
}

#[derive(Clone, Debug)]
pub struct GitClient {
    executable: OsString,
    repo: PathBuf,
}

impl GitClient {
    pub fn configured(repo: impl Into<PathBuf>) -> Self {
        let executable = env::var_os(GIT_EXECUTABLE_ENV)
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| OsString::from("git"));
        Self::with_executable(repo, executable)
    }

    pub fn with_executable(repo: impl Into<PathBuf>, executable: impl Into<OsString>) -> Self {
        Self {
            executable: executable.into(),
            repo: repo.into(),
        }
    }

    pub fn executable(&self) -> &OsStr {
        &self.executable
    }

    pub fn repo(&self) -> &Path {
        &self.repo
    }

    pub fn plan_task(
        &self,
        scoped: &ScopedPlan,
        worktree_root: &Path,
        task_id: &str,
    ) -> Result<WorktreePlan> {
        let task = scoped
            .task(task_id)
            .ok_or_else(|| Error::TaskOutsideScope {
                task_id: task_id.to_owned(),
                root_task_id: scoped.root_task_id().to_owned(),
            })?;
        let status = scoped.status(task_id).map_err(Error::Sq)?;
        if status != TaskStatus::Pending {
            return Err(Error::TaskNotReady {
                task_id: task_id.to_owned(),
                status,
            });
        }

        let branch = branch_name(scoped.root_task_id(), task_id)?;
        let path = worktree_path(worktree_root, &self.repo, task_id)?;
        if path.exists() {
            return Err(Error::WorktreeExists { path });
        }
        if self.branch_exists(&branch)? {
            return Err(Error::BranchExists { branch });
        }

        let base = match task.blocked_by() {
            [] => self.resolve_commit("HEAD")?,
            [blocker_id] => {
                ensure_closed(scoped, task_id, blocker_id)?;
                let blocker_branch = branch_name(scoped.root_task_id(), blocker_id)?;
                self.require_branch(&blocker_branch)?;
                blocker_branch
            }
            blocker_ids => {
                let mut blocker_branches = Vec::with_capacity(blocker_ids.len());
                for blocker_id in blocker_ids {
                    ensure_closed(scoped, task_id, blocker_id)?;
                    let blocker_branch = branch_name(scoped.root_task_id(), blocker_id)?;
                    self.require_branch(&blocker_branch)?;
                    blocker_branches.push(blocker_branch);
                }
                self.unique_descendant(task_id, &blocker_branches)?
            }
        };

        Ok(WorktreePlan {
            task_id: task_id.to_owned(),
            branch,
            base,
            path,
        })
    }

    pub fn verify_retained(&self, worktree: &WorktreePlan) -> Result<()> {
        if !worktree.path.is_dir() {
            return Err(Error::MissingWorktree {
                path: worktree.path.clone(),
            });
        }
        self.require_branch(&worktree.branch)?;
        let output = self.invoke_in(
            &worktree.path,
            "reading retained worktree branch",
            [
                OsStr::new("symbolic-ref"),
                OsStr::new("--short"),
                OsStr::new("HEAD"),
            ],
        )?;
        let actual = output_text(&output, "reading retained worktree branch")?;
        if actual != worktree.branch {
            return Err(Error::WorktreeBranchMismatch {
                path: worktree.path.clone(),
                expected: worktree.branch.clone(),
                actual,
            });
        }
        verify_queue_link(&worktree.path, None)
    }

    pub fn verify_report_deliverable(
        &self,
        worktree: &WorktreePlan,
        commit: Option<&str>,
        artifact: Option<&str>,
    ) -> Result<()> {
        if let Some(commit) = commit.filter(|value| !value.trim().is_empty()) {
            let commit = commit.trim();
            self.resolve_commit(commit)?;
            if !self.is_ancestor(commit, &format!("refs/heads/{}", worktree.branch))? {
                return Err(Error::CommitOutsideBranch {
                    commit: commit.to_owned(),
                    branch: worktree.branch.clone(),
                });
            }
            return Ok(());
        }

        let artifact = artifact
            .filter(|value| !value.trim().is_empty())
            .expect("validated completed reports contain a commit or artifact");
        let artifact_path = Path::new(artifact);
        let artifact_path = if artifact_path.is_absolute() {
            artifact_path.to_owned()
        } else {
            worktree.path.join(artifact_path)
        };
        if artifact_path.exists() {
            Ok(())
        } else {
            Err(Error::MissingArtifact {
                path: artifact_path,
            })
        }
    }

    fn unique_descendant(&self, task_id: &str, branches: &[String]) -> Result<String> {
        let mut descendants = Vec::new();
        for candidate in branches {
            let candidate_ref = format!("refs/heads/{candidate}");
            let mut descends_from_all = true;
            for other in branches {
                let other_ref = format!("refs/heads/{other}");
                if !self.is_ancestor(&other_ref, &candidate_ref)? {
                    descends_from_all = false;
                    break;
                }
            }
            if descends_from_all {
                descendants.push(candidate.clone());
            }
        }

        if descendants.len() == 1 {
            Ok(descendants.pop().expect("the descendant count was checked"))
        } else {
            Err(Error::AmbiguousBlockerBranches {
                task_id: task_id.to_owned(),
                branches: branches.to_vec(),
            })
        }
    }

    fn require_branch(&self, branch: &str) -> Result<()> {
        if self.branch_exists(branch)? {
            Ok(())
        } else {
            Err(Error::MissingBranch {
                branch: branch.to_owned(),
            })
        }
    }

    fn branch_exists(&self, branch: &str) -> Result<bool> {
        let reference = format!("refs/heads/{branch}");
        let output = self.invoke_allow_status(
            &self.repo,
            "checking Git branch",
            [
                OsStr::new("show-ref"),
                OsStr::new("--verify"),
                OsStr::new("--quiet"),
                OsStr::new(&reference),
            ],
        )?;
        match output.status.code() {
            Some(0) => Ok(true),
            Some(1) => Ok(false),
            _ => Err(command_failed("checking Git branch", output)),
        }
    }

    fn resolve_commit(&self, reference: &str) -> Result<String> {
        let revision = format!("{reference}^{{commit}}");
        let output = self.invoke(
            "resolving Git commit",
            [
                OsStr::new("rev-parse"),
                OsStr::new("--verify"),
                OsStr::new(&revision),
            ],
        )?;
        output_text(&output, "resolving Git commit")
    }

    fn is_ancestor(&self, ancestor: &str, descendant: &str) -> Result<bool> {
        let output = self.invoke_allow_status(
            &self.repo,
            "checking Git ancestry",
            [
                OsStr::new("merge-base"),
                OsStr::new("--is-ancestor"),
                OsStr::new(ancestor),
                OsStr::new(descendant),
            ],
        )?;
        match output.status.code() {
            Some(0) => Ok(true),
            Some(1) => Ok(false),
            _ => Err(command_failed("checking Git ancestry", output)),
        }
    }

    fn invoke<I, S>(&self, operation: &'static str, args: I) -> Result<Output>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let output = self.invoke_allow_status(&self.repo, operation, args)?;
        if output.status.success() {
            Ok(output)
        } else {
            Err(command_failed(operation, output))
        }
    }

    fn invoke_in<I, S>(&self, directory: &Path, operation: &'static str, args: I) -> Result<Output>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        let output = self.invoke_allow_status(directory, operation, args)?;
        if output.status.success() {
            Ok(output)
        } else {
            Err(command_failed(operation, output))
        }
    }

    fn invoke_allow_status<I, S>(
        &self,
        directory: &Path,
        operation: &'static str,
        args: I,
    ) -> Result<Output>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        Command::new(&self.executable)
            .arg("-C")
            .arg(directory)
            .args(args)
            .output()
            .map_err(|source| Error::Start {
                executable: self.executable.clone(),
                operation,
                source,
            })
    }
}

pub fn branch_name(root_task_id: &str, task_id: &str) -> Result<String> {
    validate_ref_component("root task", root_task_id)?;
    validate_ref_component("task", task_id)?;
    Ok(format!("{BRANCH_PREFIX}/{root_task_id}/{task_id}"))
}

pub fn worktree_path(worktree_root: &Path, repo: &Path, task_id: &str) -> Result<PathBuf> {
    validate_path_component(task_id)?;
    let repo_name = repo
        .file_name()
        .and_then(OsStr::to_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| Error::InvalidRepositoryPath {
            path: repo.to_owned(),
        })?;
    Ok(worktree_root.join(format!("{repo_name}.{task_id}")))
}

pub fn create_queue_link(worktree: &Path, queue: &Path) -> Result<PathBuf> {
    let canonical_worktree = fs::canonicalize(worktree).map_err(|source| Error::Canonicalize {
        path: worktree.to_owned(),
        source,
    })?;
    let canonical_queue = fs::canonicalize(queue).map_err(|source| Error::Canonicalize {
        path: queue.to_owned(),
        source,
    })?;
    if canonical_queue.starts_with(&canonical_worktree) {
        return Err(Error::QueueInsideWorktree {
            queue: canonical_queue,
            worktree: canonical_worktree,
        });
    }

    let link = canonical_worktree.join(QUEUE_LINK);
    if fs::symlink_metadata(&link).is_ok() {
        return Err(Error::QueueLinkExists { path: link });
    }
    let parent = link.parent().expect("the queue link has a parent");
    fs::create_dir_all(parent).map_err(|source| Error::CreateQueueLinkDirectory {
        path: parent.to_owned(),
        source,
    })?;
    create_symlink(&canonical_queue, &link).map_err(|source| Error::CreateQueueLink {
        path: link.clone(),
        target: canonical_queue,
        source,
    })?;
    Ok(link)
}

pub fn verify_queue_link(worktree: &Path, expected_queue: Option<&Path>) -> Result<()> {
    let link = worktree.join(QUEUE_LINK);
    let target = fs::read_link(&link).map_err(|source| Error::ReadQueueLink {
        path: link.clone(),
        source,
    })?;
    if !target.is_absolute() {
        return Err(Error::RelativeQueueLink { path: link, target });
    }
    if let Some(expected_queue) = expected_queue {
        let expected = fs::canonicalize(expected_queue).map_err(|source| Error::Canonicalize {
            path: expected_queue.to_owned(),
            source,
        })?;
        if target != expected {
            return Err(Error::QueueLinkMismatch {
                path: link,
                expected,
                actual: target,
            });
        }
    }
    Ok(())
}

fn ensure_closed(scoped: &ScopedPlan, task_id: &str, blocker_id: &str) -> Result<()> {
    let status = scoped.status(blocker_id).map_err(Error::Sq)?;
    if status == TaskStatus::Closed {
        Ok(())
    } else {
        Err(Error::BlockerNotClosed {
            task_id: task_id.to_owned(),
            blocker_id: blocker_id.to_owned(),
            status,
        })
    }
}

fn validate_ref_component(kind: &'static str, value: &str) -> Result<()> {
    let invalid = value.is_empty()
        || value.starts_with('.')
        || value.ends_with('.')
        || value.ends_with(".lock")
        || value.contains("..")
        || value.contains("@{")
        || value.bytes().any(|byte| {
            byte <= b' '
                || byte == 0x7f
                || matches!(byte, b'~' | b'^' | b':' | b'?' | b'*' | b'[' | b'\\' | b'/')
        });
    if invalid {
        Err(Error::InvalidRefComponent {
            kind,
            value: value.to_owned(),
        })
    } else {
        Ok(())
    }
}

fn validate_path_component(value: &str) -> Result<()> {
    if value.is_empty() || value == "." || value == ".." || value.contains(['/', '\\']) {
        Err(Error::InvalidPathComponent {
            value: value.to_owned(),
        })
    } else {
        Ok(())
    }
}

fn output_text(output: &Output, operation: &'static str) -> Result<String> {
    let value = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if value.is_empty() {
        Err(Error::EmptyOutput { operation })
    } else {
        Ok(value)
    }
}

fn command_failed(operation: &'static str, output: Output) -> Error {
    Error::CommandFailed {
        operation,
        status: output.status,
        detail: failure_detail(&output),
    }
}

fn failure_detail(output: &Output) -> String {
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
    if stderr.is_empty() {
        format!("process exited with {}", output.status)
    } else {
        stderr
    }
}

#[cfg(unix)]
fn create_symlink(target: &Path, link: &Path) -> io::Result<()> {
    std::os::unix::fs::symlink(target, link)
}

#[cfg(windows)]
fn create_symlink(target: &Path, link: &Path) -> io::Result<()> {
    std::os::windows::fs::symlink_file(target, link)
}

#[derive(Debug)]
pub enum Error {
    Sq(crate::sq::Error),
    TaskOutsideScope {
        task_id: String,
        root_task_id: String,
    },
    TaskNotReady {
        task_id: String,
        status: TaskStatus,
    },
    BlockerNotClosed {
        task_id: String,
        blocker_id: String,
        status: TaskStatus,
    },
    InvalidRefComponent {
        kind: &'static str,
        value: String,
    },
    InvalidPathComponent {
        value: String,
    },
    InvalidRepositoryPath {
        path: PathBuf,
    },
    WorktreeExists {
        path: PathBuf,
    },
    MissingWorktree {
        path: PathBuf,
    },
    BranchExists {
        branch: String,
    },
    MissingBranch {
        branch: String,
    },
    AmbiguousBlockerBranches {
        task_id: String,
        branches: Vec<String>,
    },
    WorktreeBranchMismatch {
        path: PathBuf,
        expected: String,
        actual: String,
    },
    CommitOutsideBranch {
        commit: String,
        branch: String,
    },
    MissingArtifact {
        path: PathBuf,
    },
    Start {
        executable: OsString,
        operation: &'static str,
        source: io::Error,
    },
    CommandFailed {
        operation: &'static str,
        status: ExitStatus,
        detail: String,
    },
    EmptyOutput {
        operation: &'static str,
    },
    Canonicalize {
        path: PathBuf,
        source: io::Error,
    },
    QueueInsideWorktree {
        queue: PathBuf,
        worktree: PathBuf,
    },
    QueueLinkExists {
        path: PathBuf,
    },
    CreateQueueLinkDirectory {
        path: PathBuf,
        source: io::Error,
    },
    CreateQueueLink {
        path: PathBuf,
        target: PathBuf,
        source: io::Error,
    },
    ReadQueueLink {
        path: PathBuf,
        source: io::Error,
    },
    RelativeQueueLink {
        path: PathBuf,
        target: PathBuf,
    },
    QueueLinkMismatch {
        path: PathBuf,
        expected: PathBuf,
        actual: PathBuf,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sq(error) => error.fmt(formatter),
            Self::TaskOutsideScope {
                task_id,
                root_task_id,
            } => write!(
                formatter,
                "task `{task_id}` is outside root `{root_task_id}` scope"
            ),
            Self::TaskNotReady { task_id, status } => {
                write!(
                    formatter,
                    "cannot plan worktree for task `{task_id}` with status `{status}`"
                )
            }
            Self::BlockerNotClosed {
                task_id,
                blocker_id,
                status,
            } => write!(
                formatter,
                "cannot base task `{task_id}` on blocker `{blocker_id}` with status `{status}`"
            ),
            Self::InvalidRefComponent { kind, value } => {
                write!(formatter, "{kind} ID `{value}` is not safe in a Git branch")
            }
            Self::InvalidPathComponent { value } => {
                write!(
                    formatter,
                    "task ID `{value}` is not safe in a worktree path"
                )
            }
            Self::InvalidRepositoryPath { path } => {
                write!(
                    formatter,
                    "repository path `{}` has no file name",
                    path.display()
                )
            }
            Self::WorktreeExists { path } => {
                write!(
                    formatter,
                    "worker worktree `{}` already exists",
                    path.display()
                )
            }
            Self::MissingWorktree { path } => {
                write!(
                    formatter,
                    "retained worker worktree `{}` is missing",
                    path.display()
                )
            }
            Self::BranchExists { branch } => {
                write!(formatter, "worker branch `{branch}` already exists")
            }
            Self::MissingBranch { branch } => {
                write!(formatter, "required worker branch `{branch}` is missing")
            }
            Self::AmbiguousBlockerBranches { task_id, branches } => write!(
                formatter,
                "task `{task_id}` has no unique descendant blocker branch among {}",
                branches.join(", ")
            ),
            Self::WorktreeBranchMismatch {
                path,
                expected,
                actual,
            } => write!(
                formatter,
                "retained worktree `{}` is on branch `{actual}`, expected `{expected}`",
                path.display()
            ),
            Self::CommitOutsideBranch { commit, branch } => write!(
                formatter,
                "reported commit `{commit}` is not reachable from worker branch `{branch}`"
            ),
            Self::MissingArtifact { path } => {
                write!(
                    formatter,
                    "reported artifact `{}` does not exist",
                    path.display()
                )
            }
            Self::Start {
                executable,
                operation,
                source,
            } => write!(
                formatter,
                "cannot start Git executable `{}` while {operation}: {source}",
                executable.to_string_lossy()
            ),
            Self::CommandFailed {
                operation,
                status,
                detail,
            } => write!(
                formatter,
                "Git failed while {operation} ({status}): {detail}"
            ),
            Self::EmptyOutput { operation } => {
                write!(formatter, "Git returned empty output while {operation}")
            }
            Self::Canonicalize { path, source } => {
                write!(
                    formatter,
                    "cannot canonicalize `{}`: {source}",
                    path.display()
                )
            }
            Self::QueueInsideWorktree { queue, worktree } => write!(
                formatter,
                "canonical queue `{}` must be outside worker worktree `{}`",
                queue.display(),
                worktree.display()
            ),
            Self::QueueLinkExists { path } => write!(
                formatter,
                "refusing to replace existing worker queue path `{}`",
                path.display()
            ),
            Self::CreateQueueLinkDirectory { path, source } => write!(
                formatter,
                "cannot create worker queue directory `{}`: {source}",
                path.display()
            ),
            Self::CreateQueueLink {
                path,
                target,
                source,
            } => write!(
                formatter,
                "cannot create queue symlink `{}` -> `{}`: {source}",
                path.display(),
                target.display()
            ),
            Self::ReadQueueLink { path, source } => write!(
                formatter,
                "cannot read retained queue symlink `{}`: {source}",
                path.display()
            ),
            Self::RelativeQueueLink { path, target } => write!(
                formatter,
                "retained queue symlink `{}` has relative target `{}`",
                path.display(),
                target.display()
            ),
            Self::QueueLinkMismatch {
                path,
                expected,
                actual,
            } => write!(
                formatter,
                "retained queue symlink `{}` targets `{}`, expected `{}`",
                path.display(),
                actual.display(),
                expected.display()
            ),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Sq(error) => Some(error),
            Self::Start { source, .. }
            | Self::Canonicalize { source, .. }
            | Self::CreateQueueLinkDirectory { source, .. }
            | Self::CreateQueueLink { source, .. }
            | Self::ReadQueueLink { source, .. } => Some(source),
            _ => None,
        }
    }
}
