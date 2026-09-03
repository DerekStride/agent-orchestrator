use std::{
    fmt, fs, io,
    path::{Path, PathBuf},
    thread,
    time::Duration,
};

use serde::Serialize;

use crate::{
    herdr::{
        reconcile_worker, AgentStatus, HerdrClient, WorkerDisposition, WorkerStop, WorktreeSpec,
    },
    identity::AgentIdClient,
    mail::{AgentMailClient, Handoff, ReportDisposition, ReportStatus},
    runtime::{new_run_id, now_unix, LedgerStore, RunLedger, RuntimeRecord, RuntimeStage},
    sq::{resolve_queue_path, ScopedPlan, SqClient, TaskStatus},
    worktree::{create_queue_link, verify_queue_link, GitClient},
};

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[derive(Clone, Debug)]
pub struct FinishOptions {
    pub root_task_id: String,
    pub queue: Option<PathBuf>,
    pub repo: PathBuf,
    pub state_dir: Option<PathBuf>,
    pub worktree_root: Option<PathBuf>,
    pub once: bool,
    pub poll_seconds: u64,
    pub lease_seconds: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FinishStatus {
    Complete,
    Active,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RuntimeSummary {
    pub task_id: String,
    pub stage: RuntimeStage,
    pub branch: String,
    pub worktree: PathBuf,
    pub worker: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct FinishOutput {
    pub status: FinishStatus,
    pub run_id: String,
    pub root_task_id: String,
    pub runtimes: Vec<RuntimeSummary>,
}

pub fn execute(options: FinishOptions) -> Result<FinishOutput> {
    let paths = ResolvedPaths::resolve(&options)?;
    let sq = SqClient::configured(&paths.queue);

    // The graph is always the first integration boundary. Existing tests and the
    // completion contract rely on malformed scope never reaching Herdr or Agent ID.
    sq.plan(&options.root_task_id).map_err(Error::Sq)?;
    let store =
        LedgerStore::acquire(&paths.state_dir, &options.root_task_id).map_err(Error::Runtime)?;
    let retained = store.load().map_err(Error::Runtime)?;
    let run_id = retained
        .as_ref()
        .map(|ledger| ledger.run_id.clone())
        .map(Ok)
        .unwrap_or_else(|| new_run_id(&options.root_task_id).map_err(Error::Runtime))?;
    let initial = sq
        .validated_plan(
            &options.root_task_id,
            &run_id,
            retained.as_ref().map(|ledger| &ledger.plan),
        )
        .map_err(Error::Sq)?;

    if let Some(ledger) = &retained {
        ledger
            .validate(
                &options.root_task_id,
                &paths.queue,
                &paths.repo,
                &paths.worktree_root,
            )
            .map_err(Error::Runtime)?;
        reject_orphaned_in_progress(&initial, ledger)?;
        if completion_ready(&initial, ledger)? {
            return Ok(output(ledger, FinishStatus::Complete));
        }
    } else {
        reject_fresh_in_progress(&initial)?;
        if initial.status(&options.root_task_id).map_err(Error::Sq)? == TaskStatus::Closed {
            return Ok(FinishOutput {
                status: FinishStatus::Complete,
                run_id,
                root_task_id: options.root_task_id,
                runtimes: Vec::new(),
            });
        }
    }

    let herdr = HerdrClient::configured();
    herdr.preflight().map_err(Error::Herdr)?;
    let identities = AgentIdClient::configured();
    let orchestrator = identities.current().map_err(Error::Identity)?;
    let mail = AgentMailClient::configured();
    let git = GitClient::configured(&paths.repo);

    let mut ledger = match retained {
        Some(ledger) => {
            if ledger.orchestrator.session_id != orchestrator.session_id
                || ledger.orchestrator.slug != orchestrator.slug
            {
                return Err(Error::OrchestratorMismatch {
                    expected: ledger.orchestrator.slug,
                    actual: orchestrator.slug,
                });
            }
            ledger
        }
        None => {
            let ledger = RunLedger::new(
                run_id,
                options.root_task_id.clone(),
                paths.queue.clone(),
                paths.repo.clone(),
                paths.worktree_root.clone(),
                initial.snapshot().clone(),
                orchestrator,
                now_unix().map_err(Error::Runtime)?,
            );
            store.save(&ledger).map_err(Error::Runtime)?;
            ledger
        }
    };

    loop {
        let status = run_pass(
            &options,
            &store,
            &sq,
            &git,
            &herdr,
            &identities,
            &mail,
            &mut ledger,
        )?;
        if status == FinishStatus::Complete || options.once {
            return Ok(output(&ledger, status));
        }
        thread::sleep(Duration::from_secs(options.poll_seconds));
    }
}

fn run_pass(
    options: &FinishOptions,
    store: &LedgerStore,
    sq: &SqClient,
    git: &GitClient,
    herdr: &HerdrClient,
    identities: &AgentIdClient,
    mail: &AgentMailClient,
    ledger: &mut RunLedger,
) -> Result<FinishStatus> {
    let mut plan = sq
        .validated_plan(&ledger.root_task_id, &ledger.run_id, Some(&ledger.plan))
        .map_err(Error::Sq)?;
    reject_orphaned_in_progress(&plan, ledger)?;

    let retained_ids = ledger.runtimes.keys().cloned().collect::<Vec<_>>();
    for task_id in retained_ids {
        let mut runtime = ledger
            .runtimes
            .get(&task_id)
            .cloned()
            .expect("retained IDs come from the runtime map");
        runtime.validate_reconcilable().map_err(Error::Runtime)?;
        if runtime.stage == RuntimeStage::Completed {
            if plan.status(&task_id).map_err(Error::Sq)? != TaskStatus::Closed {
                return Err(Error::CompletedRuntimeReopened { task_id });
            }
            continue;
        }
        if matches!(runtime.stage, RuntimeStage::Blocked | RuntimeStage::Failed) {
            let status = runtime.report_status.clone().ok_or_else(|| {
                Error::Runtime(crate::runtime::Error::MissingHandle {
                    task_id: task_id.clone(),
                    field: "report_status",
                })
            })?;
            return Err(Error::RetainedTerminalReport { task_id, status });
        }
        if plan.status(&task_id).map_err(Error::Sq)? == TaskStatus::Pending {
            return Err(Error::RuntimeTaskReset { task_id });
        }

        git.verify_retained(&runtime.worktree)
            .map_err(Error::Worktree)?;
        verify_queue_link(&runtime.worktree.path, Some(&ledger.queue)).map_err(Error::Worktree)?;

        if runtime.stage == RuntimeStage::ReportReceived {
            settle_retained_report(store, mail, ledger, runtime)?;
            continue;
        }

        let task = plan
            .task(&task_id)
            .expect("runtime tasks are validated against the scoped plan");
        let report = mail
            .scan_report(
                &ledger.orchestrator,
                runtime.identity().map_err(Error::Runtime)?,
                &task_id,
                &ledger.run_id,
            )
            .map_err(Error::Mail)?;
        if let Some((message_id, report)) = report {
            let disposition = crate::mail::reconcile_report(&task.stored_status(), &report)
                .map_err(Error::Mail)?;
            if disposition == ReportDisposition::Completed {
                git.verify_report_deliverable(
                    &runtime.worktree,
                    report.commit.as_deref(),
                    report.artifact.as_deref(),
                )
                .map_err(Error::Worktree)?;
            }
            runtime
                .record_report(message_id, report.status.clone())
                .map_err(Error::Runtime)?;
            ledger.runtimes.insert(task_id.clone(), runtime.clone());
            store.save(ledger).map_err(Error::Runtime)?;
            mail.mark_read(
                runtime
                    .report_message_id
                    .as_deref()
                    .expect("record_report retains the message ID"),
            )
            .map_err(Error::Mail)?;
            runtime.settle_report().map_err(Error::Runtime)?;
            ledger.runtimes.insert(task_id.clone(), runtime);
            store.save(ledger).map_err(Error::Runtime)?;
            match disposition {
                ReportDisposition::Completed => {}
                ReportDisposition::Blocked => {
                    return Err(Error::WorkerReported {
                        task_id,
                        status: ReportStatus::Blocked,
                        summary: report
                            .summary
                            .expect("validated blocked reports contain a summary"),
                    });
                }
                ReportDisposition::Failed => {
                    return Err(Error::WorkerReported {
                        task_id,
                        status: ReportStatus::Failed,
                        summary: report
                            .summary
                            .expect("validated failed reports contain a summary"),
                    });
                }
            }
            continue;
        }

        let discovered = identities
            .discover_worker(&runtime.worktree.path)
            .map_err(|error| Error::LostWorker {
                task_id: task_id.clone(),
                detail: error.to_string(),
            })?;
        let retained_identity = runtime.identity().map_err(Error::Runtime)?;
        if discovered.session_id != retained_identity.session_id
            || discovered.slug != retained_identity.slug
        {
            return Err(Error::ReplacedWorker {
                task_id,
                expected: retained_identity.slug.clone(),
                actual: discovered.slug,
            });
        }

        let worker_status = herdr
            .status(&runtime.worker().map_err(Error::Runtime)?)
            .map_err(|error| Error::LostWorker {
                task_id: task_id.clone(),
                detail: error.to_string(),
            })?;
        let now = now_unix().map_err(Error::Runtime)?;
        runtime.heartbeat(now).map_err(Error::Runtime)?;
        ledger.runtimes.insert(task_id.clone(), runtime.clone());
        store.save(ledger).map_err(Error::Runtime)?;
        match reconcile_worker(worker_status, false) {
            WorkerDisposition::Active => {
                if runtime.lease_expired(now).map_err(Error::Runtime)? {
                    return Err(Error::LeaseExpired {
                        task_id,
                        deadline: runtime
                            .lease_expires_at_unix
                            .expect("lease_expired validates the deadline"),
                    });
                }
            }
            WorkerDisposition::ReportPresent => {
                unreachable!("the report scan returned no structured report")
            }
            WorkerDisposition::Stop(reason) => {
                return Err(worker_stop_error(task_id, reason));
            }
        }
    }

    plan = sq
        .validated_plan(&ledger.root_task_id, &ledger.run_id, Some(&ledger.plan))
        .map_err(Error::Sq)?;
    reject_orphaned_in_progress(&plan, ledger)?;
    let ready = plan.ready_task_ids().map(str::to_owned).collect::<Vec<_>>();
    for task_id in ready {
        let worktree = git
            .plan_task(&plan, &ledger.worktree_root, &task_id)
            .map_err(Error::Worktree)?;
        plan = sq
            .claim(&plan, &task_id, &ledger.run_id)
            .map_err(Error::Sq)?;

        let mut runtime = RuntimeRecord::claimed(worktree, now_unix().map_err(Error::Runtime)?);
        ledger.runtimes.insert(task_id.clone(), runtime.clone());
        store.save(ledger).map_err(Error::Runtime)?;

        let task = plan
            .task(&task_id)
            .expect("a claimed task remains in the scoped plan");
        let workspace = herdr
            .create_worktree(WorktreeSpec {
                repo: &ledger.repo,
                branch: &runtime.worktree.branch,
                base: &runtime.worktree.base,
                path: &runtime.worktree.path,
                task_label: &format!("{task_id}: {}", task.title()),
            })
            .map_err(Error::Herdr)?;
        runtime
            .record_workspace(&workspace)
            .map_err(Error::Runtime)?;
        ledger.runtimes.insert(task_id.clone(), runtime.clone());
        store.save(ledger).map_err(Error::Runtime)?;

        create_queue_link(&runtime.worktree.path, &ledger.queue).map_err(Error::Worktree)?;
        runtime.record_queue_link().map_err(Error::Runtime)?;
        ledger.runtimes.insert(task_id.clone(), runtime.clone());
        store.save(ledger).map_err(Error::Runtime)?;

        let worker = herdr
            .start_omp(&workspace, &ledger.run_id, &task_id)
            .map_err(Error::Herdr)?;
        runtime.record_worker(&worker).map_err(Error::Runtime)?;
        ledger.runtimes.insert(task_id.clone(), runtime.clone());
        store.save(ledger).map_err(Error::Runtime)?;

        let worker_identity = identities
            .discover_worker(&runtime.worktree.path)
            .map_err(Error::Identity)?;
        runtime
            .record_identity(worker_identity)
            .map_err(Error::Runtime)?;
        ledger.runtimes.insert(task_id.clone(), runtime.clone());
        store.save(ledger).map_err(Error::Runtime)?;

        let handoff = Handoff {
            run_id: &ledger.run_id,
            task_id: &task_id,
            queue: &ledger.queue,
            worktree: &runtime.worktree.path,
            branch: &runtime.worktree.branch,
            dependencies: task.blocked_by(),
            acceptance_criteria: task.acceptance_criteria(),
            validation_checks: task.validation_checks(),
            orchestrator: &ledger.orchestrator,
            worker: runtime.identity().map_err(Error::Runtime)?,
        };
        let receipt = mail.send_handoff(&handoff).map_err(Error::Mail)?;
        runtime
            .record_handoff(receipt.id.clone())
            .map_err(Error::Runtime)?;
        ledger.runtimes.insert(task_id.clone(), runtime.clone());
        store.save(ledger).map_err(Error::Runtime)?;

        let prompt = format!(
            "Read AgentMail message {} from {} and execute that handoff. Report results only through AgentMail as the handoff specifies.",
            receipt.id, ledger.orchestrator.slug
        );
        let status = herdr
            .prompt_after_handoff(&worker, &receipt.id, &prompt)
            .map_err(Error::Herdr)?;
        let activated_at = now_unix().map_err(Error::Runtime)?;
        runtime
            .activate(activated_at, options.lease_seconds)
            .map_err(Error::Runtime)?;
        ledger.runtimes.insert(task_id.clone(), runtime.clone());
        store.save(ledger).map_err(Error::Runtime)?;
        match reconcile_worker(status, false) {
            WorkerDisposition::Active => {
                if runtime
                    .lease_expired(activated_at)
                    .map_err(Error::Runtime)?
                {
                    return Err(Error::LeaseExpired {
                        task_id,
                        deadline: runtime
                            .lease_expires_at_unix
                            .expect("lease_expired validates the deadline"),
                    });
                }
            }
            WorkerDisposition::ReportPresent => {
                unreachable!("a newly prompted worker has no scanned report")
            }
            WorkerDisposition::Stop(reason) => {
                return Err(worker_stop_error(task_id, reason));
            }
        }
    }

    let current = sq
        .validated_plan(&ledger.root_task_id, &ledger.run_id, Some(&ledger.plan))
        .map_err(Error::Sq)?;
    if completion_ready(&current, ledger)? {
        Ok(FinishStatus::Complete)
    } else {
        Ok(FinishStatus::Active)
    }
}

fn settle_retained_report(
    store: &LedgerStore,
    mail: &AgentMailClient,
    ledger: &mut RunLedger,
    mut runtime: RuntimeRecord,
) -> Result<()> {
    let task_id = runtime.task_id.clone();
    let message_id = runtime.report_message_id.clone().ok_or_else(|| {
        Error::Runtime(crate::runtime::Error::MissingHandle {
            task_id: task_id.clone(),
            field: "report_message_id",
        })
    })?;
    mail.mark_read(&message_id).map_err(Error::Mail)?;
    runtime.settle_report().map_err(Error::Runtime)?;
    let status = runtime
        .report_status
        .clone()
        .expect("a report-received runtime retains its report status");
    ledger.runtimes.insert(task_id.clone(), runtime);
    store.save(ledger).map_err(Error::Runtime)?;
    match status {
        ReportStatus::Completed => Ok(()),
        ReportStatus::Blocked | ReportStatus::Failed => {
            Err(Error::RetainedTerminalReport { task_id, status })
        }
    }
}

fn reject_fresh_in_progress(plan: &ScopedPlan) -> Result<()> {
    for task_id in plan.task_ids() {
        let task = plan.task(task_id).expect("task IDs belong to the plan");
        if task.stored_status() == TaskStatus::InProgress {
            return if let Some(owner) = task.owner_run_id() {
                Err(Error::MissingRuntime {
                    task_id: task_id.to_owned(),
                    owner: Some(owner.to_owned()),
                })
            } else {
                Err(Error::UnownedInProgress {
                    task_id: task_id.to_owned(),
                })
            };
        }
    }
    Ok(())
}

fn reject_orphaned_in_progress(plan: &ScopedPlan, ledger: &RunLedger) -> Result<()> {
    for task_id in plan.task_ids() {
        let task = plan.task(task_id).expect("task IDs belong to the plan");
        if task.stored_status() == TaskStatus::InProgress && !ledger.runtimes.contains_key(task_id)
        {
            return if task.owner_run_id().is_none() {
                Err(Error::UnownedInProgress {
                    task_id: task_id.to_owned(),
                })
            } else {
                Err(Error::MissingRuntime {
                    task_id: task_id.to_owned(),
                    owner: task.owner_run_id().map(str::to_owned),
                })
            };
        }
    }
    Ok(())
}

fn completion_ready(plan: &ScopedPlan, ledger: &RunLedger) -> Result<bool> {
    Ok(
        plan.status(plan.root_task_id()).map_err(Error::Sq)? == TaskStatus::Closed
            && ledger.is_complete(),
    )
}

fn output(ledger: &RunLedger, status: FinishStatus) -> FinishOutput {
    FinishOutput {
        status,
        run_id: ledger.run_id.clone(),
        root_task_id: ledger.root_task_id.clone(),
        runtimes: ledger
            .runtimes
            .values()
            .map(|runtime| RuntimeSummary {
                task_id: runtime.task_id.clone(),
                stage: runtime.stage,
                branch: runtime.worktree.branch.clone(),
                worktree: runtime.worktree.path.clone(),
                worker: runtime.worker_name.clone(),
            })
            .collect(),
    }
}

fn worker_stop_error(task_id: String, reason: WorkerStop) -> Error {
    match reason {
        WorkerStop::SettledWithoutReport(status) => Error::SettledWithoutReport { task_id, status },
        WorkerStop::Blocked => Error::WorkerBlocked { task_id },
    }
}

struct ResolvedPaths {
    queue: PathBuf,
    repo: PathBuf,
    state_dir: PathBuf,
    worktree_root: PathBuf,
}

impl ResolvedPaths {
    fn resolve(options: &FinishOptions) -> Result<Self> {
        let repo = fs::canonicalize(&options.repo).map_err(|source| Error::Canonicalize {
            kind: "repository",
            path: options.repo.clone(),
            source,
        })?;
        let queue = resolve_queue_path(options.queue.as_deref(), &repo);
        let queue = fs::canonicalize(&queue).map_err(|source| Error::Canonicalize {
            kind: "SQ queue",
            path: queue,
            source,
        })?;
        let worktree_root = options
            .worktree_root
            .clone()
            .unwrap_or_else(|| repo.parent().unwrap_or(&repo).to_owned());
        fs::create_dir_all(&worktree_root).map_err(|source| Error::CreateDirectory {
            kind: "worktree root",
            path: worktree_root.clone(),
            source,
        })?;
        let worktree_root =
            fs::canonicalize(&worktree_root).map_err(|source| Error::Canonicalize {
                kind: "worktree root",
                path: worktree_root,
                source,
            })?;
        let state_dir = options.state_dir.clone().unwrap_or_else(|| {
            repo.parent()
                .unwrap_or(&repo)
                .join(".agent-orchestrator")
                .join(repo.file_name().unwrap_or_default())
        });
        let state_dir = absolute(&state_dir)?;
        Ok(Self {
            queue,
            repo,
            state_dir,
            worktree_root,
        })
    }
}

fn absolute(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_owned())
    } else {
        std::env::current_dir()
            .map(|cwd| cwd.join(path))
            .map_err(Error::CurrentDirectory)
    }
}

#[derive(Debug)]
pub enum Error {
    Sq(crate::sq::Error),
    Herdr(crate::herdr::Error),
    Identity(crate::identity::Error),
    Mail(crate::mail::Error),
    Runtime(crate::runtime::Error),
    Worktree(crate::worktree::Error),
    CurrentDirectory(io::Error),
    Canonicalize {
        kind: &'static str,
        path: PathBuf,
        source: io::Error,
    },
    CreateDirectory {
        kind: &'static str,
        path: PathBuf,
        source: io::Error,
    },
    OrchestratorMismatch {
        expected: String,
        actual: String,
    },
    UnownedInProgress {
        task_id: String,
    },
    MissingRuntime {
        task_id: String,
        owner: Option<String>,
    },
    CompletedRuntimeReopened {
        task_id: String,
    },
    RuntimeTaskReset {
        task_id: String,
    },
    LostWorker {
        task_id: String,
        detail: String,
    },
    ReplacedWorker {
        task_id: String,
        expected: String,
        actual: String,
    },
    LeaseExpired {
        task_id: String,
        deadline: u64,
    },
    SettledWithoutReport {
        task_id: String,
        status: AgentStatus,
    },
    WorkerBlocked {
        task_id: String,
    },
    WorkerReported {
        task_id: String,
        status: ReportStatus,
        summary: String,
    },
    RetainedTerminalReport {
        task_id: String,
        status: ReportStatus,
    },
}

impl fmt::Display for Error {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sq(error) => error.fmt(formatter),
            Self::Herdr(error) => error.fmt(formatter),
            Self::Identity(error) => error.fmt(formatter),
            Self::Mail(error) => error.fmt(formatter),
            Self::Runtime(error) => error.fmt(formatter),
            Self::Worktree(error) => error.fmt(formatter),
            Self::CurrentDirectory(source) => {
                write!(formatter, "cannot determine current directory: {source}")
            }
            Self::Canonicalize { kind, path, source } => {
                write!(formatter, "cannot canonicalize {kind} `{}`: {source}", path.display())
            }
            Self::CreateDirectory { kind, path, source } => {
                write!(formatter, "cannot create {kind} `{}`: {source}", path.display())
            }
            Self::OrchestratorMismatch { expected, actual } => write!(
                formatter,
                "retained run belongs to orchestrator `{expected}`, not `{actual}`"
            ),
            Self::UnownedInProgress { task_id } => write!(
                formatter,
                "SQ task `{task_id}` is in_progress without run ownership; refusing to steal it"
            ),
            Self::MissingRuntime { task_id, owner } => write!(
                formatter,
                "SQ task `{task_id}` is in_progress for run `{}` without a retained runtime; refusing interrupted provisioning",
                owner.as_deref().unwrap_or("<missing>")
            ),
            Self::CompletedRuntimeReopened { task_id } => write!(
                formatter,
                "completed runtime task `{task_id}` is no longer closed in SQ"
            ),
            Self::RuntimeTaskReset { task_id } => write!(
                formatter,
                "retained runtime task `{task_id}` was reset to pending; refusing to retry it"
            ),
            Self::LostWorker { task_id, detail } => {
                write!(formatter, "worker for task `{task_id}` is lost: {detail}")
            }
            Self::ReplacedWorker {
                task_id,
                expected,
                actual,
            } => write!(
                formatter,
                "worker for task `{task_id}` changed from `{expected}` to `{actual}`; refusing replacement"
            ),
            Self::LeaseExpired { task_id, deadline } => write!(
                formatter,
                "worker lease for task `{task_id}` expired at Unix time {deadline}"
            ),
            Self::SettledWithoutReport { task_id, status } => write!(
                formatter,
                "worker for task `{task_id}` settled as `{status:?}` without a matching AgentMail report"
            ),
            Self::WorkerBlocked { task_id } => write!(
                formatter,
                "worker for task `{task_id}` is blocked without a matching AgentMail report"
            ),
            Self::WorkerReported {
                task_id,
                status,
                summary,
            } => write!(
                formatter,
                "worker reported task `{task_id}` {status:?}: {summary}"
            ),
            Self::RetainedTerminalReport { task_id, status } => write!(
                formatter,
                "retained worker report for task `{task_id}` is {status:?}; refusing a silent retry"
            ),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Sq(error) => Some(error),
            Self::Herdr(error) => Some(error),
            Self::Identity(error) => Some(error),
            Self::Mail(error) => Some(error),
            Self::Runtime(error) => Some(error),
            Self::Worktree(error) => Some(error),
            Self::CurrentDirectory(source)
            | Self::Canonicalize { source, .. }
            | Self::CreateDirectory { source, .. } => Some(source),
            _ => None,
        }
    }
}
