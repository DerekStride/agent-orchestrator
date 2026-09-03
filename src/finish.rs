use std::path::{Path, PathBuf};
use std::thread;
use std::time::Duration as StdDuration;

use anyhow::{bail, Context, Result};
use chrono::Utc;
use serde::Serialize;

use crate::cli::FinishArgs;
use crate::herdr::{AgentState, Herdr};
use crate::identity::{AgentId, Identity};
use crate::mail::{reconcile_report, AgentMail, Handoff, ReportDisposition};
use crate::runtime::{RunState, RuntimeLedger, RuntimeStore, TaskRun};
use crate::sq::{Sq, Task, TaskGraph, TaskStatus};
use crate::worktree::Worktrees;

#[derive(Debug, Serialize)]
#[serde(rename_all = "snake_case", tag = "status")]
enum PassOutcome {
    Complete {
        root_task_id: String,
        run_id: Option<String>,
    },
    Active {
        root_task_id: String,
        run_id: String,
        launched: Vec<String>,
        waiting: Vec<String>,
        ledger: PathBuf,
    },
}

struct Reconciler<'a> {
    args: &'a FinishArgs,
    queue: PathBuf,
    repo: PathBuf,
    orchestrator: Identity,
    worktrees: Worktrees,
    herdr: Herdr,
    mail: AgentMail,
    sq: Sq,
    store: RuntimeStore,
    ledger: RuntimeLedger,
}

pub fn execute(args: &FinishArgs) -> Result<()> {
    if args.poll_seconds == 0 {
        bail!("--poll-seconds must be greater than zero");
    }
    if args.lease_seconds == 0 {
        bail!("--lease-seconds must be greater than zero");
    }

    let queue = args
        .queue
        .canonicalize()
        .with_context(|| format!("failed to resolve SQ queue {}", args.queue.display()))?;
    let store = RuntimeStore::open(&absolute(&args.state_dir)?, &args.root_task_id)?;
    let scope = TaskGraph::load(&queue)?.scope(&args.root_task_id)?;
    if scope.root().status == TaskStatus::Closed && !store.has_ledger() {
        return print_outcome(&PassOutcome::Complete {
            root_task_id: args.root_task_id.clone(),
            run_id: None,
        });
    }

    let worktrees = Worktrees::from_environment();
    let repo = worktrees.repo_root(&args.repo)?;
    let orchestrator = AgentId::from_environment().current()?;
    let herdr = Herdr::from_environment();
    herdr.preflight()?;
    let ledger = store.load_or_create(
        &args.root_task_id,
        &queue,
        &repo,
        &orchestrator,
        &scope.snapshot(),
    )?;
    scope.validate_ownership(&ledger.run_id)?;

    let mut reconciler = Reconciler {
        args,
        sq: Sq::from_environment(&queue),
        queue,
        repo,
        orchestrator,
        worktrees,
        herdr,
        mail: AgentMail::from_environment(),
        store,
        ledger,
    };

    loop {
        let outcome = reconciler.pass()?;
        print_outcome(&outcome)?;
        match outcome {
            PassOutcome::Complete { .. } => return Ok(()),
            PassOutcome::Active { .. } if args.once => return Ok(()),
            PassOutcome::Active { .. } => thread::sleep(StdDuration::from_secs(args.poll_seconds)),
        }
    }
}

impl Reconciler<'_> {
    fn pass(&mut self) -> Result<PassOutcome> {
        let scope = TaskGraph::load(&self.queue)?.scope(&self.args.root_task_id)?;
        if scope.snapshot() != self.ledger.plan {
            bail!(
                "SQ plan drift detected for root task {}; resolve the plan before resuming",
                self.args.root_task_id
            );
        }
        scope.validate_ownership(&self.ledger.run_id)?;

        for task_id in self.ledger.task_runs.keys().cloned().collect::<Vec<_>>() {
            let task = scope
                .task(&task_id)
                .with_context(|| format!("runtime task {task_id} left the scoped SQ plan"))?
                .clone();
            self.reconcile_run(&task)?;
        }

        let outstanding = self
            .ledger
            .task_runs
            .values()
            .filter(|run| run.state != RunState::Completed)
            .map(|run| run.task_id.clone())
            .collect::<Vec<_>>();
        if scope.root().status == TaskStatus::Closed {
            if outstanding.is_empty() {
                return Ok(PassOutcome::Complete {
                    root_task_id: self.args.root_task_id.clone(),
                    run_id: Some(self.ledger.run_id.clone()),
                });
            }
            return Ok(PassOutcome::Active {
                root_task_id: self.args.root_task_id.clone(),
                run_id: self.ledger.run_id.clone(),
                launched: Vec::new(),
                waiting: outstanding,
                ledger: self.store.path().to_path_buf(),
            });
        }

        for task in scope.tasks() {
            if task.status == TaskStatus::InProgress
                && !self.ledger.task_runs.contains_key(&task.id)
            {
                bail!(
                    "SQ task {} is in progress without a runtime ledger entry; refusing to steal it",
                    task.id
                );
            }
        }

        let ready = scope
            .ready_tasks()
            .into_iter()
            .filter(|task| {
                !self.ledger.task_runs.contains_key(&task.id)
                    && task.blocked_by.iter().all(|blocker_id| {
                        self.ledger
                            .task_runs
                            .get(blocker_id)
                            .is_none_or(|run| run.state == RunState::Completed)
                    })
            })
            .cloned()
            .collect::<Vec<_>>();
        let mut launched = Vec::new();
        for task in ready {
            if let Err(error) = self.provision(&task) {
                self.fail_run(&task.id, RunState::Failed, &format!("{error:#}"));
                self.persist()?;
                return Err(error).with_context(|| format!("failed to provision task {}", task.id));
            }
            launched.push(task.id);
        }

        let waiting = self
            .ledger
            .task_runs
            .values()
            .filter(|run| run.state == RunState::Working)
            .map(|run| run.task_id.clone())
            .filter(|task_id| !launched.contains(task_id))
            .collect::<Vec<_>>();

        if launched.is_empty() && waiting.is_empty() {
            bail!(
                "scoped SQ plan for root task {} is stalled with no ready or active tasks",
                self.args.root_task_id
            );
        }

        Ok(PassOutcome::Active {
            root_task_id: self.args.root_task_id.clone(),
            run_id: self.ledger.run_id.clone(),
            launched,
            waiting,
            ledger: self.store.path().to_path_buf(),
        })
    }

    fn reconcile_run(&mut self, task: &Task) -> Result<()> {
        let run = self
            .ledger
            .task_runs
            .get(&task.id)
            .context("runtime ledger lost a task run")?
            .clone();

        match run.state {
            RunState::Completed => return Ok(()),
            RunState::Blocked | RunState::Failed | RunState::Lost => bail!(
                "task {} is {:?}: {}",
                task.id,
                run.state,
                run.detail
                    .as_deref()
                    .unwrap_or("manual intervention required")
            ),
            RunState::Provisioning => {
                self.fail_run(
                    &task.id,
                    RunState::Failed,
                    "a previous finish process stopped during provisioning",
                );
                self.persist()?;
                bail!(
                    "task {} has an interrupted provisioning run; refusing to retry it silently",
                    task.id
                );
            }
            RunState::Working => {}
        }

        let worker = run
            .worker
            .as_ref()
            .context("working runtime has no Agent ID identity")?;
        let handles = run
            .herdr
            .as_ref()
            .context("working runtime has no Herdr handles")?;

        if let Some((message_id, report)) =
            self.mail
                .scan_report(&self.orchestrator, worker, &task.id, &self.ledger.run_id)?
        {
            let disposition = reconcile_report(&task.status, &report)?;
            let entry = self.ledger.task_runs.get_mut(&task.id).unwrap();
            entry.report_message_id = Some(message_id.clone());
            entry.detail = report.summary.clone();
            entry.report = Some(report.clone());
            entry.heartbeat(self.args.lease_seconds);
            entry.state = match disposition {
                ReportDisposition::Completed => RunState::Completed,
                ReportDisposition::Blocked => RunState::Blocked,
                ReportDisposition::Failed => RunState::Failed,
            };
            self.persist()?;
            self.mail.mark_read(&message_id)?;

            return match disposition {
                ReportDisposition::Completed => Ok(()),
                ReportDisposition::Blocked => bail!(
                    "task {} is blocked: {}",
                    task.id,
                    report
                        .summary
                        .as_deref()
                        .unwrap_or("worker reported a blocker")
                ),
                ReportDisposition::Failed => bail!(
                    "task {} failed: {}",
                    task.id,
                    report
                        .summary
                        .as_deref()
                        .unwrap_or("worker reported a failure")
                ),
            };
        }

        match self.herdr.agent_state(handles) {
            Ok(AgentState::Working | AgentState::Unknown) => {
                self.ledger
                    .task_runs
                    .get_mut(&task.id)
                    .unwrap()
                    .heartbeat(self.args.lease_seconds);
                self.persist()
            }
            Ok(AgentState::Idle | AgentState::Done) => {
                self.fail_run(
                    &task.id,
                    RunState::Blocked,
                    "worker settled without a structured AgentMail report",
                );
                self.persist()?;
                bail!(
                    "task {} worker settled without a structured AgentMail report; pane state is not completion",
                    task.id
                )
            }
            Ok(AgentState::Blocked) => {
                self.fail_run(
                    &task.id,
                    RunState::Blocked,
                    "Herdr reports the worker is blocked",
                );
                self.persist()?;
                bail!(
                    "task {} is blocked in Herdr; inspect pane {}",
                    task.id,
                    handles.pane_id
                )
            }
            Err(error) if run.lease_expired() => {
                self.fail_run(
                    &task.id,
                    RunState::Lost,
                    &format!("worker lease expired while Herdr lookup failed: {error:#}"),
                );
                self.persist()?;
                bail!("task {} worker is lost: {error:#}", task.id)
            }
            Err(error) => {
                self.persist()?;
                Err(error)
                    .with_context(|| format!("failed to inspect Herdr worker for task {}", task.id))
            }
        }
    }

    fn provision(&mut self, task: &Task) -> Result<()> {
        let blocker_branches = task
            .blocked_by
            .iter()
            .filter_map(|id| self.ledger.task_runs.get(id))
            .filter(|run| run.state == RunState::Completed)
            .map(|run| run.branch.clone())
            .collect::<Vec<_>>();
        let plan = self.worktrees.plan(
            &self.repo,
            &self.args.root_task_id,
            &task.id,
            &blocker_branches,
            self.args.worktree_root.as_deref(),
        )?;

        self.ledger.task_runs.insert(
            task.id.clone(),
            TaskRun::provisioning(
                task.id.clone(),
                self.ledger.run_id.clone(),
                plan.path.clone(),
                plan.branch.clone(),
                plan.base.clone(),
                self.args.lease_seconds,
            ),
        );
        self.persist()?;

        self.sq.claim(&task.id, &self.ledger.run_id)?;
        let handles = self.worktrees.provision(
            &plan,
            &self.queue,
            &task.id,
            &self.ledger.run_id,
            &task.title,
            &self.herdr,
        )?;
        self.ledger.task_runs.get_mut(&task.id).unwrap().herdr = Some(handles.clone());
        self.persist()?;

        let worker = AgentId::from_environment().discover_worker(&plan.path)?;
        self.ledger.task_runs.get_mut(&task.id).unwrap().worker = Some(worker.clone());
        self.persist()?;

        let acceptance = task.acceptance_criteria();
        let checks = task.validation_checks();
        let receipt = self.mail.send_handoff(&Handoff {
            run_id: &self.ledger.run_id,
            task_id: &task.id,
            queue: &self.queue,
            worktree: &plan.path,
            branch: &plan.branch,
            dependencies: &task.blocked_by,
            acceptance_criteria: &acceptance,
            validation_checks: &checks,
            orchestrator: &self.orchestrator,
            worker: &worker,
        })?;
        self.ledger
            .task_runs
            .get_mut(&task.id)
            .unwrap()
            .handoff_message_id = Some(receipt.id.clone());
        self.persist()?;

        self.herdr.prompt(
            &handles,
            &format!(
                "Read AgentMail message {} from {} and execute that handoff. Report results only through AgentMail as the handoff specifies.",
                receipt.id, self.orchestrator.slug
            ),
        )?;

        let entry = self.ledger.task_runs.get_mut(&task.id).unwrap();
        entry.state = RunState::Working;
        entry.heartbeat(self.args.lease_seconds);
        self.persist()
    }

    fn fail_run(&mut self, task_id: &str, state: RunState, detail: &str) {
        if let Some(run) = self.ledger.task_runs.get_mut(task_id) {
            run.state = state;
            run.detail = Some(detail.to_owned());
        }
    }

    fn persist(&mut self) -> Result<()> {
        self.ledger.updated_at = Utc::now();
        self.store.save(&self.ledger)
    }
}

fn absolute(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    Ok(std::env::current_dir()
        .context("failed to read the current directory")?
        .join(path))
}

fn print_outcome(outcome: &PassOutcome) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(outcome)?);
    Ok(())
}
