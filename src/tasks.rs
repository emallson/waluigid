use commands::*;
use errors::*;

use std::sync::{Arc, RwLock};
use std::process::ExitStatus;
use std::path::PathBuf;
use futures::future::{Shared, Future};
use sha3::{Sha3_256, Digest};

pub type SharedTask = Arc<RwLock<Task>>;

/// A task that at some point could become runnable. It will not become runnable until all of its
/// dependencies are satisfied.
#[derive(Clone)]
pub struct Task {
    dep_tasks: Vec<SharedTask>,
    pub(crate) deps: Vec<TaskLifecycle>,
    tpl: CommandTemplate,
    logdir: PathBuf,
    pub(crate) threads: usize,
    ident: Option<String>,
}

impl Task {
    pub fn new(dep_tasks: Vec<SharedTask>, deps: Vec<TaskLifecycle>, tpl: CommandTemplate, threads: usize, logdir: Option<String>) -> Self {
        Task { dep_tasks, deps, tpl, threads, logdir: logdir.map(|p| PathBuf::from(p)).unwrap_or_else(|| PathBuf::from("logs")),
               ident: None }
    }

    pub fn ident(&self) -> Result<String> {
        self.ident.clone().ok_or(ErrorKind::NotReady.into())
    }

    fn determine_ident(&mut self) -> Result<String> {
        if self.ready() {
            self.fill_without_log()?;
            let mut hasher = Sha3_256::default();
            hasher.input(self.tpl.program().as_bytes());
            let mut pairs = Vec::new();
            for place in &self.tpl.template {
                use commands::CommandPlace::{FixedLiteral, FixedPath};
                match place {
                    &FixedLiteral { ref key, ref value } => pairs.push((key, value)),
                    &FixedPath { ref key, ref value, .. } => pairs.push((key, value)),
                    _ => continue
                }
            }

            pairs.sort(); // sort so that the identifier is stable even if --parameters are given in a different order
            for (key, value) in pairs {
                hasher.input(key.as_bytes());
                hasher.input(value.as_bytes());
            }

            Ok(format!("{:x}", hasher.result()))
        } else {
            Err(ErrorKind::NotReady.into())
        }
    }

    /// Produces the path to the log that will be present after the task is completed.
    ///
    /// The path cannot be computed until the task is ready to be run, and may return an Error
    /// prior to that.
    pub fn log_path(&self) -> Result<PathBuf> {
        Ok(self.logdir.join(self.ident()?))
    }

    /// Determines whether the task is complete or not.
    pub fn complete(&self) -> bool {
        self.log_path().map(|p| p.exists()).unwrap_or(false)
    }

    /// Determines whether this task is ready to be run.
    pub fn ready(&self) -> bool {
        self.deps.iter().all(|task| match task.peek() {
            Some(Ok(_)) => true,
            _ => false
        })
    }

    fn fill_without_log(&mut self) -> Result<()> {
            if !self.tpl.full_except_log() {
                self.tpl.fill(&self.dep_tasks.iter_mut().map(|dep| {
                    dep.read().unwrap().log_path()
                }).collect::<Result<Vec<_>>>()?)?;
                if self.tpl.full_except_log() {
                    Ok(())
                } else {
                    Err(ErrorKind::MissingField.into())
                }
            } else {
                Ok(())
            }

    }

    pub fn fill_template(&mut self) -> Result<()> {
        self.fill_without_log()?;
        if !self.tpl.full() {
            let ident = self.determine_ident()?;
            self.ident = Some(ident.clone());
            let log_path = self.log_path()?;
            self.tpl.fill_log(log_path);
            if self.tpl.full() {
                Ok(())
            } else {
                Err(ErrorKind::MissingField.into())
            }
        } else {
            Ok(())
        }
    }

    /// Produces the command to run this task.
    ///
    /// Produces an error if the task is not ready to be run.
    pub fn command(&self) -> Result<CommandLine> {
        if self.ready() {
            self.tpl.clone().to_cmd()
        } else {
            Err(ErrorKind::NotReady.into())
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub enum TaskStatus {
    Ran(ExitStatus),
    AlreadyComplete
}

impl TaskStatus {
    pub fn success(&self) -> bool {
        match self {
            &TaskStatus::Ran(ref status) => status.success(),
            &TaskStatus::AlreadyComplete => true
        }
    }

    pub fn ran(&self) -> bool {
        match self {
            &TaskStatus::AlreadyComplete => false,
            _ => true
        }
    }
}

pub type TaskLifecycle = Shared<Box<Future<Item=TaskStatus, Error=Error>>>;
