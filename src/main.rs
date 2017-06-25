extern crate futures;
#[macro_use]
extern crate chan;
#[macro_use]
extern crate error_chain;
#[macro_use]
extern crate slog;
extern crate slog_term;
extern crate slog_async;
extern crate docopt;
extern crate num_cpus;
#[macro_use]
extern crate serde_derive;

mod errors;

use std::sync::Arc;
use std::path::{PathBuf, Path};
use std::process::{Command, Child, ExitStatus};

use errors::*;

use futures::{Poll, Async, Future};
use chan::{Sender, Receiver};
use docopt::Docopt;

#[cfg_attr(rustfmt, rustfmt_ignore)]
const USAGE: &str = "
Waluigi Daemon.

Usage:
    waluigid [--threads <t>]
    waluigid (-h | --help)
    waluigid --version

Options:
    --help -h       Show this message.
    --version       Show version information.
    --threads <t>   Number of threads available. Defaults to the number of CPUs available.
";

#[derive(Deserialize, Debug)]
struct Args {
    flag_threads: Option<usize>,
}

type Token = ();

/// A task that at some point could become runnable. It will not become runnable until all of its
/// dependencies are satisfied.
struct Task {
    deps: Vec<Arc<Task>>,
    ident: String,
}

impl Task {
    /// Produces the path to the log that will be present after the task is completed.
    ///
    /// The path cannot be computed until the task is ready to be run, and may return an Error
    /// prior to that.
    pub fn log_path(&self) -> Result<PathBuf> {
        Ok(PathBuf::from(&format!("logs/{}.log", self.ident)))
    }

    /// Determines whether the task is complete or not.
    pub fn complete(&self) -> bool {
        self.log_path().map(|p| p.exists()).unwrap_or(false)
    }

    /// Determines whether this task is ready to be run.
    pub fn ready(&self) -> bool {
        self.deps.iter().all(|task| task.complete())
    }

    /// Produces the command to run this task.
    ///
    /// Produces an error if the task is not ready to be run.
    pub fn command(&self) -> Result<CommandLine> {
        if self.ready() {
            Ok(())
        } else {
            Err(ErrorKind::NotReady.into())
        }
    }
}

type CommandLine = ();

impl Future for Task {
    type Item = CommandLine;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        if self.ready() {
            Ok(Async::Ready(self.command().unwrap()))
        } else {
            Ok(Async::NotReady)
        }
    }
}

struct RunnableTask {
    cmd: CommandLine,
    send: Sender<Token>,
    recv: Receiver<Token>,
    tokens: usize,
    tokens_needed: usize,
}

impl Future for RunnableTask {
    type Item = (CommandLine, usize, Sender<Token>);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        let ref recv = self.recv;
        while self.tokens < self.tokens_needed {
            chan_select! {
                default => { return Ok(Async::NotReady); },
                recv.recv() => { self.tokens += 1 }
            }
        }
        assert!(self.tokens >= self.tokens_needed);
        Ok(Async::Ready((self.cmd, self.tokens, self.send.clone())))
    }
}

struct RunningTask {
    cmd: CommandLine,
    process: Child,
    send: Sender<Token>,
    tokens: usize,
}

impl Future for RunningTask {
    type Item = (ExitStatus, usize, Sender<Token>);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.process.try_wait() {
            Ok(Some(status)) => Ok(Async::Ready((status, self.tokens, self.send.clone()))),
            Ok(None) => Ok(Async::NotReady),
            Err(e) => Err(e.into()),
        }
    }
}

impl Drop for RunningTask {
    fn drop(&mut self) {
        for _ in 0..self.tokens {
            self.send.send(());
        }
    }
}

fn main() {
    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.deserialize())
        .unwrap_or_else(|e| e.exit());

    let num_tokens = args.flag_threads.unwrap_or_else(num_cpus::get);
    let (send, recv) = chan::sync::<Token>(num_tokens);
}
