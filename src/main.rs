extern crate futures;
// look at replacing this with multiqueue...
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
extern crate tokio_core;
#[cfg(test)]
extern crate tempdir;

mod errors;

use std::sync::Arc;
use std::path::{PathBuf, Path};
use std::process::{Command, Child, ExitStatus};
use std::collections::HashMap;

use errors::*;

use futures::{Poll, Async, Future, IntoFuture};
use futures::future::{AndThen, JoinAll, BoxFuture, join_all};
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

#[derive(Clone, Debug)]
enum CommandPlace {
    Fixed(String),
    Hole(String)
}

#[derive(Clone, Debug)]
struct CommandTemplate {
    template: Vec<CommandPlace>,
    holes: HashMap<String, usize>,
}

impl CommandTemplate {
    pub fn new(v: Vec<CommandPlace>) -> Self {
        let holes = v.iter().enumerate().filter_map(|(i, place)| {
                match place {
                    &CommandPlace::Fixed(_) => None,
                    &CommandPlace::Hole(ref s) => Some((s.clone(), i)),
                }
            }).collect();
        CommandTemplate {
            template: v,
            holes,
        }
    }

    pub fn full(&self) -> bool { 
        self.holes.len() == 0
    }

    pub fn to_cmd(self) -> CommandLine {
        CommandLine { 
            args: self.template.into_iter().map(|plc| match plc {
                CommandPlace::Fixed(s) => s,
                _ => unreachable!()
            }).collect() 
        }
    }
}

#[derive(Clone, Debug)]
struct CommandLine {
    args: Vec<String>,
}

impl CommandLine {
    pub fn program(&self) -> &str {
        &self.args[0]
    }

    pub fn args<'a>(&'a self) -> &'a [String] {
        &self.args[1..]
    }
}

/// A task that at some point could become runnable. It will not become runnable until all of its
/// dependencies are satisfied.
#[derive(Clone)]
struct Task {
    deps: Vec<TaskRef>,
    ident: Option<String>,
    tpl: CommandTemplate,
    logdir: PathBuf,
    threads: usize,
    send: Sender<Token>,
    recv: Receiver<Token>,
}

impl Task {
    pub fn new(deps: Vec<TaskRef>, tpl: CommandTemplate, recv: Receiver<Token>, send: Sender<Token>, threads: usize, logdir: Option<String>) -> Self {
        Task { deps, tpl, threads, send, recv, logdir: logdir.map(|p| PathBuf::from(p)).unwrap_or_else(|| PathBuf::from("logs")), ident: None }
    }

    /// Produces the path to the log that will be present after the task is completed.
    ///
    /// The path cannot be computed until the task is ready to be run, and may return an Error
    /// prior to that.
    pub fn log_path(&self) -> Result<PathBuf> {
        Ok(self.logdir.join(self.ident.clone().ok_or(Error::from(ErrorKind::NotReady))?))
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
            assert!(self.tpl.full());
            Ok(self.tpl.clone().to_cmd())
        } else {
            Err(ErrorKind::NotReady.into())
        }
    }
}

impl Future for Task {
    type Item = (CommandLine, Sender<Token>, Receiver<Token>, usize);
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        println!("polling task");
        if self.ready() {
            println!("task ready");
            Ok(Async::Ready((self.command().unwrap(), self.send.clone(), self.recv.clone(), self.threads)))
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

    // TODO: some way to trigger re-poll after a token is added back to the channel
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        println!("polling runnable");
        let ref recv = self.recv;
        while self.tokens < self.tokens_needed {
            chan_select! {
                default => { return Ok(Async::NotReady); },
                recv.recv() => { self.tokens += 1 }
            }
            println!("received token. have {}, need {}", self.tokens, self.tokens_needed);
        }
        assert!(self.tokens >= self.tokens_needed);
        Ok(Async::Ready((self.cmd.clone(), self.tokens, self.send.clone())))
    }
}

impl Drop for RunnableTask {
    // return all tokens to the channel if we get dropped
    fn drop(&mut self) {
        for _ in 0..self.tokens {
            self.send.send(());
        }
    }
}

struct RunningTask {
    cmd: CommandLine,
    process: Child,
    send: Sender<Token>,
    tokens: usize,
}

impl RunningTask {
    pub fn run(cmd: CommandLine, send: Sender<Token>, tokens: usize) -> Result<Self> {
        println!("running {:?}", cmd);
        let process = Command::new(cmd.program())
            .args(cmd.args())
            .spawn()?;

        Ok(RunningTask { cmd, send, tokens, process })
    }
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
    // return all tokens to the channel if we get dropped
    fn drop(&mut self) {
        for _ in 0..self.tokens {
            self.send.send(());
        }
    }
}

#[derive(Clone)]
struct TaskRef(Arc<Task>);

impl IntoFuture for TaskRef {
    type Future = Task;
    type Item = <Task as Future>::Item;
    type Error = <Task as Future>::Error;

    fn into_future(self) -> Self::Future {
        (*self.0).clone()
    }
}

impl ::std::ops::Deref for TaskRef {
    type Target = Task;

    fn deref(&self) -> &Self::Target {
        &*self.0
    }
}

fn task_life(t: TaskRef) -> BoxFuture<ExitStatus, Error>
  {
    join_all(t.deps.clone())
        .and_then(move |_deps| t)
        .and_then(|(cmd, send, recv, tokens_needed)| RunnableTask {
            cmd, send, recv, tokens_needed, tokens: 0,
        })
    .and_then(|(cmd, tokens, send)| RunningTask::run(cmd, send, tokens).unwrap())
        .map(|(status, tokens, send)| {
            for _ in 0..tokens {
                send.send(());
            }
            status
        })
        .boxed()
}

/// Build a future tree from a list of tasks.
///
/// It is assumed that all the dependencies of a task occur before it in the task list (which, of
/// course, implies that there are no circular dependencies)
fn build(tasks: &[TaskRef]) -> BoxFuture<Vec<ExitStatus>, Error> {
    join_all(tasks.iter().map(|t| task_life(t.clone())).collect::<Vec<_>>()).boxed()
}

fn main() {
    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.deserialize())
        .unwrap_or_else(|e| e.exit());

    let num_tokens = args.flag_threads.unwrap_or_else(num_cpus::get);
    let (send, recv) = chan::sync::<Token>(num_tokens);
}

#[cfg(test)]
mod test {
    use super::*;
    use super::CommandPlace::*;
    use std::sync::Arc;
    use tempdir::TempDir;
    use tokio_core::reactor::Core;

    // simple functionality test
    // the domain is the set [1, n] and each element k is dependent on its divisors.
    // we check that at the end, all n have been completed
    #[test]
    fn test_echo() {
        let logdir = TempDir::new("waluigid_test").unwrap();
        let n = 10;

        let (send, recv) = chan::sync(2);

        let mut tasks: Vec<TaskRef> = Vec::with_capacity(n);
        for i in 1..(n+1) {
            let divisors = (1..i).filter(|&j| i % j == 0).map(|j| tasks[j-1].clone()).collect();
            let tpl = CommandTemplate::new(vec![Fixed("echo".to_string()), Fixed(format!("{}", i))]);
            tasks.push(TaskRef(Arc::new(Task::new(divisors, tpl, recv.clone(), send.clone(), 1, Some(logdir.path().to_str().unwrap().to_string())))));
        }

        let tree = build(&tasks);
        let mut core = Core::new().unwrap();
        core.run(tree).unwrap();

        assert!(tasks.iter().all(|t| t.complete()));
    }

}
