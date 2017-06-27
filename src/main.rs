extern crate futures;
// look at replacing this with multiqueue...
// #[macro_use]
// extern crate chan;
extern crate multiqueue;
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

use std::sync::{Mutex, Arc};
use std::path::{PathBuf, Path};
use std::process::{Command, Child, ExitStatus};
use std::collections::HashMap;

use errors::*;

use futures::{Poll, Async, Future, IntoFuture};
use futures::future::{AndThen, JoinAll, BoxFuture, join_all, Shared, SharedError, SharedItem};
use futures::stream::{Stream, Collect, Take};
use futures::sink::Sink;
// use chan::{Sender, Receiver};
use multiqueue::{MPMCFutSender, MPMCFutReceiver, mpmc_fut_queue};
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

type Sender<T> = Arc<Mutex<MPMCFutSender<T>>>;
type Receiver<T> = Arc<Mutex<MPMCFutReceiver<T>>>;

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
    deps: Vec<Shared<BoxFuture<ExitStatus, Error>>>,
    ident: Option<String>,
    tpl: CommandTemplate,
    logdir: PathBuf,
    threads: usize,
    send: Sender<Token>,
    recv: Receiver<Token>,
}

impl Task {
    pub fn new(deps: Vec<Shared<BoxFuture<ExitStatus, Error>>>, tpl: CommandTemplate, recv: Receiver<Token>, send: Sender<Token>, threads: usize, logdir: Option<String>) -> Self {
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
        self.deps.iter().all(|task| match task.peek() {
            Some(Ok(_)) => true,
            _ => false
        })
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

struct RunnableTask {
    cmd: CommandLine,
    send: Sender<Token>,
    recv: Receiver<Token>,
    internal: Collect<Take<MPMCFutReceiver<Token>>>,
    tokens_needed: usize,
}

impl RunnableTask {
    pub fn new(cmd: CommandLine, send: Sender<Token>, recv: Receiver<Token>, tokens_needed: usize) -> Self {
        let internal = recv.lock().unwrap().take(tokens_needed as u64).collect(); 
        RunnableTask {
            cmd, send, tokens_needed, recv, internal
        }
    }
}

impl Future for RunnableTask {
    type Item = (CommandLine, usize, Sender<Token>);
    type Error = Error;

    // TODO: some way to trigger re-poll after a token is added back to the channel
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        match self.internal.poll() {
            Ok(Async::Ready(_)) => Ok(Async::Ready((self.cmd.clone(), self.tokens_needed, self.send.clone()))),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(()) => Err(ErrorKind::TokenStreamError.into()),
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

fn task_life(t: Task) -> Shared<BoxFuture<ExitStatus, Error>>
  {
    join_all(t.deps.clone())
        .map_err(move |se| *se)
        .and_then(move |_deps| RunnableTask::new(t.command().unwrap(), t.send, t.recv, t.threads))
        .and_then(|(cmd, tokens, send)| RunningTask::run(cmd, send, tokens).unwrap())
        .and_then(move |(status, tokens, send)| {
            send.lock().unwrap().send_all(futures::stream::repeat(()).take(tokens as u64)).then(move |_| futures::future::ok(status))
        })
        .boxed()
        .shared()
}

/// Build a future tree from a list of tasks.
///
/// It is assumed that all the dependencies of a task occur before it in the task list (which, of
/// course, implies that there are no circular dependencies)
fn build(tasks: &[Task]) -> BoxFuture<Vec<SharedItem<ExitStatus>>, SharedError<Error>> {
    join_all(tasks.iter().map(|&t| task_life(t)).collect::<Vec<_>>()).boxed()
}

fn main() {
    let args: Args = Docopt::new(USAGE)
        .and_then(|d| d.deserialize())
        .unwrap_or_else(|e| e.exit());

    let num_tokens = args.flag_threads.unwrap_or_else(num_cpus::get);
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
