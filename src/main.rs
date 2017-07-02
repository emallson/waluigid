extern crate futures;
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
extern crate serde_json;
extern crate tokio_core;
extern crate tokio_process;
extern crate tempfile;
extern crate libc;
extern crate sha3;
#[cfg(test)]
extern crate tempdir;

mod errors;

use std::os::unix::io::AsRawFd;
use std::fs::File;
use std::io::BufReader;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};
use std::collections::HashMap;
use std::sync::{RwLock, Arc};

use errors::*;

use futures::Future;
use futures::future::{join_all, Shared, err, ok};
use futures::stream::{Stream};
use futures::sink::Sink;
// use chan::{Sender, Receiver};
use multiqueue::{MPMCFutSender, MPMCFutReceiver};
use docopt::Docopt;
use sha3::{Digest, Sha3_256};

use tokio_process::{CommandExt};

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

type Sender<T> = MPMCFutSender<T>;
type Receiver<T> = MPMCFutReceiver<T>;
type SharedTask = Arc<RwLock<Task>>;

#[derive(Deserialize, Debug)]
struct Args {
    flag_threads: Option<usize>,
}

type Token = ();

#[derive(Copy, Clone, Debug)]
enum FillType {
    Literal,
    Path
}

#[derive(Clone, Debug)]
enum CommandPlace {
    Fixed(String),
    Hole(String, FillType),
    FixedLiteral { key: String, value: String },
    FixedPath { key: String, value: String, path: PathBuf },
}

impl CommandPlace {
    pub fn full(&self) -> bool {
        use CommandPlace::*;
        match self {
            &Hole(_, _) => false,
            _ => true
        }
    }
}

#[derive(Clone, Debug)]
struct CommandTemplate {
    template: Vec<CommandPlace>,
    holes: HashMap<String, usize>,
    log_hole: usize,
    tmpfiles: Vec<Arc<File>>,
}

impl CommandTemplate {
    pub fn new(v: Vec<CommandPlace>) -> Self {
        let holes = v.iter().enumerate().filter(|&(_, place)| {
            match place {
                &CommandPlace::Hole(ref s, _) => s != "log",
                _ => true,
            }
        }).filter_map(|(i, place)| {
                match place {
                    &CommandPlace::Hole(ref s, _) => Some((s.clone(), i)),
                    _ => None
                }
            }).collect();
        let log_hole = v.iter().enumerate().filter_map(|(i, place)| match place {
                &CommandPlace::Hole(ref s, _) => if s == "log" { Some(i) } else { None },
                _ => None
            }).nth(0).unwrap();
        CommandTemplate {
            template: v,
            holes,
            log_hole,
            tmpfiles: vec![]
        }
    }

    pub fn program(&self) -> String {
        match self.template[0] {
            CommandPlace::Fixed(ref s) => s.clone(),
            _ => unreachable!(),
        }
    }

    pub fn full(&self) -> bool { 
        self.holes.len() == 0 && self.template[self.log_hole].full()
    }

    pub fn full_except_log(&self) -> bool {
        self.holes.len() == 0
    }

    pub fn to_cmd(self) -> Result<CommandLine> {
        if !self.full() {
            return Err(ErrorKind::NotFull.into());
        }
        use CommandPlace::*;
        Ok(CommandLine { 
            args: self.template.into_iter().map(|plc| match plc {
                Fixed(s) => s,
                FixedLiteral { value, .. } => value,
                FixedPath { path, .. } => path.to_str().unwrap().to_string(),
                Hole(_, _) => unreachable!()
            }).collect() 
        })
    }

    /// Fills the hole for the log path
    pub fn fill_log(&mut self, log: PathBuf) {
        self.template[self.log_hole] = CommandPlace::FixedPath { key: "log".to_string(), value: "".to_string(), path: log };
    }

    /// Search through the given set of logs to fill holes in the command template.
    ///
    /// Every log is assumed to be a sequence of JSON objects, one on each line.
    ///
    /// This function is *lossy* in the sense that any filled hole will lose information about the
    /// kind of thing it was filled with (e.g. Hole("X", Literal) becomes Fixed("foo") so it is not
    /// possible in general to tell whether it was filled with a Literal or Path)
    pub fn fill<P: AsRef<Path>>(&mut self, logs: &[P]) -> Result<()> {
        use CommandPlace::*;
        if self.full() {
            return Ok(()); // all done
        }

        for log in logs {
            let f = File::open(log)?;
            let buf = BufReader::new(f);

            for line in buf.lines() {
                let map: HashMap<String, String> = serde_json::from_str(&line.unwrap())?;

                for (key, value) in map {
                    if let Some(&index) = self.holes.get(&key) {
                        self.holes.remove(&key);
                        let hole = self.template[index].clone();
                        self.template[index] = match hole {
                            Hole(_, FillType::Literal) => FixedLiteral { key, value },
                            Hole(_, FillType::Path) => {
                                // this *only* works on linux
                                assert!(cfg!(target_os = "linux"));
                                let mut tf = tempfile::tempfile()?;
                                let mut path = PathBuf::from("/proc/");
                                path.push(unsafe { libc::getpid().to_string() });
                                path.push("fd");
                                path.push(tf.as_raw_fd().to_string());

                                tf.write_all(value.as_bytes())?;
                                tf.flush()?;

                                self.tmpfiles.push(Arc::new(tf));

                                FixedPath { key, value, path }
                            },
                            _ => unreachable!()
                        };

                    }
                }
            }
        }

        Ok(())
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
    dep_tasks: Vec<SharedTask>,
    deps: Vec<TaskLifecycle>,
    tpl: CommandTemplate,
    logdir: PathBuf,
    threads: usize,
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
                use CommandPlace::{FixedLiteral, FixedPath};
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
enum TaskStatus {
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

type TaskLifecycle = Shared<Box<Future<Item=TaskStatus, Error=Error>>>;

fn task_life(handle: tokio_core::reactor::Handle, t: SharedTask, send: Sender<Token>, recv: Receiver<Token>) -> TaskLifecycle
  {
      let threads = t.read().unwrap().threads;
      let deps = t.read().unwrap().deps.clone();
      let b: Box<Future<Item=TaskStatus, Error=Error>> = 
          Box::new(join_all(deps)
                   .map_err(|_err| ErrorKind::DependencyError.into())
                   .and_then(move |_deps| {
                       assert!(t.read().unwrap().ready());
                       t.write().unwrap().fill_template()?;
                       if t.read().unwrap().complete() {
                           // terminate this chain with an "error"
                           return Err(ErrorKind::AlreadyComplete.into());
                       }
                       let cmd = t.read().unwrap().command()?;
                       Ok(cmd)
                   })
                   .and_then(move |cmd| {
                       recv.take(threads as u64).collect()
                           .map(move |_| cmd)
                           .map_err(|_| ErrorKind::TokenStreamError.into())
                   })
                   .and_then(move |cmd| {
                       let cmd = Command::new(cmd.program()).args(cmd.args())
                                      .spawn_async(&handle);

                       if let Err(e) = cmd {
                           err(e.into()).boxed()
                       } else {
                           cmd.unwrap()
                               .map_err(|err| err.into()).boxed()
                       }
                   })
                   .and_then(move |status| {
                       send.send_all(futures::stream::repeat(()).take(threads as u64))
                           .map(move |_| { 
                               TaskStatus::Ran(status)
                           })
                           .map_err(|_| ErrorKind::TokenStreamError.into())
                   })
                   .or_else(|e: Error| {
                       match e.kind() {
                           &ErrorKind::AlreadyComplete => ok(TaskStatus::AlreadyComplete),
                           _ => err(e)
                       }
                   }));

      b.shared()
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
    use tempdir::TempDir;
    use tokio_core::reactor::Core;

    // simple functionality test
    // the domain is the set [1, n] and each element k is dependent on its divisors.
    // we check that at the end, all n have been completed
    #[test]
    fn test_echo() {
        let logdir = TempDir::new("waluigid_test").unwrap();
        let n = 10;

        let (send, recv) = multiqueue::mpmc_fut_queue(10);
        send.clone().send_all(futures::stream::repeat(()).take(8)).poll().unwrap();

        let mut core = Core::new().unwrap();
        let mut tasks: Vec<SharedTask> = Vec::with_capacity(n);
        let mut task_lives: Vec<TaskLifecycle> = Vec::with_capacity(n);
        for i in 1..(n+1) {
            let divisors = (1..i).filter(|&j| i % j == 0).map(|j| task_lives[j-1].clone()).collect();
            let divisor_tasks = (1..i).filter(|&j| i % j == 0).map(|j| tasks[j-1].clone()).collect();
            let tpl = CommandTemplate::new(vec![Fixed("/usr/bin/time".to_string()), Fixed("-v".to_string()), 
                                           Fixed("-o".to_string()), Hole("log".to_string(), FillType::Path), 
                                           Fixed("true".to_string())]);
            let task = Arc::new(RwLock::new(Task::new(divisor_tasks, divisors, tpl, 1, Some(logdir.path().to_str().unwrap().to_string()))));
            tasks.push(task.clone());
            task_lives.push(task_life(core.handle(), task, send.clone(), recv.clone()));
        }

        core.run(join_all(task_lives)).unwrap();

        for task in tasks.into_iter() {
            assert!(task.read().unwrap().log_path().unwrap().exists());
        }
    }

    // confirms that running two tasks with identical parameters only actually runs one task.
    #[test]
    fn test_identical_paths() {
        let logdir = TempDir::new("waluigid_test").unwrap();
        let (send, recv) = multiqueue::mpmc_fut_queue(1);
        send.clone().send_all(futures::stream::repeat(()).take(1)).poll().unwrap();

        let mut core = Core::new().unwrap();
        let a_template = CommandTemplate::new(vec![Fixed("./target/debug/pairs".to_string()), 
                                                   FixedLiteral{ key: "ident".to_string(), value: "identical".to_string() },
                                                   Fixed("foo".to_string()), 
                                                   Fixed("bar".to_string()), 
                                                   Fixed("bar".to_string()),
                                                   Fixed("baz".to_string()),
                                                   Fixed("--log".to_string()),
                                                   Hole("log".to_string(), FillType::Path)]);
        let b_template = CommandTemplate::new(vec![Fixed("./target/debug/pairs".to_string()), 
                                                  FixedLiteral{ key: "ident".to_string(), value: "identical".to_string() },
                                                  Fixed("foo".to_string()), 
                                                  Fixed("bar".to_string()),
                                                  Fixed("--log".to_string()),
                                                  Hole("log".to_string(), FillType::Path)]);
        let a = Arc::new(RwLock::new(Task::new(vec![], vec![], a_template, 1, Some(logdir.path().to_str().unwrap().to_string()))));
        let a_life = task_life(core.handle(), a.clone(), send.clone(), recv.clone());
        let b = Arc::new(RwLock::new(Task::new(vec![a.clone()], vec![a_life.clone()], b_template, 1, Some(logdir.path().to_str().unwrap().to_string()))));
        let b_life = task_life(core.handle(), b.clone(), send.clone(), recv.clone());

        let res = core.run(join_all(vec![a_life, b_life])).unwrap();
        assert!(res[0].success());
        assert!(res[1].success() && !res[1].ran());
        println!("{:?}", res);

        assert!(a.read().unwrap().log_path().unwrap() == b.read().unwrap().log_path().unwrap());
    }

    // confirms that later tasks can read the results of earlier tasks from their logs.
    #[test]
    fn test_read_log() {
        let logdir = TempDir::new("waluigid_test").unwrap();
        let (send, recv) = multiqueue::mpmc_fut_queue(1);
        send.clone().send_all(futures::stream::repeat(()).take(1)).poll().unwrap();

        let mut core = Core::new().unwrap();
        let parent_template = CommandTemplate::new(vec![Fixed("./target/debug/pairs".to_string()), 
                                                   FixedLiteral{ key: "ident".to_string(), value: "parent".to_string() },
                                                   Fixed("foo".to_string()), 
                                                   Fixed("bar".to_string()), 
                                                   Fixed("bar".to_string()),
                                                   Fixed("baz".to_string()),
                                                   Fixed("--log".to_string()),
                                                   Hole("log".to_string(), FillType::Path)]);
        let child_template = CommandTemplate::new(vec![Fixed("./target/debug/pairs".to_string()), 
                                                  FixedLiteral{ key: "ident".to_string(), value: "child".to_string() },
                                                  Fixed("foo".to_string()), 
                                                  Hole("foo".to_string(), FillType::Literal), 
                                                  Fixed("bar".to_string()),
                                                  Hole("bar".to_string(), FillType::Literal),
                                                  Fixed("--log".to_string()),
                                                  Hole("log".to_string(), FillType::Path)]);
        let parent = Arc::new(RwLock::new(Task::new(vec![], vec![], parent_template, 1, Some(logdir.path().to_str().unwrap().to_string()))));
        let parent_life = task_life(core.handle(), parent.clone(), send.clone(), recv.clone());
        let child = Arc::new(RwLock::new(Task::new(vec![parent.clone()], vec![parent_life.clone()], child_template, 1, Some(logdir.path().to_str().unwrap().to_string()))));
        let child_life = task_life(core.handle(), child.clone(), send.clone(), recv.clone());

        core.run(join_all(vec![parent_life, child_life])).unwrap();

        let mut parent_contents = String::new();
        let mut child_contents = String::new();
        assert!(parent.read().unwrap().log_path().unwrap() != child.read().unwrap().log_path().unwrap());
        File::open(parent.read().unwrap().log_path().unwrap()).unwrap().read_to_string(&mut parent_contents).unwrap();
        File::open(child.read().unwrap().log_path().unwrap()).unwrap().read_to_string(&mut child_contents).unwrap();

        assert!(parent_contents == "{\"foo\":\"bar\"}\n{\"bar\":\"baz\"}\n");
        assert!(parent_contents == child_contents);
    }

    #[test]
    fn broadcast() {
        let (send, recv) = multiqueue::broadcast_fut_queue(2);
        send.send_all(futures::stream::repeat(()).take(2)).poll().unwrap();
        recv.take(2).collect().poll().unwrap();
    }

}
