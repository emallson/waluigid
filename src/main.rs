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
#[macro_use]
extern crate nom;
extern crate waluigi_lib;
#[cfg(test)]
extern crate tempdir;

mod errors;
mod commands;
mod tasks;

use std::process::Command;

use errors::*;
use tasks::*;

use futures::Future;
use futures::future::{join_all, err, ok};
use futures::stream::{Stream};
use futures::sink::Sink;
// use chan::{Sender, Receiver};
use multiqueue::{MPMCFutSender, MPMCFutReceiver};
use docopt::Docopt;

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

#[derive(Deserialize, Debug)]
struct Args {
    flag_threads: Option<usize>,
}

type Token = ();

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
    use commands::CommandPlace::*;
    use tempdir::TempDir;
    use tokio_core::reactor::Core;
    use commands::*;
    use std::fs::File;
    use std::io::prelude::*;
    use std::sync::{Arc, RwLock};
    
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
