use errors::*;

use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::sync::Arc;
use std::str::{from_utf8};
use std::fs::File;
use std::io::prelude::*;
use std::io::{BufReader, BufRead};
use std::os::unix::io::AsRawFd;
use serde_json;
use tempfile;
use libc;
use nom;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum FillType {
    Literal,
    Path
}

impl Default for FillType {
    fn default() -> Self {
        FillType::Literal
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CommandPlace {
    Fixed(String),
    Hole(String, FillType),
    FixedLiteral { key: String, value: String },
    FixedPath { key: String, value: String, path: PathBuf },
}

impl CommandPlace {
    pub fn full(&self) -> bool {
        use commands::CommandPlace::*;
        match self {
            &Hole(_, _) => false,
            _ => true
        }
    }
}

#[derive(Clone, Debug)]
pub struct CommandTemplate {
    pub(crate) template: Vec<CommandPlace>,
    holes: HashMap<String, usize>,
    log_hole: Option<usize>,
    tmpfiles: Vec<Arc<File>>,
}

impl CommandTemplate {
    fn empty() -> Self {
        CommandTemplate {
            template: vec![],
            holes: HashMap::default(),
            log_hole: None,
            tmpfiles: vec![]
        }
    }

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
            }).nth(0);
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
        self.holes.len() == 0 && (self.log_hole.is_none() || self.template[self.log_hole.unwrap()].full())
    }

    pub fn full_except_log(&self) -> bool {
        self.holes.len() == 0
    }

    pub fn to_cmd(self) -> Result<CommandLine> {
        if !self.full() {
            return Err(ErrorKind::NotFull.into());
        }
        use commands::CommandPlace::*;
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
    pub fn fill_log(&mut self, log: PathBuf) -> Result<()> {
        let log_hole = self.log_hole.ok_or(ErrorKind::NoSuchHole("log".to_string()))?;
        self.template[log_hole] = CommandPlace::FixedPath { key: "log".to_string(), value: "".to_string(), path: log };
        Ok(())
    }

    /// Search through the given set of logs to fill holes in the command template.
    ///
    /// Every log is assumed to be a sequence of JSON objects, one on each line.
    pub fn fill<P: AsRef<Path>>(&mut self, logs: &[P]) -> Result<()> {
        use commands::CommandPlace::*;
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

named!(filltype<FillType>, preceded!(tag!(":"), alt!(value!(FillType::Literal, tag!("literal")) | value!(FillType::Path, tag!("path")))));

named!(hole<CommandPlace>, map_res!(delimited!(tag!("<"), pair!(take_until_either!(":>"), opt!(filltype)), tag!(">")), 
                                           |(hole, ft): (&[u8], Option<FillType>)| {
                                               from_utf8(hole).map(|s| CommandPlace::Hole(s.to_string(), ft.unwrap_or_default())) 
                                           }));

named!(fixed<CommandPlace>, map_res!(is_not!(" \t\n\r"), |contents| from_utf8(contents).map(|s| CommandPlace::Fixed(s.to_string()))));
named!(cmd_template<CommandTemplate>, map!(fold_many1!(ws!(alt!(hole | fixed)), (0, CommandTemplate::empty()), |(i, mut tpl): (usize, CommandTemplate), place| {
    match place {
        CommandPlace::Fixed(_) => {}, // nothing extra to do
        CommandPlace::Hole(ref tag, _) => {
            if tag == "log" {
                tpl.log_hole = Some(i);
            } else {
                tpl.holes.insert(tag.clone(), i);
            }
        },
        _ => unreachable!()
    }

    tpl.template.push(place);
    (i + 1, tpl)
}), |(_, tpl)| tpl));

impl CommandTemplate {
    pub fn from_string(s: &str) -> Result<Self> {
        match cmd_template(s.as_bytes()) {
            nom::IResult::Done(_, tpl) => Ok(tpl),
            nom::IResult::Error(err) => Err(Error::from(err)),
            nom::IResult::Incomplete(_) => Err(ErrorKind::CommandTemplateIncomplete(s.to_string()).into())
        }
    }
}

#[derive(Clone, Debug)]
pub struct CommandLine {
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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn parse_hole() {
        let res = hole(b"<foo>").unwrap();
        assert!(res.1 == CommandPlace::Hole("foo".to_string(), FillType::Literal));
    }

    #[test]
    fn parse_hole_with_filltype() {
        assert!(hole(b"<foo:literal>").unwrap().1 == CommandPlace::Hole("foo".to_string(), FillType::Literal));
        assert!(hole(b"<foo:path>").unwrap().1 == CommandPlace::Hole("foo".to_string(), FillType::Path));
    }

    #[test]
    fn parse_cmd_template() {
        let (_, res) = cmd_template(b"./target/release/interdict <graph:path> <epsilon> <delta> --log <log:path> --threads <threads>").unwrap();
        use commands::CommandPlace::*;
        use commands::FillType::*;
        assert!(res.template == vec![
                Fixed("./target/release/interdict".to_string()), 
                Hole("graph".to_string(), Path),
                Hole("epsilon".to_string(), Literal),
                Hole("delta".to_string(), Literal),
                Fixed("--log".to_string()),
                Hole("log".to_string(), Path),
                Fixed("--threads".to_string()),
                Hole("threads".to_string(), Literal),
        ]);

        assert!(res.log_hole == Some(5));
    }

    #[test]
    fn cmd_tpl_from_string() {
        let tpl = CommandTemplate::from_string("./target/release/interdict <graph:path> <epsilon> <delta> --log <log:path> --threads <threads>").unwrap();

        use commands::CommandPlace::*;
        use commands::FillType::*;
        assert!(tpl.template == vec![
                Fixed("./target/release/interdict".to_string()), 
                Hole("graph".to_string(), Path),
                Hole("epsilon".to_string(), Literal),
                Hole("delta".to_string(), Literal),
                Fixed("--log".to_string()),
                Hole("log".to_string(), Path),
                Fixed("--threads".to_string()),
                Hole("threads".to_string(), Literal),
        ]);

        assert!(tpl.log_hole == Some(5));
    }
}
