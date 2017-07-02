extern crate docopt;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;

use std::fs::File;
use std::io::{Write, BufWriter};
use std::collections::HashMap;

const USAGE: &str = "
Simple test program to generate \"log\" files.

Usage:
    pairs <id> (<key> <value>)... --log <log>
    pairs (-h | --help)

Options:
    -h --help               Show this screen.
    --log <log>             File to output to.
";

#[derive(Deserialize)]
struct Args {
    #[allow(dead_code)]
    arg_id: String, /* only used to guarantee that I can run multiples with the same contents, for easy comparison. */
    arg_key: Vec<String>,
    arg_value: Vec<String>,
    flag_log: String,
}

fn main() {
    let args: Args =
        docopt::Docopt::new(USAGE).and_then(|d| d.deserialize()).unwrap_or_else(|e| e.exit());

    let out = File::create(args.flag_log).unwrap();
    let mut writer = BufWriter::new(out);
    for (key, value) in args.arg_key.into_iter().zip(args.arg_value.into_iter()) {
        let mut map: HashMap<String, String> = HashMap::default();
        map.insert(key, value);
        writeln!(writer, "{}", serde_json::to_string(&map).unwrap()).unwrap();
    }
}
