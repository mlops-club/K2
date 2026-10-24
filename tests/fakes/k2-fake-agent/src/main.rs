//! TEST ONLY: see `lib.rs`.

use std::io::{self, BufReader};
use std::path::Path;

fn main() {
    let mut argv = std::env::args();
    let argv0 = argv.next().unwrap_or_default();
    let name = Path::new(&argv0)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("k2-fake-agent")
        .to_string();
    let rest: Vec<String> = argv.collect();
    let env = match k2_fake_agent::Env::from_process() {
        Ok(e) => e,
        Err(e) => {
            eprintln!("k2-fake-agent: no cwd: {e}");
            std::process::exit(1);
        }
    };
    let stdin = io::stdin();
    let mut input = BufReader::new(stdin.lock());
    let stdout = io::stdout();
    let mut out = stdout.lock();
    let code = k2_fake_agent::run(&name, &rest, &env, &mut input, &mut out);
    std::process::exit(code);
}
