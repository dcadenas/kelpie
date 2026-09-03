use std::env;
use std::ffi::OsString;
use std::fs::DirBuilder;
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use kelpie::daemon::Daemon;
use kelpie::herdr::HerdrClient;
use kelpie::slice::Kelpie;
use kelpie::store::Store;

const USAGE: &str = "\
kelpied [--database PATH] [--socket PATH] [--herdr-socket PATH] [--herdr-wait-ms MS]
kelpied --version

Defaults:
  database       $XDG_STATE_HOME/kelpie/kelpie.sqlite3
  socket         $XDG_RUNTIME_DIR/kelpie/kelpie.sock
  Herdr socket   $HERDR_SOCKET_PATH or $XDG_CONFIG_HOME/herdr/herdr.sock
  Herdr wait     120000 ms; 0 fails immediately when Herdr is absent
";

/// How long startup waits for a Herdr socket that has not appeared yet.
const DEFAULT_HERDR_WAIT_MS: u64 = 120_000;
const HERDR_POLL: Duration = Duration::from_millis(500);

#[derive(Debug, Default, PartialEq, Eq)]
struct Options {
    database: Option<PathBuf>,
    socket: Option<PathBuf>,
    herdr_socket: Option<PathBuf>,
    herdr_wait_ms: Option<u64>,
}

#[derive(Debug, PartialEq, Eq)]
enum Invocation {
    Run(Options),
    Help,
    Version,
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("kelpied: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let invocation = parse_args(env::args_os().skip(1))?;
    let Invocation::Run(options) = invocation else {
        match invocation {
            Invocation::Help => print!("{USAGE}"),
            Invocation::Version => println!("kelpied {}", env!("CARGO_PKG_VERSION")),
            Invocation::Run(_) => unreachable!(),
        }
        return Ok(());
    };

    let database = options
        .database
        .map_or_else(kelpie::paths::database_path, Ok)?;
    let kelpie_socket = options
        .socket
        .unwrap_or_else(kelpie::paths::kelpie_socket_path);
    let herdr_socket = options
        .herdr_socket
        .map_or_else(kelpie::paths::herdr_socket_path, Ok)?;

    let herdr_wait = Duration::from_millis(options.herdr_wait_ms.unwrap_or(DEFAULT_HERDR_WAIT_MS));

    create_private_parent(&database)?;
    create_private_parent(&kelpie_socket)?;
    // Before touching the database: a second daemon over the same store is
    // the one startup mistake that must fail fast.
    kelpie::daemon::claim_socket_path(&kelpie_socket)?;

    let store = Store::open(&database)?;
    let herdr = HerdrClient::new(herdr_socket.clone(), Duration::from_secs(5));
    // Herdr's socket appears when Herdr starts, which can be well after this
    // service does. Waiting keeps a boot race from becoming a permanent failure.
    // Say so: an operator watching a silent terminal cannot tell waiting from hung.
    if !herdr_socket.exists() && !herdr_wait.is_zero() {
        eprintln!(
            "kelpied: waiting up to {herdr_wait:?} for Herdr at {}",
            herdr_socket.display()
        );
    }
    herdr.wait_until_present(herdr_wait, HERDR_POLL)?;
    let mut kelpie = Kelpie::new(store, herdr);
    kelpie.recover()?;
    let mut daemon = Daemon::bind(&kelpie_socket, kelpie)?;
    eprintln!(
        "kelpied: listening on {} (database {}, Herdr {})",
        kelpie_socket.display(),
        database.display(),
        herdr_socket.display()
    );
    kelpie::test_fault::pause("daemon_bound");
    daemon.run()?;
    Ok(())
}

fn parse_args(arguments: impl IntoIterator<Item = OsString>) -> Result<Invocation, String> {
    let arguments: Vec<OsString> = arguments.into_iter().collect();
    if arguments.len() == 1 && matches!(arguments[0].to_str(), Some("--version" | "-V")) {
        return Ok(Invocation::Version);
    }
    if arguments.len() == 1 && matches!(arguments[0].to_str(), Some("--help" | "-h")) {
        return Ok(Invocation::Help);
    }

    let mut options = Options::default();
    let mut index = 0;
    while index < arguments.len() {
        let flag = arguments[index]
            .to_str()
            .ok_or("arguments must be valid UTF-8")?;
        if matches!(flag, "--help" | "-h" | "--version" | "-V") {
            return Err(format!("{flag} cannot be combined with other arguments"));
        }
        if !matches!(
            flag,
            "--database" | "--socket" | "--herdr-socket" | "--herdr-wait-ms"
        ) {
            return Err(format!("unknown argument {flag}\n{USAGE}"));
        }
        let flag = flag.to_string();
        index += 1;
        let value = arguments
            .get(index)
            .ok_or_else(|| format!("{flag} requires a value"))?;
        if value.is_empty() {
            return Err(format!("{flag} requires a non-empty value"));
        }
        if flag == "--herdr-wait-ms" {
            if options.herdr_wait_ms.is_some() {
                return Err(format!("{flag} specified more than once"));
            }
            let milliseconds = value
                .to_str()
                .ok_or("arguments must be valid UTF-8")?
                .parse::<u64>()
                .map_err(|_| format!("{flag} requires a non-negative whole number"))?;
            options.herdr_wait_ms = Some(milliseconds);
        } else {
            let target = match flag.as_str() {
                "--database" => &mut options.database,
                "--socket" => &mut options.socket,
                _ => &mut options.herdr_socket,
            };
            if target.is_some() {
                return Err(format!("{flag} specified more than once"));
            }
            *target = Some(PathBuf::from(value));
        }
        index += 1;
    }
    Ok(Invocation::Run(options))
}

fn create_private_parent(path: &Path) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(parent)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<OsString> {
        values.iter().map(OsString::from).collect()
    }

    #[test]
    fn no_arguments_selects_conventional_defaults() {
        assert_eq!(
            parse_args([]).expect("parse"),
            Invocation::Run(Options::default())
        );
    }

    #[test]
    fn explicit_paths_override_each_default() {
        assert_eq!(
            parse_args(args(&[
                "--database",
                "/state/db",
                "--socket",
                "/run/kelpie.sock",
                "--herdr-socket",
                "/run/herdr.sock",
                "--herdr-wait-ms",
                "30000",
            ]))
            .expect("parse"),
            Invocation::Run(Options {
                database: Some(PathBuf::from("/state/db")),
                socket: Some(PathBuf::from("/run/kelpie.sock")),
                herdr_socket: Some(PathBuf::from("/run/herdr.sock")),
                herdr_wait_ms: Some(30_000),
            })
        );
    }

    #[test]
    fn unknown_duplicate_and_missing_values_fail_closed() {
        assert!(parse_args(args(&["db", "socket", "herdr"])).is_err());
        assert!(parse_args(args(&["--socket"])).is_err());
        assert!(parse_args(args(&["--socket", "a", "--socket", "b"])).is_err());
        assert!(parse_args(args(&["--herdr-wait-ms"])).is_err());
        assert!(parse_args(args(&["--herdr-wait-ms", "soon"])).is_err());
        assert!(parse_args(args(&["--herdr-wait-ms", "-1"])).is_err());
        assert!(parse_args(args(&["--herdr-wait-ms", "1", "--herdr-wait-ms", "2"])).is_err());
    }

    #[test]
    fn a_zero_herdr_wait_is_explicit_and_not_the_default() {
        assert_eq!(
            parse_args(args(&["--herdr-wait-ms", "0"])).expect("parse"),
            Invocation::Run(Options {
                herdr_wait_ms: Some(0),
                ..Options::default()
            })
        );
        assert_eq!(
            parse_args([]).expect("parse"),
            Invocation::Run(Options::default())
        );
    }
}
