use std::process::Command;

const CANONICAL: &str = include_str!("../skills/kelpie/SKILL.md");

#[test]
fn printed_skill_matches_canonical_file() {
    let output = Command::new(env!("CARGO_BIN_EXE_kelpie"))
        .arg("--skill")
        .output()
        .expect("run kelpie --skill");
    assert!(output.status.success());
    assert_eq!(output.stdout, CANONICAL.as_bytes());
}

#[test]
fn package_metadata_includes_canonical_skill() {
    let manifest = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/Cargo.toml"))
        .expect("read Cargo.toml");
    assert!(manifest.contains("\"skills/kelpie/SKILL.md\""));
    assert!(manifest.contains("\"README.md\""));
}

/// Split one documented command into argv, dropping shell-only syntax.
///
/// Placeholders like `<ask-id>` are ordinary tokens; only a redirection `<` or a
/// heredoc ends the argument list. `$VAR` stands in for a socket path.
fn documented_argv(command: &str) -> Vec<String> {
    let mut argv = Vec::new();
    for token in command.split_whitespace().skip(1) {
        if token == "<" || token.starts_with("<<") {
            break;
        }
        let token = token.trim_matches(['"', '\'']);
        if let Some(name) = token.strip_prefix('$') {
            assert!(!name.is_empty(), "empty shell variable in {command}");
            argv.push("/run/kelpie/kelpie.sock".to_string());
        } else {
            argv.push(token.to_string());
        }
    }
    argv
}

/// Every `kelpie …` line inside a fenced block, with continuations joined.
fn documented_commands(skill: &str) -> Vec<String> {
    let mut commands = Vec::new();
    let mut inside_block = false;
    let mut continued: Option<String> = None;
    for line in skill.lines() {
        if line.trim_start().starts_with("```") {
            inside_block = !inside_block;
            continued = None;
            continue;
        }
        if !inside_block {
            continue;
        }
        let trimmed = line.trim();
        let command = match continued.take() {
            Some(head) => format!("{head} {trimmed}"),
            None if trimmed == "kelpie" || trimmed.starts_with("kelpie ") => trimmed.to_string(),
            None => continue,
        };
        match command.strip_suffix('\\') {
            Some(head) => continued = Some(head.trim_end().to_string()),
            None => commands.push(command),
        }
    }
    commands
}

/// The skill teaches the CLI, so every command it shows must actually parse.
///
/// `printed_skill_matches_canonical_file` only proves the binary ships the same
/// bytes as the file; it cannot catch a documented flag the parser rejects.
#[test]
fn every_documented_command_parses() {
    let commands = documented_commands(CANONICAL);
    assert!(
        commands.len() > 10,
        "expected the skill to document commands, found {}",
        commands.len()
    );
    for command in commands {
        let argv = documented_argv(&command);
        kelpie::cli::parse_invocation(&argv).unwrap_or_else(|error| {
            panic!(
                "skill documents a command the CLI rejects\n  command: {command}\n  error: {error}"
            )
        });
    }
}

#[test]
fn daemon_prints_package_version() {
    let output = Command::new(env!("CARGO_BIN_EXE_kelpied"))
        .arg("--version")
        .output()
        .expect("run kelpied --version");
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8(output.stdout).expect("UTF-8 version output"),
        format!("kelpied {}\n", env!("CARGO_PKG_VERSION"))
    );
}
