//! Conventional user-state and socket paths shared by both binaries.

use std::env;
use std::ffi::OsString;
use std::path::PathBuf;

use thiserror::Error;

/// Failure to resolve a conventional per-user path.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum PathError {
    #[error("HOME is required when {0} is not set")]
    MissingHome(&'static str),
}

/// Default persistent Kelpie database path.
///
/// # Errors
///
/// Returns an error when neither `XDG_STATE_HOME` nor `HOME` is available.
pub fn database_path() -> Result<PathBuf, PathError> {
    database_path_with(|name| env::var_os(name))
}

/// Default live Kelpie client/daemon socket path.
#[must_use]
pub fn kelpie_socket_path() -> PathBuf {
    runtime_root_with(|name| env::var_os(name)).join("kelpie.sock")
}

/// Active Herdr API socket using Herdr's documented override and default path.
///
/// # Errors
///
/// Returns an error when no explicit socket, XDG config root, or home exists.
pub fn herdr_socket_path() -> Result<PathBuf, PathError> {
    herdr_socket_path_with(|name| env::var_os(name))
}

fn database_path_with(mut get: impl FnMut(&str) -> Option<OsString>) -> Result<PathBuf, PathError> {
    let root = match nonempty(get("XDG_STATE_HOME")) {
        Some(root) => PathBuf::from(root),
        None => {
            PathBuf::from(nonempty(get("HOME")).ok_or(PathError::MissingHome("XDG_STATE_HOME"))?)
                .join(".local/state")
        }
    };
    Ok(root.join("kelpie/kelpie.sqlite3"))
}

fn runtime_root_with(mut get: impl FnMut(&str) -> Option<OsString>) -> PathBuf {
    nonempty(get("XDG_RUNTIME_DIR")).map_or_else(
        || env::temp_dir().join("kelpie-client/kelpie"),
        |root| PathBuf::from(root).join("kelpie"),
    )
}

fn herdr_socket_path_with(
    mut get: impl FnMut(&str) -> Option<OsString>,
) -> Result<PathBuf, PathError> {
    if let Some(path) = nonempty(get("HERDR_SOCKET_PATH")) {
        return Ok(PathBuf::from(path));
    }
    let root = match nonempty(get("XDG_CONFIG_HOME")) {
        Some(root) => PathBuf::from(root),
        None => {
            PathBuf::from(nonempty(get("HOME")).ok_or(PathError::MissingHome("XDG_CONFIG_HOME"))?)
                .join(".config")
        }
    };
    Ok(root.join("herdr/herdr.sock"))
}

fn nonempty(value: Option<OsString>) -> Option<OsString> {
    value.filter(|value| !value.is_empty())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn getter(values: &[(&str, &str)]) -> impl FnMut(&str) -> Option<OsString> {
        let values: HashMap<String, OsString> = values
            .iter()
            .map(|(key, value)| ((*key).to_string(), OsString::from(value)))
            .collect();
        move |name| values.get(name).cloned()
    }

    #[test]
    fn xdg_paths_are_preferred() {
        assert_eq!(
            database_path_with(getter(&[("XDG_STATE_HOME", "/state"), ("HOME", "/home")]))
                .expect("database"),
            PathBuf::from("/state/kelpie/kelpie.sqlite3")
        );
        assert_eq!(
            runtime_root_with(getter(&[("XDG_RUNTIME_DIR", "/run/user/1")])),
            PathBuf::from("/run/user/1/kelpie")
        );
        assert_eq!(
            herdr_socket_path_with(getter(&[("XDG_CONFIG_HOME", "/config")])).expect("Herdr"),
            PathBuf::from("/config/herdr/herdr.sock")
        );
    }

    #[test]
    fn home_fallbacks_follow_xdg_conventions() {
        assert_eq!(
            database_path_with(getter(&[("HOME", "/home/alice")])).expect("database"),
            PathBuf::from("/home/alice/.local/state/kelpie/kelpie.sqlite3")
        );
        assert_eq!(
            herdr_socket_path_with(getter(&[("HOME", "/home/alice")])).expect("Herdr"),
            PathBuf::from("/home/alice/.config/herdr/herdr.sock")
        );
    }

    #[test]
    fn explicit_herdr_socket_wins() {
        assert_eq!(
            herdr_socket_path_with(getter(&[
                ("HERDR_SOCKET_PATH", "/tmp/custom.sock"),
                ("XDG_CONFIG_HOME", "/config"),
            ]))
            .expect("Herdr"),
            PathBuf::from("/tmp/custom.sock")
        );
    }
}
