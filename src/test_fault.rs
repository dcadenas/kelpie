//! Explicit subprocess fault rendezvous used only by deterministic tests.
//!
//! Production behavior is unchanged unless `KELPIE_TEST_FAULT_POINTS` contains
//! an exact compiled point name. An activated point also requires
//! `KELPIE_TEST_FAULT_SOCKET`; it connects, reports the point name, and blocks
//! until the harness writes one byte or kills the process.

use std::env;
use std::io::{Read, Write};
use std::os::unix::net::UnixStream;

const POINTS_ENV: &str = "KELPIE_TEST_FAULT_POINTS";
const SOCKET_ENV: &str = "KELPIE_TEST_FAULT_SOCKET";

/// Pause at one exact allowlisted test point, or return immediately by default.
#[doc(hidden)]
pub fn pause(point: &str) {
    let Some(points) = env::var_os(POINTS_ENV) else {
        return;
    };
    let enabled = points
        .to_string_lossy()
        .split(',')
        .any(|configured| configured == point);
    if !enabled {
        return;
    }
    let socket = env::var_os(SOCKET_ENV)
        .unwrap_or_else(|| panic!("{SOCKET_ENV} is required when {point} is activated"));
    let mut stream = UnixStream::connect(socket)
        .unwrap_or_else(|error| panic!("test fault {point} could not rendezvous: {error}"));
    stream
        .write_all(format!("{point}\n").as_bytes())
        .unwrap_or_else(|error| panic!("test fault {point} could not report: {error}"));
    let mut release = [0_u8; 1];
    stream
        .read_exact(&mut release)
        .unwrap_or_else(|error| panic!("test fault {point} was not released: {error}"));
}
