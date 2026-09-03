//! Durable coordination primitives for Herdr-managed agents.
//!
//! Kelpie owns logical identities, operation intent, messages, and obligations.
//! Live runtime facts remain exclusively owned by Herdr.

pub mod attribution;
pub mod cli;
pub mod daemon;
pub mod domain;
pub mod envelope;
pub mod herdr;
pub mod herdr_exec;
pub mod name;
pub mod paths;
pub mod slice;
pub mod store;
/// Canonical instructions shipped with every Kelpie release.
pub const SKILL: &str = include_str!("../skills/kelpie/SKILL.md");
#[doc(hidden)]
pub mod test_fault;
