//! Herdr-legal public names shared by live aliases and Ready Kelpie bindings.

use std::collections::HashSet;
use std::path::Path;

/// Herdr agent names are at most 32 characters.
pub const HERDR_NAME_MAX: usize = 32;

/// A public name is not a valid Herdr agent name or cannot be derived.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NameError(pub String);

/// Return whether `name` matches Herdr's agent-name grammar.
#[must_use]
pub fn valid_herdr_name(name: &str) -> bool {
    let mut chars = name.chars();
    matches!(chars.next(), Some('a'..='z'))
        && name.len() <= HERDR_NAME_MAX
        && chars.all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '-' | '_'))
}

/// Sanitize raw text into a Herdr-legal name, or `None` if nothing legal remains.
#[must_use]
pub fn sanitize_herdr_name(raw: &str) -> Option<String> {
    let mut out = String::new();
    for ch in raw.chars() {
        let mapped = if ch.is_ascii_uppercase() {
            Some(ch.to_ascii_lowercase())
        } else if ch.is_ascii_lowercase() || ch.is_ascii_digit() || matches!(ch, '-' | '_') {
            Some(ch)
        } else if ch.is_ascii_whitespace() || matches!(ch, '.' | ':' | '/') {
            Some('-')
        } else {
            None
        };
        if let Some(next) = mapped {
            if next == '-' && out.ends_with('-') {
                continue;
            }
            out.push(next);
        }
    }
    while out.starts_with(|ch: char| !ch.is_ascii_lowercase()) {
        out.remove(0);
    }
    while out.ends_with('-') || out.ends_with('_') {
        out.pop();
    }
    if out.is_empty() {
        return None;
    }
    out.truncate(HERDR_NAME_MAX);
    valid_herdr_name(&out).then_some(out)
}

/// Derive the preferred live name for an unnamed occupant.
///
/// Uses the working-directory basename when it sanitizes to a free Herdr name.
/// On collision, appends one pane-derived suffix. Does not prefix `adopted-`.
///
/// # Errors
///
/// Returns [`NameError`] when no legal name can be derived or the one stable
/// suffixed candidate is also taken.
pub fn aligned_live_name(
    working_directory: &str,
    pane_id: &str,
    taken: impl IntoIterator<Item = impl AsRef<str>>,
) -> Result<String, NameError> {
    let taken: HashSet<String> = taken
        .into_iter()
        .map(|name| name.as_ref().to_string())
        .collect();
    let base = basename_candidate(working_directory)
        .or_else(|| sanitize_herdr_name(&pane_id.replace(':', "")))
        .ok_or_else(|| NameError("cannot derive a Herdr-legal public name".into()))?;
    if !taken.contains(&base) {
        return Ok(base);
    }
    let suffix = sanitize_herdr_name(&pane_id.replace(':', "")).ok_or_else(|| {
        NameError("cannot derive a stable pane suffix for a colliding name".into())
    })?;
    let candidate = fit_suffixed_name(&base, &suffix).ok_or_else(|| {
        NameError("colliding name cannot fit a pane suffix within 32 characters".into())
    })?;
    if taken.contains(&candidate) {
        return Err(NameError(format!(
            "derived name {candidate} is already used"
        )));
    }
    Ok(candidate)
}

/// Derive the canonical lookup alias from a working-directory basename.
///
/// Unlike [`aligned_live_name`], this does not add a pane suffix. It is used
/// only to discover a unique unnamed live agent before normal adoption claims
/// its authoritative Herdr name.
///
/// # Errors
///
/// Returns [`NameError`] when the directory has no usable basename.
pub fn canonical_cwd_alias(working_directory: &str) -> Result<String, NameError> {
    basename_candidate(working_directory).ok_or_else(|| {
        NameError("cannot derive a Herdr-legal name from the working directory".into())
    })
}

fn basename_candidate(working_directory: &str) -> Option<String> {
    let name = Path::new(working_directory).file_name()?.to_str()?;
    sanitize_herdr_name(name)
}

fn fit_suffixed_name(base: &str, suffix: &str) -> Option<String> {
    let needed = suffix.len().checked_add(1)?;
    if needed >= HERDR_NAME_MAX {
        return sanitize_herdr_name(suffix);
    }
    let keep = HERDR_NAME_MAX - needed;
    if keep == 0 {
        return None;
    }
    let mut stem: String = base.chars().take(keep).collect();
    while stem.ends_with('-') || stem.ends_with('_') {
        stem.pop();
    }
    if stem.is_empty() {
        return None;
    }
    let name = format!("{stem}-{suffix}");
    valid_herdr_name(&name).then_some(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitization_and_truncation_match_herdr_grammar() {
        assert_eq!(
            sanitize_herdr_name("Quorum.Repo"),
            Some("quorum-repo".into())
        );
        assert_eq!(sanitize_herdr_name("123-foo"), Some("foo".into()));
        assert!(sanitize_herdr_name("!!!").is_none());
        let long = "a".repeat(40);
        let sanitized = sanitize_herdr_name(&long).expect("truncated");
        assert_eq!(sanitized.len(), HERDR_NAME_MAX);
        assert!(valid_herdr_name(&sanitized));
        assert!(!valid_herdr_name("Reviewer"));
        assert!(!valid_herdr_name(&"a".repeat(33)));
    }

    #[test]
    fn free_basename_has_no_suffix() {
        let name = aligned_live_name("/tmp/quorum", "w7:p1H", None::<&str>).expect("free");
        assert_eq!(name, "quorum");
        assert!(!name.starts_with("adopted-"));
    }

    #[test]
    fn collision_uses_one_pane_suffix() {
        let name = aligned_live_name("/tmp/quorum", "w7:p1H", ["quorum"]).expect("suffixed");
        assert_eq!(name, "quorum-w7p1h");
        assert!(valid_herdr_name(&name));
    }

    #[test]
    fn double_collision_fails_closed() {
        let error = aligned_live_name("/tmp/quorum", "w7:p1H", ["quorum", "quorum-w7p1h"])
            .expect_err("taken");
        assert!(error.0.contains("already used"));
    }

    #[test]
    fn empty_cwd_falls_back_to_pane() {
        let name = aligned_live_name("/", "w7:p1H", None::<&str>).expect("pane");
        assert_eq!(name, "w7p1h");
    }

    #[test]
    fn canonical_alias_uses_only_the_cwd_basename() {
        assert_eq!(
            canonical_cwd_alias("/tmp/Divine.Blossom").expect("alias"),
            "divine-blossom"
        );
        assert!(canonical_cwd_alias("/").is_err());
    }
}
