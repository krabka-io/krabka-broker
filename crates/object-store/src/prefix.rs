//! Helpers for addressing keys beneath a configured object-store prefix.

use object_store::path::Path;

/// Trim whitespace and separators copied from a console URL so `/tier/`,
/// `tier`, and ` tier/ ` address the same root. Empty prefixes mean no prefix.
#[must_use]
pub fn normalize_prefix(prefix: Option<&str>) -> Option<String> {
    let trimmed = prefix?.trim().trim_matches('/');
    (!trimmed.is_empty()).then(|| trimmed.to_owned())
}

/// Build the absolute object key for a path relative to an archive root.
#[must_use]
pub fn prefixed_key(prefix: Option<&str>, relative: &str) -> Path {
    match prefix {
        Some(prefix) => Path::from(format!("{prefix}/{relative}")),
        None => Path::from(relative),
    }
}
