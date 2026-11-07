//! The parse error of a `#[derive(StepParam)]` enum.

use std::{error::Error, fmt};

/// A step placeholder held a word that no variant of its enum accepts.
///
/// `#[derive(StepParam)]` returns it from `FromStr`. Its message names the
/// enum, quotes the word, and lists every word the enum accepts, so a typo
/// in a feature file reads as a fix:
///
/// ```text
/// unknown CleanupPolicy `compcat`: expected one of `delete`, `compact`, `compact,delete`, `none`
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepParamError {
    type_name: &'static str,
    value: String,
    accepted: &'static [&'static str],
}

impl StepParamError {
    /// The error for `value`, which no word in `accepted` matched.
    #[must_use]
    pub fn new(type_name: &'static str, value: &str, accepted: &'static [&'static str]) -> Self {
        Self {
            type_name,
            value: value.to_owned(),
            accepted,
        }
    }

    /// The enum the placeholder parses into.
    #[must_use]
    pub const fn type_name(&self) -> &'static str { self.type_name }

    /// The word the step held.
    #[must_use]
    pub fn value(&self) -> &str { &self.value }

    /// Every word the enum accepts, in declaration order.
    #[must_use]
    pub const fn accepted(&self) -> &'static [&'static str] { self.accepted }
}

impl fmt::Display for StepParamError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unknown {} `{}`: expected one of ", self.type_name, self.value)?;
        for (index, word) in self.accepted.iter().enumerate() {
            if index > 0 {
                f.write_str(", ")?;
            }
            write!(f, "`{word}`")?;
        }
        Ok(())
    }
}

impl Error for StepParamError {}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::StepParamError;

    #[test]
    fn message_lists_every_accepted_word() {
        let error = StepParamError::new("CleanupPolicy", "compcat", &["delete", "compact", "none"]);
        assert!(
            error.to_string()
                == "unknown CleanupPolicy `compcat`: expected one of `delete`, `compact`, `none`"
        );
    }
}
