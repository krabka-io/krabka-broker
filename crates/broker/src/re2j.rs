//! The RE2J dialect that Kafka compiles client-supplied patterns with.
//!
//! Kafka uses `com.google.re2j` for the KIP-848 `SubscribedTopicRegex` and for
//! the KIP-1038 `TransactionIdPattern` of `ListTransactions`. RE2 has no
//! backtracking, so a pattern that a client sends cannot cost more than linear
//! time in the input. Rust's `regex` gives the same guarantee and the same
//! refusals for lookaround, backreferences and possessive quantifiers.
//!
//! The two grammars are not identical. What is knowingly left:
//!
//! - **Accepted here, refused by RE2J.** `regex`'s character-class set
//!   operations (`[\w&&\d]`, `[a-z--b]`, nested `[[a-z]x]`), which RE2J reads
//!   as ordinary class members or as a syntax error. Screening these would
//!   need a class parser, and getting one wrong refuses patterns Kafka takes.
//! - **Refused here, accepted by RE2J.** `\Q...\E` literal quoting, which
//!   `regex` has no equivalent for, and a repetition large enough to exceed
//!   `regex`'s compiled-size limit.
//!
//! The inline flags are screened, because that is the divergence a client is
//! likely to hit: `regex` takes `x`, `u` and `R`, and RE2J refuses them.

/// RE2J's `ERR_INVALID_PERL_OP`, the description Kafka formats into the
/// `INVALID_REGULAR_EXPRESSION` message when a pattern names a flag RE2J does
/// not have.
pub(crate) const INVALID_PERL_OP: &str = "invalid or unsupported Perl syntax";

/// The inline flags RE2J's `Parser.parsePerlFlags` accepts, plus the `-` that
/// negates the ones that follow it.
const INLINE_FLAGS: [char; 5] = ['i', 'm', 's', 'U', '-'];

/// The first inline flag in `pattern` that RE2J's `parsePerlFlags` does not
/// accept, if any.
///
/// The scan tracks escapes and character classes so a `(` that is a literal,
/// and everything inside `[...]`, is left alone, and it hands every other
/// `(?` form -- named captures, non-capturing groups, the lookarounds both
/// engines refuse -- to the regex parser rather than judging it here.
pub(crate) fn unsupported_inline_flag(pattern: &str) -> Option<char> {
    let chars: Vec<char> = pattern.chars().collect();
    let mut index = 0;
    let mut in_class = false;
    while index < chars.len() {
        let current = chars[index];
        if current == '\\' {
            index += 2;
            continue;
        }
        if in_class {
            in_class = current != ']';
            index += 1;
            continue;
        }
        if current == '[' {
            in_class = true;
            index += 1;
            continue;
        }
        index += 1;
        if current != '(' || chars.get(index) != Some(&'?') {
            continue;
        }
        // `(?P<`, `(?<`, `(?'`, `(?:`, `(?=` and `(?!` are not flag groups.
        if matches!(
            chars.get(index + 1),
            Some('P' | ':' | '<' | '\'' | '=' | '!')
        ) {
            continue;
        }
        let mut flag = index + 1;
        while let Some(&candidate) = chars.get(flag) {
            if candidate == ')' || candidate == ':' {
                break;
            }
            if !INLINE_FLAGS.contains(&candidate) {
                return Some(candidate);
            }
            flag += 1;
        }
    }
    None
}

/// RE2J's `ERR_INVALID_REPEAT_OP`, its message for a possessive quantifier.
pub(crate) const INVALID_REPEAT_OP: &str = "invalid nested repetition operator";

/// Whether `pattern` carries a possessive quantifier (`a*+`, `a++`, `a?+`,
/// `a{2,3}+`).
///
/// RE2 has no possessive quantifiers and refuses them. Rust's `regex` reads
/// `a*+` as a repetition of a repetition and compiles it, so the scan is what
/// keeps the two engines on the same answer. It tracks escapes and character
/// classes, so a literal `+` is left alone.
pub(crate) fn possessive_quantifier(pattern: &str) -> bool {
    let chars: Vec<char> = pattern.chars().collect();
    let mut index = 0;
    let mut in_class = false;
    let mut after_repeat = false;
    while index < chars.len() {
        let current = chars[index];
        if current == '\\' {
            index += 2;
            after_repeat = false;
            continue;
        }
        if in_class {
            in_class = current != ']';
            index += 1;
            after_repeat = false;
            continue;
        }
        match current {
            '[' => {
                in_class = true;
                after_repeat = false;
            }
            '*' | '?' | '}' => after_repeat = true,
            '+' => {
                if after_repeat {
                    return true;
                }
                after_repeat = true;
            }
            _ => after_repeat = false,
        }
        index += 1;
    }
    false
}

/// `regex`'s `Display` is a multi-line diagram. Flatten it so a response's
/// `error_message` stays one line.
pub(crate) fn flatten_error(error: &regex::Error) -> String {
    error
        .to_string()
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

/// Whether `pattern` is one RE2J compiles.
///
/// # Errors
///
/// Returns the reason, in the wording Kafka puts in its
/// `INVALID_REGULAR_EXPRESSION` message.
pub(crate) fn check(pattern: &str) -> Result<(), String> {
    compile_full_match(pattern).map(|_| ())
}

/// Compiles `pattern` for Kafka's `Matcher.matches()`, which matches the whole
/// input.
///
/// The anchors are `\A` and `\z`, so an inline `(?m)` cannot turn them into
/// line anchors.
///
/// # Errors
///
/// Returns the reason, in the wording Kafka puts in its
/// `INVALID_REGULAR_EXPRESSION` message.
pub(crate) fn compile_full_match(pattern: &str) -> Result<regex::Regex, String> {
    if unsupported_inline_flag(pattern).is_some() {
        return Err(INVALID_PERL_OP.to_owned());
    }
    if possessive_quantifier(pattern) {
        return Err(INVALID_REPEAT_OP.to_owned());
    }
    // Compile the pattern on its own first, so the error text names what the
    // client sent rather than the anchors around it.
    regex::Regex::new(pattern).map_err(|error| flatten_error(&error))?;
    regex::Regex::new(&format!(r"\A(?:{pattern})\z")).map_err(|error| flatten_error(&error))
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    /// The patterns Kafka's RE2J takes, and the ones it refuses.
    #[test]
    fn the_dialect_follows_re2j() {
        // (pattern, compiles)
        let cases = [
            ("txn-.*", true),
            ("(a|b)+c", true),
            ("(?i)TXN", true),
            ("(?s).*", true),
            ("[a-z0-9_.-]+", true),
            // Lookaround, backreferences and possessive quantifiers are Perl
            // syntax that RE2 does not have.
            ("(?=txn-).*", false),
            ("(?!txn-).*", false),
            ("(txn)-\\1", false),
            ("a*+", false),
            ("a++", false),
            ("a?+", false),
            ("a{2,3}+", false),
            // A lazy quantifier is in both engines, and a literal `+` in a
            // class is not a quantifier.
            ("a+?", true),
            ("[+*]+", true),
            ("a\\++", true),
            // An inline flag RE2J does not know.
            ("(?x) txn", false),
            ("(?u)txn", false),
            // Malformed.
            ("[", false),
            ("(", false),
        ];
        let expected: Vec<(&str, bool)> = cases.to_vec();
        let actual: Vec<(&str, bool)> = cases
            .iter()
            .map(|(pattern, _)| (*pattern, compile_full_match(pattern).is_ok()))
            .collect();
        assert!(actual == expected);
    }

    /// `Matcher.matches()` matches the whole input, and an inline `(?m)` does
    /// not change that.
    #[test]
    fn a_compiled_pattern_matches_the_whole_input() {
        let pattern = compile_full_match("txn-.*").expect("compiles");
        check!(pattern.is_match("txn-1"));
        check!(!pattern.is_match("my-txn-1"));
        check!(!pattern.is_match("txn-1\nother"));

        let multiline = compile_full_match("(?m)^txn$").expect("compiles");
        check!(multiline.is_match("txn"));
        check!(!multiline.is_match("other\ntxn"));
    }

    #[test]
    fn an_unsupported_flag_carries_kafkas_message() {
        check!(compile_full_match("(?x)txn").err() == Some(INVALID_PERL_OP.to_owned()));
        check!(unsupported_inline_flag("[(?x)]").is_none());
        check!(unsupported_inline_flag("\\(?x)").is_none());
    }
}
