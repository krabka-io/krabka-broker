//! `java.util.regex` behaviour for the two rule grammars that hand a
//! user-written regular expression to Java: `ssl.principal.mapping.rules`
//! and the Kerberos `auth_to_local` rules.
//!
//! The `regex` crate differs from Java in the two places these grammars lean
//! on. `Matcher.matches()` succeeds when *some* way of matching spans the
//! whole input, including through backtracking and lazy quantifiers, where a
//! leftmost-first search stops at the first match it finds. And
//! `Matcher.replaceAll` reads `$1_x` as group 1 followed by `_x`, where the
//! `regex` crate reads a group named `1_x`. [`JavaPattern`] gives both the
//! Java answer.

use std::{collections::BTreeMap, sync::Arc};

use fancy_regex::{Captures, Regex};

use crate::client_metrics::config::java_to_fancy;

/// A `java.util.regex.Pattern` compiled for `matches()` and
/// `replaceAll`/`replaceFirst`.
#[derive(Debug, Clone)]
pub(super) struct JavaPattern(Arc<Compiled>);

/// The engines behind a [`JavaPattern`], shared so that a rule holding one is
/// small.
#[derive(Debug)]
struct Compiled {
    /// The pattern as written, for `replaceAll` and `replaceFirst`.
    search: Regex,
    /// The pattern pinned to the whole input, for `matches()`.
    whole: Regex,
    /// Group name to index, for `${name}` in a replacement.
    names: BTreeMap<String, usize>,
}

/// A replacement string Java's `Matcher.appendReplacement` refuses, which
/// Java reports as `IllegalArgumentException` or `IndexOutOfBoundsException`.
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub(super) struct ReplacementError(pub(super) String);

impl JavaPattern {
    /// Compiles `pattern`, or says why Java's `Pattern.compile` would not.
    pub(super) fn compile(pattern: &str) -> Result<Self, String> {
        let translated = java_to_fancy(pattern)
            .ok_or_else(|| format!("{pattern:?} is not a java.util.regex pattern"))?;
        let compile = |source: &str| Regex::new(source).map_err(|error| error.to_string());
        let search = compile(&translated)?;
        let whole = compile(&format!("\\A(?:{translated})\\z"))?;
        let names = search
            .capture_names()
            .enumerate()
            .filter_map(|(index, name)| Some((name?.to_owned(), index)))
            .collect();
        Ok(Self(Arc::new(Compiled {
            search,
            whole,
            names,
        })))
    }

    /// The number of capturing groups, `Matcher.groupCount()`.
    pub(super) fn group_count(&self) -> usize {
        self.0.search.captures_len() - 1
    }

    /// `Matcher.matches()`. A search that runs past the engine's backtrack
    /// limit counts as no match.
    pub(super) fn matches(&self, input: &str) -> bool {
        self.0.whole.is_match(input).unwrap_or(false)
    }

    /// `input.replaceAll(pattern, replacement)` when `all`, and
    /// `input.replaceFirst(pattern, replacement)` when not.
    pub(super) fn replace(
        &self,
        input: &str,
        replacement: &str,
        all: bool,
    ) -> Result<String, ReplacementError> {
        let mut out = String::with_capacity(input.len());
        let mut copied = 0;
        for captures in self.0.search.captures_iter(input) {
            let captures = captures.map_err(|error| ReplacementError(error.to_string()))?;
            let Some(whole) = captures.get(0) else { break };
            out.push_str(&input[copied..whole.start()]);
            self.append_replacement(&mut out, replacement, &captures)?;
            copied = whole.end();
            if !all {
                break;
            }
        }
        out.push_str(&input[copied..]);
        Ok(out)
    }

    /// `Matcher.appendReplacement`: `\x` is a literal `x`, `${name}` and `$n`
    /// are groups, and `$n` takes as many further digits as still name a
    /// group. A group that did not take part in the match adds nothing.
    fn append_replacement(
        &self,
        out: &mut String,
        replacement: &str,
        captures: &Captures<'_, str>,
    ) -> Result<(), ReplacementError> {
        let refuse = |message: &str| ReplacementError(message.to_owned());
        let group_count = self.group_count();
        let mut chars = replacement.chars().peekable();
        while let Some(character) = chars.next() {
            match character {
                '\\' => out.push(
                    chars
                        .next()
                        .ok_or_else(|| refuse("character to be escaped is missing"))?,
                ),
                '$' => {
                    let first = chars
                        .next()
                        .ok_or_else(|| refuse("illegal group reference: group index is missing"))?;
                    let group = if first == '{' {
                        let mut name = String::new();
                        while let Some(next) = chars.next_if(char::is_ascii_alphanumeric) {
                            name.push(next);
                        }
                        if name.is_empty() {
                            return Err(refuse("named capturing group has 0 length name"));
                        }
                        if chars.next() != Some('}') {
                            return Err(refuse("named capturing group is missing trailing '}'"));
                        }
                        *self
                            .0
                            .names
                            .get(&name)
                            .ok_or_else(|| ReplacementError(format!("no group with name {name}")))?
                    } else {
                        let mut group = first
                            .to_digit(10)
                            .ok_or_else(|| refuse("illegal group reference"))?
                            as usize;
                        if group > group_count {
                            return Err(ReplacementError(format!("no group {group}")));
                        }
                        while let Some(digit) = chars.peek().and_then(|next| next.to_digit(10)) {
                            let longer = group * 10 + digit as usize;
                            if longer > group_count {
                                break;
                            }
                            group = longer;
                            chars.next();
                        }
                        group
                    };
                    if let Some(text) = captures.get(group) {
                        out.push_str(text.as_str());
                    }
                }
                other => out.push(other),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    fn pattern(source: &str) -> JavaPattern {
        JavaPattern::compile(source).unwrap_or_else(|error| panic!("{source}: {error}"))
    }

    /// `Matcher.matches()` finds a whole-input match by backtracking, which a
    /// leftmost-first search does not.
    #[test]
    fn matches_backtracks_to_span_the_whole_input() {
        // (pattern, input, matches)
        let cases = [
            ("a|ab", "ab", true),
            ("a|ab", "abc", false),
            ("CN=(.*?)", "CN=abc", true),
            ("(a+)+b", "aaab", true),
            ("ali", "alice", false),
            ("ali.*", "alice", true),
        ];
        for (source, input, expected) in cases {
            check!(
                pattern(source).matches(input) == expected,
                "{source} against {input}"
            );
        }
    }

    /// `Matcher.appendReplacement` semantics for `replaceAll`.
    #[test]
    fn replace_all_expands_a_replacement_as_java_does() {
        // (pattern, input, replacement, result)
        let cases = [
            // `$1_$2` is group 1, an underscore and group 2, not a group named `1_`.
            ("(a)-(b)", "a-b", "$1_$2", "a_b"),
            ("(a)", "a", "$1suffix", "asuffix"),
            // Digits after `$n` join the group number only while it names a group.
            ("(a)", "a", "$10", "a0"),
            ("(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)", "abcdefghij", "$10", "j"),
            ("(a)", "a", "$0!", "a!"),
            ("(?<who>a)", "a", "${who}-$1", "a-a"),
            // A backslash makes the next character literal.
            ("(a)", "a", "\\$1", "$1"),
            ("a", "aa", "b", "bb"),
            // A group that took no part in the match adds nothing.
            ("(a)|(b)", "b", "[$1]", "[]"),
            ("x", "abc", "$9", "abc"),
        ];
        for (source, input, replacement, expected) in cases {
            check!(
                pattern(source)
                    .replace(input, replacement, true)
                    .ok()
                    .as_deref()
                    == Some(expected),
                "{source} on {input} with {replacement}"
            );
        }
    }

    /// The replacements Java throws on.
    #[test]
    fn replace_all_refuses_what_java_throws_on() {
        for (source, input, replacement) in [
            ("(a)", "a", "$2"),
            ("a", "a", "$1"),
            ("(a)", "a", "$"),
            ("(a)", "a", "$x"),
            ("(a)", "a", "\\"),
            ("(a)", "a", "${who}"),
            ("(a)", "a", "${}"),
            ("(a)", "a", "${who"),
        ] {
            check!(
                pattern(source).replace(input, replacement, true).is_err(),
                "{source} on {input} with {replacement}"
            );
        }
    }

    #[test]
    fn replace_first_stops_after_one_match() {
        let result = pattern("a").replace("aaa", "b", false);
        assert!(result.ok().as_deref() == Some("baa"));
    }

    #[test]
    fn a_pattern_java_refuses_does_not_compile() {
        for source in ["(?P<x>a)", "[a-", "(a"] {
            check!(JavaPattern::compile(source).is_err(), "{source}");
        }
    }
}
