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
/// Java reports as `IllegalArgumentException` or `IndexOutOfBoundsException`,
/// or a search the engine gave up on at its backtrack limit.
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

    /// `Matcher.matches()`.
    ///
    /// # Errors
    ///
    /// A search that runs past the engine's backtrack limit is an error and
    /// not a no match. Java answers such a pattern, however slowly, so
    /// reading the limit as "does not match" would let a rule that Java
    /// applies fall through to the next rule, or to `DEFAULT`, and map the
    /// principal to a name the operator never meant for it. A caller rejects
    /// the name instead.
    pub(super) fn matches(&self, input: &str) -> Result<bool, ReplacementError> {
        self.0
            .whole
            .is_match(input)
            .map_err(|error| ReplacementError(error.to_string()))
    }

    /// `input.replaceAll(pattern, replacement)` when `all`, and
    /// `input.replaceFirst(pattern, replacement)` when not.
    ///
    /// The matches are `Matcher.find`'s, not the `regex` crate's iterator's.
    /// After a match that is not empty, `find` may match the empty string at
    /// the very position the match ended, and the crate's iterator does not:
    /// `"alice".replaceAll("(.*)", "$1x")` is `alicexx` in Java. After an empty
    /// match `find` starts one character on.
    pub(super) fn replace(
        &self,
        input: &str,
        replacement: &str,
        all: bool,
    ) -> Result<String, ReplacementError> {
        let mut out = String::with_capacity(input.len());
        let mut copied = 0;
        let mut search_from = 0;
        while search_from <= input.len() {
            let captures = self
                .0
                .search
                .captures_from_pos(input, search_from)
                .map_err(|error| ReplacementError(error.to_string()))?;
            let Some(captures) = captures else { break };
            let Some(whole) = captures.get(0) else { break };
            out.push_str(&input[copied..whole.start()]);
            self.append_replacement(&mut out, replacement, &captures)?;
            copied = whole.end();
            if !all {
                break;
            }
            search_from = if whole.start() == whole.end() {
                input[whole.end()..]
                    .chars()
                    .next()
                    .map_or(input.len() + 1, |next| whole.end() + next.len_utf8())
            } else {
                whole.end()
            };
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
                pattern(source).matches(input).ok() == Some(expected),
                "{source} against {input}"
            );
        }
    }

    /// A search that runs past the engine's backtrack limit is an error. Java
    /// answers the same pattern, slowly, so "no match" would send a rule that
    /// Java applies on to the next rule, or to `DEFAULT`.
    #[test]
    fn matches_reports_a_search_the_engine_gave_up_on() {
        let hostile = format!("{}b", "a".repeat(40));

        let result = pattern("^(a|aa)+\\1$").matches(&hostile);

        assert!(result.is_err());
    }

    /// `Matcher.find` may match the empty string at the position a non-empty
    /// match ended, which the `regex` crate's iterator does not, and starts
    /// one character on after an empty match. Each row is the result of
    /// `input.replaceAll(pattern, replacement)` on JDK 21.
    #[test]
    fn replace_all_finds_an_empty_match_after_a_non_empty_one() {
        // (pattern, input, replacement, result)
        let cases = [
            ("(.*)", "alice", "$1x", "alicexx"),
            ("a*", "baaac", "-", "-b--c-"),
            ("x*", "abc", "-", "-a-b-c-"),
            ("\\w*", "ab cd", "[$0]", "[ab][] [cd][]"),
            ("(?:)", "é!", "|", "|é|!|"),
            ("", "", "|", "|"),
            ("a", "", "|", ""),
            ("b*", "aé", "-", "-a-é-"),
        ];
        for (source, input, replacement, expected) in cases {
            check!(
                pattern(source)
                    .replace(input, replacement, true)
                    .ok()
                    .as_deref()
                    == Some(expected),
                "{source} on {input}"
            );
        }
    }

    /// `replaceFirst` stops after the first match, an empty one included.
    #[test]
    fn replace_first_stops_after_the_first_match_even_when_it_is_empty() {
        // (pattern, input, replacement, result)
        for (source, input, replacement, expected) in [
            ("(.*)", "alice", "$1x", "alicex"),
            ("b*", "abc", "-", "-abc"),
        ] {
            check!(
                pattern(source)
                    .replace(input, replacement, false)
                    .ok()
                    .as_deref()
                    == Some(expected),
                "{source} on {input}"
            );
        }
    }

    /// Java reads `\w`, `\d`, `\s` and `\b` as ASCII unless `(?U)` asks for
    /// Unicode, and `.` stops at every line terminator unless `(?s)` or `(?d)`
    /// says otherwise. Each row is `Pattern.matches` on JDK 21, except `\b`,
    /// which reads ASCII from JDK 19.
    #[test]
    fn matches_reads_classes_as_ascii_and_dot_as_java_does() {
        // (pattern, input, whole input matches)
        let cases = [
            ("\\w+", "abc_09", true),
            ("\\w+", "é", false),
            ("\\W", "é", true),
            ("\\d", "٣", false),
            ("\\D", "٣", true),
            ("\\s", " \t\u{0B}", false),
            ("\\s+", " \t\u{0B}", true),
            ("\\s", "\u{a0}", false),
            ("\\s", "\u{2003}", false),
            ("\\S", "\u{a0}", true),
            ("[\\w.]+", "a.b", true),
            ("[\\w.]+", "é", false),
            ("[^\\w]", "é", true),
            ("[\\W]+", "é!", true),
            ("[\\d-]+", "1-2", true),
            ("[\\s]", "\u{a0}", false),
            ("(?U)\\w+", "é", true),
            ("(?U)\\d", "٣", true),
            ("(?U)\\s", "\u{a0}", true),
            ("(?U:\\w)\\w", "éa", true),
            ("(?U:\\w)\\w", "éé", false),
            ("(?:(?U)\\w)\\w", "éé", false),
            ("(?U)(?-U)\\w", "é", false),
            ("\\bfoo", "foo", true),
            ("a\\b", "a", true),
            ("\\Bfoo", "foo", false),
            ("a\\bé", "aé", true),
            ("\\bé", "é", false),
            ("a\\Bé", "aé", false),
            ("a.b", "axb", true),
            ("a.b", "a\nb", false),
            ("a.b", "a\rb", false),
            ("a.b", "a\u{85}b", false),
            ("a.b", "a\u{2028}b", false),
            ("a.b", "a\u{2029}b", false),
            (".*", "abc\rdef", false),
            ("(?s)a.b", "a\rb", true),
            ("(?s)a.b", "a\nb", true),
            ("(?d)a.b", "a\rb", true),
            ("(?d)a.b", "a\u{85}b", true),
            ("(?d)a.b", "a\nb", false),
            ("(?s:a.)b.", "a\nbx", true),
            ("(?s:a.)b.", "a\nb\r", false),
            ("(?s)(?-s).", "\n", false),
            ("a[.]b", "a.b", true),
            ("a\\.b", "axb", false),
        ];
        for (source, input, expected) in cases {
            check!(
                pattern(source).matches(input).ok() == Some(expected),
                "{source:?} against {input:?}"
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
