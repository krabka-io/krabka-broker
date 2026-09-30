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
//!
//! The pattern itself is rewritten by `java_to_fancy` first, which also gives
//! Java's reading of `\w`, `.`, and `(?i)`: Java folds ASCII case only, unless
//! `(?u)` asks for Unicode case as well, so `(?i)service-` does not match
//! `ſervice-` (long s).

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

    /// `(?i)` folds ASCII case only, and `(?u)`, or `(?U)`, adds Unicode case.
    /// `fancy_regex` folds Unicode whenever its `i` is on, so the translation
    /// writes both cases itself. Each row is `Pattern.matches` on JDK 17, from
    /// the `java.util.regex` documentation of `CASE_INSENSITIVE` and
    /// `UNICODE_CASE`: long s (U+017F) and the Kelvin sign (U+212A) are the
    /// letters that fold to `s` and `k` in Unicode and not in ASCII.
    #[test]
    fn matches_folds_case_as_java_does() {
        // (pattern, input, whole input matches)
        let cases = [
            ("(?i)service-", "SERVICE-", true),
            ("(?i)service-", "service-", true),
            ("(?i)service-", "\u{17f}ervice-", false),
            ("(?iu)service-", "\u{17f}ervice-", true),
            ("(?iu)service-", "SERVICE-", true),
            ("(?i)k", "\u{212a}", false),
            ("(?iu)k", "\u{212a}", true),
            ("(?ui)k", "\u{212a}", true),
            ("(?u)k", "K", false),
            ("(?i)é", "É", false),
            ("(?i)é", "é", true),
            ("(?iu)é", "É", true),
            ("(?U)(?i)k", "\u{212a}", true),
            ("(?iU)k", "\u{212a}", true),
            ("(?iU)(?-U)k", "\u{212a}", false),
            ("(?iU)(?-U)k", "K", true),
            ("(?iU)(?-u)k", "\u{212a}", false),
            ("(?iU)(?-u)\\w", "\u{e9}", true),
            ("(?i)a(?u)k", "A\u{212a}", true),
            ("(?i)a(?u)k", "AK", true),
            ("(?iu)a(?-u)k", "A\u{212a}", false),
            ("(?iu)a(?-u)k", "AK", true),
            ("(?i)ss", "\u{df}", false),
            ("(?iu)ss", "\u{df}", false),
            ("(?i)i", "\u{130}", false),
            ("(?i)i", "\u{131}", false),
            ("(?i)\\u00E5", "\u{212b}", false),
            ("(?iu)\\u00E5", "\u{212b}", true),
            ("(?i)[a-f]+", "AbCdEf", true),
            ("(?i)[a-f]", "\u{212a}", false),
            ("(?i)[k]", "\u{212a}", false),
            ("(?iu)[k]", "\u{212a}", true),
            ("(?i)[^a]", "A", false),
            ("(?i)[^a]", "b", true),
            ("(?i)[^a]", "\u{e9}", true),
            ("(?i)[^k]", "\u{212a}", true),
            ("(?iu)[^k]", "\u{212a}", false),
            ("(?i)[X-c]", "x", true),
            ("(?i)[X-c]", "A", true),
            ("(?i)[X-c]", "[", true),
            ("(?i)[X-c]", "d", false),
            ("(?i)[+-z]", "Q", true),
            ("(?i)[a-c&&[^b]]", "B", false),
            ("(?i)[a-c&&[^b]]", "C", true),
            ("(?i)[a-c&&[^b]]", "A", true),
            ("(?i)[a-c[^x-z]]", "Y", false),
            ("(?i)[a-c[^x-z]]", "B", true),
            ("(?i)[\\x41-\\x43]", "b", true),
            ("(?i)[\\u0041-\\u0043]", "b", true),
            ("(?i)[\\Qab\\E]", "B", true),
            ("(?i)[a-]", "A", true),
            ("(?i)[-a]", "A", true),
            ("(?i)[]a]", "]", true),
            ("(?i)[]a]", "A", true),
            ("(?i)[a-c-e]", "E", true),
            ("(?i)[a-c-e]", "-", true),
            ("(?i)[\\w&&[^a]]", "A", false),
            ("(?i)[\\w&&[^a]]", "B", true),
            ("(?i)[a\\-c]", "C", true),
            ("(?i)[a\\-c]", "-", true),
            ("(?i)[\\0101-\\0103]", "b", true),
            ("(?i)[\\0101-\\0103]", "d", false),
            ("(?i)[\\x41-c]", "b", true),
            ("(?i)[a-c[d-f]]", "E", true),
            ("(?i)[\u{e9}-\u{eb}]", "\u{ca}", false),
            ("(?ix) [ a - c ]", "B", true),
            ("(?ix) [ a - c ] # x", "d", false),
            ("(?i)a\\.b", "A.B", true),
            ("(?i)\\[a\\]", "[A]", true),
            ("(?i)a{1,2}b", "aAB", true),
            ("(?i)\u{e9}+", "\u{e9}\u{e9}", true),
            ("(?i)\\0101\\0102", "aB", true),
            ("(?i)\\x41", "a", true),
            ("(?i)\\u0041", "a", true),
            ("(?i)\\x{41}", "a", true),
            ("(?i)\\0101", "a", true),
            ("(?i)\\c!", "A", true),
            ("(?i)\\Qab\\E", "AB", true),
            ("(?i)\\Q.b\\E", ".B", true),
            ("(?i)\\Q.b\\E", "xB", false),
            ("(?i)\\Qk\\E", "\u{212a}", false),
            ("(?i)\\p{Lower}", "A", true),
            ("(?i)\\p{Lower}", "\u{e9}", false),
            ("(?i)\\p{Upper}", "a", true),
            ("(?i)\\p{Upper}", "\u{c9}", false),
            ("\\p{Lower}", "a", true),
            ("\\p{Lower}", "A", false),
            ("\\p{Lower}", "\u{e9}", false),
            ("\\p{Upper}", "A", true),
            ("\\p{Upper}", "\u{c9}", false),
            ("(?i)\\p{Lu}", "a", true),
            ("(?i)\\p{Lu}", "\u{e9}", true),
            ("(?i)\\p{Lu}", "1", false),
            ("(?i)\\P{Lu}", "a", false),
            ("(?i)\\P{Lu}", "1", true),
            ("\\p{Lu}", "a", false),
            ("\\p{Lu}", "A", true),
            ("(?i)[\\p{Lower}]", "A", true),
            ("(?i)[\\P{Lower}]", "A", false),
            ("(?i)[\\P{Lower}]", "1", true),
            ("(?i)\\p{IsLowercase}", "A", true),
            ("(?i)\\p{gc=Lu}", "a", true),
            ("(?i)\\p{IsLu}", "a", true),
            ("(?i:a)b", "Ab", true),
            ("(?i:a)b", "AB", false),
            ("a(?i)b", "aB", true),
            ("a(?i)b", "Ab", false),
            ("a(?i:b)c", "aBc", true),
            ("a(?i:b)c", "aBC", false),
            ("(?i)a(?-i)b", "Ab", true),
            ("(?i)a(?-i)b", "AB", false),
            ("((?i)a)b", "Ab", true),
            ("((?i)a)b", "AB", false),
            ("(?i)a|b", "B", true),
            ("(?i)(?i)(?-i)a", "A", false),
            ("(?i)(?i)(?-i)a", "a", true),
            ("(?i)a{2}", "aA", true),
            ("(?i)(?:a|b)+", "AbBa", true),
            ("(?i)^ab$", "AB", true),
            ("(?i)\\bab", "AB", true),
            ("(?i)\\w", "A", true),
            ("(?i)a.c", "AxC", true),
            ("(?i)(?<Name>a)", "A", true),
            ("(?i)(?=a)A", "A", true),
            ("(?i)(?!a)b", "B", true),
            ("(?ix) a b # comment", "AB", true),
            ("(?x)a b # Comment here", "ab", true),
            ("(?x)a b # Comment here", "AB", false),
            ("(?iu)(a)\\1", "aA", true),
            ("(?iu)(a)\\1", "ab", false),
            ("(?i)(a)(?-i)\\1", "aA", false),
            ("(?i)(a)(?-i)\\1", "aa", true),
            ("(?i)(a)(?-i:\\1)", "aA", false),
            ("(a)\\1", "aa", true),
            ("(a)\\1", "aA", false),
        ];
        for (source, input, expected) in cases {
            check!(
                pattern(source).matches(input).ok() == Some(expected),
                "{source:?} against {input:?}"
            );
        }
    }

    /// A `\Q...\E` is read out before the pattern is, as Java's `RemoveQEQuoting`
    /// does, so a quoted member takes part in a range. A `]` first in a class is a
    /// member that may start a range, a backslash before `<` or `>` is the
    /// character, `\w`, `\p{Lower}` and `\p{Upper}` are the ASCII letters under
    /// `(?iu)`, and a backreference compares ASCII case-insensitively under
    /// `(?i)`. Each row is `Pattern.matches` on JDK 17.
    #[test]
    fn matches_reads_quotes_classes_and_references_as_java_does() {
        // (pattern, input, whole input matches)
        let cases = [
            // \Q...\E is expanded before the pattern is read, so a quoted member takes part in a range
            ("(?i)[\\Qa\\E-c]", "B", true),
            ("(?i)[\\Qa\\E-c]", "b", true),
            ("(?i)[\\Qa\\E-c]", "A", true),
            ("(?i)[\\Qa\\E-c]", "X", false),
            ("(?i)[a-\\Qc\\E]", "B", true),
            ("(?i)[a-\\Qc\\E]", "X", false),
            ("(?i)[\\Qa\\E-\\Qc\\E]", "B", true),
            ("(?i)[\\Qa\\E-\\Qc\\E]", "X", false),
            ("(?i)[\\Qa\\Eb-d]", "C", true),
            ("(?i)[\\Qa\\Eb-d]", "A", true),
            ("(?i)[\\Qa\\Eb-d]", "X", false),
            ("(?i)[\\Q\\Ea-c]", "B", true),
            ("(?i)[a-\\Q\\Ec]", "B", true),
            ("(?i)[a-\\Q\\Ec]", "X", false),
            ("[\\Qa\\E-c]", "b", true),
            ("[\\Qa\\E-c]", "B", false),
            ("[\\Qa\\E-c]", "X", false),
            ("(?i)[\\Q]\\E-c]", "A", true),
            ("(?i)[\\Q]\\E-c]", "X", false),
            ("(?i)[\\Q1\\E-3]", "2", true),
            // a quoted dash is not a range
            ("(?i)[\\Qa-c\\E]", "B", false),
            ("(?i)[\\Qa-c\\E]", "-", true),
            ("(?i)[\\Qa-c\\E]", "C", true),
            ("(?i)[a\\Q-\\Ec]", "B", false),
            ("(?i)[a\\Q-\\Ec]", "-", true),
            ("(?i)[a\\Q-\\Ec]", "C", true),
            // a quote with no \E runs to the end of the pattern
            ("(?i)x\\Qa", "XA", true),
            ("(?i)x\\Qa\\E", "XA", true),
            ("(?i)a\\Q\\E+", "AA", true),
            ("\\Q1\\E2", "12", true),
            ("\\Qa\\\\E", "a\\", true),
            ("\\Q\\\\E", "\\", true),
            ("\\Qa.b\\E", "a.b", true),
            ("\\Qa.b\\E", "axb", false),
            ("(?i)\\Qk\\E", "\u{212a}", false),
            ("(?iu)\\Qk\\E", "\u{212a}", true),
            // a backslash before < or > is the character
            ("\\<", "<", true),
            ("\\>", ">", true),
            ("[\\<]", "<", true),
            ("[\\>]", ">", true),
            ("\\Q<\\E", "<", true),
            ("[\\Q<\\E]", "<", true),
            ("\\Q>\\E", ">", true),
            // a ] first in a class is a member, and starts a range
            ("(?i)[]-c]", "A", true),
            ("(?i)[]-c]", "B", true),
            ("(?i)[]-c]", "_", true),
            ("(?i)[]-c]", "]", true),
            ("(?i)[]-c]", "X", false),
            ("(?i)[]-c]", "d", false),
            ("(?i)[]-c]", "D", false),
            ("(?i)[^]-c]", "A", false),
            ("(?i)[^]-c]", "X", true),
            ("(?i)[^]-c]", "d", true),
            ("(?i)[]-cx]", "X", true),
            ("(?i)[]-]", "-", true),
            ("(?i)[]-]", "]", true),
            ("(?iu)[]-c]", "A", true),
            ("(?ix)[ ]-c ]", "A", true),
            ("[]-c]", "a", true),
            ("[]-c]", "A", false),
            ("[]-c]", "]", true),
            // \p{Lower} and \p{Upper} are the ASCII letters under (?iu), whatever fancy_regex's i would fold
            ("(?iu)\\p{Lower}", "\u{17f}", false),
            ("(?iu)\\p{Lower}", "\u{212a}", false),
            ("(?iu)\\p{Lower}", "A", true),
            ("(?iu)\\p{Lower}", "a", true),
            ("(?iu)\\p{Upper}", "\u{17f}", false),
            ("(?iu)\\p{Upper}", "\u{212a}", false),
            ("(?iu)\\p{Upper}", "a", true),
            ("(?iu)\\p{Upper}", "\u{e9}", false),
            ("(?iu)\\P{Lower}", "\u{17f}", true),
            ("(?iu)\\P{Lower}", "A", false),
            ("(?iu)\\P{Lower}", "1", true),
            ("(?iu)\\P{Upper}", "\u{212a}", true),
            ("(?u)\\p{Lower}", "\u{17f}", false),
            ("(?u)\\p{Lower}", "A", false),
            // \w and \W are ASCII under (?iu), whatever fancy_regex's i would fold
            ("(?iu)\\w", "\u{17f}", false),
            ("(?iu)\\w", "\u{212a}", false),
            ("(?iu)\\w", "a", true),
            ("(?iu)\\w", "_", true),
            ("(?iu)\\W", "\u{17f}", true),
            ("(?iu)\\W", "\u{212a}", true),
            ("(?iu)\\W", "a", false),
            ("(?iu)\\W", "-", true),
            ("(?iu)\\w+", "k\u{212a}", false),
            ("(?iu)k\\w", "k\u{212a}", false),
            ("(?iu)\\w\\p{Lower}", "k\u{212a}", false),
            ("(?i)\\w", "\u{17f}", false),
            ("(?u)\\w", "\u{17f}", false),
            ("(?iU)\\w", "\u{17f}", true),
            // a backreference compares ASCII case-insensitively under (?i)
            ("(?i)(a)\\1", "aA", true),
            ("(?i)(a)\\1", "aa", true),
            ("(?i)(a)\\1", "ab", false),
            ("(?i)(\\d+)-\\1", "12-12", true),
            ("(?i)(\\d+)-\\1", "12-13", false),
            ("(?i)(?<x>a)\\k<x>", "aA", true),
            ("(?i)(?<x>a)\\k<x>", "ab", false),
            ("(?i)(a+)\\1", "aaAA", true),
            ("(?i)(a)\\1+", "aAAa", true),
            ("(?i)(a)(\\1)", "aA", true),
            ("(?i)(k)\\1", "kK", true),
            ("(?i)(k)\\1", "k\u{212a}", false),
            ("(?i:(a)\\1)", "aA", true),
            ("a(?i:(b)\\1)", "aBb", true),
            ("a(?i:(b)\\1)", "aBB", true),
            ("a(?i:(b)\\1)", "AbB", false),
            ("(?i)(a)((b))\\3\\2\\1", "aBBBA", true),
            ("(?i)(a)((b))\\3\\2\\1", "aBbBa", true),
        ];
        for (source, input, expected) in cases {
            check!(
                pattern(source).matches(input).ok() == Some(expected),
                "{source:?} against {input:?}"
            );
        }
    }

    /// A backreference under an ASCII fold is written `(?i:\1)`, and
    /// `fancy_regex`'s `i` compares the text of the group Unicode-insensitively
    /// when either text is not ASCII. So `(?i)(é)\1` matches `éÉ` here, where
    /// Java's `CIBackRef` does not, and the two agree on every ASCII text. The
    /// divergence is documented in `java_fold`; this test pins its extent, so
    /// that a change to it is noticed.
    #[test]
    fn a_backreference_under_an_ascii_fold_is_unicode_insensitive_for_non_ascii_text() {
        // (pattern, input, whole input matches here)
        for (source, input, expected) in [
            ("(?i)(\u{e9})\\1", "\u{e9}\u{c9}", true),
            ("(?i)(\u{e9})\\1", "\u{e9}\u{e9}", true),
        ] {
            check!(
                pattern(source).matches(input).ok() == Some(expected),
                "{source:?} against {input:?}"
            );
        }
    }

    /// `\N{name}` is a character in Java, which `fancy_regex` reads as `\N` and
    /// the text of the name, so a rule that has one is refused and not misread.
    #[test]
    fn a_named_character_is_refused() {
        for source in ["\\N{LATIN SMALL LETTER A}", "(?i)\\N{LATIN SMALL LETTER A}"] {
            check!(JavaPattern::compile(source).is_err(), "{source:?}");
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
