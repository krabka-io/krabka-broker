//! Java's `java.util.regex` in Rust: a [`JavaPattern`] compiles a
//! `java.util.regex.Pattern` and answers `Matcher.matches()`,
//! `String.replaceAll` and `String.replaceFirst` as JDK 25 does.
//!
//! Krabka reads three Apache Kafka configs whose values are Java regular
//! expressions: `ssl.principal.mapping.rules`, the Kerberos `auth_to_local`
//! rules, and the `match` patterns of a KIP-714 `CLIENT_METRICS`
//! subscription. A rule that matches differently from Java maps a principal
//! to another name, so the crate's job is Java's answer, quirks included.
//!
//! ```
//! use {assert2::assert, krabka_java_regex::JavaPattern};
//!
//! let rule = JavaPattern::compile("CN=([^,]+),OU=(?i)ops.*")?;
//! assert!(rule.matches("CN=alice,OU=OPS-EU")?);
//! assert!(rule.replace("CN=alice,OU=ops", "$1", true)? == "alice");
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # How it reads a pattern as Java does
//!
//! The `regex` crate differs from Java in the two places these lean on.
//! `Matcher.matches()` succeeds when *some* way of matching spans the whole
//! input, including through backtracking and lazy quantifiers, where a
//! leftmost-first search stops at the first match it finds. And
//! `Matcher.replaceAll` reads `$1_x` as group 1 followed by `_x`, where the
//! `regex` crate reads a group named `1_x`. [`JavaPattern`] gives both the
//! Java answer.
//!
//! The pattern itself is rewritten by `translate_java` first, which also gives
//! Java's reading of `\w`, `.`, and `(?i)`: Java folds ASCII case only, unless
//! `(?u)` asks for Unicode case as well, so `(?i)service-` does not match
//! `ſervice-` (long s). A backreference under a case flag, `\R`, and a group
//! inside a look-around or an atomic group match in ways `fancy_regex` cannot
//! give, so a pattern that has one runs on the matcher in `backtrack`
//! instead. So does a pattern `fancy_regex` cannot compile, such as a
//! look-behind that is not of one length with `\R` in it, or a very large
//! counted repeat. A pattern Java's `Pattern.compile` refuses is refused.
//!
//! # Limits
//!
//! A search takes at most 1,000,000 backtracking steps, `fancy_regex`'s
//! default, and the backtracking matcher keeps its alternatives on a stack of
//! its own, so a long input cannot overflow the thread's. A search past the
//! limit is a [`MatchError`], not a no match. Java answers such a search,
//! however slowly.
//!
//! The matcher answers a pattern `fancy_regex` cannot compile, where it
//! knows every node of it: a look-behind that is not of one length with
//! `\R`, a look-around, an atomic group or a group in it, such as
//! `(?<=\R?x)y`, and a counted repeat too large for the `regex` crate to
//! build at its default size limit, such as `x{1,1000000000}`, in a
//! look-behind or outside one. It counts the iterations of a repeat, so such
//! a pattern compiles in milliseconds, but it tries every start of a
//! look-behind, and a long input can take it past the step limit.
//!
//! [`JavaPattern::compile`] refuses the patterns below, which Java compiles,
//! rather than match them another way. Each is a [`PatternError`].
//!
//! - A Unicode block, such as `\p{InGreek}`, and `\X`.
//! - A named char, `\N{name}`.
//! - A quantifier of a look-around, such as `(?=(a))*`, which `fancy_regex`
//!   cannot parse.
//! - A backreference to a group the pattern does not have, such as `(a)\2`,
//!   which `fancy_regex` refuses, and which never matches in Java.
//! - A look-behind whose maximum length overflows Java's `int`, such as
//!   `(?<=a*b*)`, where Java then tries fewer starts than the look-behind
//!   allows. Where the overflowed maximum still covers every start, as in
//!   `(?<=a*bc)`, the pattern compiles.
//! - A pattern with a char past U+FFFF anywhere in it, whenever a look-behind's
//!   maximum overflows, as in `😀x(?<=a*b*c*x)y`. Java reads one with the char
//!   before the look-behind as it reads one without.
//!
//! One more input limit applies at match time. Where an overflowed maximum is
//! at least 2^30 and covers every start only up to that length, as in
//! `(?<=a*b*c*x)`, an input of more UTF-16 units than the maximum is a
//! [`MatchError`].

mod backtrack;
mod look_behind;
mod translate;

use {
    self::{
        backtrack::Program,
        look_behind::{LookBehinds, Refusal, check_look_behinds},
        translate::{JavaTranslation, translate_java},
    },
    fancy_regex::{CompileError, Error as FancyError, Expr, Regex},
    std::{collections::BTreeMap, fmt::Display, ops::Range, sync::Arc},
};

/// A `java.util.regex.Pattern` compiled for `matches()` and
/// `replaceAll`/`replaceFirst`.
#[derive(Debug, Clone)]
pub struct JavaPattern(Arc<Compiled>);

/// The engines behind a [`JavaPattern`], shared so that a rule holding one is
/// small.
#[derive(Debug)]
struct Compiled {
    /// The engine that answers.
    engine: Engine,
    /// The groups, group 0 included.
    groups: usize,
    /// Group name to index, for `${name}` in a replacement.
    names: BTreeMap<String, usize>,
    /// The most UTF-16 units of input the pattern is read for as Java reads
    /// it, for a look-behind whose maximum length wraps in Java's `int`.
    input_limit: Option<usize>,
}

/// The engine that answers for a [`JavaPattern`].
#[derive(Debug)]
enum Engine {
    /// `fancy_regex`, for a pattern it reads as Java does.
    Fancy {
        /// The pattern as written, for `replaceAll` and `replaceFirst`.
        search: Regex,
        /// The pattern pinned to the whole input, for `matches()`.
        whole: Regex,
    },
    /// The backtracking matcher, for a pattern that `fancy_regex` does not
    /// read as Java does, or cannot compile.
    Backtrack(Program),
}

/// A pattern Java's `Pattern.compile` refuses with a
/// `PatternSyntaxException`, or one this crate cannot read as Java does.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct PatternError(String);

/// A match this crate gives no answer for: a replacement string Java's
/// `Matcher.appendReplacement` refuses, which Java reports as
/// `IllegalArgumentException` or `IndexOutOfBoundsException`, or a search
/// that ran past the backtrack limit.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct MatchError(String);

impl MatchError {
    /// A search that gave no answer, as the error a caller rejects the name
    /// by.
    fn search(error: impl Display) -> Self { Self(error.to_string()) }
}

impl JavaPattern {
    /// Compiles `pattern`, `Pattern.compile(pattern)`.
    ///
    /// # Errors
    ///
    /// [`PatternError`] for a pattern Java refuses, and for one this crate
    /// cannot read as Java does, which it refuses rather than match it
    /// another way.
    pub fn compile(pattern: &str) -> Result<Self, PatternError> {
        let JavaTranslation {
            pattern: translated,
            backref_folds,
            linebreak_atoms,
            group_repeats,
        } = translate_java(pattern)
            .ok_or_else(|| PatternError(format!("{pattern:?} is not a java.util.regex pattern")))?;
        let tree =
            Expr::parse_tree(&translated).map_err(|error| PatternError(error.to_string()))?;
        let groups = 1 + count_groups(&tree.expr);
        // `fancy_regex` refuses a reference to a group the pattern does not
        // have. It is not asked where a look-behind is too large for it, and
        // the matcher does not refuse one, so the check is made here.
        if let Some(group) = backref_past(&tree.expr, groups) {
            let error = FancyError::CompileError(Box::new(CompileError::InvalidBackref(group)));
            return Err(PatternError(error.to_string()));
        }
        let refuse = |refusal: Refusal| PatternError(format!("{pattern:?} {}", refusal.reason()));
        // `fancy_regex` would take minutes to build a look-behind too large
        // for the `regex` crate, so it is not asked to.
        let LookBehinds {
            input_limit,
            too_large,
        } = check_look_behinds(pattern, &tree.expr).map_err(refuse)?;
        let fancy = if too_large {
            Err(FancyFailure::Unbuilt(refuse(Refusal::TooLarge)))
        } else {
            compile_fancy(&translated)
        };
        let names = tree.named_groups.into_iter().collect();
        let program = Program::new(
            &translated,
            groups,
            &backref_folds,
            &linebreak_atoms,
            &group_repeats,
        );
        // A pattern `fancy_regex` reads as Java does, or one with a node the
        // matcher does not know, keeps `fancy_regex`. The matcher answers
        // for one `fancy_regex` cannot build.
        let engine = match (fancy, program) {
            (Ok(_), Some(program)) if program.differs_from_fancy() => Engine::Backtrack(program),
            (Ok((search, whole)), _) => Engine::Fancy { search, whole },
            (Err(FancyFailure::Unbuilt(_)), Some(program)) => Engine::Backtrack(program),
            (Err(FancyFailure::Unbuilt(error) | FancyFailure::Refused(error)), _) => {
                return Err(error);
            }
        };
        Ok(Self(Arc::new(Compiled {
            engine,
            groups,
            names,
            input_limit,
        })))
    }

    /// The number of capturing groups, `Matcher.groupCount()`.
    #[must_use]
    pub fn group_count(&self) -> usize { self.0.groups - 1 }

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
    ///
    /// An input longer than the pattern's input limit is an error too: see
    /// the crate's `# Limits`.
    pub fn matches(&self, input: &str) -> Result<bool, MatchError> {
        check_input_length(self.0.input_limit, input)?;
        match &self.0.engine {
            Engine::Backtrack(program) => program.matches(input).map_err(MatchError::search),
            Engine::Fancy { whole, .. } => whole.is_match(input).map_err(MatchError::search),
        }
    }

    /// `Matcher.find()`: the span of each group of the first match that starts
    /// at `from` or after it, group 0 the whole match. `last` is where the
    /// match before ended, or `from` for the first, which `\G` matches. Only
    /// the backtracking matcher reads it: a pattern with `\G` keeps
    /// `fancy_regex` only where the matcher does not know a node of it.
    fn find(
        &self,
        input: &str,
        from: usize,
        last: usize,
    ) -> Result<Option<Vec<Option<Range<usize>>>>, MatchError> {
        let search = match &self.0.engine {
            Engine::Backtrack(program) => {
                return program.find(input, from, last).map_err(MatchError::search);
            }
            Engine::Fancy { search, .. } => search,
        };
        let captures = search
            .captures_from_pos(input, from)
            .map_err(MatchError::search)?;
        Ok(captures.map(|captures| {
            captures
                .iter()
                .map(|group| group.map(|group| group.range()))
                .collect()
        }))
    }

    /// `input.replaceAll(pattern, replacement)` when `all`, and
    /// `input.replaceFirst(pattern, replacement)` when not.
    ///
    /// The matches are `Matcher.find`'s, not the `regex` crate's iterator's.
    /// After a match that is not empty, `find` may match the empty string at
    /// the very position the match ended, and the crate's iterator does not:
    /// `"alice".replaceAll("(.*)", "$1x")` is `alicexx` in Java. After an empty
    /// match `find` starts one character on, and `\G` stays where that match
    /// ended.
    ///
    /// # Errors
    ///
    /// [`MatchError`] for a replacement Java refuses, for a search past the
    /// backtrack limit, and for an input longer than the pattern's input
    /// limit.
    pub fn replace(&self, input: &str, replacement: &str, all: bool) -> Result<String, MatchError> {
        check_input_length(self.0.input_limit, input)?;
        let mut out = String::with_capacity(input.len());
        let mut copied = 0;
        let mut search_from = 0;
        while search_from <= input.len() {
            let Some(groups) = self.find(input, search_from, copied)? else {
                break;
            };
            let Some(whole) = groups.first().cloned().flatten() else {
                break;
            };
            out.push_str(&input[copied..whole.start]);
            self.append_replacement(&mut out, replacement, input, &groups)?;
            copied = whole.end;
            if !all {
                break;
            }
            search_from = if whole.is_empty() {
                input[whole.end..]
                    .chars()
                    .next()
                    .map_or(input.len() + 1, |next| whole.end + next.len_utf8())
            } else {
                whole.end
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
        input: &str,
        groups: &[Option<Range<usize>>],
    ) -> Result<(), MatchError> {
        let refuse = |message: &str| MatchError(message.to_owned());
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
                            .ok_or_else(|| MatchError(format!("no group with name {name}")))?
                    } else {
                        let mut group = first
                            .to_digit(10)
                            .ok_or_else(|| refuse("illegal group reference"))?
                            as usize;
                        if group > group_count {
                            return Err(MatchError(format!("no group {group}")));
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
                    if let Some(Some(text)) = groups.get(group) {
                        out.push_str(&input[text.clone()]);
                    }
                }
                other => out.push(other),
            }
        }
        Ok(())
    }
}

/// Why `fancy_regex` gives no engine for a pattern, with the error the
/// pattern is refused by where the matcher does not answer either.
#[derive(Debug)]
enum FancyFailure {
    /// `fancy_regex` cannot build the pattern: a look-behind that is not of
    /// one length with a node it runs itself in it, such as `\R` or a
    /// look-ahead, or a part too large for the `regex` crate. The matcher
    /// answers instead where it knows every node.
    Unbuilt(PatternError),
    /// `fancy_regex` refuses the pattern for another reason, such as a
    /// backreference to a group the pattern does not have.
    Refused(PatternError),
}

/// `fancy_regex`'s pair for `translated`: the pattern as written, and the
/// pattern pinned to the whole input.
fn compile_fancy(translated: &str) -> Result<(Regex, Regex), FancyFailure> {
    let compile = |source: &str| {
        Regex::new(source).map_err(|error| {
            let unbuilt = matches!(
                &error,
                FancyError::CompileError(compile_error) if matches!(
                    **compile_error,
                    CompileError::FeatureNotYetSupported(_)
                        | CompileError::LookBehindNotConst
                        | CompileError::InnerError(_)
                        | CompileError::DfaBuildError(..)
                )
            );
            let error = PatternError(error.to_string());
            if unbuilt {
                FancyFailure::Unbuilt(error)
            } else {
                FancyFailure::Refused(error)
            }
        })
    };
    Ok((
        compile(translated)?,
        compile(&format!("\\A(?:{translated})\\z"))?,
    ))
}

/// The first group past the last of `groups`, group 0 included, that a
/// backreference in `expr` refers to. `fancy_regex` refuses such a pattern.
fn backref_past(expr: &Expr, groups: usize) -> Option<usize> {
    match expr {
        Expr::Backref { group, .. } if *group >= groups => Some(*group),
        _ => expr
            .children_iter()
            .find_map(|child| backref_past(child, groups)),
    }
}

/// The capturing groups in `expr`, which the parser numbers in order.
fn count_groups(expr: &Expr) -> usize {
    usize::from(matches!(expr, Expr::Group(_)))
        + expr.children_iter().map(count_groups).sum::<usize>()
}

/// Refuses an `input` of more UTF-16 units than `limit`, the most a pattern
/// is read for as Java reads it. The UTF-8 length is never less than the
/// UTF-16 one, so the units are counted only past it.
fn check_input_length(limit: Option<usize>, input: &str) -> Result<(), MatchError> {
    match limit {
        Some(limit) if input.len() > limit && input.encode_utf16().count() > limit => {
            Err(MatchError(format!(
                "the input is longer than the {limit} UTF-16 units that krabka reads a \
                 look-behind of this pattern for as Java does"
            )))
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::{assert, check},
    };

    fn pattern(source: &str) -> JavaPattern {
        JavaPattern::compile(source).unwrap_or_else(|error| panic!("{source}: {error}"))
    }

    fn assert_pattern_matches(source: &str, input: &str, expected: bool) {
        check!(
            pattern(source).matches(input).ok() == Some(expected),
            "{source:?} against {input:?}"
        );
    }

    /// One whole-input match: whether `source` matches all of `input`.
    struct MatchCase<'a> {
        source: &'a str,
        input: &'a str,
        expected: bool,
    }

    /// A [`MatchCase`] of `source` against `input`.
    const fn row<'a>(source: &'a str, input: &'a str, expected: bool) -> MatchCase<'a> {
        MatchCase {
            source,
            input,
            expected,
        }
    }

    /// One replacement: `source` on `input` with `replacement` gives `expected`.
    struct ReplaceCase<'a> {
        source: &'a str,
        input: &'a str,
        replacement: &'a str,
        expected: &'a str,
    }

    /// A [`ReplaceCase`] of `source` on `input` with `replacement`.
    const fn replace_row<'a>(
        source: &'a str,
        input: &'a str,
        replacement: &'a str,
        expected: &'a str,
    ) -> ReplaceCase<'a> {
        ReplaceCase {
            source,
            input,
            replacement,
            expected,
        }
    }

    fn check_matches<'a>(cases: impl IntoIterator<Item = MatchCase<'a>>, debug_names: bool) {
        for MatchCase {
            source,
            input,
            expected,
        } in cases
        {
            let context = if debug_names {
                format!("{source:?} against {input:?}")
            } else {
                format!("{source} against {input}")
            };
            check!(
                pattern(source).matches(input).ok() == Some(expected),
                "{context}"
            );
        }
    }

    fn check_replacements<'a>(
        cases: impl IntoIterator<Item = ReplaceCase<'a>>,
        all: bool,
        include_replacement: bool,
    ) {
        for ReplaceCase {
            source,
            input,
            replacement,
            expected,
        } in cases
        {
            let context = if include_replacement {
                format!("{source} on {input} with {replacement}")
            } else {
                format!("{source} on {input}")
            };
            check!(
                pattern(source)
                    .replace(input, replacement, all)
                    .ok()
                    .as_deref()
                    == Some(expected),
                "{context}"
            );
        }
    }

    /// `Matcher.matches()` finds a whole-input match by backtracking, which a
    /// leftmost-first search does not.
    #[test]
    fn matches_backtracks_to_span_the_whole_input() {
        // (pattern, input, matches)
        let cases = [
            row("a|ab", "ab", true),
            row("a|ab", "abc", false),
            row("CN=(.*?)", "CN=abc", true),
            row("(a+)+b", "aaab", true),
            row("ali", "alice", false),
            row("ali.*", "alice", true),
        ];
        check_matches(cases, false);
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
            replace_row("(.*)", "alice", "$1x", "alicexx"),
            replace_row("a*", "baaac", "-", "-b--c-"),
            replace_row("x*", "abc", "-", "-a-b-c-"),
            replace_row("\\w*", "ab cd", "[$0]", "[ab][] [cd][]"),
            replace_row("(?:)", "é!", "|", "|é|!|"),
            replace_row("", "", "|", "|"),
            replace_row("a", "", "|", ""),
            replace_row("b*", "aé", "-", "-a-é-"),
        ];
        check_replacements(cases, true, false);
    }

    /// `replaceFirst` stops after the first match, an empty one included.
    #[test]
    fn replace_first_stops_after_the_first_match_even_when_it_is_empty() {
        // (pattern, input, replacement, result)
        check_replacements(
            [
                replace_row("(.*)", "alice", "$1x", "alicex"),
                replace_row("b*", "abc", "-", "-abc"),
            ],
            false,
            false,
        );
    }

    /// Java reads `\w`, `\d`, `\s` and `\b` as ASCII unless `(?U)` asks for
    /// Unicode, and `.` stops at every line terminator unless `(?s)` or `(?d)`
    /// says otherwise. Each row is `Pattern.matches` on JDK 21, except `\b`,
    /// which reads ASCII from JDK 19.
    #[test]
    fn matches_reads_classes_as_ascii_and_dot_as_java_does() {
        // (pattern, input, whole input matches)
        let cases = [
            row("\\w+", "abc_09", true),
            row("\\w+", "é", false),
            row("\\W", "é", true),
            row("\\d", "٣", false),
            row("\\D", "٣", true),
            row("\\s", " \t\u{0B}", false),
            row("\\s+", " \t\u{0B}", true),
            row("\\s", "\u{a0}", false),
            row("\\s", "\u{2003}", false),
            row("\\S", "\u{a0}", true),
            row("[\\w.]+", "a.b", true),
            row("[\\w.]+", "é", false),
            row("[^\\w]", "é", true),
            row("[\\W]+", "é!", true),
            row("[\\d-]+", "1-2", true),
            row("[\\s]", "\u{a0}", false),
            row("(?U)\\w+", "é", true),
            row("(?U)\\d", "٣", true),
            row("(?U)\\s", "\u{a0}", true),
            row("(?U:\\w)\\w", "éa", true),
            row("(?U:\\w)\\w", "éé", false),
            row("(?:(?U)\\w)\\w", "éé", false),
            row("(?U)(?-U)\\w", "é", false),
            row("\\bfoo", "foo", true),
            row("a\\b", "a", true),
            row("\\Bfoo", "foo", false),
            row("a\\bé", "aé", true),
            row("\\bé", "é", false),
            row("a\\Bé", "aé", false),
            row("a.b", "axb", true),
            row("a.b", "a\nb", false),
            row("a.b", "a\rb", false),
            row("a.b", "a\u{85}b", false),
            row("a.b", "a\u{2028}b", false),
            row("a.b", "a\u{2029}b", false),
            row(".*", "abc\rdef", false),
            row("(?s)a.b", "a\rb", true),
            row("(?s)a.b", "a\nb", true),
            row("(?d)a.b", "a\rb", true),
            row("(?d)a.b", "a\u{85}b", true),
            row("(?d)a.b", "a\nb", false),
            row("(?s:a.)b.", "a\nbx", true),
            row("(?s:a.)b.", "a\nb\r", false),
            row("(?s)(?-s).", "\n", false),
            row("a[.]b", "a.b", true),
            row("a\\.b", "axb", false),
        ];
        check_matches(cases, true);
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
            row("(?i)service-", "SERVICE-", true),
            row("(?i)service-", "service-", true),
            row("(?i)service-", "\u{17f}ervice-", false),
            row("(?iu)service-", "\u{17f}ervice-", true),
            row("(?iu)service-", "SERVICE-", true),
            row("(?i)k", "\u{212a}", false),
            row("(?iu)k", "\u{212a}", true),
            row("(?ui)k", "\u{212a}", true),
            row("(?u)k", "K", false),
            row("(?i)é", "É", false),
            row("(?i)é", "é", true),
            row("(?iu)é", "É", true),
            row("(?U)(?i)k", "\u{212a}", true),
            row("(?iU)k", "\u{212a}", true),
            row("(?iU)(?-U)k", "\u{212a}", false),
            row("(?iU)(?-U)k", "K", true),
            row("(?iU)(?-u)k", "\u{212a}", false),
            row("(?iU)(?-u)\\w", "\u{e9}", true),
            row("(?i)a(?u)k", "A\u{212a}", true),
            row("(?i)a(?u)k", "AK", true),
            row("(?iu)a(?-u)k", "A\u{212a}", false),
            row("(?iu)a(?-u)k", "AK", true),
            row("(?i)ss", "\u{df}", false),
            row("(?iu)ss", "\u{df}", false),
            row("(?i)i", "\u{130}", false),
            row("(?i)i", "\u{131}", false),
            row("(?i)\\u00E5", "\u{212b}", false),
            row("(?iu)\\u00E5", "\u{212b}", true),
            row("(?i)[a-f]+", "AbCdEf", true),
            row("(?i)[a-f]", "\u{212a}", false),
            row("(?i)[k]", "\u{212a}", false),
            row("(?iu)[k]", "\u{212a}", true),
            row("(?i)[^a]", "A", false),
            row("(?i)[^a]", "b", true),
            row("(?i)[^a]", "\u{e9}", true),
            row("(?i)[^k]", "\u{212a}", true),
            row("(?iu)[^k]", "\u{212a}", false),
            row("(?i)[X-c]", "x", true),
            row("(?i)[X-c]", "A", true),
            row("(?i)[X-c]", "[", true),
            row("(?i)[X-c]", "d", false),
            row("(?i)[+-z]", "Q", true),
            row("(?i)[a-c&&[^b]]", "B", false),
            row("(?i)[a-c&&[^b]]", "C", true),
            row("(?i)[a-c&&[^b]]", "A", true),
            row("(?i)[a-c[^x-z]]", "Y", false),
            row("(?i)[a-c[^x-z]]", "B", true),
            row("(?i)[\\x41-\\x43]", "b", true),
            row("(?i)[\\u0041-\\u0043]", "b", true),
            row("(?i)[\\Qab\\E]", "B", true),
            row("(?i)[a-]", "A", true),
            row("(?i)[-a]", "A", true),
            row("(?i)[]a]", "]", true),
            row("(?i)[]a]", "A", true),
            row("(?i)[a-c-e]", "E", true),
            row("(?i)[a-c-e]", "-", true),
            row("(?i)[\\w&&[^a]]", "A", false),
            row("(?i)[\\w&&[^a]]", "B", true),
            row("(?i)[a\\-c]", "C", true),
            row("(?i)[a\\-c]", "-", true),
            row("(?i)[\\0101-\\0103]", "b", true),
            row("(?i)[\\0101-\\0103]", "d", false),
            row("(?i)[\\x41-c]", "b", true),
            row("(?i)[a-c[d-f]]", "E", true),
            row("(?i)[\u{e9}-\u{eb}]", "\u{ca}", false),
            row("(?ix) [ a - c ]", "B", true),
            row("(?ix) [ a - c ] # x", "d", false),
            row("(?i)a\\.b", "A.B", true),
            row("(?i)\\[a\\]", "[A]", true),
            row("(?i)a{1,2}b", "aAB", true),
            row("(?i)\u{e9}+", "\u{e9}\u{e9}", true),
            row("(?i)\\0101\\0102", "aB", true),
            row("(?i)\\x41", "a", true),
            row("(?i)\\u0041", "a", true),
            row("(?i)\\x{41}", "a", true),
            row("(?i)\\0101", "a", true),
            row("(?i)\\c!", "A", true),
            row("(?i)\\Qab\\E", "AB", true),
            row("(?i)\\Q.b\\E", ".B", true),
            row("(?i)\\Q.b\\E", "xB", false),
            row("(?i)\\Qk\\E", "\u{212a}", false),
            row("(?i)\\p{Lower}", "A", true),
            row("(?i)\\p{Lower}", "\u{e9}", false),
            row("(?i)\\p{Upper}", "a", true),
            row("(?i)\\p{Upper}", "\u{c9}", false),
            row("\\p{Lower}", "a", true),
            row("\\p{Lower}", "A", false),
            row("\\p{Lower}", "\u{e9}", false),
            row("\\p{Upper}", "A", true),
            row("\\p{Upper}", "\u{c9}", false),
            row("(?i)\\p{Lu}", "a", true),
            row("(?i)\\p{Lu}", "\u{e9}", true),
            row("(?i)\\p{Lu}", "1", false),
            row("(?i)\\P{Lu}", "a", false),
            row("(?i)\\P{Lu}", "1", true),
            row("\\p{Lu}", "a", false),
            row("\\p{Lu}", "A", true),
            row("(?i)[\\p{Lower}]", "A", true),
            row("(?i)[\\P{Lower}]", "A", false),
            row("(?i)[\\P{Lower}]", "1", true),
            row("(?i)\\p{IsLowercase}", "A", true),
            row("(?i)\\p{gc=Lu}", "a", true),
            row("(?i)\\p{IsLu}", "a", true),
            row("(?i:a)b", "Ab", true),
            row("(?i:a)b", "AB", false),
            row("a(?i)b", "aB", true),
            row("a(?i)b", "Ab", false),
            row("a(?i:b)c", "aBc", true),
            row("a(?i:b)c", "aBC", false),
            row("(?i)a(?-i)b", "Ab", true),
            row("(?i)a(?-i)b", "AB", false),
            row("((?i)a)b", "Ab", true),
            row("((?i)a)b", "AB", false),
            row("(?i)a|b", "B", true),
            row("(?i)(?i)(?-i)a", "A", false),
            row("(?i)(?i)(?-i)a", "a", true),
            row("(?i)a{2}", "aA", true),
            row("(?i)(?:a|b)+", "AbBa", true),
            row("(?i)^ab$", "AB", true),
            row("(?i)\\bab", "AB", true),
            row("(?i)\\w", "A", true),
            row("(?i)a.c", "AxC", true),
            row("(?i)(?<Name>a)", "A", true),
            row("(?i)(?=a)A", "A", true),
            row("(?i)(?!a)b", "B", true),
            row("(?ix) a b # comment", "AB", true),
            row("(?x)a b # Comment here", "ab", true),
            row("(?x)a b # Comment here", "AB", false),
            row("(?iu)(a)\\1", "aA", true),
            row("(?iu)(a)\\1", "ab", false),
            row("(?i)(a)(?-i)\\1", "aA", false),
            row("(?i)(a)(?-i)\\1", "aa", true),
            row("(?i)(a)(?-i:\\1)", "aA", false),
            row("(a)\\1", "aa", true),
            row("(a)\\1", "aA", false),
        ];
        check_matches(cases, true);
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
            row("(?i)[\\Qa\\E-c]", "B", true),
            row("(?i)[\\Qa\\E-c]", "b", true),
            row("(?i)[\\Qa\\E-c]", "A", true),
            row("(?i)[\\Qa\\E-c]", "X", false),
            row("(?i)[a-\\Qc\\E]", "B", true),
            row("(?i)[a-\\Qc\\E]", "X", false),
            row("(?i)[\\Qa\\E-\\Qc\\E]", "B", true),
            row("(?i)[\\Qa\\E-\\Qc\\E]", "X", false),
            row("(?i)[\\Qa\\Eb-d]", "C", true),
            row("(?i)[\\Qa\\Eb-d]", "A", true),
            row("(?i)[\\Qa\\Eb-d]", "X", false),
            row("(?i)[\\Q\\Ea-c]", "B", true),
            row("(?i)[a-\\Q\\Ec]", "B", true),
            row("(?i)[a-\\Q\\Ec]", "X", false),
            row("[\\Qa\\E-c]", "b", true),
            row("[\\Qa\\E-c]", "B", false),
            row("[\\Qa\\E-c]", "X", false),
            row("(?i)[\\Q]\\E-c]", "A", true),
            row("(?i)[\\Q]\\E-c]", "X", false),
            row("(?i)[\\Q1\\E-3]", "2", true),
            // a quoted dash is not a range
            row("(?i)[\\Qa-c\\E]", "B", false),
            row("(?i)[\\Qa-c\\E]", "-", true),
            row("(?i)[\\Qa-c\\E]", "C", true),
            row("(?i)[a\\Q-\\Ec]", "B", false),
            row("(?i)[a\\Q-\\Ec]", "-", true),
            row("(?i)[a\\Q-\\Ec]", "C", true),
            // a quote with no \E runs to the end of the pattern
            row("(?i)x\\Qa", "XA", true),
            row("(?i)x\\Qa\\E", "XA", true),
            row("(?i)a\\Q\\E+", "AA", true),
            row("\\Q1\\E2", "12", true),
            row("\\Qa\\\\E", "a\\", true),
            row("\\Q\\\\E", "\\", true),
            row("\\Qa.b\\E", "a.b", true),
            row("\\Qa.b\\E", "axb", false),
            row("(?i)\\Qk\\E", "\u{212a}", false),
            row("(?iu)\\Qk\\E", "\u{212a}", true),
            // a backslash before < or > is the character
            row("\\<", "<", true),
            row("\\>", ">", true),
            row("[\\<]", "<", true),
            row("[\\>]", ">", true),
            row("\\Q<\\E", "<", true),
            row("[\\Q<\\E]", "<", true),
            row("\\Q>\\E", ">", true),
            // a ] first in a class is a member, and starts a range
            row("(?i)[]-c]", "A", true),
            row("(?i)[]-c]", "B", true),
            row("(?i)[]-c]", "_", true),
            row("(?i)[]-c]", "]", true),
            row("(?i)[]-c]", "X", false),
            row("(?i)[]-c]", "d", false),
            row("(?i)[]-c]", "D", false),
            row("(?i)[^]-c]", "A", false),
            row("(?i)[^]-c]", "X", true),
            row("(?i)[^]-c]", "d", true),
            row("(?i)[]-cx]", "X", true),
            row("(?i)[]-]", "-", true),
            row("(?i)[]-]", "]", true),
            row("(?iu)[]-c]", "A", true),
            row("(?ix)[ ]-c ]", "A", true),
            row("[]-c]", "a", true),
            row("[]-c]", "A", false),
            row("[]-c]", "]", true),
            // \p{Lower} and \p{Upper} are the ASCII letters under (?iu), whatever fancy_regex's i would fold
            row("(?iu)\\p{Lower}", "\u{17f}", false),
            row("(?iu)\\p{Lower}", "\u{212a}", false),
            row("(?iu)\\p{Lower}", "A", true),
            row("(?iu)\\p{Lower}", "a", true),
            row("(?iu)\\p{Upper}", "\u{17f}", false),
            row("(?iu)\\p{Upper}", "\u{212a}", false),
            row("(?iu)\\p{Upper}", "a", true),
            row("(?iu)\\p{Upper}", "\u{e9}", false),
            row("(?iu)\\P{Lower}", "\u{17f}", true),
            row("(?iu)\\P{Lower}", "A", false),
            row("(?iu)\\P{Lower}", "1", true),
            row("(?iu)\\P{Upper}", "\u{212a}", true),
            row("(?u)\\p{Lower}", "\u{17f}", false),
            row("(?u)\\p{Lower}", "A", false),
            // \w and \W are ASCII under (?iu), whatever fancy_regex's i would fold
            row("(?iu)\\w", "\u{17f}", false),
            row("(?iu)\\w", "\u{212a}", false),
            row("(?iu)\\w", "a", true),
            row("(?iu)\\w", "_", true),
            row("(?iu)\\W", "\u{17f}", true),
            row("(?iu)\\W", "\u{212a}", true),
            row("(?iu)\\W", "a", false),
            row("(?iu)\\W", "-", true),
            row("(?iu)\\w+", "k\u{212a}", false),
            row("(?iu)k\\w", "k\u{212a}", false),
            row("(?iu)\\w\\p{Lower}", "k\u{212a}", false),
            row("(?i)\\w", "\u{17f}", false),
            row("(?u)\\w", "\u{17f}", false),
            row("(?iU)\\w", "\u{17f}", true),
            // a backreference compares ASCII case-insensitively under (?i)
            row("(?i)(a)\\1", "aA", true),
            row("(?i)(a)\\1", "aa", true),
            row("(?i)(a)\\1", "ab", false),
            row("(?i)(\\d+)-\\1", "12-12", true),
            row("(?i)(\\d+)-\\1", "12-13", false),
            row("(?i)(?<x>a)\\k<x>", "aA", true),
            row("(?i)(?<x>a)\\k<x>", "ab", false),
            row("(?i)(a+)\\1", "aaAA", true),
            row("(?i)(a)\\1+", "aAAa", true),
            row("(?i)(a)(\\1)", "aA", true),
            row("(?i)(k)\\1", "kK", true),
            row("(?i)(k)\\1", "k\u{212a}", false),
            row("(?i:(a)\\1)", "aA", true),
            row("a(?i:(b)\\1)", "aBb", true),
            row("a(?i:(b)\\1)", "aBB", true),
            row("a(?i:(b)\\1)", "AbB", false),
            row("(?i)(a)((b))\\3\\2\\1", "aBBBA", true),
            row("(?i)(a)((b))\\3\\2\\1", "aBbBa", true),
        ];
        check_matches(cases, true);
    }

    /// Under `(?iu)` a class member is the set that Java's predicates accept, and not what
    /// `fancy_regex`'s `i` folds a whole class to: `\w`, `\W`, `\p{Lower}` and `\p{Upper}` in a
    /// class are ASCII, a range that has `K` and not `k` leaves out the Kelvin sign, and `İ`
    /// and `ı` are members of a range that has `i`. Each row is `Pattern.matches` on JDK 17.
    #[test]
    fn matches_folds_unicode_case_in_a_class_as_java_does() {
        // (pattern, input, whole input matches)
        let cases = [
            // \w, \W, \p{Lower}, \p{Upper} in a class are ASCII under (?iu), so the letters that fold to s and k are not members
            row("(?iu)[\\w]", "\u{17f}", false),
            row("(?iu)[\\w]", "\u{212a}", false),
            row("(?iu)[\\w]", "a", true),
            row("(?iu)[\\w]", "K", true),
            row("(?iu)[\\w]", "_", true),
            row("(?iu)[\\w]", "\u{e9}", false),
            row("(?iu)[\\w-]", "\u{17f}", false),
            row("(?iu)[\\w-]", "\u{212a}", false),
            row("(?iu)[\\w-]", "-", true),
            row("(?iu)[\\w-]", "z", true),
            row("(?iu)[\\W]", "\u{17f}", true),
            row("(?iu)[\\W]", "\u{212a}", true),
            row("(?iu)[\\W]", "a", false),
            row("(?iu)[\\W]", "-", true),
            row("(?iu)[^\\w]", "\u{17f}", true),
            row("(?iu)[^\\w]", "k", false),
            row("(?iu)[\\p{Lower}]", "\u{17f}", false),
            row("(?iu)[\\p{Lower}]", "\u{212a}", false),
            row("(?iu)[\\p{Lower}]", "A", true),
            row("(?iu)[\\p{Lower}]", "a", true),
            row("(?iu)[\\p{Lower}]", "\u{e9}", false),
            row("(?iu)[\\p{Upper}]", "\u{17f}", false),
            row("(?iu)[\\p{Upper}]", "\u{212a}", false),
            row("(?iu)[\\p{Upper}]", "a", true),
            row("(?iu)[\\p{Upper}]", "\u{c9}", false),
            row("(?iu)[\\P{Lower}]", "\u{17f}", true),
            row("(?iu)[\\P{Lower}]", "A", false),
            row("(?iu)[\\P{Lower}]", "1", true),
            row("(?iu)[\\P{Upper}]", "\u{212a}", true),
            row("(?iu)[\\p{Lower}k]", "\u{17f}", false),
            row("(?iu)[\\p{Lower}k]", "\u{212a}", true),
            row("(?iu)[\\p{Lower}s]", "\u{17f}", true),
            row("(?iu)[\\w&&[^a]]", "A", false),
            row("(?iu)[\\w&&[^a]]", "b", true),
            row("(?iu)[\\w&&[^k]]", "\u{212a}", false),
            row("(?iu)[\\w&&[^k]]", "K", false),
            row("(?iu)[^\\p{Lower}]", "\u{17f}", true),
            row("(?iu)[^\\p{Lower}]", "a", false),
            row("(?iu)[^\\p{Lower}]", "1", true),
            // a range that holds K and not k, or s and not S, compares the upper case and the lower case of the upper case of the char
            row("(?iu)[a-z]", "\u{212a}", true),
            row("(?iu)[a-z]", "\u{17f}", true),
            row("(?iu)[a-z]", "\u{130}", true),
            row("(?iu)[a-z]", "\u{131}", true),
            row("(?iu)[a-z]", "K", true),
            row("(?iu)[a-z]", "\u{df}", false),
            row("(?iu)[A-Z]", "\u{212a}", false),
            row("(?iu)[A-Z]", "\u{17f}", true),
            row("(?iu)[A-Z]", "\u{130}", false),
            row("(?iu)[A-Z]", "\u{131}", true),
            row("(?iu)[A-Z]", "k", true),
            row("(?iu)[A-K]", "\u{212a}", false),
            row("(?iu)[A-K]", "k", true),
            row("(?iu)[A-K]", "\u{17f}", false),
            row("(?iu)[A-c]", "\u{212a}", false),
            row("(?iu)[A-c]", "k", true),
            row("(?iu)[A-c]", "C", true),
            row("(?iu)[A-c]", "d", true),
            row("(?iu)[A-k]", "\u{212a}", true),
            row("(?iu)[k-k]", "\u{212a}", true),
            row("(?iu)[K-K]", "\u{212a}", false),
            row("(?iu)[K-K]", "k", true),
            row("(?iu)[s-s]", "\u{17f}", true),
            row("(?iu)[S-S]", "\u{17f}", true),
            row("(?iu)[i-i]", "\u{130}", true),
            row("(?iu)[I-I]", "\u{131}", true),
            row("(?iu)[^A-Z]", "\u{212a}", true),
            row("(?iu)[^A-Z]", "\u{17f}", false),
            row("(?iu)[^a-z]", "\u{212a}", false),
            row("(?iu)[^A-K]", "\u{212a}", true),
            row("(?iu)[^A-K]", "k", false),
            row("(?iu)[\\x{E0}-\\x{FE}]", "\u{c9}", true),
            row("(?iu)[\\x{E0}-\\x{FE}]", "\u{178}", false),
            row("(?iu)[\\x{C0}-\\x{DE}]", "\u{ff}", false),
            row("(?iu)[\\x{C0}-\\x{DE}]", "\u{178}", false),
            row("(?iu)[\\x{391}-\\x{3A9}]", "\u{3c3}", true),
            row("(?iu)[\\x{391}-\\x{3A9}]", "\u{3c2}", true),
            row("(?iu)[\\x{3B1}-\\x{3C9}]", "\u{3a3}", true),
            row("(?iu)[\\x{410}-\\x{42F}]", "\u{44f}", true),
            row("(?iu)[\\x{410}-\\x{42F}]", "\u{436}", true),
            row("(?iu)[\\x{DF}-\\x{DF}]", "\u{1e9e}", true),
            row("(?iu)[\\x{1E9E}-\\x{1E9E}]", "\u{df}", false),
            // a single member: Latin-1 members go in a bit class, the other members compare keys
            row("(?iu)[k]", "\u{212a}", true),
            row("(?iu)[K]", "\u{212a}", true),
            row("(?iu)[s]", "\u{17f}", true),
            row("(?iu)[S]", "\u{17f}", true),
            row("(?iu)[i]", "\u{130}", true),
            row("(?iu)[i]", "\u{131}", true),
            row("(?iu)[I]", "\u{130}", true),
            row("(?iu)[I]", "\u{131}", true),
            row("(?iu)[\\x{E5}]", "\u{212b}", true),
            row("(?iu)[\\x{C5}]", "\u{212b}", true),
            row("(?iu)[\\x{FF}]", "\u{178}", true),
            row("(?iu)[\\x{B5}]", "\u{3bc}", true),
            row("(?iu)[\\x{B5}]", "\u{39c}", true),
            row("(?iu)[\\x{3BC}]", "\u{b5}", true),
            row("(?iu)[\\x{E9}]", "\u{c9}", true),
            row("(?iu)[\\x{C9}]", "\u{e9}", true),
            row("(?iu)[\\x{DF}]", "\u{1e9e}", false),
            row("(?iu)[\\x{1E9E}]", "\u{df}", true),
            row("(?iu)[\\x{3C3}]", "\u{3c2}", true),
            row("(?iu)[\\x{3C2}]", "\u{3a3}", true),
            row("(?iu)[\\x{3A3}]", "\u{3c2}", true),
            row("(?iu)[\\x{1C6}]", "\u{1c5}", true),
            row("(?iu)[\\x{1C5}]", "\u{1c4}", true),
            row("(?iu)[\\x{1F88}]", "\u{1f80}", true),
            row("(?iu)[\\x{1F80}]", "\u{1f88}", true),
            row("(?iu)[\\x{10428}]", "\u{10400}", true),
            row("(?iu)[\\x{10400}-\\x{1044F}]", "\u{10428}", true),
            row("(?iu)[z]", "Z", true),
            row("(?iu)[1]", "1", true),
            // with && and a nested class
            row("(?iu)[a-z&&[^k]]", "\u{212a}", false),
            row("(?iu)[a-z&&[^k]]", "K", false),
            row("(?iu)[a-z&&[^k]]", "j", true),
            row("(?iu)[a-z&&k]", "\u{212a}", true),
            row("(?iu)[a-z&&k]", "K", true),
            row("(?iu)[a-c[x-z]]", "X", true),
            row("(?iu)[a-c[^x-z]]", "Y", false),
            row("(?iu)[a-c[^x-z]]", "B", true),
            row("(?iu)[^a-z&&[^k]]", "k", true),
            row("(?iu)[^a-z&&[^k]]", "\u{212a}", true),
            row("(?iu)[a&&-1]", "-", false),
            row("(?iu)[a&&-1]", "a", false),
            row("(?iu)[a-c&&b-d]", "B", true),
            row("(?iu)[a-c&&b-d]", "A", false),
            // under (?iU) the classes are Unicode and the members fold the same
            row("(?iU)[\\w]", "\u{17f}", true),
            row("(?iU)[\\w]", "\u{e9}", true),
            row("(?iU)[a-z]", "\u{212a}", true),
            row("(?iU)[A-Z]", "\u{212a}", false),
            row("(?iU)[\\p{Lower}]", "A", true),
            row("(?iU)[\\p{Upper}]", "a", true),
            row("(?iU)[\\p{Lower}]", "\u{1c5}", true),
            row("(?iU)[\\p{Lu}]", "a", true),
            row("(?iU)[\\p{Lu}]", "1", false),
            // only the ASCII fold, which does not reach these
            row("(?i)[a-z]", "\u{212a}", false),
            row("(?i)[a-z]", "\u{17f}", false),
            row("(?i)[A-K]", "k", true),
            row("(?i)[k]", "\u{212a}", false),
            row("(?i)[\\w]", "\u{17f}", false),
            row("(?u)[a-z]", "A", false),
            row("(?u)[k]", "\u{212a}", false),
            row("[a-z]", "K", false),
            // the fold ends where the flag does
            row("(?iu)(?-i)[a-z]", "A", false),
            row("(?iu)(?-i)[a-z]", "\u{212a}", false),
            row("(?iu)(?-u)[a-z]", "A", true),
            row("(?iu)(?-u)[a-z]", "\u{212a}", false),
            row("(?iu)(?-u)[k]", "\u{212a}", false),
            row("(?iu:[a-z])A", "AA", true),
            row("(?iu:[a-z])A", "Aa", false),
            row("(?iu:[a-z])[A-Z]", "aa", false),
            row("(?iu:[a-z])[A-Z]", "\u{212a}a", false),
        ];
        check_matches(cases, true);
    }

    /// The surrogates are not chars, so a range that has them in the middle, or at an end, holds
    /// the chars on either side, and a negated one leaves out the chars on either side. Each row
    /// is `Pattern.matches` on JDK 17.
    #[test]
    fn matches_reads_a_class_across_the_surrogates_as_java_does() {
        // (pattern, input, whole input matches)
        let cases = [
            row("(?iu)[^\\x{3c2}-\\x{10975}]", "\u{e000}", false),
            row("(?iu)[^\\x{3c2}-\\x{10975}]", "\u{d7ff}", false),
            row("(?iu)[^\\x{3c2}-\\x{10975}]", "\u{e001}", false),
            row("(?iu)[^\\x{3c2}-\\x{10975}]", "\u{10976}", true),
            row("(?iu)[^\\x{3c2}-\\x{10975}]", "a", true),
            row("(?iu)[\\x{D7FF}-\\x{E000}]", "\u{d7ff}", true),
            row("(?iu)[\\x{D7FF}-\\x{E000}]", "\u{e000}", true),
            row("(?iu)[\\x{D7FF}-\\x{E000}]", "\u{e001}", false),
            row("(?iu)[^\\x{D7FF}-\\x{E000}]", "\u{d7ff}", false),
            row("(?iu)[^\\x{D7FF}-\\x{E000}]", "\u{e000}", false),
            row("(?iu)[^\\x{D7FF}-\\x{E000}]", "\u{d7fe}", true),
            row("(?iu)[^\\x{D7FF}-\\x{E000}]", "\u{e001}", true),
            row("(?iu)[^\\x{D800}-\\x{E001}]", "\u{e000}", false),
            row("(?iu)[^\\x{D800}-\\x{E001}]", "\u{e002}", true),
            row("(?iu)[^\\x{41}-\\x{DBFF}]", "\u{d7ff}", false),
            row("(?iu)[^\\x{41}-\\x{DBFF}]", "\u{e000}", true),
            row("(?iu)[\\x{D800}-\\x{DFFF}]", "a", false),
            row("(?iu)[^\\x{D800}-\\x{DFFF}]", "a", true),
            row("(?iu)[^\\x{D800}-\\x{DFFF}]", "\u{e000}", true),
            row("(?iu)[a\\x{D800}-\\x{DFFF}]", "a", true),
            row("(?iu)[a\\x{D800}-\\x{DFFF}]", "\u{d7ff}", false),
            row("(?iu)[^a\\x{D800}-\\x{DFFF}]", "a", false),
            row("(?iu)[^a\\x{D800}-\\x{DFFF}]", "b", true),
        ];
        check_matches(cases, true);
    }

    /// The POSIX classes are ASCII unless `(?U)` is on, when they are the Unicode properties, and
    /// no case flag folds them or the other properties, except that `Lower`, `Upper`, `Lu`, `Ll`
    /// and `Lt` take in every cased letter under `(?i)`. `fancy_regex` reads the names as Unicode
    /// and folds a property under its `i`. Each row is `Pattern.matches` on JDK 17.
    #[test]
    fn matches_reads_the_posix_classes_and_properties_as_java_does() {
        // (pattern, input, whole input matches)
        let cases = [
            // the POSIX classes are ASCII without (?U), where fancy_regex reads them as Unicode
            row("\\p{Alpha}", "a", true),
            row("\\p{Alpha}", "Z", true),
            row("\\p{Alpha}", "\u{e9}", false),
            row("\\p{Alpha}", "\u{3b1}", false),
            row("\\p{Alpha}", "1", false),
            row("\\p{Alpha}", "_", false),
            row("\\P{Alpha}", "\u{e9}", true),
            row("\\P{Alpha}", "a", false),
            row("\\p{Alnum}", "5", true),
            row("\\p{Alnum}", "q", true),
            row("\\p{Alnum}", "_", false),
            row("\\p{Alnum}", "\u{e9}", false),
            row("\\p{Alnum}", "\u{663}", false),
            row("\\p{Digit}", "7", true),
            row("\\p{Digit}", "\u{663}", false),
            row("\\p{Digit}", "\u{ff17}", false),
            row("\\p{Punct}", "!", true),
            row("\\p{Punct}", "$", true),
            row("\\p{Punct}", "_", true),
            row("\\p{Punct}", "~", true),
            row("\\p{Punct}", "`", true),
            row("\\p{Punct}", "@", true),
            row("\\p{Punct}", "a", false),
            row("\\p{Punct}", "\u{a1}", false),
            row("\\p{Punct}", "\u{2014}", false),
            row("\\P{Punct}", "\u{a1}", true),
            row("\\P{Punct}", "!", false),
            row("\\p{ASCII}", "a", true),
            row("\\p{ASCII}", "\u{7f}", true),
            row("\\p{ASCII}", "\u{0}", true),
            row("\\p{ASCII}", "\u{80}", false),
            row("\\p{ASCII}", "\u{e9}", false),
            row("\\P{ASCII}", "\u{e9}", true),
            row("\\p{XDigit}", "f", true),
            row("\\p{XDigit}", "F", true),
            row("\\p{XDigit}", "9", true),
            row("\\p{XDigit}", "g", false),
            row("\\p{XDigit}", "\u{ff11}", false),
            row("\\p{Blank}", " ", true),
            row("\\p{Blank}", "\t", true),
            row("\\p{Blank}", "\n", false),
            row("\\p{Blank}", "\u{a0}", false),
            row("\\p{Cntrl}", "\u{1f}", true),
            row("\\p{Cntrl}", "\u{7f}", true),
            row("\\p{Cntrl}", "\u{0}", true),
            row("\\p{Cntrl}", " ", false),
            row("\\p{Cntrl}", "\u{80}", false),
            row("\\p{Graph}", "!", true),
            row("\\p{Graph}", "~", true),
            row("\\p{Graph}", " ", false),
            row("\\p{Graph}", "\u{e9}", false),
            row("\\p{Print}", " ", true),
            row("\\p{Print}", "~", true),
            row("\\p{Print}", "\u{7f}", false),
            row("\\p{Print}", "\u{e9}", false),
            row("\\p{Space}", " ", true),
            row("\\p{Space}", "\u{b}", true),
            row("\\p{Space}", "\u{a0}", false),
            row("\\p{Space}", "\u{2003}", false),
            row("[\\p{Alpha}]", "\u{e9}", false),
            row("[\\p{Alpha}]", "b", true),
            row("[\\p{Alpha}\\d]", "7", true),
            row("[\\p{Alnum}_]", "_", true),
            row("[\\p{Alnum}_]", "\u{e9}", false),
            row("[\\p{Punct}]", "\u{a1}", false),
            row("[\\p{Punct}]", ";", true),
            row("[^\\p{Punct}]", ";", false),
            row("[^\\p{Punct}]", "\u{a1}", true),
            row("[\\P{Punct}]", ";", false),
            row("[\\p{ASCII}]", "\u{e9}", false),
            row("[\\p{ASCII}-]", "-", true),
            row("[\\p{XDigit}]", "\u{ff11}", false),
            row("[\\p{Blank}]", "\u{a0}", false),
            row("[\\p{Space}x]", "\u{a0}", false),
            row("[\\p{Cntrl}]", "\u{80}", false),
            row("[\\p{Graph}]", "\u{e9}", false),
            row("[\\p{Print}]", "\u{e9}", false),
            row("[\\p{Alpha}&&[^a]]", "a", false),
            row("[\\p{Alpha}&&[^a]]", "b", true),
            // the case flags do not fold them, and under (?i) Lower and Upper are the ASCII letters of both cases
            row("(?i)\\p{Alpha}", "\u{17f}", false),
            row("(?i)\\p{Alpha}", "\u{212a}", false),
            row("(?iu)\\p{Alpha}", "\u{17f}", false),
            row("(?iu)\\p{Alpha}", "\u{212a}", false),
            row("(?iu)\\p{ASCII}", "\u{17f}", false),
            row("(?iu)\\p{ASCII}", "\u{212a}", false),
            row("(?iu)\\p{ASCII}", "K", true),
            row("(?iu)\\p{Punct}", "\u{212a}", false),
            row("(?iu)\\p{Digit}", "1", true),
            row("(?iu)[\\p{Alnum}]", "\u{17f}", false),
            row("(?iu)[\\p{Alnum}]", "\u{212a}", false),
            row("(?iu)[\\p{ASCII}]", "\u{212a}", false),
            row("(?iu)[\\p{XDigit}]", "\u{212a}", false),
            row("(?iu)[\\p{Punct}k]", "\u{212a}", true),
            row("(?i)\\p{Lower}", "A", true),
            row("(?i)\\p{Upper}", "a", true),
            row("(?i)\\p{Lower}", "\u{e9}", false),
            row("(?iu)\\p{Lower}", "\u{17f}", false),
            row("(?iu)\\p{Upper}", "\u{212a}", false),
            // the properties that are not the ASCII ones are not folded either
            row("(?iu)\\p{IsGreek}", "\u{345}", false),
            row("(?iu)\\p{IsLatin}", "\u{212a}", true),
            row("(?iu)\\p{IsLatin}", "\u{17f}", true),
            row("(?iu)\\p{L}", "\u{17f}", true),
            row("(?iu)\\p{Nd}", "\u{663}", true),
            row("(?iu)[\\p{IsGreek}]", "\u{345}", false),
            row("(?iu)\\p{Lu}", "a", true),
            row("(?iu)\\p{Lu}", "\u{df}", true),
            row("(?iu)\\p{Lu}", "\u{138}", true),
            row("(?iu)\\p{Lu}", "1", false),
            row("(?iu)\\p{Ll}", "A", true),
            row("(?iu)\\p{Lt}", "a", true),
            row("(?iu)\\P{Lu}", "a", false),
            row("(?iu)\\P{Lu}", "1", true),
            row("(?iu)[\\p{Lu}]", "\u{df}", true),
            row("(?iu)[\\p{Lu}]", "\u{138}", true),
            row("(?iu)[^\\p{Lu}]", "\u{df}", false),
            row("(?iu)[^\\p{Lu}]", "1", true),
            row("(?iu)\\p{IsLowercase}", "A", true),
            row("(?iu)\\p{IsUppercase}", "a", true),
            row("(?iu)\\p{IsLowercase}", "\u{2102}", true),
            row("(?iu)\\p{gc=Lu}", "\u{138}", true),
            row("(?iu)\\p{IsLu}", "\u{138}", true),
            row("(?iu)\\p{Lower}", "A", true),
            // under (?U) the POSIX classes are the Unicode properties
            row("(?U)\\p{Alpha}", "\u{e9}", true),
            row("(?U)\\p{Alpha}", "1", false),
            row("(?U)\\p{Alnum}", "\u{e9}", true),
            row("(?U)\\p{Alnum}", "\u{663}", true),
            row("(?U)\\p{Alnum}", "_", false),
            row("(?U)\\p{Digit}", "\u{663}", true),
            row("(?U)\\p{Digit}", "a", false),
            row("(?U)\\p{Punct}", "\u{a1}", true),
            row("(?U)\\p{Punct}", "$", false),
            row("(?U)\\p{Punct}", "!", true),
            row("(?U)\\p{Space}", "\u{a0}", true),
            row("(?U)\\p{Space}", "\u{85}", true),
            row("(?U)\\p{Space}", "\u{2028}", true),
            row("(?U)\\p{Space}", "a", false),
            row("(?U)\\p{XDigit}", "\u{ff11}", true),
            row("(?U)\\p{XDigit}", "\u{663}", true),
            row("(?U)\\p{XDigit}", "g", false),
            row("(?U)\\p{Blank}", "\u{a0}", true),
            row("(?U)\\p{Blank}", "\t", true),
            row("(?U)\\p{Blank}", "\n", false),
            row("(?U)\\p{Cntrl}", "\u{80}", true),
            row("(?U)\\p{Cntrl}", " ", false),
            row("(?U)\\p{Graph}", "\u{e9}", true),
            row("(?U)\\p{Graph}", " ", false),
            row("(?U)\\p{Graph}", "\u{a0}", false),
            row("(?U)\\p{Graph}", "\u{2028}", false),
            row("(?U)\\p{Print}", "\u{e9}", true),
            row("(?U)\\p{Print}", " ", true),
            row("(?U)\\p{Print}", "\u{a0}", true),
            row("(?U)\\p{Print}", "\n", false),
            row("(?U)\\p{Print}", "\u{2028}", false),
            row("(?U)\\p{Lower}", "\u{e9}", true),
            row("(?U)\\p{Lower}", "A", false),
            row("(?U)\\p{Upper}", "\u{c9}", true),
            row("(?U)\\p{alpha}", "\u{e9}", true),
            row("(?U)\\P{Graph}", " ", true),
            row("(?U)\\P{Graph}", "a", false),
            row("(?U)[\\p{Alpha}]", "\u{e9}", true),
            row("(?U)[\\p{Graph}]", "\u{e9}", true),
            row("(?U)[\\P{Graph}x]", " ", true),
            row("(?U)[\\P{Graph}x]", "x", true),
            row("(?U)[\\P{Graph}x]", "a", false),
            row("(?iU)\\p{Lower}", "A", true),
            row("(?iU)\\p{Lower}", "\u{1c5}", true),
            row("(?iU)\\p{Upper}", "a", true),
            row("(?iU)\\p{Lower}", "\u{2102}", true),
            row("(?iU)\\p{Alpha}", "\u{212a}", true),
            row("(?iU)\\p{Punct}", "\u{a1}", true),
            row("(?iU)\\p{Graph}", "\u{212a}", true),
        ];
        check_matches(cases, true);
    }

    /// Under `(?iu)` a literal matches the code points with its key, `Character.toLowerCase` of
    /// `Character.toUpperCase`, which is not the simple case folding that `fancy_regex`'s `i`
    /// compares: `İ` and `ı` match `i`. Each row is `Pattern.matches` on JDK 17.
    #[test]
    fn matches_folds_unicode_case_of_a_literal_as_java_does() {
        // (pattern, input, whole input matches)
        let cases = [
            // a literal compares Character.toLowerCase(Character.toUpperCase(ch)), not the simple case folding of fancy_regex
            row("(?iu)i", "\u{130}", true),
            row("(?iu)i", "\u{131}", true),
            row("(?iu)I", "\u{130}", true),
            row("(?iu)I", "\u{131}", true),
            row("(?iu)\u{130}", "i", true),
            row("(?iu)\u{130}", "\u{131}", true),
            row("(?iu)\u{131}", "I", true),
            row("(?iu)\u{131}", "\u{130}", true),
            row("(?iu)i", "I", true),
            row("(?iu)id", "\u{130}D", true),
            row("(?iu)k", "\u{212a}", true),
            row("(?iu)K", "\u{212a}", true),
            row("(?iu)\u{212a}", "k", true),
            row("(?iu)s", "\u{17f}", true),
            row("(?iu)\u{17f}", "S", true),
            row("(?iu)\u{e5}", "\u{212b}", true),
            row("(?iu)\u{212b}", "\u{c5}", true),
            row("(?iu)\u{3c3}", "\u{3c2}", true),
            row("(?iu)\u{3c2}", "\u{3a3}", true),
            row("(?iu)\u{3a3}", "\u{3c3}", true),
            row("(?iu)\u{b5}", "\u{39c}", true),
            row("(?iu)\u{b5}", "\u{3bc}", true),
            row("(?iu)\u{1c5}", "\u{1c6}", true),
            row("(?iu)\u{1f80}", "\u{1f88}", true),
            row("(?iu)\u{1f88}", "\u{1f80}", true),
            row("(?iu)\u{1f80}", "\u{1f80}", true),
            row("(?iu)\u{ff}", "\u{178}", true),
            row("(?iu)\u{10428}", "\u{10400}", true),
            row("(?iu)\u{e9}", "\u{c9}", true),
            row("(?iu)\u{436}", "\u{416}", true),
            row("(?iu)1", "1", true),
            row("(?iu)ss", "\u{df}", false),
            row("(?iu)SS", "\u{1e9e}", false),
            row("(?iu)\u{df}", "\u{df}", true),
            // with the fold off, in ASCII, or ended, the letters stay apart
            row("(?i)i", "\u{130}", false),
            row("(?i)i", "\u{131}", false),
            row("(?i)k", "\u{212a}", false),
            row("(?u)i", "I", false),
            row("(?u)i", "\u{130}", false),
            row("(?iu)(?-i)i", "I", false),
            row("(?iu)(?-i)i", "\u{130}", false),
            row("(?iu)(?-u)i", "\u{130}", false),
            row("(?iu)(?-u)i", "I", true),
            row("(?iu:i)", "\u{130}", true),
            row("(?iu:i)i", "\u{130}i", true),
            row("(?iu:i)i", "\u{130}I", false),
        ];
        check_matches(cases, true);
    }

    /// Java reads a literal in a run of two or more as a slice, which also matches `ẞ` for `ß`,
    /// and a lone literal, or one that a quantifier follows, on its own. Each row is
    /// `Pattern.matches` on JDK 17.
    #[test]
    fn matches_reads_a_run_of_literals_as_java_does() {
        // (pattern, input, whole input matches)
        let cases = [
            // a lone ß matches only itself, and one in a run of two or more also matches ẞ
            row("(?iu)\u{df}", "\u{1e9e}", false),
            row("(?iu)\u{df}", "\u{df}", true),
            row("(?iu)\u{df}\u{df}", "\u{1e9e}\u{1e9e}", true),
            row("(?iu)\u{df}a", "\u{1e9e}a", true),
            row("(?iu)a\u{df}", "a\u{1e9e}", true),
            row("(?iu)\u{df}*", "\u{1e9e}", false),
            row("(?iu)\u{df}a*", "\u{1e9e}", false),
            row("(?iu)\u{df}a*", "\u{1e9e}a", false),
            row("(?iu)\u{df}ab*", "\u{1e9e}ab", true),
            row("(?iu)\u{df}ab*", "\u{1e9e}a", true),
            row("(?iu)a\u{df}*", "a\u{1e9e}", false),
            row("(?iu)a\u{df}*", "a", true),
            row("(?iu)a{2}\u{df}", "aa\u{1e9e}", false),
            row("(?iu)a{2}\u{df}\u{df}", "aa\u{1e9e}\u{1e9e}", true),
            row("(?iu)(\u{df})", "\u{1e9e}", false),
            row("(?iu)(\u{df}\u{df})", "\u{1e9e}\u{1e9e}", true),
            row("(?iu)\u{df}|\u{df}", "\u{1e9e}", false),
            row("(?iu)\u{df}.", "\u{1e9e}x", false),
            row("(?iu)\u{df}\\.", "\u{1e9e}.", true),
            row("(?iu)\u{df}\\w", "\u{1e9e}x", false),
            row("(?iu)\u{df}{1,2}", "\u{1e9e}", false),
            row("(?iu)\u{df}\u{df}?", "\u{1e9e}", false),
            row("(?iu)\u{df}\u{df}?", "\u{1e9e}\u{1e9e}", false),
            row("(?iu)x\u{df}", "x\u{1e9e}", true),
            row("(?iu)\u{df}+x", "\u{1e9e}x", false),
            row("(?iu)\u{df}x", "\u{1e9e}x", true),
            row("(?iu)(?:\u{df})\u{df}", "\u{1e9e}\u{1e9e}", false),
            row("(?iu)(?:\u{df})", "\u{1e9e}", false),
            row("(?iu)\u{df}$", "\u{1e9e}", false),
            row("(?iu)^\u{df}", "\u{1e9e}", false),
            row("(?iu)\u{df}[a]", "\u{1e9e}a", false),
            row("(?iu)\u{df}\\x{DF}", "\u{1e9e}\u{1e9e}", true),
            row("(?iu)\\x{DF}", "\u{1e9e}", false),
            row("(?iu)\\Q\u{df}\\E", "\u{1e9e}", false),
            row("(?iu)\\Q\u{df}\u{df}\\E", "\u{1e9e}\u{1e9e}", true),
            row("(?iux)\u{df} \u{df}", "\u{1e9e}\u{1e9e}", true),
            row("(?iux)\u{df} #c\n\u{df}", "\u{1e9e}\u{1e9e}", true),
            row("(?iux)\u{df} #c\n", "\u{1e9e}", false),
            row("(?iu)\u{1e9e}", "\u{df}", true),
            row("(?iu)\u{1e9e}\u{1e9e}", "\u{df}\u{df}", true),
        ];
        check_matches(cases, true);
    }

    /// The flag `x` ends with the group it is set in, and the whitespace and the `#` after that
    /// are text. Each row is `Pattern.matches` on JDK 17.
    #[test]
    fn matches_ends_flag_x_with_its_group_as_java_does() {
        // (pattern, input, whole input matches)
        let cases = [
            // the flag x ends with the group it is set in
            row("(?u)(?<n>.(?x))A(?i)I ", "&A\u{131} ", true),
            row("(?u)(?<n>.(?x))A(?i)I ", "&A\u{131}", false),
            row("(?u)(?<n>.(?x))A(?i)I", "&A\u{131}", true),
            row("(?u)(.(?x))A#", "xA#", true),
            row("(?u)(.(?x))A#", "xA", false),
            row("(?x)(.(?x))A#c\nB", "xAB", true),
            row("(?x:a )b", "ab", true),
            row("(?x:a )b", "a b", false),
            row("(?x:a)#b", "a#b", true),
            row("(?x:a)#b", "a", false),
        ];
        check_matches(cases, true);
    }

    /// A `^` that Java reads as a class member stays one under `(?x)`, where
    /// the white space and comments between it and the `[` are dropped, and a
    /// `-` that does not join two members is the character, whatever member
    /// comes before it, `\w` and `\s` included. Each row is `Pattern.matches`
    /// on JDK 17.
    #[test]
    fn matches_reads_a_literal_caret_and_dash_in_a_class_as_java_does() {
        // (pattern, input, whole input matches)
        let cases = [
            // `^` is a negation only right after the `[`
            row("(?x)[ ^a]", "^", true),
            row("(?x)[ ^a]", "a", true),
            row("(?x)[ ^a]", "b", false),
            row("(?ix)[ ^a]", "^", true),
            row("(?ix)[ ^a]", "A", true),
            row("(?ix)[ ^a]", "b", false),
            row("(?ix)[#c\n^a]", "^", true),
            row("(?ix)[#c\n^a]", "A", true),
            row("(?ix)[#c\n^a]", "b", false),
            row("(?x)[ ^-b]", "^", true),
            row("(?x)[ ^-b]", "a", true),
            row("(?x)[ ^-b]", "c", false),
            row("(?x)[a[ ^b]]", "^", true),
            row("(?x)[a[ ^b]]", "b", true),
            row("(?x)[a[ ^b]]", "c", false),
            row("(?x)[^ a]", "a", false),
            row("(?x)[^ a]", "b", true),
            row("(?x)[^ ^a]", "^", false),
            row("(?x)[^ ^a]", "b", true),
            row("[\\[^a]", "^", true),
            row("[\\[^a]", "b", false),
            // outside a class it is an anchor, after a `\[` too
            row("\\[^a", "[a", false),
            row("\\[^a", "[^a", false),
            row("(?x)\\[ ^a", "[^a", false),
            // a `-` that is not a range operator, after `\w`, `\s`, a folded
            // letter, a property, and a range
            row("[\\w-.]", "-", true),
            row("[\\w-.]", ".", true),
            row("[\\w-.]", "a", true),
            row("[\\w-.]", ",", false),
            row("[\\w-a]", "-", true),
            row("[\\w-a]", "a", true),
            row("[\\w-a]", "`", false),
            row("[\\s-.]", "-", true),
            row("[\\s-.]", ".", true),
            row("[\\s-.]", " ", true),
            row("[\\s-.]", ",", false),
            row("(?i)[a-[b]]", "-", true),
            row("(?i)[a-[b]]", "A", true),
            row("(?i)[a-[b]]", "B", true),
            row("(?i)[a-[b]]", "X", false),
            row("(?i)[a-[b]]", "[", false),
            row("[\\p{Lower}-z]", "-", true),
            row("[\\p{Lower}-z]", ".", false),
            row("[\\p{L}-z]", "-", true),
            row("[\\p{L}-z]", "\u{e9}", true),
            row("[\\p{L}-z]", ".", false),
            row("[\\w-\\d]", "-", true),
            row("[\\d-a]", "-", true),
            row("[\\d-a]", "a", true),
            row("[\\d-a]", "b", false),
            row("[a-b-c]", "-", true),
            row("[a-b-c]", "c", true),
            row("[a-b-c]", "d", false),
            row("(?i)[a-b-c]", "-", true),
            row("(?i)[a-b-c]", "C", true),
            row("(?i)[a-b-c]", "D", false),
            row("[a-c-e]", "-", true),
            row("[a-c-e]", "b", true),
            row("[a-c-e]", "d", false),
            row("[a-c-e]", "e", true),
            // first, last and negated
            row("[-a]", "-", true),
            row("[^-a]", "-", false),
            row("[^-a]", "b", true),
            row("[a-]", "-", true),
            // a `-` as either end of a range
            row("[+--]", ",", true),
            row("[+--]", "-", true),
            row("[+--]", ".", false),
            row("(?i)[+--]", "-", true),
            row("[--/]", ".", true),
            row("[--/]", "0", false),
            row("[!--]", ",", true),
            row("[!--]", ".", false),
            // outside a class it is a plain character
            row("a-b", "a-b", true),
            row("(?i)a-b", "A-B", true),
        ];
        check_matches(cases, true);
    }

    /// Under `(?x)` Java tells a `-` that ends a class from one that starts a
    /// range by the char right after it, without skipping white space or a
    /// comment: `(?x)[+- ]]` is the range from `+` to the first `]`, then the
    /// class ends at the second. Each row is `Pattern.matches` on JDK 17.
    #[test]
    fn matches_reads_a_range_that_ends_past_x_flag_white_space_as_java_does() {
        // (pattern, input, whole input matches)
        let cases = [
            row("(?x)[+- ]]", ",", true),
            row("(?x)[+- ]]", "a", false),
            row("(?x)[+- ]]", "]", true),
            row("(?x)[+- ]]", "-", true),
            row("(?x)[+- [b]]", "X]", true),
            row("(?x)[+- [b]]", "b]", true),
            row("(?x)[+- [b]]", "c]", false),
            row("(?x)[+- [b]]", "-]", true),
            row("(?x)[+-#c\n]]", "a", false),
            row("(?x)[+-#c\n]]", ",", true),
            row("(?x)[+-#c\n]]", "]", true),
            row("(?x)[a-b- ]", "-", true),
            row("(?x)[a-b- ]", "b", true),
            row("(?x)[a-b- ]", "c", false),
            row("(?x)[a - c]", "b", true),
            row("(?x)[a - c]", "-", false),
            row("(?x)[a - c]", " ", false),
            row("(?x)[a-\n b]", "a", true),
            row("(?x)[a-\n b]", "b", true),
            row("(?x)[a-\n b]", "-", false),
            row("(?x)[a-#c\nb]", "b", true),
            row("(?x)[a-#c\nb]", "-", false),
            row("(?x)[a-]", "-", true),
            row("(?x)[a-[b]]", "-", true),
            row("(?x)[a-[b]]", "b", true),
            row("(?x)[a-[b]]", "c", false),
        ];
        check_matches(cases, true);
    }

    /// The range ends at the `]` or the `[` past the white space, which Java
    /// refuses as a reversed range or an unclosed class.
    #[yare::parameterized(
        range_ends_at_space = { "(?x)[.- ]" },
        range_ends_at_space_after_letter = { "(?x)[a- ]" },
        range_ends_at_newline = { "(?x)[a-\n]" },
        range_ends_at_newline_after_dot = { "(?x)[.-\n]" },
        nested_class_range_ends_at_space = { "(?x)[_- [b]]" },
        comment_then_range_ends_at_close = { "(?x)[a-#c\n]]" },
    )]
    fn extended_class_ranges_ending_after_whitespace_are_refused(source: &str) {
        check!(JavaPattern::compile(source).is_err(), "{source:?}");
    }

    /// A `)` with no `(` before it is `Unmatched closing ')'` in Java, which
    /// the `\A(?:...)\z` wrapper would otherwise read as the end of a group,
    /// and a range that ends in `\w`, `\s` or `\p{..}` is `Illegal character
    /// range` there. Each row is refused by `Pattern.compile` on JDK 17.
    #[yare::parameterized(
        alternation_with_stray_close = { "a)|(b" },
        simple_stray_close = { "a)" },
        double_stray_close = { "(a))" },
        flag_group_then_stray_close = { "a(?i))" },
        lone_close = { ")" },
        extended_mode_stray_close = { "(?x)a )" },
        noncapturing_group_then_stray_close = { "(?:a))" },
        lookahead_then_stray_close = { "(?=a))" },
        scoped_flag_group_then_stray_close = { "(?i:a))" },
        reversed_character_range = { "[a--b]" },
        range_to_word = { "[a-\\w]" },
        range_to_digit = { "[a-\\d]" },
        range_to_digit_from_bang = { "[!-\\d]" },
        range_to_word_from_plus = { "[+-\\w]" },
        range_to_unicode_property = { "[a-\\p{L}]" },
        range_to_space = { "[a-\\s]" },
        range_to_nonword = { "[!-\\W]" },
        range_from_hyphen_to_word = { "[--\\w]" },
        case_insensitive_range_to_word = { "(?i)[a-\\w]" },
        extended_mode_range_to_word = { "(?x)[a- \\w]" },
    )]
    fn a_stray_closing_paren_or_a_range_to_a_shorthand_does_not_compile(source: &str) {
        check!(JavaPattern::compile(source).is_err(), "{source:?}");
    }

    /// A `)` that Java reads as a member, or as part of a comment, is not
    /// stray. Each row is `Pattern.matches` on JDK 17.
    #[yare::parameterized(
        close_inside_class = { "[)]", ")", true },
        escaped_close = { "\\)", ")", true },
        quoted_close = { "\\Q)\\E", ")", true },
        close_inside_comment = { "(?x)a #c)\n", "a", true },
        close_in_alternative = { "(a)|(b)", "b", true },
        close_in_case_insensitive_alternative = { "(?i)(a)|b", "B", true },
    )]
    fn a_closing_paren_that_closes_no_group_is_still_a_character_where_java_reads_one(
        source: &str,
        input: &str,
        expected: bool,
    ) {
        assert_pattern_matches(source, input, expected);
    }

    /// A backreference under a case flag compares the text of its group as
    /// Java's `CIBackRef` does: under `(?i)` by `ASCII.toLower`, so a
    /// non-ASCII letter does not match its other case, and under `(?iu)` by
    /// `Character.toLowerCase` of `Character.toUpperCase`, so `i` matches `İ`
    /// and `ı`, and `s` matches `ſ`, whatever their length in bytes. Each row is
    /// `Pattern.matches` on JDK 25.0.4.
    #[test]
    fn a_backreference_under_a_case_flag_compares_as_java_does() {
        // (pattern, input, whole input matches)
        let cases = [
            // (?i) folds ASCII letters only
            row("(?i)(\u{e9})\\1", "\u{e9}\u{c9}", false),
            row("(?i)(\u{e9})\\1", "\u{e9}\u{e9}", true),
            row("(?i)(\u{3c3})\\1", "\u{3c3}\u{3c2}", false),
            row("(?i)(a\u{e9})\\1", "a\u{e9}A\u{e9}", true),
            row("(?i)(a\u{e9})\\1", "A\u{e9}a\u{c9}", false),
            row("(?i)(k)\\1", "k\u{212a}", false),
            row("(?i)(s)\\1", "s\u{17f}", false),
            row("(?i)(i)\\1", "i\u{130}", false),
            row("(?i)(ab)\\1", "abAB", true),
            row("(?i)(\\w+)-\\1", "Ab-aB", true),
            row("(?i)(?<n>\u{e9})\\k<n>", "\u{e9}\u{c9}", false),
            // (?iu) compares Character.toLowerCase(Character.toUpperCase(ch))
            row("(?iu)(i)\\1", "i\u{130}", true),
            row("(?iu)(i)\\1", "i\u{131}", true),
            row("(?iu)(\u{130})\\1", "\u{130}i", true),
            row("(?iu)(\u{130})\\1", "\u{130}I", true),
            row("(?iu)(\u{131})\\1", "\u{131}I", true),
            row("(?iu)(\u{130})\\1", "\u{130}\u{131}", true),
            row("(?iu)(s)\\1", "s\u{17f}", true),
            row("(?iu)(S)\\1", "S\u{17f}", true),
            row("(?iu)(k)\\1", "k\u{212a}", true),
            row("(?iu)(\u{df})\\1", "\u{df}\u{1e9e}", true),
            row("(?iu)(\u{e5})\\1", "\u{e5}\u{212b}", true),
            row("(?iu)(a)\\1", "aA", true),
            row("(?iu)(a)\\1", "ab", false),
            row("(?iu)(\u{e9})\\1", "\u{e9}\u{c9}", true),
            row("(?iu)(\u{3c3})\\1", "\u{3c3}\u{3c2}", true),
            row("(?iu)(\u{3c3})\\1", "\u{3c3}\u{3a3}", true),
            row("(?iu)(\u{1c6})\\1", "\u{1c6}\u{1c5}", true),
            row("(?iu)(\u{3b8})\\1", "\u{3b8}\u{3d1}", true),
            row("(?iu)(\u{3c9})\\1", "\u{3c9}\u{2126}", true),
            row("(?iu)(\u{b5})\\1", "\u{b5}\u{3bc}", true),
            row("(?iu)(\\p{L}+)\\1", "\u{e9}\u{c9}", true),
            row("(?iu)(?<n>i)\\k<n>", "i\u{130}", true),
            row("(?iU)(s)\\1", "s\u{17f}", true),
            // one char for one char: no full case mapping
            row("(?iu)(ss)\\1", "ss\u{df}", false),
            row("(?iu)(\u{df})\\1", "\u{df}ss", false),
            row("(?iu)(\\w+)\\1", "sS", true),
            row("(?iu)(.+)\\1", "stra\u{df}eSTRA\u{1e9e}E", true),
            // the flags in force at the reference decide, not those at the group
            row("(?u)(?i:(s))\\1", "s\u{17f}", false),
            row("(?i)(?u:(s)\\1)", "s\u{17f}", true),
            row("(?iu)(s)(?-u)\\1", "s\u{17f}", false),
            row("(?iu)(s)(?-i)\\1", "s\u{17f}", false),
            // one code point for one outside the BMP too, where JDK 17 counted
            // UTF-16 units and threw past the input
            row("(?i)(.)\\1", "\u{10400}\u{10428}", false),
            row("(?iu)(.)\\1", "\u{10400}\u{10428}", true),
            row("(?iu)(.)\\1", "\u{10400}\u{10400}", true),
            row("(?iu)(.)\\1x", "\u{10400}\u{10428}x", true),
            row("(?iu)(.)\\1.", "\u{10400}\u{10400}\u{10400}", true),
            row("(?iu)(.)\\1.", "\u{10400}\u{10400}b", true),
            row("(?iu)(.)(.)\\1", "\u{10400}\u{10400}\u{10400}", true),
            row("(?iu)(.)\\1\\1", "\u{10400}\u{10400}\u{10400}", true),
            row("(?iu)(.)\\1", "\u{10400}a", false),
            row("(?iu)(a)\\1", "a\u{10400}", false),
            row("(?iu)(.\u{10428})\\1", "a\u{10400}A\u{10400}", true),
        ];
        check_matches(cases, true);
    }

    /// `replaceAll` and `replaceFirst` find the matches of a pattern with a
    /// backreference under a case flag as Java does too.
    #[test]
    fn replace_compares_a_backreference_under_a_case_flag_as_java_does() {
        // (pattern, input, replacement, replaceAll result)
        let cases = [
            replace_row(
                "(?i)(\\w)\\1",
                "aAbB\u{e9}\u{c9}",
                "<$1>",
                "<a><b>\u{e9}\u{c9}",
            ),
            replace_row("(?iu)(\\w)\\1", "sS", "<$1>", "<s>"),
            replace_row("(?iu)(.)\\1", "i\u{130}s\u{17f}", "<$1>", "<i><s>"),
            replace_row(
                "(?iu)CN=(\\w+),OU=\\1",
                "CN=kim,OU=KIM",
                "$0",
                "CN=kim,OU=KIM",
            ),
            replace_row("(?iu)CN=(.+),OU=\\1", "CN=\u{131},OU=I", "$1", "\u{131}"),
            replace_row(
                "(?i)CN=(.+),OU=\\1",
                "CN=\u{131},OU=I",
                "$1",
                "CN=\u{131},OU=I",
            ),
            replace_row("(?i)\\G(a)\\1", "aAaAbaA", "x", "xxbaA"),
        ];
        check_replacements(cases, true, true);
    }

    /// The matcher for a backreference under a case flag reads the rest of the
    /// pattern as `fancy_regex` does: look-around, atomic groups, possessive
    /// and lazy repeats, alternation and anchors. Each row is
    /// `Pattern.matches` on JDK 17.0.20.
    #[test]
    fn a_pattern_with_a_folded_backreference_reads_the_rest_as_java_does() {
        // (pattern, input, whole input matches)
        let cases = [
            row("(?i)(a|ab)(c|bcd)\\1(d*)", "abcdAd", true),
            row("(?i)(a|ab)(c|bcd)\\1(d*)", "abcdAbd", false),
            row("(?i)(a+?)\\1", "aaAA", true),
            row("(?i)(a+)a\\1", "aaA", true),
            row("(?i)(a++)a\\1", "aaA", false),
            row("(?i)(?>(a+))a\\1", "aaA", false),
            row("(?i)(a)(?=b)\\w\\1", "abA", true),
            row("(?i)(a)(?!b)\\w\\1", "abA", false),
            row("(?i)(a)\\w(?<=b)\\1", "abA", true),
            row("(?i)(a)\\w(?<!b)\\1", "abA", false),
            row("(?i)(a){2,3}\\1", "aaA", true),
            row("(?i)(a){2,3}\\1", "aA", false),
            row("(?i)(a){2,3}\\1", "aaaaA", false),
            row("(?i)(a|)*\\1", "aa", true),
            // an empty iteration counts toward the minimum of a repeat
            row("(?i)((?=())|(?=())){2}\\2\\3", "", true),
            row("(?i)((?=())|(?=())){2}\\2\\3", "x", false),
            row("(?i)(a?){3}\\1", "aA", true),
            row("(?i)(a|()){2}\\2b", "ab", true),
            row("(?i)(?:(a)|b?){2}\\1", "bA", false),
            row("(?i)(x)?\\1", "", false),
            row("(?i)^(a)\\1$", "aA", true),
            row("(?i)(a)\\1\\z", "aA", true),
            row("(?i)(?m)(a)$\\n^\\1", "a\nA", true),
            row("(?i)(?s)(.)\\1", "\n\n", true),
            row("(?i)(.)\\1", "\n\n", false),
            row("(?iU)(\\w+)\\b\\1", "ab", false),
            row("(?iU)(\\w)\\B\\1", "aA", true),
            row("(?i)\\G(a)\\1", "aA", true),
            row("(?i)(a)\\R\\1", "a\r\nA", true),
            row("(?i)(a)\\R\\1", "a\rA", true),
            row("(?i)(a)\\R\\1", "a\n\nA", false),
            row("(?i)(a)\\R\\1", "a\u{2028}A", true),
        ];
        check_matches(cases, true);
    }

    /// The matcher keeps its alternatives on a stack of its own, so a long
    /// input does not overflow the thread's, and it gives up at the backtrack
    /// limit as `fancy_regex` does.
    #[test]
    fn a_pattern_with_a_folded_backreference_takes_a_long_input_and_gives_up_at_the_limit() {
        let long = format!("{}A", "a".repeat(200_000));
        check!(pattern("(?i)(a)(a*)\\1").matches(&long).ok() == Some(true));
        check!(pattern("(?i)(a*)*b\\1").matches(&"a".repeat(64)).is_err());
    }

    /// A group that matched inside a look-around, an atomic group or a
    /// possessive quantifier keeps its text when the match backtracks out of
    /// it, as Java's does, and so does one inside a negated look-around whose
    /// condition matched. A group outside them does not. Java sets a group
    /// when it ends, so a reference inside it sees its text from before. Each
    /// row is `Pattern.matches` on JDK 17.0.20.
    #[test]
    fn a_group_keeps_its_text_where_java_keeps_it() {
        // (pattern, input, whole input matches)
        let cases = [
            row("(?:(?=(a))ax|a)\\1", "aa", true),
            row("(?:(?!(a))|a)\\1", "aa", true),
            row("a(?:(?<=(a))x|)\\1", "aa", true),
            row("a(?:(?<!(a))|)\\1", "aa", true),
            row("(?:(?>(a))x|a)\\1", "aa", true),
            row("(?:(a)++x|a)\\1", "aa", true),
            row("(?:(a)?+x|a)\\1", "aa", true),
            row("(?:(?:(a))*+x|a)\\1", "aa", true),
            row("(?:(a){1,2}+x|a)\\1", "aa", true),
            row("(?:(a)x|a)\\1", "aa", false),
            row("(?:(?=(a))ax|a)\\1", "a", false),
            row("((?=())|(?=()))*\\2\\3", "", true),
            row("((?=())|(?=())){1,2}\\2\\3", "", true),
            row("(a|b\\1)+", "aba", true),
            row("(a|b\\1)+", "abb", false),
            row("(?i)(a|b\\1)+", "abA", true),
            row("(a\\1?)+", "aaa", true),
            row("(a\\1?){2}", "aaa", true),
        ];
        check_matches(cases, true);
    }

    /// `replaceAll` sees a group that kept its text, and `Matcher.find` clears
    /// the groups once for all the start positions it tries, so a group that
    /// kept its text at one start is still set at the next. Each row is
    /// `String.replaceAll` on JDK 17.0.20.
    #[test]
    fn replace_sees_a_group_that_kept_its_text_as_java_does() {
        // (pattern, input, replacement, replaceAll result)
        let cases = [
            replace_row("(?:(?=(b))bx|b)", "b", "[$1]", "[b]"),
            replace_row("(?:(?=(b))bx|b)", "ab", "[$1]", "a[b]"),
            replace_row("(?=(a)c)|b", "ab", "[$1]", "a[]"),
            replace_row("(?:(?=(a))ax|b)", "ab", "[$1]", "a[a]"),
            replace_row("(?:(?!(a))|b)", "ab", "[$1]", "a[a]b[]"),
            replace_row("\\R", "a\r\nb", "|", "a|b"),
            replace_row("\\R\\n", "a\r\nb", "|", "a|b"),
        ];
        check_replacements(cases, true, true);
    }

    /// `\R` is `\r\n` or one line terminator, and gives back the `\n` of
    /// `\r\n` when the rest of the pattern needs it, as Java's `LineEnding`
    /// does. Under a quantifier of `\R`, or of a group with no alternation and
    /// no range of counts, Java takes the first way only, but a `?` of a group
    /// such as `(?:\R)` backtracks into it. Each row is
    /// `Pattern.matches` on JDK 17.0.20.
    #[test]
    fn a_linebreak_gives_back_its_newline_where_java_does() {
        // (pattern, input, whole input matches)
        let cases = [
            row("(a)\\R\\n\\1", "a\r\na", true),
            row("\\R", "\r", true),
            row("\\R\\R", "\r\n", true),
            row("\\R", "\r\n", true),
            row("\\R", "\n\r", false),
            row("\\R", "\u{2028}", true),
            row("\\R", "\u{85}", true),
            row("(?-u)\\R", "\u{2028}", true),
            row("\\R\\n", "\r\n", true),
            row("(?>\\R)\\n", "\r\n", false),
            row("\\R{2}", "\r\n", false),
            row("\\R?\\n", "\r\n", false),
            row("x\\R+\\n", "x\r\n\r\n", false),
            row("(\\R)?\\n", "\r\n", true),
            row("(?:a\\R){2}\\n", "a\r\na\r\n", false),
            row("(?:a\\R|b){2}\\n", "a\r\na\r\n", true),
            row("(?=\\R\\n)\\R", "\r\n", true),
            row("(?:\\R)?\\n", "\r\n", true),
            row("(?:\\R)??\\n", "\r\n", true),
            row("(?:\\R){0,1}\\n", "\r\n", true),
            row("(?:(?:\\R))?\\n", "\r\n", true),
            row("(?x)(?:\\R) ?\\n", "\r\n", true),
            row("(?x)\\R ?\\n", "\r\n", false),
            row("(?x)\\R #c\n?\\n", "\r\n", false),
            row("(?:\\R)?+\\n", "\r\n", false),
            row("(?:\\R)*\\n", "\r\n", false),
            row("(?:\\R)+\\n", "\r\n", false),
            row("(?:\\R?)?\\n", "\r\n", false),
            row("a(?:\\R)?\\n\\R?", "a\r\n\r\n", true),
        ];
        check_matches(cases, true);
    }

    /// `$` and `\Z` match at the end, or before a line terminator that ends
    /// the input, and under `(?m)` `$` matches before any line terminator and
    /// `^` after one, as Java's `Dollar` and `Caret` do: `\r\n` is one line
    /// terminator, as are `\r`, U+0085, U+2028 and U+2029, and under `(?d)`
    /// only `\n` is. `(?m)^` never matches at the end of the input. A
    /// quantified anchor is the anchor, or nothing where the quantifier may
    /// match zero times. Each row is `Pattern.matches` on JDK 17.0.20.
    #[test]
    fn anchors_match_at_line_terminators_as_java_does() {
        // (pattern, input, whole input matches)
        let cases = [
            row("a$\\n", "a\n", true),
            row("a$\\r\\n", "a\r\n", true),
            row("a\\r$\\n", "a\r\n", false),
            row("a$\u{2028}", "a\u{2028}", true),
            row("a$\\u0085", "a\u{85}", true),
            row("a$\\r", "a\r", true),
            row("a$\\nb", "a\nb", false),
            row("a\\Z\\n", "a\n", true),
            row("a\\Z\\n\\n", "a\n\n", false),
            row("a$\\n\\n", "a\n\n", false),
            row("(?d)a$\\r", "a\r", false),
            row("(?d)a$\\n", "a\n", true),
            row("(?d)a\\Z\\n", "a\n", true),
            row("(?m)a$\\r\\nb", "a\r\nb", true),
            row("(?m)a\\r$\\nb", "a\r\nb", false),
            row("(?m)a$\u{2028}b", "a\u{2028}b", true),
            row("(?m)a$\\rb", "a\rb", true),
            row("(?md)a$\\rb", "a\rb", false),
            row("(?md)a$\\nb", "a\nb", true),
            row("(?m)a\\n^b", "a\nb", true),
            row("(?m)a\\r^b", "a\rb", true),
            row("(?m)a\\r^\\nb", "a\r\nb", false),
            row("(?m)a\\n^", "a\n", false),
            row("(?m)a\\u0085^b", "a\u{85}b", true),
            row("(?md)a\\r^b", "a\rb", false),
            row("(?md)a\\n^b", "a\nb", true),
            row("(?m)^", "", false),
            row("^", "", true),
            row("$", "", true),
            row("(?m)$", "", true),
            row("a$?b", "ab", true),
            row("(?m:a$)\\n^b", "a\nb", false),
            row("(?m:x)a$\\nb", "a\nb", false),
            row("a$*b", "ab", true),
            row("a$+b", "ab", false),
            row("a$+", "a", true),
            row("a\\Z?b", "ab", true),
            row("(?m)a\\n^?b", "a\nb", true),
            row("(?m)a^+b", "ab", false),
            row("(?m)a\\n^+b", "a\nb", true),
            row("(?x)a$ ?b", "ab", true),
        ];
        check_matches(cases, true);
    }

    /// `replaceAll` finds the anchors where Java finds them. Each row is
    /// `String.replaceAll` on JDK 17.0.20.
    #[test]
    fn replace_finds_anchors_at_line_terminators_as_java_does() {
        // (pattern, input, replacement, replaceAll result)
        let cases = [
            replace_row("a$", "a\n", "X", "X\n"),
            replace_row("$", "a\n", "|", "a|\n|"),
            replace_row("(?m)$", "a\r\nb", "|", "a|\r\nb|"),
            replace_row("(?m)^", "a\r\nb", "|", "|a\r\n|b"),
            replace_row("(?m)^", "a\n", "|", "|a\n"),
            replace_row("\\Z", "a\r\n", "|", "a|\r\n|"),
            replace_row("\\h+", "a\u{a0}\u{3000}b", "_", "a_b"),
        ];
        check_replacements(cases, true, true);
    }

    /// `\h` and `\v` are Java's horizontal and vertical whitespace, and `\H`
    /// and `\V` their negations, with or without `(?U)`. In a class `\v` is
    /// the char `\x0B` where a `-` follows it, as in Java. Each row is
    /// `Pattern.matches` on JDK 17.0.20.
    #[test]
    fn horizontal_and_vertical_whitespace_are_javas() {
        // (pattern, input, whole input matches)
        let cases = [
            row("\\h", "\u{a0}", true),
            row("\\h", "\u{3000}", true),
            row("\\h", "\u{2001}", true),
            row("\\h", "\n", false),
            row("\\H", "a", true),
            row("\\H", "\t", false),
            row("[\\h]", "\u{2000}", true),
            row("[\\H]", "\u{2000}", false),
            row("[\\h-a]", "-", true),
            row("(?U)\\h", "\u{a0}", true),
            row("\\v", "\n", true),
            row("\\v", "\u{2028}", true),
            row("\\v", "\u{c}", true),
            row("\\V", "\n", false),
            row("\\V", "a", true),
            row("[\\v]", "\r", true),
            row("[\\v-\\x0d]", "\u{c}", true),
            row("[\\v-]", "-", true),
            row("[\\v-]", "\n", false),
            row("[^\\h]", "a", true),
        ];
        check_matches(cases, true);
    }

    /// Java reads `(?i)*` as a dangling quantifier: inline flags are no node,
    /// so nothing precedes the `*`.
    #[yare::parameterized(
        star_after_unicode_flags = { "(?iu)*" },
        plus_after_case_flag = { "(?i)+a" },
        optional_after_dotall_flag = { "(?d)?" },
        star_after_extended_flags = { "(?ix) *" },
    )]
    fn a_star_plus_or_question_mark_after_inline_flags_is_refused(source: &str) {
        check!(JavaPattern::compile(source).is_err(), "{source:?}");
    }

    /// `\N{name}` is a character in Java, which `fancy_regex` reads as `\N` and
    /// the text of the name, so a rule that has one is refused and not misread.
    #[yare::parameterized(
        named_character = { "\\N{LATIN SMALL LETTER A}" },
        case_insensitive_named_character = { "(?i)\\N{LATIN SMALL LETTER A}" },
    )]
    fn a_named_character_is_refused(source: &str) {
        check!(JavaPattern::compile(source).is_err(), "{source:?}");
    }

    /// `Matcher.appendReplacement` semantics for `replaceAll`.
    #[test]
    fn replace_all_expands_a_replacement_as_java_does() {
        // (pattern, input, replacement, result)
        let cases = [
            // `$1_$2` is group 1, an underscore and group 2, not a group named `1_`.
            replace_row("(a)-(b)", "a-b", "$1_$2", "a_b"),
            replace_row("(a)", "a", "$1suffix", "asuffix"),
            // Digits after `$n` join the group number only while it names a group.
            replace_row("(a)", "a", "$10", "a0"),
            replace_row("(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)", "abcdefghij", "$10", "j"),
            replace_row("(a)", "a", "$0!", "a!"),
            replace_row("(?<who>a)", "a", "${who}-$1", "a-a"),
            // A backslash makes the next character literal.
            replace_row("(a)", "a", "\\$1", "$1"),
            replace_row("a", "aa", "b", "bb"),
            // A group that took no part in the match adds nothing.
            replace_row("(a)|(b)", "b", "[$1]", "[]"),
            replace_row("x", "abc", "$9", "abc"),
        ];
        check_replacements(cases, true, true);
    }

    /// The replacements Java throws on.
    #[yare::parameterized(
        group_number_out_of_range = { "(a)", "a", "$2" },
        group_number_without_groups = { "a", "a", "$1" },
        trailing_dollar = { "(a)", "a", "$" },
        nonnumeric_group_reference = { "(a)", "a", "$x" },
        trailing_backslash = { "(a)", "a", "\\" },
        unknown_named_group = { "(a)", "a", "${who}" },
        empty_named_group = { "(a)", "a", "${}" },
        unclosed_named_group = { "(a)", "a", "${who" },
        group_number_on_the_matcher_alone = { "(?<=\\R?x)y", "xy", "$1" },
        group_number_past_a_large_repeat = { "(?<n>x){1,1000000000}", "x", "$2" },
        unknown_named_group_past_a_large_repeat = { "(?<n>x){1,1000000000}", "x", "${m}" },
    )]
    fn replace_all_refuses_what_java_throws_on(source: &str, input: &str, replacement: &str) {
        check!(
            pattern(source).replace(input, replacement, true).is_err(),
            "{source} on {input} with {replacement}"
        );
    }

    #[test]
    fn replace_first_stops_after_one_match() {
        let result = pattern("a").replace("aaa", "b", false);
        assert!(result.ok().as_deref() == Some("baa"));
    }

    #[yare::parameterized(
        python_named_group = { "(?P<x>a)" },
        unclosed_character_class = { "[a-" },
        unclosed_group = { "(a" },
    )]
    fn a_pattern_java_refuses_does_not_compile(source: &str) {
        check!(JavaPattern::compile(source).is_err(), "{source}");
    }

    /// What `fancy_regex` takes and Java's `Pattern.compile` refuses is refused:
    /// a group name already defined, a `\k<name>` of no named group before it,
    /// a quantifier after a quantifier and its `?` or `+`, a repeat range that
    /// is backwards or past `i32::MAX`, a boundary, anchor, `\R`, `\X` or
    /// reference in a class, a look-behind with no obvious maximum length, and
    /// a property name `Pattern.family` does not take. Each row is refused by
    /// `Pattern.compile` on JDK 25.0.4.
    #[test]
    fn a_pattern_java_refuses_where_fancy_regex_takes_it_does_not_compile() {
        let sources = [
            "(?<a>x)(?<a>y)",
            "(?<a>x)|(?<a>y)",
            "\\k<1>",
            "(x)\\k<1>",
            "\\k<a>(?<a>x)",
            "(?<a>x)\\k<b>",
            "(?<a>x)\\k<a",
            "(?<a>x)\\k<>",
            "(?<a>x)\\k",
            "(?<_a>x)",
            "(?<a_b>x)",
            "a*?+",
            "a**",
            "a*+*",
            "a*??",
            "a+?+",
            "a?*",
            "a??+",
            "a{2}++",
            "a{2}*",
            "a{2}?+",
            "(?x)a*? +",
            "(?x)a* * ",
            "a*(?i)+",
            "*a",
            "(*a)",
            "a|*",
            "x{2,1}",
            "x{3,2}",
            "x{,2}",
            "x{2147483648}",
            "x{1,2147483648}",
            "[\\R]",
            "[\\X]",
            "[\\b]",
            "[\\B]",
            "[\\A]",
            "[\\G]",
            "[\\z]",
            "[\\Z]",
            "[\\k<a>]",
            "[\\1]",
            "(a)(?<=\\1)",
            "(?<=(a)\\1)b",
            "(?<=(?:ab)*)c",
            "(?<=(ab)*)c",
            "(?<!(a)\\1)b",
            "\\p{IsHyphen}",
            "\\p{Greek}",
            "\\p{lu}",
            "\\p{LU}",
            "\\p{isLu}",
            "\\p{Islu}",
            "\\p{IsOldItalic}",
            "\\p{IsOld Italic}",
            "\\p{sc=OldItalic}",
            "\\p{gc=lu}",
            "\\p{gc=Uppercase_Letter}",
            "\\p{Uppercase_Letter}",
            "\\p{Letter}",
            "\\p{Alphabetic}",
            "\\p{IsWhite Space}",
            "\\p{White_Space}",
            "\\p{Emoji}",
            "\\p{Any}",
            "\\p{IsAny}",
            "\\p{Ascii}",
            "\\p{alpha}",
            "\\p{Hex_Digit}",
            "\\p{Ideographic}",
            "\\p{Han}",
            "\\p{Common}",
            "\\p{Decimal_Number}",
            "\\p{Math}",
            "\\p{IsMath}",
            "\\p{Dash}",
            "\\p{IsDash}",
            "\\p{Cased}",
            "\\p{IsCased}",
            "\\pl",
            "\\pX",
            "\\p{L&}",
            "\\p{IsL&}",
            "(?i)\\p{lu}",
            "(?<=a{0,1073741824}b{0,1073741824})c",
            "(?<=(?:ab){0,1073741824})c",
            "(?<=(?:a|b){1,2})c",
            "(?<=(a|b)*)c",
            "(?<=\\R*)c",
            "(?<=\\R{0,1073741824})c",
            "(?<=(?>ab)*)c",
        ];
        for source in sources {
            check!(JavaPattern::compile(source).is_err(), "{source:?}");
        }
    }

    /// The patterns next to those Java refuses, which it takes, still compile
    /// and match as Java matches them: a second group of another name, a
    /// reference to a group opened before it, one `?` or `+` after a
    /// quantifier, a zero-width node under a quantifier, `\h`, `\v` and a
    /// property in a class, a look-behind whose maximum Java's study finds,
    /// and the property names `Pattern.family` takes in each spelling it
    /// takes. Each row is `Pattern.matches` on JDK 25.0.4.
    #[test]
    fn a_pattern_next_to_one_java_refuses_compiles_and_matches_as_java_does() {
        // (pattern, input, whole input matches)
        let cases = [
            row("(?<a>x)(?<b>y)", "xy", true),
            row("(?<ab>x)(?<a>y)", "xy", true),
            row("(?<a1>x)\\k<a1>", "xx", true),
            row("(?<a>\\k<a>x)", "x", false),
            row("(?<a>x)\\k<a>?", "x", true),
            row("a{2}+", "aa", true),
            row("a{2}{3}", "aaaaaa", false),
            row("(?x)a* +", "aa", true),
            row("^*", "", true),
            row("$*", "", true),
            row("\\b*", "", true),
            row("x{1,1}", "x", true),
            row("x{0,0}", "", true),
            row("[\\h]", " ", true),
            row("[\\v]", "\u{b}", true),
            row("[\\p{L}]", "a", true),
            row("(?<=a*)b", "b", true),
            row("(?<=a+)b", "ab", false),
            row("(?<=a{2,})b", "aab", false),
            row("(?<=a{0,1000})b", "b", true),
            row("(?<=.*)b", "b", true),
            row("(?<=a|bc)d", "bcd", false),
            row("(?<=(?:ab){1,2})c", "abc", false),
            row("(?<=\\b)a", "a", true),
            row("(?<=(?=a*)a)b", "ab", false),
            row("(?<=(?!a*)x)b", "xb", false),
            row("(?<!a*)b", "b", false),
            row("(?<=a*?)b", "b", true),
            row("(?<=a?)b", "b", true),
            row("(?<=\\w+)b", "ab", false),
            row("(?<=\\Ga*)b", "b", true),
            row("\\p{Lu}", "A", true),
            row("\\p{IsLu}", "A", true),
            row("\\p{IsLatin}", "a", true),
            row("\\p{Islatin}", "a", true),
            row("\\p{IsLATIN}", "a", true),
            row("\\p{IsLatn}", "a", true),
            row("\\p{Isold_italic}", "a", false),
            row("\\p{IsOld_Italic}", "\u{10300}", true),
            row("\\p{sc=Latin}", "a", true),
            row("\\p{SC=Latin}", "a", true),
            row("\\p{sc=latin}", "a", true),
            row("\\p{sc=Latn}", "a", true),
            row("\\p{Script=Latin}", "a", true),
            row("\\p{gc=Lu}", "A", true),
            row("\\p{general_category=Lu}", "A", true),
            row("\\p{gc=L}", "A", true),
            row("\\p{gc=LC}", "A", true),
            row("\\p{L}", "A", true),
            row("\\p{IsL}", "A", true),
            row("\\p{IsLetter}", "A", true),
            row("\\p{Isletter}", "A", true),
            row("\\p{IsAlphabetic}", "A", true),
            row("\\p{IsWhite_Space}", " ", true),
            row("\\p{IsWhiteSpace}", " ", true),
            row("\\p{IsEmoji}", "\u{1f600}", true),
            row("\\p{IsAssigned}", "a", true),
            row("\\p{ASCII}", "a", true),
            row("\\p{IsASCII}", "a", true),
            row("\\p{Alpha}", "a", true),
            row("(?U)\\p{alpha}", "a", true),
            row("(?U)\\p{ALPHA}", "a", true),
            row("\\p{IsAlpha}", "a", true),
            row("\\p{Isalpha}", "a", true),
            row("\\p{IsDigit}", "1", true),
            row("\\p{IsPunct}", "!", true),
            row("\\p{IsSpace}", " ", true),
            row("\\p{IsCntrl}", "\u{1}", true),
            row("\\p{IsLower}", "a", true),
            row("\\p{IsUpper}", "A", true),
            row("\\p{XDigit}", "F", true),
            row("\\p{IsHex_Digit}", "F", true),
            row("\\p{IsHexDigit}", "F", true),
            row("\\p{IsIdeographic}", "\u{4e00}", true),
            row("\\p{IsHan}", "\u{4e00}", true),
            row("\\p{IsHani}", "\u{4e00}", true),
            row("\\p{IsCommon}", "!", true),
            row("\\p{IsZyyy}", "!", true),
            row("\\p{IsInherited}", "\u{301}", true),
            row("\\p{Nd}", "1", true),
            row("\\p{IsNd}", "1", true),
            row("\\p{N}", "1", true),
            row("\\pL", "a", true),
            row("\\pN", "1", true),
            row("\\pZ", " ", true),
            row("\\pS", "+", true),
            row("\\pC", "\u{1}", true),
            row("\\pM", "\u{301}", true),
            row("\\pP", "!", true),
            row("\\p{Ll}", "a", true),
            row("\\p{Lo}", "\u{4e00}", true),
            row("\\p{Cn}", "\u{378}", true),
            row("\\p{Co}", "\u{e000}", true),
            row("\\p{Cs}", "a", false),
            row("\\p{Lm}", "\u{2b0}", true),
            row("(?<=ab*)c", "ac", false),
            row("(?<=a*b)c", "bc", false),
            row("(?<=(?:a){1,2})c", "ac", false),
            row("(?<=(?:a)*)c", "c", true),
            row("(?<=(a)*)c", "c", true),
            row("(?<=a|b*)c", "c", true),
            row("(?<=(?=b*)a*)c", "c", true),
            row("(?<=(?<=a*)b*)c", "c", true),
            row("(?<=[ab]*)c", "c", true),
            row("(?<=\\x{1F600}*)c", "c", true),
            row("(?<=(?i)a*)c", "c", true),
            row("(?<=(?iu)\\u00df*)c", "c", true),
            row("(?<=a*?b)c", "bc", false),
            row("(?<=a??b*)c", "c", true),
            row("(?<=a?b?)c", "c", true),
            row("(?<=(ab)?)c", "c", true),
        ];
        check_matches(cases, true);
    }

    /// A class with `&&` is the set Java's `Pattern.clazz` builds: an empty
    /// side of `&&` takes the operand before it, or leaves the side after it,
    /// `&&` binds to the right, and the single chars below U+0100 of a class
    /// are one set that a char after the intersection still joins, so
    /// `[a&&&b]` matches `a`, `&` and `b`. Each row is `Pattern.matches` on
    /// JDK 25.0.4.
    #[test]
    fn a_class_intersection_with_an_empty_side_is_the_set_java_builds() {
        // (pattern, input, whole input matches)
        let cases = [
            row("[a-z&&]", "a", true),
            row("[a-z&&]", "A", false),
            row("[&&a]", "a", true),
            row("[&&a]", "b", false),
            row("[a&&&b]", "a", true),
            row("[a&&&b]", "b", true),
            row("[a&&&b]", "&", true),
            row("[a&&]", "a", true),
            row("[a&&]", "b", false),
            row("[^a&&]", "a", false),
            row("[^a&&]", "b", true),
            row("[^&&a]", "b", true),
            row("[a&&&&b]", "a", false),
            row("[a&&[b]&&]", "b", false),
            row("[a&&[b]&&]", "a", false),
            row("[[a]&&]", "a", true),
            row("[a&& ]", "a", false),
            row("[a&& ]", " ", false),
            row("(?x)[a&& ]", "a", true),
            row("(?x)[a&& b]", "a", false),
            row("[a-c&&&&b]", "b", true),
            row("[a-c&&&&b]", "a", false),
            row("[a&&\\w]", "a", true),
            row("[\\w&&]", "a", true),
            row("[&&[a]]", "a", true),
            row("[b&&[a]]", "a", false),
            row("[a[b]&&]", "b", true),
            row("[a[b]&&c]", "b", false),
            row("[a&&b[c]]", "c", false),
            row("[ab-c&&]", "a", false),
            row("[ab-c&&]", "b", true),
            row("[a&&[b]&c]", "a", true),
            row("[a&&[b]&c]", "b", false),
            row("[a&&[b]&c]", "c", true),
            row("[a&&[b]&c]", "&", true),
            row("[a&&b&&c]", "a", false),
            row("[a-c&&b-d&&c]", "c", true),
            row("[a-c&&b-d&&c]", "b", false),
            row("[^a-c&&b]", "b", false),
            row("[^a-c&&b]", "a", true),
            row("[^a-c&&]", "d", true),
            row("[^a-c&&]", "a", false),
            row("(?i)[a-c&&]", "B", true),
            row("(?i)[a&&]", "A", true),
            row("(?i)[A&&&b]", "B", true),
            row("(?iu)[k&&]", "\u{212a}", true),
            row("(?iu)[k&&&a]", "a", true),
            row("[\\w&&\\d]", "5", true),
            row("[\\w&&\\d]", "a", false),
            row("[\\d&&]", "5", true),
            row("[a\\d&&]", "a", false),
            row("[a\\d&&]", "5", true),
            row("[[a-c]&&]", "b", true),
            row("[x[a-c]&&]", "x", false),
            row("[x[a-c]&&]", "b", true),
            row("[a-z&&[^aeiou]]", "b", true),
            row("[a-z&&[^aeiou]]", "a", false),
            row("[a&&[b]c]", "c", false),
            row("[ab&&b]", "b", true),
            row("[ab&&b]", "a", false),
            row("[\u{100}&&]", "\u{100}", true),
            row("[a\u{100}&&]", "a", false),
            row("[a&&&&b]", "b", false),
            row("[a-c&&&&b]", "c", false),
            row("[&&a-c]", "b", true),
            row("[&&[a]&&]", "a", true),
            row("[a&&]]", "]", false),
            row("[]&&]]", "]", false),
            row("[]a&&]", "a", true),
        ];
        check_matches(cases, true);
    }

    /// Where a side of `&&` is empty and there is no operand to take its
    /// place, Java answers `Bad class syntax` or `Bad intersection syntax`.
    /// Each row is refused by `Pattern.compile` on JDK 25.0.4.
    #[yare::parameterized(
        both_sides_empty = { "[&&]" },
        both_sides_empty_then_ampersand = { "[&&&a]" },
        empty_side_after_a_single_char = { "[a-cd&&]" },
        empty_side_after_a_single_char_after_a_class = { "[[a-c]x&&]" },
        empty_nested_class = { "[a&&[]]" },
    )]
    fn a_class_intersection_java_refuses_does_not_compile(source: &str) {
        check!(JavaPattern::compile(source).is_err(), "{source:?}");
    }

    /// A repeat of a group with a group in it, whose last iteration matches
    /// nothing, keeps the groups that iteration set, as Java's `Loop` does,
    /// where `fancy_regex` takes no such iteration. Each row is
    /// `String.replaceAll` on JDK 25.0.4.
    #[test]
    fn replace_sees_the_groups_of_an_empty_last_iteration_as_java_does() {
        // (pattern, input, replacement, replaceAll result)
        let cases = [
            replace_row("(a*)*", "aa", "[$1]", "[][]"),
            replace_row("(a|)*", "aa", "[$1]", "[][]"),
            replace_row("(a*)+", "aa", "[$1]", "[][]"),
            replace_row("(a*){2,}", "aa", "[$1]", "[][]"),
            replace_row("(a*){1,3}", "aa", "[$1]", "[][]"),
            replace_row("(a*){0,5}", "aaa", "[$1]", "[][]"),
            replace_row("(a*)*?", "aa", "[$1]", "[]a[]a[]"),
            replace_row("(a*)*+", "aa", "[$1]", "[][]"),
            replace_row("(a*)+?b", "aab", "[$1]", "[aa]"),
            replace_row("(?:(a)|b|)*", "ab", "[$1]", "[a][]"),
            replace_row("(?:(a)|)*", "aa", "[$1]", "[a][]"),
            replace_row("(?:(a)?)*", "aa", "[$1]", "[a][]"),
            replace_row("(?:b|(a?))*", "ab", "[$1]", "[][]"),
            replace_row("((a)|)*", "aa", "[$1|$2]", "[|a][|]"),
            replace_row("(a?)*", "aa", "[$1]", "[][]"),
            replace_row("(a?)+", "a", "[$1]", "[][]"),
            replace_row("(a??)*", "aa", "[$1]", "[]a[]a[]"),
            replace_row("()*", "a", "[$1]", "[]a[]"),
            replace_row("(\\b)*", "a", "[$1]", "[]a[]"),
            replace_row("(a{0})*", "a", "[$1]", "[]a[]"),
            replace_row("(?:a(b*))*", "abab", "[$1]", "[b][]"),
            replace_row("(?:a(b*))*", "aba", "[$1]", "[][]"),
            replace_row("(?:(a*)b)*", "abb", "[$1]", "[][]"),
            replace_row("((a*)*)*", "aa", "[$1|$2]", "[|][|]"),
            replace_row("(a*)*x", "aax", "[$1]", "[]"),
            replace_row("(a|b*)*c", "abc", "[$1]", "[]"),
        ];
        check_replacements(cases, true, true);
    }

    /// A backreference sees the group an empty last iteration set. Each row is
    /// `Pattern.matches` on JDK 25.0.4.
    #[test]
    fn a_reference_sees_the_group_of_an_empty_last_iteration_as_java_does() {
        // (pattern, input, whole input matches)
        let cases = [
            row("(a*)*\\1", "aa", true),
            row("(a*)+\\1", "aa", true),
            row("(a|)*\\1", "aa", true),
            row("(?:(a)|)*\\1", "aa", true),
            row("(a*)*b\\1", "aab", true),
            row("(a*)*b\\1", "aabaa", true),
            row("(a|)+b\\1", "ab", true),
            row("(a|)+b\\1", "aba", true),
            row("()*+\\1", "", true),
            row("(){1,}\\1", "", true),
            row("()+\\1", "", true),
            row("(()|a)*\\2", "aa", true),
            row("(?:())*\\1", "", true),
            row("(())*\\2", "", true),
        ];
        check_matches(cases, true);
    }

    /// `\G` matches where the last match ended. After an empty match the next
    /// search starts one char on and `\G` stays behind it, so it matches no
    /// more, as Java's `LastMatch` does, in a look-around too. Each row is
    /// `String.replaceAll` on JDK 25.0.4.
    #[test]
    fn last_match_anchor_stays_where_the_last_match_ended() {
        // (pattern, input, replacement, replaceAll result)
        let cases = [
            replace_row("\\G", "aa", "-", "-aa"),
            replace_row("\\Ga", "aaba", "-", "--ba"),
            replace_row("\\G|a", "aa", "-", "-a--"),
            replace_row("\\G|b", "ab", "-", "-a--"),
            replace_row("a|\\G", "baa", "-", "-b---"),
            replace_row("(?<=\\G.)", "abc", "-", "a-b-c-"),
            replace_row("(?=\\G)", "aa", "-", "-aa"),
            replace_row("x*\\G", "aa", "-", "-aa"),
            replace_row("\\G(a|)", "aab", "[$1]", "[a][a][]b"),
            replace_row("a*?\\G", "aa", "-", "-aa"),
        ];
        check_replacements(cases, true, true);
    }

    /// A look-behind whose maximum length wraps in Java's `int`, and whose
    /// window then leaves out starts the body can match from, is refused. The
    /// comment on each row gives its `replaceAll("<$0>")` on JDK 25.0.4, and
    /// then what the look-behind read without the wrap gives.
    #[yare::parameterized(
        // `rmax` wraps to -2: `abc`, not `ab<c>`.
        two_greedy_repeats = { "(?<=a*b*)c" },
        // `ab<c>`, not `abc`.
        two_greedy_repeats_negated = { "(?<!a*b*)c" },
        // `ab`, not `<>a<>b<>`.
        two_greedy_repeats_alone = { "(?<=a*b*)" },
        // `rmax` wraps to 1, under `rmin` 3: `cdef`, not `cde<f>`.
        wrapped_below_the_minimum = { "(?<=a*b*cde)f" },
        // `rmax` wraps to 0: `cdf`, not `cd<f>`.
        wrapped_to_zero = { "(?<=a*b*cd)f" },
        // `rmax` wraps to -1: `cf`, not `c<f>`.
        wrapped_to_minus_one = { "(?<=a*b*c)f" },
        // `de`, not `d<e>`.
        four_greedy_repeats = { "(?<=a*b*c*d*)e" },
        // `1x`, not `1<x>`.
        two_greedy_classes = { "(?<=\\w*\\d*)x" },
        // `abcd`, not `abc<d>`.
        four_greedy_repeats_with_a_class = { "(?<=a*b*c*\\w*)d" },
        // `rmax` wraps to 0, under `rmin` 2: `1bcd`, not `1bc<d>`.
        wrapped_to_zero_below_the_minimum = { "(?<=a*\\d*bc)d" },
        // The branch's maximum is 2: `cdef`, not `cde<f>`.
        wrapped_branch = { "(?<=xy|a*b*cde)f" },
        // Java counts the window in code points: `bc😀d`, not `bc😀<d>`.
        supplementary_literal = { "(?<=a*bc😀)d" },
        // `abc😀<d>`, as without the wrap, but counted in code points.
        supplementary_literal_wrapped_to_a_large_maximum = { "(?<=a*b*c*😀)d" },
    )]
    fn a_look_behind_whose_maximum_wraps_is_refused_where_java_reads_it_otherwise(source: &str) {
        check!(
            JavaPattern::compile(source).err()
                == Some(PatternError(format!(
                    "{source:?} {}",
                    Refusal::MaximumOverflows.reason()
                )))
        );
    }

    /// A look-behind whose maximum length wraps in Java's `int` still compiles
    /// where Java's window holds every start the body can match from: the
    /// body matches the empty string anywhere, the wrapped maximum is at
    /// least 2^30, or it is negative and the body is longer than the last `i`
    /// with an empty window. Each row is `String.replaceAll` on JDK 25.0.4.
    #[test]
    fn a_look_behind_whose_maximum_wraps_matches_as_java_does() {
        // (pattern, input, replacement, replaceAll result)
        let cases = [
            // The body matches the empty string at `i`.
            replace_row("(?<=a*b*c*)d", "abcd", "<$0>", "abc<d>"),
            replace_row("(?<=a*b*c*)d", "xd", "<$0>", "x<d>"),
            replace_row("(?<!a*b*c*)d", "abcd", "<$0>", "abcd"),
            replace_row("(?<!a*b*c*)d", "d", "<$0>", "d"),
            replace_row("(?<=b|a*c*)d", "bd", "<$0>", "b<d>"),
            replace_row("(?<=x|a*b*)c", "c", "<$0>", "<c>"),
            replace_row("(?<=x|a*b*)c", "abc", "<$0>", "ab<c>"),
            // `rmax` wraps to 2147483646.
            replace_row("(?<=a*b*c*x)y", "abcxy", "<$0>", "abcx<y>"),
            replace_row("(?<=a*b*c*x)y", "xy", "<$0>", "x<y>"),
            replace_row("(?<=a*b*c*x)y", "y", "<$0>", "y"),
            replace_row("(?<=(?:a*b*c*)?x)y", "xy", "<$0>", "x<y>"),
            // An escape is not a supplementary char of the pattern, so Java
            // counts in UTF-16 units.
            replace_row("(?<=a*b*c*\\x{1F600})d", "abc😀d", "<$0>", "abc😀<d>"),
            // `rmax` wraps to `i32::MIN + 1`, so the window is empty only at
            // `i` 0, where `bc` cannot end.
            replace_row("(?<=a*bc)d", "bcd", "<$0>", "bc<d>"),
            replace_row("(?<=a*bc)d", "d", "<$0>", "d"),
            replace_row("(?<!a*bc)d", "d", "<$0>", "<d>"),
            replace_row("(?<!a*bc)d", "abcd", "<$0>", "abcd"),
            replace_row("(?<!a*bc)d", "bdd", "<$0>", "b<d><d>"),
            replace_row("(?<=a*bc\\x{1F600})d", "bc😀d", "<$0>", "bc😀<d>"),
            // `rmax` wraps to `i32::MIN`, so the window is empty only below
            // `i` 0.
            replace_row("(?<=a??b*)c", "c", "<$0>", "<c>"),
            // No wrap.
            replace_row("(?<=a*)c", "aac", "<$0>", "aa<c>"),
        ];
        check_replacements(cases, true, false);
    }

    /// A look-behind accepted with a wrapped maximum of at least 2^30 limits
    /// the input to that many UTF-16 units, the smallest such maximum where
    /// there are several.
    #[test]
    fn a_wrapped_look_behind_maximum_limits_the_input() {
        // (pattern, input limit)
        let cases = [
            ("(?<=a*b*c*x)y", Some(2_147_483_646)),
            ("(?<=(?:a*b*c*)?x)y", Some(2_147_483_646)),
            ("(?<=a*b*c*xy)z(?<=a*b*c*x)", Some(2_147_483_646)),
            // Four greedy repeats wrap to -4, and the counted repeat brings it
            // back to 1,099,999,996. The look-behind is too large for
            // `fancy_regex` too, and keeps its limit on the matcher.
            ("(?<=a*b*c*d*x{1,1100000000})y", Some(1_099_999_996)),
            ("(?<=a*b*c*)d", None),
            ("(?<=a*bc)d", None),
            ("(?<=a*)c", None),
        ];
        for (source, expected) in cases {
            check!(pattern(source).0.input_limit == expected, "{source}");
        }
    }

    /// An input of more UTF-16 units than the limit is refused. A char past
    /// U+FFFF is two units in four bytes.
    #[test]
    fn an_input_past_the_limit_is_refused() {
        // (input limit, input, refused)
        let cases = [
            (None, "abc", false),
            (Some(3), "abc", false),
            (Some(2), "abc", true),
            (Some(0), "", false),
            (Some(1), "é", false),
            (Some(2), "😀", false),
            (Some(1), "😀", true),
            (Some(3), "a😀", false),
            (Some(2), "a😀", true),
        ];
        for (limit, input, refused) in cases {
            check!(
                check_input_length(limit, input).is_err() == refused,
                "{limit:?} {input}"
            );
        }
    }

    /// A look-behind that is not of one length, with a node in it that
    /// `fancy_regex` runs itself, such as `\R`, a look-around, an atomic
    /// group, a possessive quantifier, `\G` or a group, is one `fancy_regex`
    /// cannot build. The matcher answers for it. Each row is
    /// `String.replaceAll` on JDK 25.0.4.
    #[test]
    fn a_look_behind_fancy_regex_cannot_build_matches_as_java_does() {
        // (pattern, input, replacement, replaceAll result)
        let cases = [
            replace_row("(?<=\\R?x)y", "xy", "<$0>", "x<y>"),
            replace_row("(?<=\\R?x)y", "\nxy", "<$0>", "\nx<y>"),
            replace_row("(?<=\\R?x)y", "\r\nxy", "<$0>", "\r\nx<y>"),
            replace_row("(?<=\\R?x)y", "y", "<$0>", "y"),
            replace_row("(?<=\\R?x)y", "zy", "<$0>", "zy"),
            replace_row("(?<!\\R?x)y", "xy", "<$0>", "xy"),
            replace_row("(?<!\\R?x)y", "\nxy", "<$0>", "\nxy"),
            replace_row("(?<!\\R?x)y", "zy", "<$0>", "z<y>"),
            replace_row("(?<!\\R?x)y", "y", "<$0>", "<y>"),
            replace_row("(?<=\\Rx|y)z", "\nxz", "<$0>", "\nx<z>"),
            replace_row("(?<=\\Rx|y)z", "\r\nxz", "<$0>", "\r\nx<z>"),
            replace_row("(?<=\\Rx|y)z", "yz", "<$0>", "y<z>"),
            replace_row("(?<=\\Rx|y)z", "xz", "<$0>", "xz"),
            replace_row("(?<=\\R{1,2})x", "\r\nx", "<$0>", "\r\n<x>"),
            replace_row("(?<=\\R{1,2})x", "\n\nx", "<$0>", "\n\n<x>"),
            replace_row("(?<=\\R{1,2})x", "x", "<$0>", "x"),
            replace_row("(?<=a\\R?)b", "a\r\nb", "<$0>", "a\r\n<b>"),
            replace_row("(?<=a\\R?)b", "b", "<$0>", "b"),
            replace_row("(?<=(?=a)ab?)c", "ac", "<$0>", "a<c>"),
            replace_row("(?<=(?=a)ab?)c", "abc", "<$0>", "ab<c>"),
            replace_row("(?<=(?=a)ab?)c", "bc", "<$0>", "bc"),
            replace_row("(?<!(?=a)ab?)c", "abc", "<$0>", "abc"),
            replace_row("(?<!(?=a)ab?)c", "bc", "<$0>", "b<c>"),
            replace_row("(?<=a(?!b)c?)d", "acd", "<$0>", "ac<d>"),
            replace_row("(?<=a(?!b)c?)d", "abd", "<$0>", "abd"),
            replace_row("(?<=(?<=a)b?)c", "abc", "<$0>", "ab<c>"),
            replace_row("(?<=(?<=a)b?)c", "bc", "<$0>", "bc"),
            // The atomic group keeps its first way, so `ab` leaves no `b`.
            replace_row("(?<=(?>a|ab)b?)c", "ac", "<$0>", "a<c>"),
            replace_row("(?<=(?>a|ab)b?)c", "abc", "<$0>", "ab<c>"),
            replace_row("(?<=(?>a|ab)b?)c", "abbc", "<$0>", "abbc"),
            replace_row("(?<=(?>ab|a)b?)c", "abbc", "<$0>", "abb<c>"),
            replace_row("(?<=a{1,3}+b?)c", "aabc", "<$0>", "aab<c>"),
            replace_row("(?<=a{1,3}+b?)c", "bc", "<$0>", "bc"),
            replace_row("(?<=\\Gab?)c", "acac", "<$0>", "a<c>a<c>"),
            // A group in the look-behind keeps its text.
            replace_row("(?<=(a)|bc)d", "ad", "[$1$0]", "a[ad]"),
            replace_row("(?<=(a)|bc)d", "bcd", "[$1$0]", "bc[d]"),
            replace_row("(?<=\\R?(x))(y)", "xy", "[$2$1]", "x[yx]"),
            replace_row("(?<=\\R?(?<n>x))y", "\nxy", "[${n}$0]", "\nx[xy]"),
        ];
        check_replacements(cases, true, true);
    }

    /// A counted repeat too large for the `regex` crate at its default size
    /// limit, in a look-behind or outside one, is one `fancy_regex` cannot
    /// build, and in a look-behind would build for minutes. The matcher
    /// counts the iterations instead. Each row is `Matcher.matches()` on
    /// JDK 25.0.4.
    #[test]
    fn a_counted_repeat_too_large_to_build_matches_as_java_does() {
        // (pattern, input, matches)
        let cases = [
            row("x{1,1000000000}", "xxx", true),
            row("x{1,1000000000}", "x", true),
            row("x{1,1000000000}", "", false),
            row("x{1,1000000000}", "y", false),
            row("x{1000000000}", "xxx", false),
            row("x{2,1000000000}y", "xxy", true),
            row("x{2,1000000000}y", "xy", false),
            row("(?:ab){1,600000000}", "abab", true),
            row("(?:ab){1,600000000}", "aba", false),
            row("(x){1,1000000000}", "xxx", true),
            row("(?i)x{1,1000000000}", "XXX", true),
        ];
        check_matches(cases, false);
    }

    /// A look-behind too large for `fancy_regex` to build, and a counted
    /// repeat too large outside one, match on the matcher. Each row is
    /// `String.replaceAll` on JDK 25.0.4.
    #[test]
    fn a_look_behind_too_large_to_build_matches_as_java_does() {
        // (pattern, input, replacement, replaceAll result)
        let cases = [
            replace_row("(?<=x{1,1000000000})y", "xy", "<$0>", "x<y>"),
            replace_row("(?<=x{1,1000000000})y", "xxy", "<$0>", "xx<y>"),
            replace_row("(?<=x{1,1000000000})y", "y", "<$0>", "y"),
            replace_row("(?<!x{1,1000000000})y", "xy", "<$0>", "xy"),
            replace_row("(?<!x{1,1000000000})y", "y", "<$0>", "<y>"),
            replace_row("(?<=(?:ab){1,600000000})y", "aby", "<$0>", "ab<y>"),
            replace_row("(?<=(?:ab){1,600000000})y", "by", "<$0>", "by"),
            replace_row("(?<=a{0,2147483647}b)c", "abc", "<$0>", "ab<c>"),
            replace_row("(?<=a{0,2147483647}b)c", "bc", "<$0>", "b<c>"),
            replace_row("(?<=a{0,2147483647}b)c", "c", "<$0>", "c"),
            replace_row("(?<=a{1,2147483647})b", "ab", "<$0>", "a<b>"),
            replace_row("(?<=a{1,2147483647})b", "b", "<$0>", "b"),
            replace_row("(?<=[ab]{1,1500000000})c", "abc", "<$0>", "ab<c>"),
            replace_row("(?<=[ab]{1,1500000000})c", "c", "<$0>", "c"),
            replace_row("(?<=z|x{1,1000000000})y", "xy", "<$0>", "x<y>"),
            replace_row("(?<=z|x{1,1000000000})y", "zy", "<$0>", "z<y>"),
            replace_row("(?<=z|x{1,1000000000})y", "y", "<$0>", "y"),
            replace_row("(?<=(?=x)x{1,1000000000})y", "xy", "<$0>", "x<y>"),
            replace_row("(?<=(?=x)x{1,1000000000})y", "y", "<$0>", "y"),
            replace_row("(?<=(?i)x{1,1000000000})y", "Xy", "<$0>", "X<y>"),
            replace_row("(?<=x{1,1000000000})(y)", "xy", "[$1]", "x[y]"),
            replace_row("(?:ab){1,600000000}", "aba", "<$0>", "<ab>a"),
            replace_row("(x){1,1000000000}", "xxx", "[$1]", "[x]"),
            replace_row("(?<n>x){1,1000000000}", "xxx", "[${n}]", "[x]"),
        ];
        check_replacements(cases, true, true);
    }

    /// A long input against a very large counted repeat ends in Java's
    /// answer or in a [`MatchError`] at the backtrack limit, never in
    /// another answer. Each `expected` is `Matcher.matches()` and
    /// `String.replaceAll` on JDK 25.0.4, with `x` 3,000 times as the input.
    #[test]
    fn a_long_input_against_a_large_repeat_gives_java_s_answer_or_an_error() {
        let xs = "x".repeat(3000);
        let xs_y = format!("{xs}y");
        // (pattern, input, matches, replaceAll result with `<$0>`)
        let cases = [
            ("x{1,1000000000}", xs.clone(), true, format!("<{xs}>")),
            ("x{1,1000000000}y", xs.clone(), false, xs.clone()),
            (
                "(?<=x{1,1000000000})y",
                xs_y.clone(),
                false,
                format!("{xs}<y>"),
            ),
            ("(?<!x{1,1000000000})y", xs_y.clone(), false, xs_y.clone()),
        ];
        for (source, input, matches, replaced) in cases {
            let compiled = pattern(source);
            check!(
                compiled.matches(&input).ok() != Some(!matches),
                "{source} matches"
            );
            check!(
                compiled
                    .replace(&input, "<$0>", true)
                    .map_or(true, |out| out == replaced),
                "{source} replaceAll"
            );
        }
    }

    /// A pattern `fancy_regex` refuses for a reason other than a size or a
    /// look-behind it cannot build stays refused, the matcher answering or
    /// not: here a backreference to a group the pattern does not have, which
    /// JDK 25.0.4 compiles, and which never matches there.
    #[yare::parameterized(
        alone = { "(a)\\2" },
        beside_a_look_behind_too_large_to_build = { "(?<=x{1,1000000000})y(a)\\2" },
        beside_a_look_behind_fancy_regex_cannot_build = { "(?<=\\R?x)y(a)\\2" },
        after_a_large_repeat = { "x{1,1000000000}(a)\\2" },
    )]
    fn a_backreference_to_a_missing_group_is_refused(source: &str) {
        check!(
            JavaPattern::compile(source).err()
                == Some(PatternError(
                    "Error compiling regex: Invalid back reference to group 2".to_owned()
                ))
        );
    }

    /// A large look-behind under the size limit still compiles. Each row is
    /// `String.replaceAll` on JDK 25.0.4.
    #[test]
    fn a_large_look_behind_under_the_size_limit_matches_as_java_does() {
        // (pattern, input, replacement, replaceAll result)
        let cases = [
            replace_row("(?<=x{2,20000})y", "xxy", "<$0>", "xx<y>"),
            replace_row("(?<=x{2,20000})y", "xy", "<$0>", "xy"),
        ];
        check_replacements(cases, true, false);
    }
}
