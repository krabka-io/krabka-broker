//! Java's ASCII-only `(?i)`, for [`java_to_fancy`](super::java_to_fancy).
//!
//! Java's `CASE_INSENSITIVE` (`(?i)`) folds ASCII case only. It folds Unicode
//! case as well when `UNICODE_CASE` (`(?u)`) is on, and `(?U)`
//! (`UNICODE_CHARACTER_CLASS`) turns that on. `fancy_regex` has one `i` flag,
//! and it always folds Unicode: `(?i)service-` matches `ſervice-` (long s)
//! there, and not in Java. The inline `(?-u)` cannot switch it off
//! (`ChangingUnicodeModeUnsupported`), and `RegexBuilder::unicode_mode(false)`
//! is a switch for the whole pattern that also refuses `.`, `\W` and `\p{..}`.
//!
//! So while an `i` without `u` is in force the rewrite folds the case itself
//! and leaves `fancy_regex`'s `i` off. It writes each ASCII letter as both its
//! cases, `[aA]`, or `aA` in a character class, and a class range as the range
//! plus the ranges of the other case of the letters it covers, which is what
//! Java's `CIRange` matches. It leaves a non-ASCII character as it is. The
//! escapes that name a letter, `\x41`, `\x{41}`, `\uhhhh`, `\0101` and the
//! text of `\Q...\E`, fold as the letter does. Java also widens the properties
//! for a case under `(?i)`: `\p{Lower}` and `\p{Upper}` are the ASCII letters
//! of both cases, and `\p{Lu}`, `\p{Ll}` and `\p{Lt}` are all cased letters.
//! [`property_class`] writes those.
//!
//! An `i` with `u` stays `fancy_regex`'s own Unicode fold, which is Unicode
//! simple case folding. Java compares `Character.toUpperCase` and
//! `toLowerCase` instead, and the two differ for a few letters: `İ` (U+0130)
//! and `ı` (U+0131) match `i` in Java under `(?iu)`, and not in `fancy_regex`.
//!
//! A backreference is the one thing it cannot write. Java's `CIBackRef` compares
//! the text of the group ASCII-insensitively, and `fancy_regex` has no
//! per-reference flag: its `i` compares Unicode-insensitively, and `(?-i)` makes
//! the comparison exact. The rewrite refuses such a pattern, as it refuses the
//! Java-incompatible group openers.

use std::fmt::Write as _;

use super::Scope;

/// A backslash escape, as [`read_escape`] reads it.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Escape {
    /// `\Q`, which quotes the text up to the next `\E`.
    Quote,
    /// An escape that stands for one character: `\x41`, `\x{41}`,
    /// `\uhhhh`, `\0101`, `\cA`, `\t`, or a backslash before a character that is not a
    /// letter or a digit, such as `\.`.
    Char(u32),
    /// `\p{name}`, `\P{name}`, or `\pL`.
    Property { name: String, negated: bool },
    /// `\1` to `\9`, or `\k<name>`: the text a group matched.
    BackRef,
    /// `\N{name}`, a character by its Unicode name.
    Named,
    /// Any other escape, such as `\w`, `\b` or `\A`.
    Other,
}

/// The escape that starts at `chars[at]`, a backslash, and the number of chars
/// it takes. `in_class` is true inside a character class, where `\1` and
/// `\k<name>` are not references.
///
/// An escape Java's `Pattern.compile` refuses, such as `\x` without digits, is
/// [`Escape::Other`] with the length of the backslash and the char after it.
pub(super) fn read_escape(chars: &[char], at: usize, in_class: bool) -> (Escape, usize) {
    let rest = &chars[at + 1..];
    let Some(&kind) = rest.first() else {
        return (Escape::Other, 1);
    };
    let after = &rest[1..];
    let read = match kind {
        'Q' => Some((Escape::Quote, 2)),
        't' => Some((Escape::Char(0x09), 2)),
        'n' => Some((Escape::Char(0x0A), 2)),
        'r' => Some((Escape::Char(0x0D), 2)),
        'f' => Some((Escape::Char(0x0C), 2)),
        'a' => Some((Escape::Char(0x07), 2)),
        'e' => Some((Escape::Char(0x1B), 2)),
        'x' => hex_escape(after).map(|(code, len)| (Escape::Char(code), 2 + len)),
        'u' => after
            .get(..4)
            .and_then(parse_hex)
            .map(|code| (Escape::Char(code), 6)),
        '0' => octal_escape(after).map(|(code, len)| (Escape::Char(code), 2 + len)),
        'c' => after
            .first()
            .map(|&control| (Escape::Char(u32::from(control) ^ 64), 3)),
        'p' | 'P' => property_name(after).map(|(name, len)| {
            let negated = kind == 'P';
            (Escape::Property { name, negated }, 2 + len)
        }),
        'k' if !in_class => delimited(after, '<', '>').map(|len| (Escape::BackRef, 2 + len)),
        'N' => delimited(after, '{', '}').map(|len| (Escape::Named, 2 + len)),
        '1'..='9' if !in_class => Some((Escape::BackRef, 2)),
        other if !other.is_ascii_alphanumeric() => Some((Escape::Char(u32::from(other)), 2)),
        _ => None,
    };
    read.unwrap_or((Escape::Other, 2))
}

/// The number `digits` spell in base 16, or `None` for a char that is not an
/// ASCII hex digit or a number past `char::MAX`.
fn parse_hex(digits: &[char]) -> Option<u32> {
    let code = digits.iter().try_fold(0u32, |code, digit| {
        code.checked_mul(16)?.checked_add(digit.to_digit(16)?)
    })?;
    (code <= u32::from(char::MAX)).then_some(code)
}

/// `hh` or `{h...h}`, after `\x`, and the chars it takes.
fn hex_escape(after: &[char]) -> Option<(u32, usize)> {
    if after.first() == Some(&'{') {
        let close = after.iter().position(|&c| c == '}')?;
        let digits = &after[1..close];
        if digits.is_empty() {
            return None;
        }
        Some((parse_hex(digits)?, close + 1))
    } else {
        Some((parse_hex(after.get(..2)?)?, 2))
    }
}

/// One to three octal digits after `\0`, and the chars they take: the third
/// digit only when the first is at most 3, as Java's `Pattern.o` reads it.
fn octal_escape(after: &[char]) -> Option<(u32, usize)> {
    let digit = |at: usize| after.get(at).and_then(|c| c.to_digit(8));
    let first = digit(0)?;
    Some(match (digit(1), digit(2)) {
        (Some(second), Some(third)) if first <= 3 => (first * 64 + second * 8 + third, 3),
        (Some(second), _) => (first * 8 + second, 2),
        _ => (first, 1),
    })
}

/// The name in `{name}`, or the one char, after `\p` or `\P`, and the chars
/// it takes.
fn property_name(after: &[char]) -> Option<(String, usize)> {
    if after.first() == Some(&'{') {
        let close = after.iter().position(|&c| c == '}')?;
        (close > 1).then(|| (after[1..close].iter().collect(), close + 1))
    } else {
        after.first().map(|&c| (c.to_string(), 1))
    }
}

/// The chars that `after` takes for a run from `open` to `close`, where it
/// is not empty.
fn delimited(after: &[char], open: char, close: char) -> Option<usize> {
    let end = after.iter().position(|&c| c == close)?;
    (after.first() == Some(&open) && end > 1).then_some(end + 1)
}

/// The chars at the start of `rest` that Java's `COMMENTS` flag ignores: white
/// space, and `#` comments, each through the line terminator that ends it.
pub(super) fn ignorable_len(rest: &[char], unix_lines: bool) -> usize {
    let is_terminator = |c: char| {
        c == '\n' || (!unix_lines && matches!(c, '\r' | '\u{85}' | '\u{2028}' | '\u{2029}'))
    };
    let mut len = 0;
    while let Some(&c) = rest.get(len) {
        match c {
            ' ' | '\t' | '\n' | '\u{0B}' | '\u{0C}' | '\r' => len += 1,
            '#' => {
                len += 1;
                while let Some(&comment) = rest.get(len) {
                    len += 1;
                    if is_terminator(comment) {
                        break;
                    }
                }
            }
            _ => break,
        }
    }
    len
}

/// The code and the length of the char at `chars[at]`, where it is a member of
/// a class or the end of a range in one: a plain char, or an escape that stands
/// for one char.
pub(super) fn read_member(chars: &[char], at: usize) -> Option<(u32, usize)> {
    let &c = chars.get(at)?;
    if c != '\\' {
        return Some((u32::from(c), 1));
    }
    match read_escape(chars, at, true) {
        (Escape::Char(code), len) => Some((code, len)),
        _ => None,
    }
}

/// `code` as an ASCII letter, or `None` for any other code point.
fn ascii_letter(code: u32) -> Option<char> {
    let byte = u8::try_from(code).ok().filter(u8::is_ascii_alphabetic)?;
    Some(char::from(byte))
}

/// Writes `source`, the chars of one character of the pattern that stands for
/// `code`, but as the letter itself when it is an ASCII letter. `fancy_regex`
/// reads `\x41` and `A`, and not the octal `\0101`.
pub(super) fn push_member(out: &mut String, source: &[char], code: u32) {
    match ascii_letter(code) {
        Some(letter) => out.push(letter),
        None => out.extend(source),
    }
}

/// Writes `source`, the chars of one character of the pattern that stands for
/// `code`, as a set of both its cases when it is an ASCII letter: `[aA]`
/// outside a class, and `aA` inside one. Any other character is `source`.
pub(super) fn push_folded(out: &mut String, source: &[char], code: u32, in_class: bool) {
    let Some(letter) = ascii_letter(code) else {
        out.extend(source);
        return;
    };
    let (lower, upper) = (letter.to_ascii_lowercase(), letter.to_ascii_uppercase());
    if in_class {
        out.push(lower);
        out.push(upper);
    } else {
        let _ = write!(out, "[{lower}{upper}]");
    }
}

/// Writes, inside a class, the ranges of the other case of the ASCII letters
/// that the range `lo` to `hi` covers. Java's `CIRange` accepts a letter when
/// it, or the other case of it, is in the range.
pub(super) fn push_other_case_ranges(out: &mut String, lo: u32, hi: u32) {
    for (first, last) in [('A', 'Z'), ('a', 'z')] {
        let (start, end) = (lo.max(u32::from(first)), hi.min(u32::from(last)));
        if start <= end {
            let _ = write!(out, "\\x{{{:X}}}-\\x{{{:X}}}", start ^ 0x20, end ^ 0x20);
        }
    }
}

/// The members of the class of `\p{name}` where `fancy_regex` reads the name
/// differently from Java, as the text of a class body.
///
/// - `Lower` and `Upper` are the ASCII letters `[a-z]` and `[A-Z]`, or both
///   cases of them under `(?i)`, where `fancy_regex`'s are Unicode.
/// - `Lu`, `Ll` and `Lt`, `IsLowercase`, `IsUppercase`, `IsTitlecase`,
///   `IsLower` and `IsUpper` take in all three cases under `(?i)`, whether or
///   not `(?u)` is on. Under an ASCII fold `fancy_regex`'s `i` is off, so the
///   rewrite writes the three cases.
fn property_members(name: &str, scope: Scope) -> Option<&'static str> {
    if !scope.unicode_classes {
        match (name, scope.case.ignore) {
            ("Lower", false) => return Some("a-z"),
            ("Upper", false) => return Some("A-Z"),
            ("Lower" | "Upper", true) => return Some("a-zA-Z"),
            _ => {}
        }
    }
    if !scope.ascii_fold() {
        return None;
    }
    if is_cased_category(name) {
        Some("\\p{Lu}\\p{Ll}\\p{Lt}")
    } else if is_cased_property(name) {
        Some("\\p{Lowercase}\\p{Uppercase}\\p{Lt}")
    } else {
        None
    }
}

/// Whether `\p{name}` is `Lu`, `Ll` or `Lt`, spelled as Java takes it: bare, with
/// an `Is` prefix, or as the value of `gc=` or `general_category=`.
fn is_cased_category(name: &str) -> bool {
    let value = match name.split_once('=') {
        Some((key, value)) if matches!(key.to_lowercase().as_str(), "gc" | "general_category") => {
            value
        }
        Some(_) => return false,
        None => name.strip_prefix("Is").unwrap_or(name),
    };
    matches!(value, "Lu" | "Ll" | "Lt")
}

/// Whether `\p{name}` is one of the `Is` properties for a case, which Java
/// matches in any name case.
fn is_cased_property(name: &str) -> bool {
    name.strip_prefix("Is").is_some_and(|short| {
        matches!(
            short.to_uppercase().as_str(),
            "LOWERCASE" | "UPPERCASE" | "TITLECASE" | "LOWER" | "UPPER"
        )
    })
}

/// `\p{name}` or `\P{name}`, written for `fancy_regex` as [`property_members`]
/// says, or `None` where the escape is fine as it stands.
pub(super) fn property_class(
    name: &str,
    negated: bool,
    in_class: bool,
    scope: Scope,
) -> Option<String> {
    let members = property_members(name, scope)?;
    Some(match (negated, in_class) {
        (false, true) => members.to_owned(),
        (false, false) => format!("[{members}]"),
        (true, _) => format!("[^{members}]"),
    })
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    fn escape(text: &str, in_class: bool) -> (Escape, usize) {
        let chars: Vec<char> = text.chars().collect();
        read_escape(&chars, 0, in_class)
    }

    /// Each row is the escape Java's `Pattern.escape` reads, and the chars it
    /// takes.
    #[test]
    fn read_escape_reads_what_java_reads() {
        let property = |name: &str, negated| Escape::Property {
            name: name.to_owned(),
            negated,
        };
        // (text, in a class, escape, chars taken)
        let cases = [
            ("\\Qa\\E", false, Escape::Quote, 2),
            ("\\x41", false, Escape::Char(0x41), 4),
            ("\\x{41}", false, Escape::Char(0x41), 6),
            ("\\x{1F600}", false, Escape::Char(0x1F600), 9),
            ("\\x{110000}", false, Escape::Other, 2),
            ("\\x{}", false, Escape::Other, 2),
            ("\\x4", false, Escape::Other, 2),
            ("\\u0041b", false, Escape::Char(0x41), 6),
            ("\\u004", false, Escape::Other, 2),
            ("\\0101", false, Escape::Char(0x41), 5),
            ("\\0777", false, Escape::Char(0o77), 4),
            ("\\07", false, Escape::Char(7), 3),
            ("\\00", false, Escape::Char(0), 3),
            ("\\0", false, Escape::Other, 2),
            ("\\cA", false, Escape::Char(1), 3),
            ("\\c!", false, Escape::Char(0x61), 3),
            ("\\t", false, Escape::Char(9), 2),
            ("\\.", false, Escape::Char(0x2E), 2),
            ("\\-", true, Escape::Char(0x2D), 2),
            ("\\pL", false, property("L", false), 3),
            ("\\P{Lu}x", false, property("Lu", true), 6),
            ("\\p{}", false, Escape::Other, 2),
            ("\\p{Lu", false, Escape::Other, 2),
            ("\\1", false, Escape::BackRef, 2),
            ("\\1", true, Escape::Other, 2),
            ("\\k<name>", false, Escape::BackRef, 8),
            ("\\k<>", false, Escape::Other, 2),
            ("\\N{LATIN SMALL LETTER A}", false, Escape::Named, 24),
            ("\\w", false, Escape::Other, 2),
            ("\\b", false, Escape::Other, 2),
            ("\\", false, Escape::Other, 1),
        ];
        for (text, in_class, expected, len) in cases {
            check!(escape(text, in_class) == (expected, len), "{text:?}");
        }
    }

    #[test]
    fn a_class_range_gets_the_other_case_of_the_letters_it_covers() {
        // (lo, hi, extra ranges)
        let cases = [
            ('a', 'f', "\\x{41}-\\x{46}"),
            ('B', 'D', "\\x{62}-\\x{64}"),
            ('X', 'c', "\\x{78}-\\x{7A}\\x{41}-\\x{43}"),
            ('+', 'z', "\\x{61}-\\x{7A}\\x{41}-\\x{5A}"),
            ('0', '9', ""),
            ('[', '`', ""),
            ('\u{e9}', '\u{ff}', ""),
        ];
        for (lo, hi, expected) in cases {
            let mut out = String::new();
            push_other_case_ranges(&mut out, u32::from(lo), u32::from(hi));
            check!(out == expected, "{lo:?}-{hi:?}");
        }
    }
}
