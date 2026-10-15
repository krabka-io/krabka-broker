//! The RE2J dialect that Kafka compiles client-supplied patterns with.
//!
//! Kafka uses `com.google.re2j` for the KIP-848 `SubscribedTopicRegex` and for
//! the KIP-1038 `TransactionIdPattern` of `ListTransactions`. RE2 has no
//! backtracking, so a pattern that a client sends cannot cost more than linear
//! time in the input. Rust's `regex` gives the same guarantee and the same
//! refusals for lookaround, backreferences and possessive quantifiers.
//!
//! The two grammars are not identical. [`translate`] rewrites a pattern as
//! RE2J 1.8's `Parser` reads it before `regex` compiles it: `\Q...\E` quoting,
//! escapes RE2J reads as literals (`\<`, `\_`, octal), and character classes,
//! where RE2J has no set operations or nested classes, and counted
//! repetition, where RE2J reads a `{` that opens no count as a literal and
//! refuses a count over 1000. `regex`'s compiled-size limit is the one known
//! acceptance difference left.
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

/// RE2J's `ERR_INVALID_REPEAT_OP`, its message for a repetition of a
/// repetition, a possessive quantifier among them.
pub(crate) const INVALID_REPEAT_OP: &str = "invalid nested repetition operator";

/// RE2J's `ERR_INVALID_CHAR_RANGE`.
const INVALID_CHAR_RANGE: &str = "invalid character class range";

/// RE2J's `ERR_INVALID_ESCAPE`.
const INVALID_ESCAPE: &str = "invalid escape sequence";

/// RE2J's `ERR_MISSING_BRACKET`.
const MISSING_BRACKET: &str = "missing closing ]";

/// RE2J's `ERR_INVALID_REPEAT_SIZE`.
const INVALID_REPEAT_SIZE: &str = "invalid repeat count";

/// RE2J's `ERR_TRAILING_BACKSLASH`.
const TRAILING_BACKSLASH: &str = "trailing backslash at end of expression";

/// The largest count RE2J's `Parser.parseRepeat` takes.
const MAX_REPEAT: u32 = 1000;

/// Kafka's `String` protocol type is bounded by a signed 16-bit byte length.
const MAX_PATTERN_BYTES: usize = i16::MAX as usize;

/// The POSIX class names in RE2J's `CharGroup.POSIX_GROUPS`. RE2J also takes
/// each one negated, as `[:^name:]`, and Rust's `regex` reads the same names.
const POSIX_CLASSES: [&str; 14] = [
    "alnum", "alpha", "ascii", "blank", "cntrl", "digit", "graph", "lower", "print", "punct",
    "space", "upper", "word", "xdigit",
];

/// The Perl class escapes in RE2J's `CharGroup.PERL_GROUPS`.
const PERL_CLASSES: [char; 6] = ['d', 'D', 's', 'S', 'w', 'W'];

/// Translates an RE2J pattern into one Rust's `regex` reads the same way.
///
/// It follows RE2J 1.8's `Parser`: `\Q...\E` becomes escaped literals, an
/// escape that RE2J reads as a literal becomes that literal, and a character
/// class is rebuilt member by member, so `&&`, `--`, `~~` and a nested `[`
/// stay the plain members RE2J reads rather than Rust's set operations. A
/// `{` that opens no count becomes a literal. Assertions and Perl and Unicode classes pass through as written.
fn translate(pattern: &str) -> Result<String, String> {
    let chars: Vec<char> = pattern.chars().collect();
    let mut out = String::with_capacity(pattern.len());
    let mut index = 0;
    while let Some(&current) = chars.get(index) {
        index = match current {
            '[' => translate_class(&chars, index, &mut out)?,
            '\\' => translate_escape(&chars, index, &mut out)?,
            '{' => translate_count(&chars, index, &mut out)?,
            _ => {
                out.push(current);
                index + 1
            }
        };
    }
    Ok(out)
}

/// Translates the `{` at `chars[start]`, outside a class, as RE2J's
/// `Parser.parseRepeat` reads it, and returns the index after it.
///
/// Only `{n}`, `{n,}` and `{n,m}` are counts. Any other `{` is a literal, `{,n}`
/// and a count with a leading zero among them, where Rust's `regex` either
/// refuses the pattern or reads a count. RE2J refuses a count over 1000 that
/// `regex` would take.
fn translate_count(chars: &[char], start: usize, out: &mut String) -> Result<usize, String> {
    let Some((min, max, close)) = counted_repeat(chars, start) else {
        out.push_str(r"\{");
        return Ok(start + 1);
    };
    if min > MAX_REPEAT || max.is_some_and(|max| max > MAX_REPEAT || max < min) {
        return Err(INVALID_REPEAT_SIZE.to_owned());
    }
    out.extend(&chars[start..=close]);
    Ok(close + 1)
}

/// Reads the count `{n}`, `{n,}` or `{n,m}` at `chars[start]`, returning its
/// minimum, its maximum (`None` when open) and the index of its `}`.
fn counted_repeat(chars: &[char], start: usize) -> Option<(u32, Option<u32>, usize)> {
    let (min, after_min) = decimal(chars, start + 1)?;
    let (max, close) = if chars.get(after_min) != Some(&',') {
        (Some(min), after_min)
    } else if chars.get(after_min + 1) == Some(&'}') {
        (None, after_min + 1)
    } else {
        let (max, after_max) = decimal(chars, after_min + 1)?;
        (Some(max), after_max)
    };
    (chars.get(close) == Some(&'}')).then_some((min, max, close))
}

/// Reads the decimal at `chars[start]` as RE2J's `Parser.parseInt` does,
/// returning its value and the index after it. An empty number and one with a
/// leading zero are not numbers.
fn decimal(chars: &[char], start: usize) -> Option<(u32, usize)> {
    let digits: Vec<u32> = chars
        .get(start..)?
        .iter()
        .map_while(|digit| digit.to_digit(10))
        .collect();
    if digits.is_empty() || (digits.len() > 1 && digits[0] == 0) {
        return None;
    }
    // Nine digits fit a `u32`, and any count past them is refused all the same.
    let value = digits
        .iter()
        .take(9)
        .fold(0, |value, digit| value * 10 + digit);
    Some((value, start + digits.len()))
}

/// Translates the escape at `chars[start]`, outside a class, and returns the
/// index after it.
fn translate_escape(chars: &[char], start: usize, out: &mut String) -> Result<usize, String> {
    let Some(&kind) = chars.get(start + 1) else {
        return Err(TRAILING_BACKSLASH.to_owned());
    };
    let end = match kind {
        'Q' => {
            // Quoting ends at the first `\E`, or runs to the end of the pattern.
            let body = start + 2;
            let close = chars[body..]
                .windows(2)
                .position(|pair| pair == ['\\', 'E'])
                .map(|offset| body + offset);
            let quoted: String = chars[body..close.unwrap_or(chars.len())].iter().collect();
            out.push_str(&regex::escape(&quoted));
            return Ok(close.map_or(chars.len(), |close| close + 2));
        }
        'C' => return Err(INVALID_ESCAPE.to_owned()),
        'A' | 'b' | 'B' | 'z' => start + 2,
        'p' | 'P' => {
            // A bracketed Unicode class keeps the scans for inline flags and
            // possessive quantifiers from reading `{Greek}` as a repetition.
            let end = unicode_class_end(chars, start);
            out.push('[');
            out.extend(&chars[start..end]);
            out.push(']');
            return Ok(end);
        }
        _ if PERL_CLASSES.contains(&kind) => start + 2,
        _ => {
            let (literal, end) = literal_escape(chars, start)?;
            push_literal(out, literal);
            return Ok(end);
        }
    };
    out.extend(&chars[start..end]);
    Ok(end)
}

/// The index after the Unicode class `\p...` or `\P...` at `chars[start]`.
///
/// The name is one letter or runs to the next `}`. A malformed name is left
/// for the regex parser, which refuses it as RE2J does.
fn unicode_class_end(chars: &[char], start: usize) -> usize {
    let name = start + 2;
    if chars.get(name) != Some(&'{') {
        return (name + 1).min(chars.len());
    }
    chars[name..]
        .iter()
        .position(|&candidate| candidate == '}')
        .map_or(chars.len(), |offset| name + offset + 1)
}

/// Reads the escape at `chars[start]` as RE2J's `Parser.parseEscape` does,
/// returning the character it stands for and the index after it.
fn literal_escape(chars: &[char], start: usize) -> Result<(char, usize), String> {
    let Some(&kind) = chars.get(start + 1) else {
        return Err(TRAILING_BACKSLASH.to_owned());
    };
    if !kind.is_ascii_alphanumeric() {
        // RE2J reads every escaped non-word character as itself, `\<`, `\_`
        // and `\é` among them.
        return Ok((kind, start + 2));
    }
    let mut end = start + 2;
    let value = match kind {
        '0'..='7' => {
            // Up to three octal digits. A lone `\1` to `\7` is a backreference.
            let mut value = u32::from(kind) - u32::from('0');
            let mut digits = 1;
            while digits < 3 {
                let Some(digit) = chars.get(end).and_then(|next| next.to_digit(8)) else {
                    break;
                };
                value = value * 8 + digit;
                digits += 1;
                end += 1;
            }
            (kind == '0' || digits > 1).then_some(value)
        }
        'x' => hex_escape(chars, &mut end),
        'a' => Some(7),
        'f' => Some(12),
        'n' => Some(10),
        'r' => Some(13),
        't' => Some(9),
        'v' => Some(11),
        _ => None,
    };
    value
        .and_then(char::from_u32)
        .map(|literal| (literal, end))
        .ok_or_else(|| INVALID_ESCAPE.to_owned())
}

/// Reads the digits of a `\x` escape, `\xHH` or `\x{H...}`, starting at
/// `chars[*end]` and moving `end` past them.
fn hex_escape(chars: &[char], end: &mut usize) -> Option<u32> {
    let first = *chars.get(*end)?;
    *end += 1;
    if first != '{' {
        let second = chars.get(*end)?.to_digit(16)?;
        *end += 1;
        return Some(first.to_digit(16)? * 16 + second);
    }
    let close = *end
        + chars[*end..]
            .iter()
            .position(|&candidate| candidate == '}')?;
    let digits: String = chars[*end..close].iter().collect();
    *end = close + 1;
    if digits.is_empty() || !digits.chars().all(|digit| digit.is_ascii_hexdigit()) {
        return None;
    }
    u32::from_str_radix(&digits, 16)
        .ok()
        .filter(|&value| value <= u32::from(char::MAX))
}

/// Translates the character class that opens at `chars[start]`, as RE2J's
/// `Parser.parseClass` reads it, and returns the index after its `]`.
fn translate_class(chars: &[char], start: usize, out: &mut String) -> Result<usize, String> {
    let mut index = start + 1;
    out.push('[');
    if chars.get(index) == Some(&'^') {
        out.push('^');
        index += 1;
    }
    // A `]` right after the `[` or `[^` is a member.
    let mut first = true;
    loop {
        let Some(&current) = chars.get(index) else {
            return Err(MISSING_BRACKET.to_owned());
        };
        if current == ']' && !first {
            out.push(']');
            return Ok(index + 1);
        }
        first = false;

        if let Some(end) = posix_class_end(chars, index)? {
            out.extend(&chars[index..end]);
            index = end;
            continue;
        }
        if current == '\\' {
            let end = match chars.get(index + 1) {
                Some('p' | 'P') => Some(unicode_class_end(chars, index)),
                Some(kind) if PERL_CLASSES.contains(kind) => Some(index + 2),
                _ => None,
            };
            if let Some(end) = end {
                out.extend(&chars[index..end]);
                index = end;
                continue;
            }
        }

        let (low, end) = class_char(chars, index)?;
        index = end;
        push_literal(out, low);
        // `a-` before the closing `]` is the two members `a` and `-`.
        if chars.get(index) == Some(&'-') && chars.get(index + 1).is_some_and(|&next| next != ']') {
            let (high, end) = class_char(chars, index + 1)?;
            if high < low {
                return Err(INVALID_CHAR_RANGE.to_owned());
            }
            out.push('-');
            push_literal(out, high);
            index = end;
        }
    }
}

/// The index after the POSIX class `[:name:]` at `chars[start]`, if one
/// starts there.
///
/// RE2J takes the text up to the first `:]` as the name, wherever that is,
/// and refuses a name it does not know.
fn posix_class_end(chars: &[char], start: usize) -> Result<Option<usize>, String> {
    if chars.get(start..start + 2) != Some(&['[', ':']) {
        return Ok(None);
    }
    let Some(offset) = chars[start + 1..]
        .windows(2)
        .position(|pair| pair == [':', ']'])
    else {
        return Ok(None);
    };
    let end = start + 1 + offset + 2;
    let name: String = chars[start..end].iter().collect();
    let known = name
        .strip_prefix("[:")
        .and_then(|name| name.strip_suffix(":]"))
        .map(|name| name.strip_prefix('^').unwrap_or(name))
        .is_some_and(|name| POSIX_CLASSES.contains(&name));
    if known {
        Ok(Some(end))
    } else {
        Err(INVALID_CHAR_RANGE.to_owned())
    }
}

/// Reads one class member or range end at `chars[start]`, as RE2J's
/// `Parser.parseClassChar` does.
fn class_char(chars: &[char], start: usize) -> Result<(char, usize), String> {
    match chars.get(start) {
        Some('\\') => literal_escape(chars, start),
        Some(&member) => Ok((member, start + 1)),
        None => Err(MISSING_BRACKET.to_owned()),
    }
}

/// Appends `literal` escaped, so Rust's `regex` reads it as itself both
/// inside and outside a class.
fn push_literal(out: &mut String, literal: char) {
    out.push_str(&regex::escape(literal.encode_utf8(&mut [0; 4])));
}

/// Whether `pattern` stacks one repetition on another (`a**`, `a{2}{3}`,
/// `a*??`), possessive quantifiers (`a*+`, `a{2,3}+`) among them.
///
/// RE2J's `Parser.repeat` refuses a repetition operator right after another,
/// past the one `?` that makes it lazy. Rust's `regex` reads `a**` as a
/// repetition of a repetition and compiles it, so the scan is what keeps the
/// two engines on the same answer. It runs on the translated pattern, where an
/// unescaped `{` always opens a count, and it tracks escapes and character
/// classes, so a literal `+` is left alone.
pub(crate) fn stacked_repetition(pattern: &str) -> bool {
    let chars: Vec<char> = pattern.chars().collect();
    let mut index = 0;
    let mut in_class = false;
    let mut after_repeat = false;
    let mut lazy = false;
    while let Some(&current) = chars.get(index) {
        index += 1;
        if current == '\\' {
            index += 1;
            after_repeat = false;
            continue;
        }
        if in_class {
            in_class = current != ']';
            after_repeat = false;
            continue;
        }
        match current {
            '*' | '+' | '?' | '{' => {
                if after_repeat {
                    if current == '?' && !lazy {
                        lazy = true;
                        continue;
                    }
                    return true;
                }
                after_repeat = true;
                lazy = false;
                if current == '{' {
                    index += chars[index..]
                        .iter()
                        .position(|&candidate| candidate == '}')
                        .map_or(chars.len() - index, |offset| offset + 1);
                }
            }
            _ => {
                in_class = current == '[';
                after_repeat = false;
            }
        }
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
pub(crate) fn check(pattern: &str) -> Result<(), String> { compile_full_match(pattern).map(|_| ()) }

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
    if pattern.len() > MAX_PATTERN_BYTES {
        return Err(format!(
            "pattern exceeds Kafka string limit of {MAX_PATTERN_BYTES} bytes"
        ));
    }
    let translated = translate(pattern)?;
    if unsupported_inline_flag(&translated).is_some() {
        return Err(INVALID_PERL_OP.to_owned());
    }
    if stacked_repetition(&translated) {
        return Err(INVALID_REPEAT_OP.to_owned());
    }
    // Compile the pattern on its own first, so the error text names what the
    // client sent rather than the anchors around it.
    regex::Regex::new(&translated).map_err(|error| flatten_error(&error))?;
    regex::Regex::new(&format!(r"\A(?:{translated})\z")).map_err(|error| flatten_error(&error))
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    /// The patterns Kafka's RE2J takes, and the ones it refuses.
    #[yare::parameterized(
        plain_pattern = { "txn-.*", true },
        alternation_and_repeat = { "(a|b)+c", true },
        case_insensitive_flag = { "(?i)TXN", true },
        dotall_flag = { "(?s).*", true },
        character_class = { "[a-z0-9_.-]+", true },
        positive_lookahead = { "(?=txn-).*", false },
        negative_lookahead = { "(?!txn-).*", false },
        backreference = { "(txn)-\\1", false },
        possessive_star = { "a*+", false },
        possessive_plus = { "a++", false },
        possessive_optional = { "a?+", false },
        possessive_range = { "a{2,3}+", false },
        lazy_quantifier = { "a+?", true },
        class_quantifiers = { "[+*]+", true },
        escaped_plus = { "a\\++", true },
        unsupported_extended_flag = { "(?x) txn", false },
        unsupported_unicode_flag = { "(?u)txn", false },
        unmatched_open_bracket = { "[", false },
        unmatched_open_group = { "(", false },
    )]
    fn the_dialect_follows_re2j(pattern: &str, compiles: bool) {
        check!(
            compile_full_match(pattern).is_ok() == compiles,
            "{pattern:?} should {}compile",
            if compiles { "" } else { "not " }
        );
    }

    /// Patterns RE2J 1.8 (Kafka 4.3.1's `com.google.re2j`) compiles, with
    /// inputs `Matcher.matches()` accepts and refuses.
    #[yare::parameterized(
        quoted_dot = { r"txn\Q.\E1", &["txn.1"], &["txnx1"] },
        quoted_metacharacters = { r"\Q[a.*]\E", &["[a.*]"], &["a"] },
        quote_runs_to_end = { r"prefix\Q.+[", &["prefix.+["], &["prefixx+["] },
        quote_ends_at_first_terminator = { r"\Q.\E+", &[".", "..."], &["+", "x"] },
        quoted_backslash = { r"\Q\d\E", &[r"\d"], &["1"] },
        empty_quote = { r"a\Q\Eb", &["ab"], &["a\\Eb"] },
        posix_class = { "[[:alpha:]]+", &["abc"], &[":", "a1"] },
        negated_posix_class = { "[[:^digit:]]", &["a"], &["1"] },
        posix_class_beside_members = { "[x[:digit:]-]+", &["x1-"], &["a"] },
        ampersands_are_members = { "[a&&b]", &["a", "&", "b"], &["c"] },
        perl_classes_beside_ampersands = { r"[\w&&\d]", &["x", "7", "&"], &["-"] },
        tildes_are_members = { "[a~~b]", &["a", "~", "b"], &["c"] },
        open_bracket_is_member = { "[[a]", &["[", "a"], &["]"] },
        nested_class_is_member_then_literal = { "[a[b]]", &["a]", "[]", "b]"], &["a", "]"] },
        hyphen_range_after_range = { "[a-z--b]", &["q", "-", "0", "b"], &["~"] },
        leading_hyphen_range = { "[--a]", &["-", "0", "a"], &["b"] },
        negated_leading_hyphen_range = { "[^--a]", &["z"], &["-", "0", "a"] },
        hyphen_to_hyphen = { "[---]", &["-"], &["a"] },
        trailing_hyphen = { "[a-]", &["a", "-"], &["b"] },
        escaped_hyphen = { r"[a\-z]", &["a", "-", "z"], &["b"] },
        initial_close_bracket = { "[]a]", &["]", "a"], &["b"] },
        negated_initial_close_bracket = { "[^]]", &["a"], &["]"] },
        caret_after_negation = { "[^^][a]", &["xa"], &["^a"] },
        hex_range = { r"[\x41-\x{43}]+", &["ABC"], &["D"] },
        octal_escape = { r"a\101\0", &["aA\0"], &["a"] },
        escaped_angle_brackets = { r"\<txn\>", &["<txn>"], &["txn"] },
        escaped_non_ascii = { "\\é", &["é"], &["e"] },
        escaped_underscore = { r"a\_b", &["a_b"], &["ab"] },
        perl_class_in_class = { r"[\d_]+", &["1_2"], &["a"] },
        repeated_unicode_class = { r"\p{Greek}+", &["αβ"], &["a"] },
        repeated_one_letter_unicode_class = { r"\pL{2}", &["ab"], &["a"] },
        case_folded_class = { "(?i)[a-c]+", &["aBC"], &["d"] },
        exact_count = { "a{2}", &["aa"], &["a", "aaa"] },
        open_count = { "a{2,}", &["aa", "aaaa"], &["a"] },
        lazy_bounded_count = { "a{2,3}?", &["aa", "aaa"], &["a"] },
        largest_count = { "a{1000}", &[], &["a"] },
        missing_minimum_is_literal = { "a{,5}", &["a{,5}"], &["a", "aaa"] },
        unclosed_brace_is_literal = { "a{", &["a{"], &["a"] },
        word_in_braces_is_literal = { "a{x}", &["a{x}"], &["a"] },
        leading_zero_is_literal = { "a{01}", &["a{01}"], &["a"] },
        malformed_maximum_is_literal = { "a{2,1x}", &["a{2,1x}"], &["aa"] },
        lone_braces_are_literal = { "{}", &["{}"], &[""] },
        repeated_group_of_count = { "(a{2})*", &["", "aaaa"], &["a"] },
    )]
    fn accepted_patterns_match_like_re2j(pattern: &str, matches: &[&str], misses: &[&str]) {
        let compiled = compile_full_match(pattern)
            .unwrap_or_else(|error| panic!("{pattern:?} compiles in RE2J: {error}"));
        for input in matches {
            check!(compiled.is_match(input), "{pattern:?} matches {input:?}");
        }
        for input in misses {
            check!(
                !compiled.is_match(input),
                "{pattern:?} does not match {input:?}"
            );
        }
    }

    /// Patterns RE2J 1.8 refuses, with its message.
    #[yare::parameterized(
        descending_range = { "[z-a]", INVALID_CHAR_RANGE },
        descending_hyphen_range = { "[a-z--,]", INVALID_CHAR_RANGE },
        unknown_posix_class = { "[[:foo:]]", INVALID_CHAR_RANGE },
        unterminated_class = { "[a", MISSING_BRACKET },
        empty_class = { "[]", MISSING_BRACKET },
        quote_inside_class = { r"[\Qa\E]", INVALID_ESCAPE },
        perl_class_as_range_end = { r"[a-\d]", INVALID_ESCAPE },
        word_boundary_in_class = { r"[\b]", INVALID_ESCAPE },
        any_byte = { r"\C", INVALID_ESCAPE },
        backreference = { r"(a)\1", INVALID_ESCAPE },
        unknown_letter_escape = { r"\q", INVALID_ESCAPE },
        bad_hex_escape = { r"\x{}", INVALID_ESCAPE },
        trailing_backslash = { "a\\", TRAILING_BACKSLASH },
        count_over_limit = { "a{1001}", INVALID_REPEAT_SIZE },
        maximum_over_limit = { "a{2,1001}", INVALID_REPEAT_SIZE },
        descending_count = { "a{3,2}", INVALID_REPEAT_SIZE },
        overflowing_count = { "a{123456789}", INVALID_REPEAT_SIZE },
        stacked_counts = { "a{2}{3}", INVALID_REPEAT_OP },
        stacked_stars = { "a**", INVALID_REPEAT_OP },
        count_after_star = { "a*{2}", INVALID_REPEAT_OP },
        star_after_count = { "a{2}*", INVALID_REPEAT_OP },
        stacked_lazy_markers = { "a*??", INVALID_REPEAT_OP },
    )]
    fn refused_patterns_carry_re2j_messages(pattern: &str, message: &str) {
        check!(compile_full_match(pattern).err().as_deref() == Some(message));
    }

    #[test]
    fn rejects_patterns_over_kafka_string_bound_before_translation() {
        let pattern = format!("\\Q{}\\E", "x".repeat(MAX_PATTERN_BYTES));
        let error = compile_full_match(&pattern).expect_err("oversized pattern rejected");
        check!(error.contains("Kafka string limit"));
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
