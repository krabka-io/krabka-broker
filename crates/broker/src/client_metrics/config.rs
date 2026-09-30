//! Validation and parsing for KIP-714 `CLIENT_METRICS` config resources.
//!
//! There are three keys only, which match
//! `org.apache.kafka.server.metrics.ClientMetricsConfigs`. `metrics` is a CSV
//! prefix list, where the single token `"*"` means all. `interval.ms` is an
//! int in `100..=3_600_000` with default 300000. `match` is a CSV of
//! `selector=regex`, where the regex is a `java.util.regex.Pattern`.

mod java_fold;

use std::collections::BTreeMap;

use fancy_regex::Regex;

use self::java_fold::Escape;

pub(crate) const KEY_METRICS: &str = "metrics";
pub(crate) const KEY_INTERVAL_MS: &str = "interval.ms";
pub(crate) const KEY_MATCH: &str = "match";

/// Kafka's `ClientMetricsConfigs.INTERVAL_MS_DEFAULT`: the push interval of a
/// subscription with no `interval.ms`, and the cap every client starts from.
pub(crate) const INTERVAL_MS_DEFAULT: i32 = 300_000;
const MIN_INTERVAL_MS: i32 = 100;
const MAX_INTERVAL_MS: i32 = 3_600_000;
pub(crate) const ALL_METRICS: &str = "*";

/// Why `ClientMetricsConfigs.validate` refused a subscription's configs. The
/// variant is the Kafka exception, so it decides the wire error code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ConfigError {
    /// `InvalidRequestException`: `INVALID_REQUEST` (42).
    InvalidRequest(String),
    /// `ConfigException` or `InvalidConfigurationException`: `INVALID_CONFIG`
    /// (40).
    InvalidConfig(String),
}

impl ConfigError {
    /// The wire error code Kafka's `ConfigurationControlManager` answers.
    pub(crate) fn code(&self) -> i16 {
        match self {
            Self::InvalidRequest(_) => crate::codes::INVALID_REQUEST,
            Self::InvalidConfig(_) => crate::codes::INVALID_CONFIG,
        }
    }

    /// The error message Kafka puts on the wire.
    pub(crate) fn message(&self) -> &str {
        match self {
            Self::InvalidRequest(message) | Self::InvalidConfig(message) => message,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MatchSelector {
    InstanceId,
    Id,
    SoftwareName,
    SoftwareVersion,
    SourceAddress,
    SourcePort,
}

impl MatchSelector {
    fn parse(s: &str) -> Option<Self> {
        Some(match s {
            "client_instance_id" => Self::InstanceId,
            "client_id" => Self::Id,
            "client_software_name" => Self::SoftwareName,
            "client_software_version" => Self::SoftwareVersion,
            "client_source_address" => Self::SourceAddress,
            "client_source_port" => Self::SourcePort,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone)]
pub(crate) struct MatchRule {
    pub selector: MatchSelector,
    pub pattern: Regex,
}

/// Kafka's `ClientMetricsConfigs.validate(subscriptionName, configs)`, run on
/// the subscription's whole config map after the alteration is applied.
///
/// The checks run in Kafka's order: the subscription name, then every key
/// against the three known names (`InvalidRequestException`), then
/// `ConfigDef.parse` in definition order (`metrics`, `interval.ms`, `match`,
/// each a `ConfigException`), then the `interval.ms` range
/// (`InvalidRequestException`), then the `match` patterns
/// (`InvalidConfigurationException`).
pub(crate) fn validate(
    subscription: &str,
    configs: &BTreeMap<String, String>,
) -> Result<(), ConfigError> {
    if subscription.is_empty() {
        return Err(ConfigError::InvalidRequest(
            "Subscription name can't be empty".into(),
        ));
    }
    if let Some(key) = configs
        .keys()
        .find(|key| !matches!(key.as_str(), KEY_METRICS | KEY_INTERVAL_MS | KEY_MATCH))
    {
        return Err(ConfigError::InvalidRequest(format!(
            "Unknown client metrics configuration: {key}"
        )));
    }
    if let Some(value) = configs.get(KEY_METRICS) {
        ensure_valid_list(KEY_METRICS, &parse_list(value))?;
    }
    let interval = configs
        .get(KEY_INTERVAL_MS)
        .map(|value| {
            java_trim(value).parse::<i32>().map_err(|_| {
                ConfigError::InvalidConfig(format!(
                    "Invalid value {value} for configuration {KEY_INTERVAL_MS}: Not a number of \
                     type INT"
                ))
            })
        })
        .transpose()?;
    let patterns = configs.get(KEY_MATCH).map(|value| parse_list(value));
    if let Some(patterns) = &patterns {
        ensure_valid_list(KEY_MATCH, patterns)?;
    }
    if let Some(interval) = interval
        && !(MIN_INTERVAL_MS..=MAX_INTERVAL_MS).contains(&interval)
    {
        return Err(ConfigError::InvalidRequest(format!(
            "Invalid value {interval} for {KEY_INTERVAL_MS}, interval must be between 100 and \
             3600000 (1 hour)"
        )));
    }
    if let Some(patterns) = patterns {
        parse_match_patterns(&patterns)?;
    }
    Ok(())
}

/// `ConfigDef.ValidList.anyNonDuplicateValues(true, false)`: an empty list is
/// allowed, a repeated item or an empty item is not.
fn ensure_valid_list(key: &str, values: &[&str]) -> Result<(), ConfigError> {
    let distinct: std::collections::HashSet<&&str> = values.iter().collect();
    if distinct.len() != values.len() {
        return Err(ConfigError::InvalidConfig(format!(
            "Configuration '{key}' values must not be duplicated."
        )));
    }
    if values.iter().any(|value| value.is_empty()) {
        return Err(ConfigError::InvalidConfig(format!(
            "Configuration '{key}' values must not be empty."
        )));
    }
    Ok(())
}

/// Java's `String.trim`: strips every leading and trailing char at or below
/// U+0020.
fn java_trim(value: &str) -> &str {
    value.trim_matches(|c: char| c <= ' ')
}

/// Java's `\s`: space, tab, newline, vertical tab, form feed, carriage return.
fn is_java_whitespace(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\u{0B}' | '\u{0C}' | '\r')
}

/// `ConfigDef.parseType` for a `LIST`: the trimmed value, empty meaning an
/// empty list, otherwise split on `\s*,\s*` keeping empty items.
fn parse_list(value: &str) -> Vec<&str> {
    let trimmed = java_trim(value);
    if trimmed.is_empty() {
        return Vec::new();
    }
    trimmed
        .split(',')
        .map(|item| item.trim_matches(is_java_whitespace))
        .collect()
}

/// Effective push interval for a subscription's override map. The function
/// returns `default_interval_ms` when the key is unset.
pub(crate) fn effective_interval_ms(
    configs: &BTreeMap<String, String>,
    default_interval_ms: i32,
) -> i32 {
    configs
        .get(KEY_INTERVAL_MS)
        .and_then(|v| java_trim(v).parse::<i32>().ok())
        .unwrap_or(default_interval_ms)
}

/// Parse the `metrics` value into prefixes, as `ConfigDef` parses a `LIST`.
/// `"*"` collapses to `["*"]`. An empty string gives an empty list, which
/// means no metrics.
pub(crate) fn parse_metrics(value: &str) -> Vec<String> {
    parse_list(value).into_iter().map(str::to_string).collect()
}

/// Parse the `match` value into compiled selector rules. An empty value
/// matches all.
pub(crate) fn parse_match_rules(value: &str) -> Result<Vec<MatchRule>, ConfigError> {
    parse_match_patterns(&parse_list(value))
}

/// Kafka's `ClientMetricsConfigs.parseMatchingPatterns`: each entry splits on
/// every `=` as Java's `String.split("=")` does (trailing empty parts
/// dropped) and must give exactly two parts, a known selector and a pattern
/// `java.util.regex.Pattern` compiles. Every refusal is
/// `InvalidConfigurationException` naming the whole entry.
fn parse_match_patterns(patterns: &[&str]) -> Result<Vec<MatchRule>, ConfigError> {
    let mut rules = Vec::new();
    for &entry in patterns {
        let illegal =
            || ConfigError::InvalidConfig(format!("Illegal client matching pattern: {entry}"));
        let mut parts: Vec<&str> = entry.split('=').collect();
        while parts.last().is_some_and(|part| part.is_empty()) {
            parts.pop();
        }
        let [name, pattern] = parts.as_slice() else {
            return Err(illegal());
        };
        let selector = MatchSelector::parse(java_trim(name)).ok_or_else(illegal)?;
        let pattern = java_to_fancy(java_trim(pattern)).ok_or_else(illegal)?;
        // Kafka tests a selector with `Matcher.matches()`, a full match. The
        // anchored group gives the same answer for an alternation such as
        // `app|app-1`, where a leftmost-first search would stop at `app`.
        let pattern = Regex::new(&format!("^(?:{pattern})$")).map_err(|_| illegal())?;
        // Kafka's `parseMatchingPatterns` puts each entry into a map by
        // selector, so a repeated selector keeps its last pattern.
        rules.retain(|rule: &MatchRule| rule.selector != selector);
        rules.push(MatchRule { selector, pattern });
    }
    Ok(rules)
}

/// Rewrite a `java.util.regex.Pattern` into `fancy_regex` syntax, or `None`
/// for a group opener Java refuses, or a `\N{name}`, which the rewrite cannot
/// read.
///
/// `fancy_regex` accepts Python and Oniguruma forms Java refuses, such as
/// `(?P<name>x)`, `(?P=name)`, `(?'name'x)` and `(?~x)`, so every `(?` outside
/// a character class must be one of Java's openers: `(?:`, `(?=`, `(?!`,
/// `(?>`, `(?<=`, `(?<!`, `(?<name>` with an ASCII-letter-then-alphanumeric
/// name, or inline flags from `idmsuxU-`. Java's `\Q...\E` quoting, which
/// `fancy_regex` lacks, becomes escaped literals before anything else is read,
/// as Java does it.
///
/// Four things read differently by default, and the rewrite gives the Java
/// reading, following the flags in force at each place (`(?s)`, `(?d)`,
/// `(?U)`, `(?i)`, `(?u)` and `(?x)`, alone or scoped to a group):
///
/// - `\w`, `\d`, `\s`, `\b` and their negations are ASCII, where
///   `fancy_regex`'s are Unicode, until `(?U)` (`UNICODE_CHARACTER_CLASS`)
///   asks for the Unicode ones.
/// - `.` does not match `\n`, `\r`, U+0085, U+2028 or U+2029, where
///   `fancy_regex`'s stops at `\n` only, until `(?s)` (`DOTALL`) lets it match
///   any, or `(?d)` (`UNIX_LINES`) leaves `\n` the only line terminator.
/// - `(?i)` (`CASE_INSENSITIVE`) folds ASCII case only, where `fancy_regex`'s
///   `i` folds Unicode case, until `(?u)` (`UNICODE_CASE`) or `(?U)` asks for
///   Unicode folding. While an ASCII fold is in force the rewrite writes both
///   cases of each ASCII letter itself, and a backreference as `(?i:\1)`. See
///   [`java_fold`] for the two places where that differs from Java.
/// - Under `(?x)` (`COMMENTS`) the rewrite drops the whitespace and the `#`
///   comments Java ignores, so that the text of a comment is never read as
///   pattern.
pub(crate) fn java_to_fancy(pattern: &str) -> Option<String> {
    let chars = java_fold::remove_qe_quoting(&pattern.chars().collect::<Vec<char>>());
    let mut translator = Translator {
        chars: &chars,
        out: String::with_capacity(pattern.len()),
        scope: Scope::default(),
        outer: Vec::new(),
        class_depth: 0,
        class_start: false,
        at: 0,
    };
    while translator.at < chars.len() {
        translator.step()?;
    }
    Some(translator.out)
}

/// The state of one [`java_to_fancy`] pass over a pattern.
struct Translator<'a> {
    chars: &'a [char],
    out: String,
    /// The Java flags in force here.
    scope: Scope,
    /// The flags of the groups around here, the innermost last.
    outer: Vec<Scope>,
    /// How many character classes are open here.
    class_depth: usize,
    /// The next char is the first in a class, where a `]` is a member.
    class_start: bool,
    /// The index of the next char to read.
    at: usize,
}

impl Translator<'_> {
    fn in_class(&self) -> bool {
        self.class_depth > 0
    }

    /// Copies `len` chars as they are.
    fn copy(&mut self, len: usize) {
        self.out.extend(&self.chars[self.at..self.at + len]);
        self.at += len;
    }

    /// Reads and writes the next token: a char, an escape, a group opener, or a
    /// run of `(?x)` whitespace and comment.
    fn step(&mut self) -> Option<()> {
        let c = self.chars[self.at];
        let first_in_class = std::mem::take(&mut self.class_start);
        let ignored = self.ignored_len(self.at);
        if ignored > 0 {
            self.class_start = first_in_class;
            self.at += ignored;
            return Some(());
        }
        match c {
            '\\' => self.escape()?,
            '(' if !self.in_class() => self.group()?,
            ')' if !self.in_class() => {
                self.scope = self.outer.pop().unwrap_or(self.scope);
                self.copy(1);
            }
            '[' => self.open_class(),
            // First in a class, a `]` is a member, and may start a range.
            ']' if self.in_class() && !first_in_class => {
                self.class_depth -= 1;
                self.out.push(']');
                self.at += 1;
            }
            '.' if !self.in_class() && !self.scope.dot.dotall && !self.scope.dot.unix_lines => {
                self.out.push_str(JAVA_DOT);
                self.at += 1;
            }
            _ => self.atom(u32::from(c), 1),
        }
        Some(())
    }

    /// The chars of `(?x)` whitespace and comment at `at`: none unless the
    /// `x` flag is on.
    fn ignored_len(&self, at: usize) -> usize {
        if self.scope.comments {
            java_fold::ignorable_len(&self.chars[at..], self.scope.dot.unix_lines)
        } else {
            0
        }
    }

    /// The escape at `at`.
    fn escape(&mut self) -> Option<()> {
        let in_class = self.in_class();
        let (escape, len) = java_fold::read_escape(self.chars, self.at, in_class);
        match escape {
            // `fancy_regex` reads `\N{name}` as `\N` and the text of the name,
            // and the rewrite has no table of Unicode names to write the char.
            Escape::Named => return None,
            // Java compares the text of the group ASCII-insensitively, which
            // `fancy_regex` can do only for a whole reference. See
            // [`java_fold`] for what that leaves different.
            Escape::BackRef if self.scope.ascii_fold() => {
                self.out.push_str("(?i:");
                self.copy(len);
                self.out.push(')');
            }
            Escape::Char(code) => self.atom(code, len),
            Escape::Property { name, negated } => {
                match java_fold::property_class(&name, negated, in_class, self.scope) {
                    Some(class) => {
                        self.out.push_str(&class);
                        self.at += len;
                    }
                    None => self.copy(len),
                }
            }
            Escape::BackRef | Escape::Other => {
                let ascii = self
                    .chars
                    .get(self.at + 1)
                    .and_then(|&next| ascii_shorthand(next, in_class));
                match ascii {
                    Some(ascii) if !self.scope.unicode_classes => {
                        let class = java_fold::unfolded(ascii, in_class, self.scope);
                        self.out.push_str(&class);
                        self.at += len;
                    }
                    _ => self.copy(len),
                }
            }
        }
        Some(())
    }

    /// One char, `len` chars long in the pattern and standing for `code`, and
    /// in a class the range it starts. Under an ASCII fold a letter is both of
    /// its cases, and a range takes the other case of the letters in it.
    fn atom(&mut self, code: u32, len: usize) {
        let (chars, start, in_class) = (self.chars, self.at, self.in_class());
        let fold = self.scope.ascii_fold();
        let after = start + len;
        let range = if in_class {
            self.range_end(after)
        } else {
            None
        };
        if let Some((end, end_code, end_len)) = range {
            let (first, last) = (&chars[start..after], &chars[end..end + end_len]);
            java_fold::push_member(&mut self.out, first, code);
            self.out.push('-');
            java_fold::push_member(&mut self.out, last, end_code);
            if fold {
                java_fold::push_other_case_ranges(&mut self.out, code, end_code);
            }
            self.at = end + end_len;
        } else {
            let source = &chars[start..after];
            if fold {
                java_fold::push_folded(&mut self.out, source, code, in_class);
            } else {
                java_fold::push_member(&mut self.out, source, code);
            }
            self.at = after;
        }
    }

    /// Where the range that starts before `after` ends, and the code and the
    /// length of its last char. Java reads a `-` after a member and before a
    /// char that is not `]` or `[` as the range of the two.
    fn range_end(&self, after: usize) -> Option<(usize, u32, usize)> {
        let dash = after + self.ignored_len(after);
        if self.chars.get(dash) != Some(&'-') {
            return None;
        }
        let end = dash + 1 + self.ignored_len(dash + 1);
        if matches!(self.chars.get(end), Some(']' | '[')) {
            return None;
        }
        let (code, len) = java_fold::read_member(self.chars, end)?;
        Some((end, code, len))
    }

    /// A `[`, which opens a class, or a class in a class.
    fn open_class(&mut self) {
        self.class_depth += 1;
        self.class_start = true;
        self.copy(1);
        if self.chars.get(self.at) == Some(&'^') {
            self.copy(1);
        }
    }

    /// A `(`: a group, a lookaround, or inline flags.
    fn group(&mut self) -> Option<()> {
        let chars = self.chars;
        if chars.get(self.at + 1) != Some(&'?') {
            self.outer.push(self.scope);
            self.copy(1);
            return Some(());
        }
        let rest = &chars[self.at + 2..];
        let opener = match rest.first() {
            Some(':' | '=' | '!' | '>') => 3,
            Some('<') => match rest.get(1) {
                Some('=' | '!') => 4,
                Some(first) if first.is_ascii_alphabetic() => {
                    let name = rest[2..].iter().position(|c| !c.is_ascii_alphanumeric())?;
                    if rest[2 + name] != '>' {
                        return None;
                    }
                    name + 5
                }
                _ => return None,
            },
            Some(_) => return self.flag_group(rest),
            None => return None,
        };
        self.outer.push(self.scope);
        self.copy(opener);
        Some(())
    }

    /// Inline flags, `rest` starting just after their `(?`.
    fn flag_group(&mut self, rest: &[char]) -> Option<()> {
        let group = java_flag_group(rest)?;
        let fancy_ignore_case = self.scope.fancy_ignore_case();
        // `(?flags:x)` is a group the flags last to the end of, `(?flags)`
        // lasts to the end of the group it is in.
        if group.scoped {
            self.outer.push(self.scope);
        }
        for &(flag, on) in &group.changes {
            self.scope.set(flag, on);
        }
        let ignore_case = match (fancy_ignore_case, self.scope.fancy_ignore_case()) {
            (false, true) => Some(true),
            (true, false) => Some(false),
            _ => None,
        };
        let translation = group.translation(ignore_case);
        // Java refuses `(?i)*` as a dangling quantifier; an emptied group must
        // not hand it to the atom before it.
        if translation.is_empty() && matches!(rest.get(group.len), Some('*' | '+' | '?')) {
            return None;
        }
        self.out.push_str(&translation);
        self.at += 2 + group.len;
        Some(())
    }
}

/// The Java flags that change what [`java_to_fancy`] writes for a construct.
#[derive(Debug, Clone, Copy, Default)]
struct Scope {
    /// `s` and `d`: what `.` matches.
    dot: Dot,
    /// `U` (`UNICODE_CHARACTER_CLASS`): `\w`, `\d`, `\s` and `\b` are Unicode.
    unicode_classes: bool,
    /// `i` and `u`: how letters match.
    case: Case,
    /// `x` (`COMMENTS`): whitespace and `#` comments are ignored.
    comments: bool,
}

/// The [`Scope`] flags for `.` and the line terminators.
#[derive(Debug, Clone, Copy, Default)]
struct Dot {
    /// `s` (`DOTALL`): `.` matches a line terminator.
    dotall: bool,
    /// `d` (`UNIX_LINES`): `\n` is the only line terminator.
    unix_lines: bool,
}

/// The [`Scope`] flags for the case of letters.
#[derive(Debug, Clone, Copy, Default)]
struct Case {
    /// `i` (`CASE_INSENSITIVE`): letters match in both cases.
    ignore: bool,
    /// `u` (`UNICODE_CASE`), which `U` turns on too: `i` folds Unicode case,
    /// and not only ASCII.
    unicode: bool,
}

impl Scope {
    fn set(&mut self, flag: char, on: bool) {
        match flag {
            's' => self.dot.dotall = on,
            'd' => self.dot.unix_lines = on,
            'i' => self.case.ignore = on,
            'u' => self.case.unicode = on,
            'x' => self.comments = on,
            // As in `Pattern.addFlag` and `Pattern.subFlag`, `U` takes
            // `UNICODE_CASE` with it, in both directions.
            'U' => {
                self.unicode_classes = on;
                self.case.unicode = on;
            }
            _ => {}
        }
    }

    /// `i` without `u`: Java folds ASCII case only, which `fancy_regex` cannot
    /// do, so the rewrite writes both cases and leaves the fancy `i` off.
    fn ascii_fold(self) -> bool {
        self.case.ignore && !self.case.unicode
    }

    /// `i` with `u`: Java folds Unicode case, as the fancy `i` does.
    fn fancy_ignore_case(self) -> bool {
        self.case.ignore && self.case.unicode
    }
}

/// What Java's `.` matches by default: anything but the line terminators
/// `\n`, `\r`, U+0085, U+2028 and U+2029 (`Pattern.Dot`, without `DOTALL`
/// or `UNIX_LINES`).
const JAVA_DOT: &str = "[^\\n\\r\\x{85}\\x{2028}\\x{2029}]";

/// The ASCII form of a Java shorthand class, or word boundary, which is what
/// Java reads without `(?U)`: `\w` is `[a-zA-Z_0-9]`, `\d` is `[0-9]`, `\s` is
/// `[ \t\n\x0B\f\r]`, and `\b` is the edge of a run of `\w` (as of JDK 19,
/// where it stopped reading Unicode letters and digits).
///
/// `escape` is the character after the backslash. Inside a character class
/// the positive forms are the bare members, and the negations are a nested
/// negated class. `None` for anything else, and for a boundary in a class.
fn ascii_shorthand(escape: char, in_class: bool) -> Option<&'static str> {
    Some(match (escape, in_class) {
        ('w', false) => "[0-9A-Za-z_]",
        ('w', true) => "0-9A-Za-z_",
        ('W', _) => "[^0-9A-Za-z_]",
        ('d', false) => "[0-9]",
        ('d', true) => "0-9",
        ('D', _) => "[^0-9]",
        ('s', false) => "[ \\t\\n\\x0B\\x0C\\r]",
        ('s', true) => " \\t\\n\\x0B\\x0C\\r",
        ('S', _) => "[^ \\t\\n\\x0B\\x0C\\r]",
        ('b', false) => "(?:(?<=[0-9A-Za-z_])(?![0-9A-Za-z_])|(?<![0-9A-Za-z_])(?=[0-9A-Za-z_]))",
        ('B', false) => "(?:(?<=[0-9A-Za-z_])(?=[0-9A-Za-z_])|(?<![0-9A-Za-z_])(?![0-9A-Za-z_]))",
        _ => return None,
    })
}

/// Translate a Java inline flag group, `rest` starting just after its `(?`,
/// into the flags that `fancy_regex` shares and the flags [`Scope`] follows.
/// Returns `None` where `Pattern.compile` answers "Unknown inline modifier".
///
/// Java's `Pattern.addFlag` takes `idmsuxcU`, then optionally one `-` and
/// the same letters to clear, then `)` or `:`; an empty group such as `(?)`
/// or `(?-:x)` is valid. `fancy_regex` refuses empty groups and gives `U` a
/// different meaning, so only the flags it shares keep their letter:
///
/// - `m`, `s` and `x` (`MULTILINE`, `DOTALL`, `COMMENTS`) pass through, and
///   [`Scope`] follows `s` and `x`.
/// - `i` (`CASE_INSENSITIVE`) and `u` (`UNICODE_CASE`) are read by
///   [`Scope`]: `fancy_regex`'s `i` folds Unicode case, which is Java's `i`
///   with `u` and not Java's `i` alone, so [`Translator`] turns it on only
///   where both are on. See [`java_fold`].
/// - `d` (`UNIX_LINES`) is dropped from the output, and read by
///   [`java_to_fancy`] to translate `.`.
/// - `U` (`UNICODE_CHARACTER_CLASS`) is dropped from the output, and read by
///   [`java_to_fancy`] to keep `\w`, `\d`, `\s` and `\b` Unicode where it
///   otherwise makes them ASCII, and to fold Unicode case, which Java's `U`
///   turns on. Passing it through would swap greediness in `fancy_regex`.
/// - `c` (`CANON_EQ`) is dropped: canonical-equivalence matching has no
///   `fancy_regex` form, and it changes nothing for text already in one
///   normalization form.
fn java_flag_group(rest: &[char]) -> Option<FlagGroup> {
    let mut on = String::new();
    let mut off = String::new();
    let mut changes = Vec::new();
    let mut clearing = false;
    for (n, &c) in rest.iter().enumerate() {
        match c {
            'm' | 's' | 'x' => {
                if clearing {
                    off.push(c);
                } else {
                    on.push(c);
                }
                changes.push((c, !clearing));
            }
            'i' | 'u' | 'd' | 'U' => changes.push((c, !clearing)),
            'c' => {}
            '-' if !clearing => clearing = true,
            ')' | ':' => {
                return Some(FlagGroup {
                    on,
                    off,
                    len: n + 1,
                    scoped: c == ':',
                    changes,
                });
            }
            _ => return None,
        }
    }
    None
}

/// A Java inline flag group, as [`java_flag_group`] reads it.
struct FlagGroup {
    /// The flags `fancy_regex` shares that it turns on.
    on: String,
    /// The flags `fancy_regex` shares that it turns off.
    off: String,
    /// The chars of the group after its `(?`, through the closing `)` or `:`.
    len: usize,
    /// Whether it ends in `:`, a group that the flags last to the end of.
    scoped: bool,
    /// The flags [`Scope`] follows that it sets (`true`) or clears (`false`),
    /// in the order Java applies them.
    changes: Vec<(char, bool)>,
}

impl FlagGroup {
    /// The `fancy_regex` text for the group. `ignore_case` turns the fancy `i`
    /// on (`Some(true)`) or off (`Some(false)`) as well, when the flags that
    /// [`Scope`] follows changed what it must be. It is empty for a `(?flags)`
    /// group that has no flag of `fancy_regex`'s to write, which
    /// `fancy_regex` refuses.
    fn translation(&self, ignore_case: Option<bool>) -> String {
        let (mut on, mut off) = (self.on.clone(), self.off.clone());
        match ignore_case {
            Some(true) => on.push('i'),
            Some(false) => off.push('i'),
            None => {}
        }
        let end = if self.scoped { ':' } else { ')' };
        match (on.is_empty() && off.is_empty(), self.scoped) {
            (true, false) => String::new(),
            (true, true) => "(?:".to_string(),
            (false, _) if off.is_empty() => format!("(?{on}{end}"),
            (false, _) => format!("(?{on}-{off}{end}"),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    fn validate_one(key: &str, value: &str) -> Result<(), ConfigError> {
        validate(
            "sub",
            &maplit::btreemap! {key.to_string() => value.to_string()},
        )
    }

    #[test]
    fn validate_matches_client_metrics_configs() {
        let request = |m: &str| Err(ConfigError::InvalidRequest(m.to_string()));
        let config = |m: &str| Err(ConfigError::InvalidConfig(m.to_string()));
        let range = |n: &str| {
            request(&format!(
                "Invalid value {n} for interval.ms, interval must be between 100 and 3600000 (1 \
                 hour)"
            ))
        };
        let not_int = |v: &str| {
            config(&format!(
                "Invalid value {v} for configuration interval.ms: Not a number of type INT"
            ))
        };
        let illegal = |e: &str| config(&format!("Illegal client matching pattern: {e}"));
        let cases = [
            ("interval.ms", "300000", Ok(())),
            ("interval.ms", "100", Ok(())),
            ("interval.ms", "3600000", Ok(())),
            ("interval.ms", " 1000 ", Ok(())),
            ("interval.ms", "99", range("99")),
            ("interval.ms", "3600001", range("3600001")),
            ("interval.ms", "not-a-number", not_int("not-a-number")),
            ("interval.ms", "3000000000", not_int("3000000000")),
            (
                "bogus.key",
                "x",
                request("Unknown client metrics configuration: bogus.key"),
            ),
            ("metrics", "*", Ok(())),
            ("metrics", "", Ok(())),
            (
                "metrics",
                "org.apache.kafka.consumer., org.apache.kafka.producer.",
                Ok(()),
            ),
            (
                "metrics",
                "a.,",
                config("Configuration 'metrics' values must not be empty."),
            ),
            (
                "metrics",
                "a.,a.",
                config("Configuration 'metrics' values must not be duplicated."),
            ),
            ("match", "client_id=my-app.*", Ok(())),
            (
                "match",
                "client_software_name=apache-kafka-java,client_id=svc-.*",
                Ok(()),
            ),
            ("match", " client_id = a ", Ok(())),
            ("match", "client_id=(?i)app(?!-x)", Ok(())),
            // Kafka splits on every `=`, so a lookahead `(?=` can never pass.
            ("match", "client_id=a(?=b)", illegal("client_id=a(?=b)")),
            ("match", "client_id=(a)\\1", Ok(())),
            ("match", "client_id=(?i)(a)\\1", Ok(())),
            ("match", "client_id=(?iu)(a)\\1", Ok(())),
            ("match", "client_id=a==", Ok(())),
            ("match", "client_foo=x", illegal("client_foo=x")),
            ("match", "client_id", illegal("client_id")),
            ("match", "=x", illegal("=x")),
            ("match", "client_id=a=b", illegal("client_id=a=b")),
            (
                "match",
                "client_id=[unclosed",
                illegal("client_id=[unclosed"),
            ),
            ("match", "client_id=(?P<n>x)", illegal("client_id=(?P<n>x)")),
            ("match", "client_id=(?P=n)", illegal("client_id=(?P=n)")),
            ("match", "client_id=(?'n'x)", illegal("client_id=(?'n'x)")),
            ("match", "client_id=(?<1n>x)", illegal("client_id=(?<1n>x)")),
            ("match", "client_id=[(?P<n>]", Ok(())),
            ("match", "client_id=\\Q(?P<\\E", Ok(())),
        ];
        for (key, value, expected) in cases {
            check!(validate_one(key, value) == expected, "{key}={value:?}");
        }
    }

    #[test]
    fn empty_subscription_name_is_an_invalid_request() {
        check!(
            validate("", &BTreeMap::new())
                == Err(ConfigError::InvalidRequest(
                    "Subscription name can't be empty".into()
                ))
        );
    }

    #[test]
    fn unknown_key_is_reported_before_a_bad_value() {
        let configs = maplit::btreemap! {
            "interval.ms".to_string() => "abc".to_string(),
            "zzz".to_string() => "x".to_string(),
        };
        check!(
            validate("sub", &configs)
                == Err(ConfigError::InvalidRequest(
                    "Unknown client metrics configuration: zzz".into()
                ))
        );
    }

    #[test]
    fn error_codes_follow_the_exception() {
        check!(ConfigError::InvalidRequest(String::new()).code() == crate::codes::INVALID_REQUEST);
        check!(ConfigError::InvalidConfig(String::new()).code() == crate::codes::INVALID_CONFIG);
    }

    #[test]
    fn effective_interval_defaults_and_trims() {
        let mut m = BTreeMap::new();
        check!(effective_interval_ms(&m, INTERVAL_MS_DEFAULT) == 300_000);
        check!(effective_interval_ms(&m, 600_000) == 600_000);
        m.insert("interval.ms".to_string(), " 60000 ".to_string());
        check!(effective_interval_ms(&m, INTERVAL_MS_DEFAULT) == 60_000);
    }

    #[test]
    fn match_rules_fully_match_with_java_constructs() {
        let rules = parse_match_rules("client_id=(?!test).*, client_software_name=java").unwrap();
        let selectors: Vec<MatchSelector> = rules.iter().map(|r| r.selector).collect();
        check!(selectors == vec![MatchSelector::Id, MatchSelector::SoftwareName]);
        for (input, expected) in [("app", true), ("test-app", false), ("", true)] {
            check!(
                rules[0].pattern.is_match(input).unwrap() == expected,
                "{input}"
            );
        }
        assert!(!rules[1].pattern.is_match("java-1").unwrap());
        let quoted = parse_match_rules("client_id=\\Qa.b\\E").unwrap();
        for (input, expected) in [("a.b", true), ("axb", false)] {
            check!(
                quoted[0].pattern.is_match(input).unwrap() == expected,
                "{input}"
            );
        }
    }

    /// A selector pattern reads as `java.util.regex.Pattern` does by default:
    /// `\w`, `\d` and `\s` are ASCII, and `.` stops at every line terminator.
    /// The flags that change that are `(?U)`, `(?s)` and `(?d)`.
    #[test]
    fn match_patterns_read_ascii_classes_and_a_dot_that_stops_at_a_terminator() {
        // (pattern, input, whole input matches)
        let cases = [
            ("\\w+", "app_1", true),
            ("\\w+", "appé", false),
            ("(?U)\\w+", "appé", true),
            ("\\d+", "١٢٣", false),
            ("app\\s1", "app\u{a0}1", false),
            ("app.1", "app\r1", false),
            ("app.1", "app\u{2028}1", false),
            ("(?s)app.1", "app\r1", true),
            ("(?d)app.1", "app\r1", true),
            ("[.]", "\r", false),
        ];
        for (pattern, input, expected) in cases {
            let rules = parse_match_rules(&format!("client_id={pattern}")).unwrap();
            check!(
                rules[0].pattern.is_match(input).unwrap() == expected,
                "{pattern:?} on {input:?}"
            );
        }
    }

    /// `(?i)` in a selector folds ASCII case only, as `Pattern.CASE_INSENSITIVE`
    /// does. Long s (U+017F) and the Kelvin sign (U+212A) match `s` and `k`
    /// only under `(?u)`, which `Pattern.UNICODE_CASE` sets. Each row is
    /// `Matcher.matches` on JDK 17.
    #[test]
    fn match_patterns_fold_ascii_case_unless_unicode_case_is_on() {
        // (pattern, input, whole input matches)
        let cases = [
            ("(?i)app-", "APP-", true),
            ("(?i)service-.*", "SERVICE-1", true),
            ("(?i)service-.*", "\u{17f}ervice-1", false),
            ("(?iu)service-.*", "\u{17f}ervice-1", true),
            ("(?i)k", "\u{212a}", false),
            ("(?i)[a-c]+", "AbC", true),
            ("(?i)[^a]", "A", false),
            ("(?i)\\p{Lower}+", "aBc", true),
            ("(?i)(?<Id>app)-\\d", "APP-1", true),
            ("app(?i)-x", "app-X", true),
            ("app(?i)-x", "APP-X", false),
            // A quoted member takes part in a range, a `]` first in a class may
            // start one, `\p{Lower}` and `\w` stay ASCII under `(?iu)`, and a
            // backreference folds ASCII case.
            ("(?i)[\\Qa\\E-c]", "B", true),
            ("(?i)[\\Qa\\E-c]", "X", false),
            ("(?i)[]-c]", "A", true),
            ("(?i)[]-c]", "X", false),
            ("(?iu)\\p{Lower}", "\u{17f}", false),
            ("(?iu)\\p{Lower}", "A", true),
            ("(?iu)\\w", "\u{17f}", false),
            ("(?i)(a)\\1", "aA", true),
            ("(?i)(a)\\1", "ab", false),
            ("(?i)(\\d+)-\\1", "12-12", true),
            ("(?i)(\\d+)-\\1", "12-13", false),
        ];
        for (pattern, input, expected) in cases {
            let rules = parse_match_rules(&format!("client_id={pattern}")).unwrap();
            check!(
                rules[0].pattern.is_match(input).unwrap() == expected,
                "{pattern:?} on {input:?}"
            );
        }
    }

    /// Java's inline flags, each case checked against `Pattern.compile` and
    /// `Matcher.matches` on JDK 21: `None` is a refusal, otherwise the
    /// inputs the compiled pattern must and must not fully match.
    #[test]
    fn java_inline_flags_mean_what_they_mean_in_java() {
        type Case = (&'static str, Option<&'static [(&'static str, bool)]>);
        let cases: [Case; 18] = [
            ("(?d)^foo$", Some(&[("foo", true), ("xfoo", false)])),
            ("(?i-d)foo", Some(&[("FOO", true)])),
            ("(?U)\\w+", Some(&[("é", true), ("a b", false)])),
            // `U` swapped greediness would make the atomic group take both.
            ("(?U)(?>a+?)a", Some(&[("aa", true)])),
            ("(?iu)é", Some(&[("É", true), ("e", false)])),
            ("(?-i)A", Some(&[("A", true), ("a", false)])),
            ("(?i)(?-i)A", Some(&[("A", true), ("a", false)])),
            ("(?i:abc)d", Some(&[("ABCd", true), ("ABCD", false)])),
            ("(?-:a)b", Some(&[("ab", true)])),
            ("(?)a", Some(&[("a", true)])),
            ("(?c)a", Some(&[("a", true)])),
            ("(?ix)A b", Some(&[("ab", true), ("a b", false)])),
            ("(?x)\\Q a#\\E", Some(&[(" a#", true), ("a", false)])),
            ("(?q)a", None),
            ("(?i--x)a", None),
            ("(?-i-x)a", None),
            ("(?i", None),
            ("a(?d)*", None),
        ];
        for (pattern, expected) in cases {
            let entry = format!("client_id={pattern}");
            match (parse_match_rules(&entry), expected) {
                (Ok(rules), Some(inputs)) => {
                    for &(input, matches) in inputs {
                        check!(
                            rules[0].pattern.is_match(input).unwrap() == matches,
                            "{pattern} on {input:?}"
                        );
                    }
                }
                (Err(error), None) => {
                    check!(
                        error
                            == ConfigError::InvalidConfig(format!(
                                "Illegal client matching pattern: {entry}"
                            ))
                    );
                }
                (result, _) => {
                    check!(result.is_ok() == expected.is_some(), "{pattern}");
                }
            }
        }
    }

    #[test]
    fn parse_metrics_splits_like_config_def() {
        check!(parse_metrics("*") == vec!["*".to_string()]);
        check!(parse_metrics("  ") == Vec::<String>::new());
        check!(parse_metrics(" a. , b. ") == vec!["a.".to_string(), "b.".to_string()]);
    }
}
