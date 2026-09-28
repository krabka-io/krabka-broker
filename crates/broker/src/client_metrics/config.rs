//! Validation and parsing for KIP-714 `CLIENT_METRICS` config resources.
//!
//! There are three keys only, which match
//! `org.apache.kafka.server.metrics.ClientMetricsConfigs`. `metrics` is a CSV
//! prefix list, where the single token `"*"` means all. `interval.ms` is an
//! int in `100..=3_600_000` with default 300000. `match` is a CSV of
//! `selector=regex`, where the regex is a `java.util.regex.Pattern`.

use std::{collections::BTreeMap, fmt::Write as _};

use fancy_regex::Regex;

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
/// returns [`INTERVAL_MS_DEFAULT`] when the key is unset.
pub(crate) fn effective_interval_ms(configs: &BTreeMap<String, String>) -> i32 {
    configs
        .get(KEY_INTERVAL_MS)
        .and_then(|v| java_trim(v).parse::<i32>().ok())
        .unwrap_or(INTERVAL_MS_DEFAULT)
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
/// for a group opener Java refuses.
///
/// `fancy_regex` accepts Python and Oniguruma forms Java refuses, such as
/// `(?P<name>x)`, `(?P=name)`, `(?'name'x)` and `(?~x)`, so every `(?` outside
/// a character class must be one of Java's openers: `(?:`, `(?=`, `(?!`,
/// `(?>`, `(?<=`, `(?<!`, `(?<name>` with an ASCII-letter-then-alphanumeric
/// name, or inline flags from `idmsuxU-`. Java's `\Q...\E` quoting, which
/// `fancy_regex` lacks, becomes escaped literals.
fn java_to_fancy(pattern: &str) -> Option<String> {
    let chars: Vec<char> = pattern.chars().collect();
    let mut out = String::with_capacity(pattern.len());
    let mut class_depth = 0usize;
    let mut quoted = false;
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if quoted {
            if c == '\\' && chars.get(i + 1) == Some(&'E') {
                quoted = false;
                i += 1;
            } else if c.is_whitespace() || c == '#' {
                // A quoted space or `#` stays literal under Java's `(?x)`;
                // `regex::escape` leaves them bare, and `fancy_regex`'s
                // `x` would drop them.
                let _ = write!(out, "\\x{{{:X}}}", u32::from(c));
            } else {
                out.push_str(&regex::escape(c.encode_utf8(&mut [0; 4])));
            }
        } else if c == '\\' {
            match chars.get(i + 1) {
                Some('Q') => quoted = true,
                Some(&next) => {
                    out.push(c);
                    out.push(next);
                }
                None => out.push(c),
            }
            i += 1;
        } else {
            if c == '[' {
                class_depth += 1;
            } else if c == ']' && class_depth > 0 {
                class_depth -= 1;
            } else if c == '(' && class_depth == 0 && chars.get(i + 1) == Some(&'?') {
                let rest = &chars[i + 2..];
                let ok = match rest.first() {
                    Some(':' | '=' | '!' | '>') => true,
                    Some('<') => match rest.get(1) {
                        Some('=' | '!') => true,
                        Some(first) if first.is_ascii_alphabetic() => rest[2..]
                            .iter()
                            .find(|c| !c.is_ascii_alphanumeric())
                            .is_some_and(|&end| end == '>'),
                        _ => false,
                    },
                    Some(_) => {
                        let (flags, len) = java_flag_group(rest)?;
                        // Java refuses `(?i)*` as a dangling quantifier;
                        // an emptied group must not hand it to the atom
                        // before it.
                        if flags.is_empty() && matches!(rest.get(len), Some('*' | '+' | '?')) {
                            return None;
                        }
                        out.push_str(&flags);
                        i += 2 + len;
                        continue;
                    }
                    None => false,
                };
                if !ok {
                    return None;
                }
            }
            out.push(c);
        }
        i += 1;
    }
    Some(out)
}

/// Translate a Java inline flag group, `rest` starting just after its `(?`,
/// into `fancy_regex` syntax. Returns the translation and the chars consumed
/// through the closing `)` or `:`, or `None` where `Pattern.compile` answers
/// "Unknown inline modifier".
///
/// Java's `Pattern.addFlag` takes `idmsuxcU`, then optionally one `-` and
/// the same letters to clear, then `)` or `:`; an empty group such as `(?)`
/// or `(?-:x)` is valid. `fancy_regex` refuses empty groups and gives `U` a
/// different meaning, so only the flags it shares keep their letter:
///
/// - `i`, `m`, `s` and `x` (`CASE_INSENSITIVE`, `MULTILINE`, `DOTALL`,
///   `COMMENTS`) pass through. `fancy_regex`'s `i` folds Unicode case, which
///   is Java's `i` with `u`; Java's `i` alone folds ASCII only, and
///   `fancy_regex` cannot turn Unicode folding off.
/// - `d` (`UNIX_LINES`) is dropped: `fancy_regex` already treats `\n` as the
///   only line terminator for `.`, `^` and `$`.
/// - `u` (`UNICODE_CASE`) is dropped: its effect, Unicode folding under `i`,
///   is how `fancy_regex`'s `i` always folds.
/// - `U` (`UNICODE_CHARACTER_CLASS`) is dropped: `fancy_regex`'s `\w`, `\d`,
///   `\s` and `\b` are always Unicode, and it implies `u`, which already
///   holds. Passing it through would swap greediness in `fancy_regex`.
/// - `c` (`CANON_EQ`) is dropped: canonical-equivalence matching has no
///   `fancy_regex` form, and it changes nothing for text already in one
///   normalization form.
fn java_flag_group(rest: &[char]) -> Option<(String, usize)> {
    let mut on = String::new();
    let mut off = String::new();
    let mut clearing = false;
    for (n, &c) in rest.iter().enumerate() {
        match c {
            'i' | 'm' | 's' | 'x' => {
                if clearing {
                    off.push(c);
                } else {
                    on.push(c);
                }
            }
            'd' | 'u' | 'c' | 'U' => {}
            '-' if !clearing => clearing = true,
            ')' | ':' => {
                let scoped = c == ':';
                let flags = match (on.is_empty() && off.is_empty(), scoped) {
                    (true, false) => String::new(),
                    (true, true) => "(?:".to_string(),
                    (false, _) if off.is_empty() => format!("(?{on}{c}"),
                    (false, _) => format!("(?{on}-{off}{c}"),
                };
                return Some((flags, n + 1));
            }
            _ => return None,
        }
    }
    None
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
        check!(effective_interval_ms(&m) == 300_000);
        m.insert("interval.ms".to_string(), " 60000 ".to_string());
        check!(effective_interval_ms(&m) == 60_000);
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
