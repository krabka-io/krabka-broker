//! Validation and parsing for KIP-714 `CLIENT_METRICS` config resources.
//!
//! There are three keys only, which match
//! `org.apache.kafka.server.metrics.ClientMetricsConfigs`. `metrics` is a CSV
//! prefix list, where the single token `"*"` means all. `interval.ms` is an
//! int in `100..=3_600_000` with default 300000. `match` is a CSV of
//! `selector=regex`, where the regex is a `java.util.regex.Pattern`.

use {
    crate::{
        codes::{INVALID_CONFIG, INVALID_REQUEST},
        config_keys::parse::{check_valid_list, int_value, java_trim, list_value, parse_int},
    },
    krabka_java_regex::JavaPattern,
    std::collections::BTreeMap,
};

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
            Self::InvalidRequest(_) => INVALID_REQUEST,
            Self::InvalidConfig(_) => INVALID_CONFIG,
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
    pub pattern: JavaPattern,
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
        check_valid_list(KEY_METRICS, value, &[], true).map_err(ConfigError::InvalidConfig)?;
    }
    let interval = configs
        .get(KEY_INTERVAL_MS)
        .map(|value| parse_int(KEY_INTERVAL_MS, value).map_err(ConfigError::InvalidConfig))
        .transpose()?;
    let patterns = configs
        .get(KEY_MATCH)
        .map(|value| check_valid_list(KEY_MATCH, value, &[], true))
        .transpose()
        .map_err(ConfigError::InvalidConfig)?;
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

/// Effective push interval for a subscription's override map. The function
/// returns `default_interval_ms` when the key is unset.
pub(crate) fn effective_interval_ms(
    configs: &BTreeMap<String, String>,
    default_interval_ms: i32,
) -> i32 {
    configs
        .get(KEY_INTERVAL_MS)
        .and_then(|v| int_value(v.as_str()))
        .unwrap_or(default_interval_ms)
}

/// Parse the `metrics` value into prefixes, as `ConfigDef` parses a `LIST`.
/// `"*"` collapses to `["*"]`. An empty string gives an empty list, which
/// means no metrics.
pub(crate) fn parse_metrics(value: &str) -> Vec<String> {
    list_value(value).into_iter().map(str::to_string).collect()
}

/// Parse the `match` value into compiled selector rules. An empty value
/// matches all.
pub(crate) fn parse_match_rules(value: &str) -> Result<Vec<MatchRule>, ConfigError> {
    parse_match_patterns(&list_value(value))
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
        // Kafka tests a selector with `Matcher.matches()`, a full match, which
        // `JavaPattern::matches` gives.
        let pattern = JavaPattern::compile(java_trim(pattern)).map_err(|_| illegal())?;
        // Kafka's `parseMatchingPatterns` puts each entry into a map by
        // selector, so a repeated selector keeps its last pattern.
        rules.retain(|rule: &MatchRule| rule.selector != selector);
        rules.push(MatchRule { selector, pattern });
    }
    Ok(rules)
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::{assert, check},
    };

    fn validate_one(key: &str, value: &str) -> Result<(), ConfigError> {
        validate(
            "sub",
            &maplit::btreemap! {key.to_string() => value.to_string()},
        )
    }

    /// A client id pattern, an input, and whether the whole input matches.
    #[derive(Clone, Copy)]
    struct PatternCase {
        pattern: &'static str,
        input: &'static str,
        expected: bool,
    }

    fn check_client_id_patterns(cases: &[PatternCase]) {
        for &PatternCase {
            pattern,
            input,
            expected,
        } in cases
        {
            let rules = parse_match_rules(&format!("client_id={pattern}")).unwrap();
            check!(
                rules[0].pattern.matches(input).unwrap() == expected,
                "{pattern:?} on {input:?}"
            );
        }
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
            // A `)` that closes no group is Java's `Unmatched closing ')'`, and
            // a range that ends in a shorthand class is `Illegal character
            // range`; a `)` in a class or an escape is a character.
            ("match", "client_id=a)|(b", illegal("client_id=a)|(b")),
            ("match", "client_id=(a))", illegal("client_id=(a))")),
            ("match", "client_id=[a-\\w]", illegal("client_id=[a-\\w]")),
            ("match", "client_id=[)]", Ok(())),
            ("match", "client_id=\\)", Ok(())),
            ("match", "client_id=[\\w-.]", Ok(())),
            // A range that runs backwards is `Illegal character range`, under a
            // Unicode fold as under none, and a `&` alone is a class member.
            ("match", "client_id=[z-a]", illegal("client_id=[z-a]")),
            (
                "match",
                "client_id=(?iu)[z-a]",
                illegal("client_id=(?iu)[z-a]"),
            ),
            ("match", "client_id=[a&b]", Ok(())),
            ("match", "client_id=(?iu)[a&b]", Ok(())),
            // A `{` outside a class and an escape starts the bounds of a
            // quantifier, and any other is `Illegal repetition`.
            ("match", "client_id=a{2}", Ok(())),
            ("match", "client_id=[{]", Ok(())),
            ("match", "client_id=\\{", Ok(())),
            ("match", "client_id=\\Q{\\E", Ok(())),
            ("match", "client_id={", illegal("client_id={")),
            ("match", "client_id=a{x}", illegal("client_id=a{x}")),
            (
                "match",
                "client_id=(?iu)a{x}",
                illegal("client_id=(?iu)a{x}"),
            ),
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
                rules[0].pattern.matches(input).unwrap() == expected,
                "{input}"
            );
        }
        assert!(!rules[1].pattern.matches("java-1").unwrap());
        let quoted = parse_match_rules("client_id=\\Qa.b\\E").unwrap();
        for (input, expected) in [("a.b", true), ("axb", false)] {
            check!(
                quoted[0].pattern.matches(input).unwrap() == expected,
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
            PatternCase {
                pattern: "\\w+",
                input: "app_1",
                expected: true,
            },
            PatternCase {
                pattern: "\\w+",
                input: "appé",
                expected: false,
            },
            PatternCase {
                pattern: "(?U)\\w+",
                input: "appé",
                expected: true,
            },
            PatternCase {
                pattern: "\\d+",
                input: "١٢٣",
                expected: false,
            },
            PatternCase {
                pattern: "app\\s1",
                input: "app\u{a0}1",
                expected: false,
            },
            PatternCase {
                pattern: "app.1",
                input: "app\r1",
                expected: false,
            },
            PatternCase {
                pattern: "app.1",
                input: "app\u{2028}1",
                expected: false,
            },
            PatternCase {
                pattern: "(?s)app.1",
                input: "app\r1",
                expected: true,
            },
            PatternCase {
                pattern: "(?d)app.1",
                input: "app\r1",
                expected: true,
            },
            PatternCase {
                pattern: "[.]",
                input: "\r",
                expected: false,
            },
            PatternCase {
                pattern: "[a&b]",
                input: "&",
                expected: true,
            },
            PatternCase {
                pattern: "[a&b]",
                input: "b",
                expected: true,
            },
            PatternCase {
                pattern: "[a&b]",
                input: "c",
                expected: false,
            },
            PatternCase {
                pattern: "(?iu)[a&b]",
                input: "&",
                expected: true,
            },
            PatternCase {
                pattern: "a{2}",
                input: "aa",
                expected: true,
            },
            PatternCase {
                pattern: "a{2}",
                input: "a",
                expected: false,
            },
        ];
        check_client_id_patterns(&cases);
    }

    /// `(?i)` in a selector folds ASCII case only, as `Pattern.CASE_INSENSITIVE`
    /// does. Long s (U+017F) and the Kelvin sign (U+212A) match `s` and `k`
    /// only under `(?u)`, which `Pattern.UNICODE_CASE` sets. Each row is
    /// `Matcher.matches` on JDK 17.
    #[test]
    fn match_patterns_fold_ascii_case_unless_unicode_case_is_on() {
        // (pattern, input, whole input matches)
        let cases = [
            PatternCase {
                pattern: "(?i)app-",
                input: "APP-",
                expected: true,
            },
            PatternCase {
                pattern: "(?i)service-.*",
                input: "SERVICE-1",
                expected: true,
            },
            PatternCase {
                pattern: "(?i)service-.*",
                input: "\u{17f}ervice-1",
                expected: false,
            },
            PatternCase {
                pattern: "(?iu)service-.*",
                input: "\u{17f}ervice-1",
                expected: true,
            },
            PatternCase {
                pattern: "(?i)k",
                input: "\u{212a}",
                expected: false,
            },
            PatternCase {
                pattern: "(?i)[a-c]+",
                input: "AbC",
                expected: true,
            },
            PatternCase {
                pattern: "(?i)[^a]",
                input: "A",
                expected: false,
            },
            PatternCase {
                pattern: "(?i)\\p{Lower}+",
                input: "aBc",
                expected: true,
            },
            PatternCase {
                pattern: "(?i)(?<Id>app)-\\d",
                input: "APP-1",
                expected: true,
            },
            PatternCase {
                pattern: "app(?i)-x",
                input: "app-X",
                expected: true,
            },
            PatternCase {
                pattern: "app(?i)-x",
                input: "APP-X",
                expected: false,
            },
            // A quoted member takes part in a range, a `]` first in a class may
            // start one, `\p{Lower}` and `\w` stay ASCII under `(?iu)`, and a
            // backreference folds ASCII case.
            PatternCase {
                pattern: "(?i)[\\Qa\\E-c]",
                input: "B",
                expected: true,
            },
            PatternCase {
                pattern: "(?i)[\\Qa\\E-c]",
                input: "X",
                expected: false,
            },
            PatternCase {
                pattern: "(?i)[]-c]",
                input: "A",
                expected: true,
            },
            PatternCase {
                pattern: "(?i)[]-c]",
                input: "X",
                expected: false,
            },
            PatternCase {
                pattern: "(?iu)\\p{Lower}",
                input: "\u{17f}",
                expected: false,
            },
            PatternCase {
                pattern: "(?iu)\\p{Lower}",
                input: "A",
                expected: true,
            },
            PatternCase {
                pattern: "(?iu)\\w",
                input: "\u{17f}",
                expected: false,
            },
            PatternCase {
                pattern: "(?i)(a)\\1",
                input: "aA",
                expected: true,
            },
            PatternCase {
                pattern: "(?i)(a)\\1",
                input: "ab",
                expected: false,
            },
            PatternCase {
                pattern: "(?i)(\\d+)-\\1",
                input: "12-12",
                expected: true,
            },
            PatternCase {
                pattern: "(?i)(\\d+)-\\1",
                input: "12-13",
                expected: false,
            },
        ];
        check_client_id_patterns(&cases);
    }

    /// A selector reads the constructs `fancy_regex` reads differently from
    /// Java as `java.util.regex` does: a backreference under a case flag
    /// compares by Java's fold, `\R` gives back the `\n` of `\r\n` where Java's
    /// does, and a group keeps the text it matched in a look-around or a
    /// possessive quantifier. Each row is `Matcher.matches` on JDK 25.0.4.
    #[test]
    fn match_patterns_read_references_and_linebreaks_as_java_does() {
        // (pattern, input, whole input matches)
        let cases = [
            ("(?i)(\u{e9})\\1", "\u{e9}\u{c9}", false),
            ("(?i)(a\u{e9})\\1", "a\u{e9}A\u{e9}", true),
            ("(?iu)(i)\\1", "i\u{130}", true),
            ("(?iu)(s)\\1", "s\u{17f}", true),
            ("(?iu)(k)\\1", "k\u{212a}", true),
            ("(?iu)(.)\\1", "\u{10400}\u{10428}", true),
            ("(a)\\R\\n\\1", "a\r\na", true),
            ("\\R\\n", "\r\n", true),
            ("\\R{2}", "\r\n", false),
            // `(?=` and `(?<=` hold the `=` the entry splits on, so a selector
            // has only the negated look-arounds
            ("(?:(?!(a))|a)\\1", "aa", true),
            ("(?:(?>(a))x|a)\\1", "aa", true),
            ("(?:(a)++x|a)\\1", "aa", true),
            ("(?:(a)x|a)\\1", "aa", false),
            ("(a|b\\1)+", "abb", false),
        ];
        for (pattern, input, expected) in cases {
            let rules = parse_match_rules(&format!("client_id={pattern}")).unwrap();
            check!(
                rules[0].pattern.matches(input).ok() == Some(expected),
                "{pattern:?} on {input:?}"
            );
        }
    }

    /// `(?iu)` in a selector folds Unicode case as Java does, by
    /// `Character.toLowerCase` of `Character.toUpperCase`, in a class as well:
    /// `\w` and `\p{Lower}` in a class do not match long s or the Kelvin sign, a
    /// range that has `K` and not `k` does not match the Kelvin sign, and `İ`
    /// matches `i`. The POSIX classes are ASCII unless `(?U)` is on. Each row
    /// is `Matcher.matches` on JDK 17.
    #[test]
    fn match_patterns_fold_unicode_case_as_java_does() {
        // (pattern, input, whole input matches)
        let cases = [
            PatternCase {
                pattern: "(?iu)[\\w]",
                input: "\u{17f}",
                expected: false,
            },
            PatternCase {
                pattern: "(?iu)[\\w-]",
                input: "\u{212a}",
                expected: false,
            },
            PatternCase {
                pattern: "(?iu)[\\w-]",
                input: "-",
                expected: true,
            },
            PatternCase {
                pattern: "(?iu)[\\W]",
                input: "\u{17f}",
                expected: true,
            },
            PatternCase {
                pattern: "(?iu)[\\p{Lower}]",
                input: "\u{17f}",
                expected: false,
            },
            PatternCase {
                pattern: "(?iu)[\\p{Upper}]",
                input: "\u{212a}",
                expected: false,
            },
            PatternCase {
                pattern: "(?iu)[a-z]",
                input: "\u{212a}",
                expected: true,
            },
            PatternCase {
                pattern: "(?iu)[A-Z]",
                input: "\u{212a}",
                expected: false,
            },
            PatternCase {
                pattern: "(?iu)[A-K]",
                input: "\u{212a}",
                expected: false,
            },
            PatternCase {
                pattern: "(?iu)[A-c]",
                input: "\u{212a}",
                expected: false,
            },
            PatternCase {
                pattern: "(?iu)[A-Z]",
                input: "\u{17f}",
                expected: true,
            },
            PatternCase {
                pattern: "(?iu)[a-z]",
                input: "\u{130}",
                expected: true,
            },
            PatternCase {
                pattern: "(?iu)[A-Z]",
                input: "\u{130}",
                expected: false,
            },
            PatternCase {
                pattern: "(?iu)[a-z&&[^k]]",
                input: "\u{212a}",
                expected: false,
            },
            PatternCase {
                pattern: "(?iu)i",
                input: "\u{130}",
                expected: true,
            },
            PatternCase {
                pattern: "(?iu)[i]",
                input: "\u{131}",
                expected: true,
            },
            PatternCase {
                pattern: "(?iu)service-.*",
                input: "\u{17f}ervice-1",
                expected: true,
            },
            PatternCase {
                pattern: "(?iu)[\\x{391}-\\x{3a9}]",
                input: "\u{3c2}",
                expected: true,
            },
            PatternCase {
                pattern: "\\p{Alpha}",
                input: "\u{e9}",
                expected: false,
            },
            PatternCase {
                pattern: "\\p{Alnum}+",
                input: "a1",
                expected: true,
            },
            PatternCase {
                pattern: "\\p{Punct}",
                input: "\u{a1}",
                expected: false,
            },
            PatternCase {
                pattern: "[\\p{Digit}]",
                input: "\u{663}",
                expected: false,
            },
            PatternCase {
                pattern: "(?U)\\p{Alpha}",
                input: "\u{e9}",
                expected: true,
            },
            PatternCase {
                pattern: "(?U)\\p{Digit}",
                input: "\u{663}",
                expected: true,
            },
        ];
        check_client_id_patterns(&cases);
    }

    /// Java's inline flags, each case checked against `Pattern.compile` and
    /// `Matcher.matches` on JDK 21: `None` is a refusal, otherwise the
    /// inputs the compiled pattern must and must not fully match.
    #[test]
    fn java_inline_flags_mean_what_they_mean_in_java() {
        /// An input and whether the compiled pattern fully matches it.
        struct Input {
            text: &'static str,
            matches: bool,
        }
        /// A pattern, and the inputs it must and must not match, or `None` for
        /// a pattern Java refuses.
        struct Case {
            pattern: &'static str,
            inputs: Option<&'static [Input]>,
        }
        let cases: [Case; 18] = [
            Case {
                pattern: "(?d)^foo$",
                inputs: Some(&[
                    Input {
                        text: "foo",
                        matches: true,
                    },
                    Input {
                        text: "xfoo",
                        matches: false,
                    },
                ]),
            },
            Case {
                pattern: "(?i-d)foo",
                inputs: Some(&[Input {
                    text: "FOO",
                    matches: true,
                }]),
            },
            Case {
                pattern: "(?U)\\w+",
                inputs: Some(&[
                    Input {
                        text: "é",
                        matches: true,
                    },
                    Input {
                        text: "a b",
                        matches: false,
                    },
                ]),
            },
            // `U` swapped greediness would make the atomic group take both.
            Case {
                pattern: "(?U)(?>a+?)a",
                inputs: Some(&[Input {
                    text: "aa",
                    matches: true,
                }]),
            },
            Case {
                pattern: "(?iu)é",
                inputs: Some(&[
                    Input {
                        text: "É",
                        matches: true,
                    },
                    Input {
                        text: "e",
                        matches: false,
                    },
                ]),
            },
            Case {
                pattern: "(?-i)A",
                inputs: Some(&[
                    Input {
                        text: "A",
                        matches: true,
                    },
                    Input {
                        text: "a",
                        matches: false,
                    },
                ]),
            },
            Case {
                pattern: "(?i)(?-i)A",
                inputs: Some(&[
                    Input {
                        text: "A",
                        matches: true,
                    },
                    Input {
                        text: "a",
                        matches: false,
                    },
                ]),
            },
            Case {
                pattern: "(?i:abc)d",
                inputs: Some(&[
                    Input {
                        text: "ABCd",
                        matches: true,
                    },
                    Input {
                        text: "ABCD",
                        matches: false,
                    },
                ]),
            },
            Case {
                pattern: "(?-:a)b",
                inputs: Some(&[Input {
                    text: "ab",
                    matches: true,
                }]),
            },
            Case {
                pattern: "(?)a",
                inputs: Some(&[Input {
                    text: "a",
                    matches: true,
                }]),
            },
            Case {
                pattern: "(?c)a",
                inputs: Some(&[Input {
                    text: "a",
                    matches: true,
                }]),
            },
            Case {
                pattern: "(?ix)A b",
                inputs: Some(&[
                    Input {
                        text: "ab",
                        matches: true,
                    },
                    Input {
                        text: "a b",
                        matches: false,
                    },
                ]),
            },
            Case {
                pattern: "(?x)\\Q a#\\E",
                inputs: Some(&[
                    Input {
                        text: " a#",
                        matches: true,
                    },
                    Input {
                        text: "a",
                        matches: false,
                    },
                ]),
            },
            Case {
                pattern: "(?q)a",
                inputs: None,
            },
            Case {
                pattern: "(?i--x)a",
                inputs: None,
            },
            Case {
                pattern: "(?-i-x)a",
                inputs: None,
            },
            Case {
                pattern: "(?i",
                inputs: None,
            },
            Case {
                pattern: "a(?d)*",
                inputs: None,
            },
        ];
        for Case { pattern, inputs } in cases {
            let entry = format!("client_id={pattern}");
            match (parse_match_rules(&entry), inputs) {
                (Ok(rules), Some(inputs)) => {
                    for &Input {
                        text: input,
                        matches,
                    } in inputs
                    {
                        check!(
                            rules[0].pattern.matches(input).unwrap() == matches,
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
                    check!(result.is_ok() == inputs.is_some(), "{pattern}");
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
