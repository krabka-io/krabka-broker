//! The lint ratchet: findings a crate already had when a rule arrived.
//!
//! A baseline file lists the findings that are accepted for now, one entry per
//! line as `path<TAB>rule-id<TAB>message<TAB>count`, without line numbers so
//! that an edit elsewhere in the file does not move them. [`ratchet`] fails a
//! finding the baseline does not hold, and an entry that no longer fires, so
//! the baseline only shrinks.

use {
    crate::lint::Finding,
    std::{collections::BTreeMap, fmt::Write},
};

/// The comment that opens a rendered baseline.
const HEADER: &str = "# krabka-bdd lint baseline: path<TAB>rule-id<TAB>message<TAB>count\n";

/// One accepted finding, and how many times it fires.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Entry {
    /// The manifest-relative path of the feature file.
    pub path: String,
    /// The kebab-case rule id.
    pub rule: String,
    pub message: String,
    /// How many findings in the file have this rule and message.
    pub count: usize,
}

/// The accepted findings, in path, rule and message order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Baseline {
    pub entries: Vec<Entry>,
}

/// What a run of the lint changed against its baseline.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Ratchet {
    /// Findings beyond the counts the baseline accepts.
    pub new: Vec<Finding>,
    /// Baseline entries that fire fewer times than they say, with the count
    /// that no longer fires.
    pub gone: Vec<Entry>,
}

impl Ratchet {
    /// Whether the findings match the baseline exactly.
    #[must_use]
    pub const fn is_clean(&self) -> bool { self.new.is_empty() && self.gone.is_empty() }
}

/// The key a finding and an entry are matched by.
type Key = (String, String, String);

impl Baseline {
    /// Parses a baseline file. Blank lines and lines that start with `#` are
    /// skipped; entries with the same path, rule and message add up.
    ///
    /// # Errors
    ///
    /// Returns a message naming the first line that is not four tab-separated
    /// fields ending in a positive count.
    pub fn parse(text: &str) -> Result<Self, String> {
        let mut counts: BTreeMap<Key, usize> = BTreeMap::new();
        for (index, line) in text.lines().enumerate() {
            if line.trim().is_empty() || line.starts_with('#') {
                continue;
            }
            let fields: Vec<&str> = line.split('\t').collect();
            let entry = match fields.as_slice() {
                [path, rule, message, count] => count
                    .parse::<usize>()
                    .ok()
                    .filter(|count| *count > 0)
                    .map(|count| ((unescape(path), unescape(rule), unescape(message)), count)),
                _ => None,
            };
            let Some((key, count)) = entry else {
                return Err(format!(
                    "baseline line {} is not `path<TAB>rule-id<TAB>message<TAB>count`: {line}",
                    index + 1
                ));
            };
            *counts.entry(key).or_default() += count;
        }
        Ok(Self {
            entries: entries(counts),
        })
    }

    /// The baseline file that accepts exactly `findings`.
    #[must_use]
    pub fn render(findings: &[Finding]) -> String {
        let mut text = HEADER.to_owned();
        for entry in entries(count(findings)) {
            // Writing to a `String` cannot fail.
            let _ = writeln!(
                text,
                "{}\t{}\t{}\t{}",
                escape(&entry.path),
                escape(&entry.rule),
                escape(&entry.message),
                entry.count
            );
        }
        text
    }
}

/// Compares `findings` with `baseline`.
///
/// For each path, rule and message, findings past the baseline's count are
/// new, in the order given; a baseline count the findings fall short of is
/// gone by the difference.
#[must_use]
pub fn ratchet(baseline: &Baseline, findings: &[Finding]) -> Ratchet {
    let mut allowed: BTreeMap<Key, usize> = baseline
        .entries
        .iter()
        .map(|entry| {
            (
                (
                    entry.path.clone(),
                    entry.rule.clone(),
                    entry.message.clone(),
                ),
                entry.count,
            )
        })
        .collect();
    let mut new = Vec::new();
    for finding in findings {
        match allowed.get_mut(&key(finding)) {
            Some(remaining) if *remaining > 0 => *remaining -= 1,
            _ => new.push(finding.clone()),
        }
    }
    allowed.retain(|_, remaining| *remaining > 0);
    Ratchet {
        new,
        gone: entries(allowed),
    }
}

fn key(finding: &Finding) -> Key {
    (
        finding.path.clone(),
        finding.rule.id().to_owned(),
        finding.message.clone(),
    )
}

fn count(findings: &[Finding]) -> BTreeMap<Key, usize> {
    let mut counts = BTreeMap::new();
    for finding in findings {
        *counts.entry(key(finding)).or_default() += 1;
    }
    counts
}

fn entries(counts: BTreeMap<Key, usize>) -> Vec<Entry> {
    counts
        .into_iter()
        .map(|((path, rule, message), count)| Entry {
            path,
            rule,
            message,
            count,
        })
        .collect()
}

/// Escapes `\`, tab and newline so a field stays on its line.
fn escape(field: &str) -> String {
    let mut out = String::with_capacity(field.len());
    for character in field.chars() {
        match character {
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            other => out.push(other),
        }
    }
    out
}

fn unescape(field: &str) -> String {
    let mut out = String::with_capacity(field.len());
    let mut characters = field.chars();
    while let Some(character) = characters.next() {
        if character != '\\' {
            out.push(character);
            continue;
        }
        match characters.next() {
            Some('t') => out.push('\t'),
            Some('n') => out.push('\n'),
            Some(other) => out.push(other),
            None => out.push('\\'),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::lint::{LintRule, Severity},
        assert2::assert,
    };

    fn finding(path: &str, rule: LintRule, line: usize, message: &str) -> Finding {
        Finding {
            rule,
            severity: Severity::Error,
            path: path.to_owned(),
            line,
            col: 3,
            message: message.to_owned(),
        }
    }

    fn entry(path: &str, rule: &str, message: &str, count: usize) -> Entry {
        Entry {
            path: path.to_owned(),
            rule: rule.to_owned(),
            message: message.to_owned(),
            count,
        }
    }

    const LOG: &str = "tests/features/log/log.feature";
    const GROUP: &str = "tests/features/group/group.feature";

    fn findings() -> Vec<Finding> {
        vec![
            finding(LOG, LintRule::OneWhen, 4, "scenario `a` has 2 `When` steps"),
            finding(
                GROUP,
                LintRule::QuoteInStep,
                7,
                "step `a\tb \\ c` quotes a value",
            ),
            finding(LOG, LintRule::OneWhen, 9, "scenario `a` has 2 `When` steps"),
        ]
    }

    #[test]
    fn render_counts_and_escapes_then_parse_reads_it_back() {
        let text = Baseline::render(&findings());
        let expected_text = format!(
            "{HEADER}{GROUP}\tquote-in-step\tstep `a\\tb \\\\ c` quotes a value\t1\n{LOG}\tone-when\
             \tscenario `a` has 2 `When` steps\t2\n"
        );
        assert!(text == expected_text);
        let expected = Baseline {
            entries: vec![
                entry(GROUP, "quote-in-step", "step `a\tb \\ c` quotes a value", 1),
                entry(LOG, "one-when", "scenario `a` has 2 `When` steps", 2),
            ],
        };
        assert!(Baseline::parse(&text) == Ok(expected));
    }

    #[test]
    fn parse_adds_up_repeated_entries_and_skips_comments() {
        let text = format!("# comment\n\n{LOG}\tone-when\tm\t1\n{LOG}\tone-when\tm\t2\n");
        let expected = Baseline {
            entries: vec![entry(LOG, "one-when", "m", 3)],
        };
        assert!(Baseline::parse(&text) == Ok(expected));
    }

    #[test]
    fn parse_rejects_a_malformed_line() {
        let error = Baseline::parse(&format!("{LOG}\tone-when\tm\n")).unwrap_err();
        assert!(error.starts_with("baseline line 1 is not"));
        let error = Baseline::parse(&format!("{LOG}\tone-when\tm\t0\n")).unwrap_err();
        assert!(error.starts_with("baseline line 1 is not"));
    }

    #[test]
    fn ratchet_of_the_rendered_baseline_is_clean() {
        let baseline = Baseline::parse(&Baseline::render(&findings())).unwrap();
        let ratchet = ratchet(&baseline, &findings());
        assert!(ratchet == Ratchet::default());
        assert!(ratchet.is_clean());
    }

    #[test]
    fn ratchet_reports_new_findings_and_gone_entries() {
        let baseline = Baseline {
            entries: vec![
                entry(GROUP, "quote-in-step", "fixed since", 1),
                entry(LOG, "one-when", "scenario `a` has 2 `When` steps", 3),
            ],
        };
        let current = vec![
            finding(LOG, LintRule::OneWhen, 4, "scenario `a` has 2 `When` steps"),
            finding(LOG, LintRule::MaxSteps, 5, "scenario `a` has 6 steps"),
        ];
        let expected = Ratchet {
            new: vec![finding(
                LOG,
                LintRule::MaxSteps,
                5,
                "scenario `a` has 6 steps",
            )],
            gone: vec![
                entry(GROUP, "quote-in-step", "fixed since", 1),
                entry(LOG, "one-when", "scenario `a` has 2 `When` steps", 2),
            ],
        };
        assert!(ratchet(&baseline, &current) == expected);
    }

    #[test]
    fn ratchet_reports_findings_past_the_accepted_count() {
        let baseline = Baseline {
            entries: vec![entry(LOG, "one-when", "scenario `a` has 2 `When` steps", 1)],
        };
        let expected = Ratchet {
            new: vec![finding(
                LOG,
                LintRule::OneWhen,
                9,
                "scenario `a` has 2 `When` steps",
            )],
            gone: Vec::new(),
        };
        let current = vec![
            finding(LOG, LintRule::OneWhen, 4, "scenario `a` has 2 `When` steps"),
            finding(LOG, LintRule::OneWhen, 9, "scenario `a` has 2 `When` steps"),
        ];
        assert!(ratchet(&baseline, &current) == expected);
    }
}
