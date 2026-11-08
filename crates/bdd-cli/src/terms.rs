//! `krabka-bdd glossary`: an advisory report on the glossary's domain terms
//! against the words steps use.
//!
//! A candidate term is a lowercase word, or two adjacent ones written with a
//! `-` between them, that enough steps use and the glossary does not define.
//! The heuristic is deliberately plain; a reader decides which candidates are
//! domain words.

use {
    crate::{Report, feature::Loaded},
    gherkin::{Feature, Step},
    krabka_bdd_model::glossary::Glossary,
    std::{
        collections::{BTreeMap, BTreeSet},
        fmt::Write,
    },
};

/// Words that say nothing about the domain, the step keywords among them.
const STOPWORDS: &[&str] = &[
    "given", "when", "then", "and", "but", "the", "a", "an", "is", "are", "was", "were", "be",
    "been", "being", "has", "have", "had", "having", "it", "its", "of", "to", "in", "on", "at",
    "for", "with", "without", "from", "by", "as", "that", "this", "these", "those", "which", "who",
    "whose", "not", "no", "nor", "or", "if", "than", "so", "into", "onto", "over", "under",
    "after", "before", "again", "each", "every", "all", "any", "some", "one", "two", "three",
    "four", "five", "their", "there", "they", "them", "does", "do", "did", "done", "can", "cannot",
    "will", "would", "should", "may", "must", "only", "also", "still", "same", "other", "another",
    "more", "most", "less", "least", "now", "yet", "both", "either", "neither", "such", "own",
    "per", "via", "between", "through", "while", "until", "up", "down", "out", "off", "about",
    "above", "below", "back", "first", "second", "last", "next", "new", "old", "returns",
    "contains", "holds", "gets", "uses", "says",
];

/// Shorter words are rarely domain terms.
const MIN_WORD_LEN: usize = 3;

/// What the glossary report found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TermReport {
    /// Candidate terms with the number of steps that use each, most used
    /// first and then by name.
    pub candidates: Vec<(String, usize)>,
    /// Glossary terms no step uses, by name.
    pub unused: Vec<String>,
}

/// The text of every step in `feature`, Backgrounds included.
#[must_use]
pub fn step_texts(feature: &Feature) -> Vec<String> {
    let mut steps: Vec<&Step> = Vec::new();
    steps.extend(
        feature
            .background
            .iter()
            .flat_map(|background| &background.steps),
    );
    steps.extend(
        feature
            .scenarios
            .iter()
            .flat_map(|scenario| &scenario.steps),
    );
    for rule in &feature.rules {
        steps.extend(
            rule.background
                .iter()
                .flat_map(|background| &background.steps),
        );
        steps.extend(rule.scenarios.iter().flat_map(|scenario| &scenario.steps));
    }
    steps.into_iter().map(|step| step.value.clone()).collect()
}

/// The words of a step, lowercase as written: `<placeholders>` are dropped,
/// and a word is a run of letters, digits, `-` and `_`.
#[must_use]
pub fn words(step: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut current = String::new();
    let mut in_placeholder = false;
    for character in step.chars() {
        if in_placeholder {
            in_placeholder = character != '>';
            continue;
        }
        if character.is_alphanumeric() || character == '-' || character == '_' {
            current.push(character);
            continue;
        }
        in_placeholder = character == '<';
        flush(&mut current, &mut words);
    }
    flush(&mut current, &mut words);
    words
}

fn flush(current: &mut String, words: &mut Vec<String>) {
    let word = current.trim_matches('-');
    if !word.is_empty() {
        words.push(word.to_owned());
    }
    current.clear();
}

/// Whether `word` could be part of a term: lowercase letters and `-` only,
/// long enough and no stopword.
fn is_term_word(word: &str) -> bool {
    word.chars().count() >= MIN_WORD_LEN
        && word
            .chars()
            .all(|character| character.is_ascii_lowercase() || character == '-')
        && !STOPWORDS.contains(&word)
}

/// The candidate terms one step offers: each term word, and each pair of
/// adjacent term words joined by `-`.
#[must_use]
pub fn candidates(step: &str) -> BTreeSet<String> {
    let words = words(step);
    let mut found = BTreeSet::new();
    let mut previous: Option<&str> = None;
    for word in &words {
        if is_term_word(word) {
            found.insert(word.clone());
            if let Some(before) = previous {
                found.insert(format!("{before}-{word}"));
            }
            previous = Some(word);
        } else {
            previous = None;
        }
    }
    found
}

/// Whether `candidate` is `term` or its plural, with `-` and a space the
/// same.
#[must_use]
pub fn names_term(candidate: &str, term: &str) -> bool {
    let candidate = candidate.replace(' ', "-");
    let term = term.replace(' ', "-");
    candidate == term
        || candidate.strip_suffix('s') == Some(term.as_str())
        || candidate.strip_suffix("es") == Some(term.as_str())
}

/// Whether `step` uses `term`, whose `-` means a space, or its plural.
#[must_use]
pub fn step_uses(step: &str, term: &str) -> bool {
    let phrase: Vec<String> = words(step)
        .iter()
        .flat_map(|word| {
            word.to_lowercase()
                .split('-')
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .collect();
    let term_words: Vec<&str> = term.split('-').collect();
    let width = term_words.len();
    phrase.windows(width).any(|window| {
        window
            .iter()
            .zip(&term_words)
            .enumerate()
            .all(|(index, (word, wanted))| {
                if index + 1 == width {
                    names_term(word, wanted)
                } else {
                    word == wanted
                }
            })
    })
}

/// Compares the words `steps` use with the glossary's terms.
#[must_use]
pub fn report(steps: &[String], glossary: &Glossary, min_steps: usize) -> TermReport {
    let mut uses: BTreeMap<String, usize> = BTreeMap::new();
    for step in steps {
        for candidate in candidates(step) {
            *uses.entry(candidate).or_default() += 1;
        }
    }
    let mut candidates: Vec<(String, usize)> = uses
        .into_iter()
        .filter(|(_, count)| *count >= min_steps)
        .filter(|(candidate, _)| {
            !glossary
                .terms
                .keys()
                .any(|term| names_term(candidate, term))
        })
        .collect();
    candidates.sort_by(|left, right| right.1.cmp(&left.1).then_with(|| left.0.cmp(&right.0)));
    let unused = glossary
        .terms
        .keys()
        .filter(|term| !steps.iter().any(|step| step_uses(step, term)))
        .cloned()
        .collect();
    TermReport { candidates, unused }
}

/// The glossary report over `loaded`, as Markdown. It never fails: every
/// finding is advisory, and a file that does not parse adds no steps.
#[must_use]
pub fn render(loaded: &[Loaded], glossary: &Glossary, min_steps: usize) -> Report {
    let steps: Vec<String> = loaded
        .iter()
        .filter_map(|entry| entry.feature.as_ref().ok())
        .flat_map(step_texts)
        .collect();
    let found = report(&steps, glossary, min_steps);
    let mut text = String::from("# Glossary report\n\n");
    // Writing to a `String` cannot fail.
    let _ = writeln!(
        text,
        "{} steps in {} feature files, against {} glossary terms.\n",
        steps.len(),
        loaded.len(),
        glossary.terms.len()
    );
    let _ = writeln!(
        text,
        "## Candidate terms\n\nWords at least {min_steps} steps use that the glossary does not \
         define.\n"
    );
    if found.candidates.is_empty() {
        text.push_str("None.\n");
    } else {
        text.push_str("| Term | Steps |\n| :--- | ---: |\n");
        for (candidate, count) in &found.candidates {
            let _ = writeln!(text, "| {candidate} | {count} |");
        }
    }
    text.push_str("\n## Unused terms\n\nGlossary terms no step uses.\n\n");
    if found.unused.is_empty() {
        text.push_str("None.\n");
    } else {
        for term in &found.unused {
            let _ = writeln!(text, "- `{term}`");
        }
    }
    Report {
        text,
        failed: false,
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert, krabka_bdd_model::glossary::Term, rstest::rstest};

    #[rstest]
    #[case::plain("a topic has been created", &["a", "topic", "has", "been", "created"])]
    #[case::placeholder_dropped("the append is <outcome> now", &["the", "append", "is", "now"])]
    #[case::hyphen_and_code("log-start offset of INVALID_EPOCH, 42", &["log-start", "offset", "of", "INVALID_EPOCH", "42"])]
    #[case::stray_dashes("a value - then -x", &["a", "value", "then", "x"])]
    fn words_split_a_step(#[case] step: &str, #[case] expected: &[&str]) {
        assert!(words(step) == expected);
    }

    #[rstest]
    #[case::words_and_bigrams(
        "the leader epoch is bumped",
        &["bumped", "epoch", "leader", "leader-epoch"],
    )]
    #[case::stopword_breaks_a_pair("a segment of records", &["records", "segment"])]
    #[case::codes_and_short_words_are_no_terms("an ACL on Topic ab", &[])]
    #[case::hyphenated_word("the high-watermark moves", &["high-watermark", "high-watermark-moves", "moves"])]
    fn candidates_of_a_step(#[case] step: &str, #[case] expected: &[&str]) {
        let expected: BTreeSet<String> = expected.iter().map(|word| (*word).to_owned()).collect();
        assert!(candidates(step) == expected);
    }

    #[rstest]
    #[case::same("partition", "partition", true)]
    #[case::plural_s("partitions", "partition", true)]
    #[case::plural_es("indexes", "index", true)]
    #[case::bigram("leader-epochs", "leader-epoch", true)]
    #[case::spaced("leader epoch", "leader-epoch", true)]
    #[case::different("partitioner", "partition", false)]
    fn names_term_allows_plurals(
        #[case] candidate: &str,
        #[case] term: &str,
        #[case] expected: bool,
    ) {
        assert!(names_term(candidate, term) == expected);
    }

    #[rstest]
    #[case::single_word("Then the Partition is gone", "partition", true)]
    #[case::two_words("the leader epochs differ", "leader-epoch", true)]
    #[case::hyphenated_in_the_step("the high-watermark moves", "high-watermark", true)]
    #[case::split_words("the leader has an epoch", "leader-epoch", false)]
    fn step_uses_a_term(#[case] step: &str, #[case] term: &str, #[case] expected: bool) {
        assert!(step_uses(step, term) == expected);
    }

    fn glossary(terms: &[&str]) -> Glossary {
        Glossary {
            tags: BTreeMap::new(),
            terms: terms
                .iter()
                .map(|term| {
                    ((*term).to_owned(), Term {
                        definition: "d".to_owned(),
                    })
                })
                .collect(),
        }
    }

    #[test]
    fn report_lists_frequent_undefined_words_and_unused_terms() {
        let steps: Vec<String> = [
            "a replica has fallen behind the leader",
            "the replica fetches from the leader",
            "every replica catches up",
            "the partitions have a leader",
            "a replica rejoins",
        ]
        .iter()
        .map(|step| (*step).to_owned())
        .collect();
        let found = report(&steps, &glossary(&["partition", "leader", "segment"]), 3);
        let expected = TermReport {
            candidates: vec![("replica".to_owned(), 4)],
            unused: vec!["segment".to_owned()],
        };
        assert!(found == expected);
    }
}
