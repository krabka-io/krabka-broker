//! `krabka-bdd docs`: a living-documentation index of the features, grouped
//! by capability.

use {
    crate::{
        Report,
        feature::{self, JOURNEY, Loaded},
        fs,
    },
    gherkin::{Feature, Scenario},
    std::{collections::BTreeMap, fmt::Write},
};

/// The directory, relative to a crate's manifest, that holds its features.
const FEATURES_DIR: &str = "tests/features/";

/// The group of a feature that sits outside `tests/features/<capability>/`.
const NO_CAPABILITY: &str = "(outside tests/features)";

/// The capability folder of a manifest-relative feature path: the directory
/// right under `tests/features/`.
#[must_use]
pub fn capability(lint_path: &str) -> &str {
    lint_path
        .strip_prefix(FEATURES_DIR)
        .and_then(|rest| rest.split_once('/'))
        .map_or(NO_CAPABILITY, |(capability, _)| capability)
}

/// The index of `loaded`, with each feature linked by its path relative to
/// `out_dir`, the absolute directory the index is written to.
///
/// A file that does not parse is listed with a note, and fails the report.
#[must_use]
pub fn render(loaded: &[Loaded], cwd: &str, out_dir: &str) -> Report {
    let mut groups: BTreeMap<&str, Vec<&Loaded>> = BTreeMap::new();
    for entry in loaded {
        groups
            .entry(capability(&entry.file.lint_path))
            .or_default()
            .push(entry);
    }
    let mut text = String::from("# Living documentation\n\n");
    // Writing to a `String` cannot fail.
    let _ = writeln!(
        text,
        "{} features in {} capabilities.",
        loaded.len(),
        groups.len()
    );
    let mut failed = false;
    for (capability, entries) in &groups {
        let _ = writeln!(text, "\n## {capability}");
        for entry in entries {
            let link = fs::relative(out_dir, &entry.file.absolute);
            let crate_dir = fs::relative(cwd, &entry.file.manifest_dir);
            match &entry.feature {
                Ok(feature) => {
                    text.push('\n');
                    text.push_str(&feature_section(feature, &link, &crate_dir));
                }
                Err(error) => {
                    failed = true;
                    let _ = writeln!(
                        text,
                        "\n### [{}]({link})\n\nDoes not parse: {error}",
                        entry.file.shown
                    );
                }
            }
        }
    }
    Report { text, failed }
}

/// One feature's entry in the index.
#[must_use]
pub fn feature_section(feature: &Feature, link: &str, crate_dir: &str) -> String {
    let mut text = String::new();
    let _ = writeln!(text, "### [{}]({link})\n", feature.name);
    let _ = write!(text, "Crate `{crate_dir}`");
    if !feature.tags.is_empty() {
        let _ = write!(text, " · tags {}", tag_list(&feature.tags));
    }
    text.push_str("\n\n");
    let mut journeys: Vec<&Scenario> = Vec::new();
    for rule in &feature.rules {
        let _ = writeln!(text, "- Rule: {}{}", rule.name, tag_suffix(&rule.tags));
        for scenario in &rule.scenarios {
            let _ = writeln!(text, "  - {}{}", scenario.name, tag_suffix(&scenario.tags));
        }
    }
    for scenario in &feature.scenarios {
        let inherited: [&[String]; 1] = [&feature.tags];
        if feature::effective_tags(scenario, &inherited)
            .iter()
            .any(|tag| tag == JOURNEY)
        {
            journeys.push(scenario);
        } else {
            let _ = writeln!(text, "- {}{}", scenario.name, tag_suffix(&scenario.tags));
        }
    }
    if !journeys.is_empty() {
        text.push_str("- Journeys:\n");
        for scenario in journeys {
            let _ = writeln!(text, "  - {}{}", scenario.name, tag_suffix(&scenario.tags));
        }
    }
    text
}

fn tag_list(tags: &[String]) -> String {
    tags.iter()
        .map(|tag| format!("`@{tag}`"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn tag_suffix(tags: &[String]) -> String {
    if tags.is_empty() {
        String::new()
    } else {
        format!(" {}", tag_list(tags))
    }
}

#[cfg(test)]
mod tests {
    use {super::*, crate::discover::FeatureFile, assert2::assert, rstest::rstest};

    #[rstest]
    #[case::capability_folder("tests/features/log_policy/log_policy.feature", "log_policy")]
    #[case::nested_folder("tests/features/journeys/deep/a.feature", "journeys")]
    #[case::directly_under_features("tests/features/a.feature", NO_CAPABILITY)]
    #[case::elsewhere("docs/a.feature", NO_CAPABILITY)]
    fn capability_is_the_folder_under_tests_features(#[case] path: &str, #[case] expected: &str) {
        assert!(capability(path) == expected);
    }

    fn loaded(crate_dir: &str, lint_path: &str, source: &str) -> Loaded {
        let absolute = format!("/repo/{crate_dir}/{lint_path}");
        Loaded {
            file: FeatureFile {
                shown: format!("{crate_dir}/{lint_path}"),
                manifest_dir: format!("/repo/{crate_dir}"),
                lint_path: lint_path.to_owned(),
                absolute,
            },
            feature: feature::parse(source),
        }
    }

    const ROLL: &str = "\
@docker
Feature: Segments roll on size

  Rule: A full segment rolls

    @formulated
    Scenario: A segment at its limit rolls
      Given a segment has reached its size limit
      When a batch is appended
      Then a new segment is opened
";

    const JOURNEY_FEATURE: &str = "\
Feature: A consumer group survives a restart

  @journey
  Scenario: A group resumes from committed offsets
    * a consumer group has committed offsets
    * every member restarts
    * the group resumes from the committed offsets
";

    #[test]
    fn render_groups_by_capability_and_links_from_the_out_dir() {
        let features = vec![
            loaded("crates/log", "tests/features/roll/roll.feature", ROLL),
            loaded(
                "crates/group",
                "tests/features/journeys/restart.feature",
                JOURNEY_FEATURE,
            ),
            loaded(
                "crates/log",
                "tests/features/roll/broken.feature",
                "Feature without colon",
            ),
        ];
        let report = render(&features, "/repo", "/repo/docs/bdd");
        let expected = "\
# Living documentation

3 features in 2 capabilities.

## journeys

### [A consumer group survives a restart](../../crates/group/tests/features/journeys/restart.feature)

Crate `crates/group`

- Journeys:
  - A group resumes from committed offsets `@journey`

## roll

### [Segments roll on size](../../crates/log/tests/features/roll/roll.feature)

Crate `crates/log` · tags `@docker`

- Rule: A full segment rolls
  - A segment at its limit rolls `@formulated`

### [crates/log/tests/features/roll/broken.feature](../../crates/log/tests/features/roll/broken.feature)

Does not parse: ";
        assert!(report.text.starts_with(expected));
        assert!(report.failed);
    }
}
