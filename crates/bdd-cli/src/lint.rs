//! `krabka-bdd lint`: the house-style lint over feature files, with an
//! optional ratchet baseline.

use {
    crate::{
        Report,
        discover::{self, FeatureFile},
        fs::{self, EntryKind, FileSystem},
    },
    krabka_bdd_model::{
        baseline::{self, Baseline, Ratchet},
        glossary::Glossary,
        lint::{self, Finding, LintContext, Severity},
    },
    std::fmt::Write,
};

/// What `krabka-bdd lint` was asked to do.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LintOptions {
    /// The files and directories to lint, taken from the current directory.
    pub paths: Vec<String>,
    /// The baseline file, taken from the current directory.
    pub baseline: Option<String>,
    /// Whether to write the current error findings to the baseline instead
    /// of comparing them with it.
    pub write_baseline: bool,
}

/// Every finding in `files`, in file order, each named by its shown path.
///
/// The lint sees each file by its manifest-relative path, and resolves a
/// `Depends on:` path against the file's manifest directory.
///
/// # Errors
///
/// Returns the message of a file that cannot be read.
pub fn findings(
    filesystem: &dyn FileSystem,
    glossary: &Glossary,
    files: &[FeatureFile],
) -> Result<Vec<Finding>, String> {
    let mut all = Vec::new();
    for file in files {
        let source = filesystem.read(&file.absolute)?;
        let file_exists = |path: &str| {
            let target = fs::absolute(&file.manifest_dir, path);
            // A file, as `scenarios!` checks: a directory is not a feature.
            filesystem.kind(&target) == Some(EntryKind::File)
        };
        let context = LintContext {
            glossary,
            file_exists: &file_exists,
        };
        all.extend(
            lint::lint(&file.lint_path, &source, &context)
                .into_iter()
                .map(|finding| Finding {
                    path: file.shown.clone(),
                    ..finding
                }),
        );
    }
    Ok(all)
}

/// Runs `krabka-bdd lint` from `cwd`.
///
/// # Errors
///
/// Returns a message for a usage error, a path that does not exist, a file
/// that cannot be read or written, or a baseline that does not parse.
pub fn run(
    filesystem: &dyn FileSystem,
    cwd: &str,
    glossary: &Glossary,
    options: &LintOptions,
) -> Result<Report, String> {
    let baseline_path = options
        .baseline
        .as_deref()
        .map(|path| fs::absolute(cwd, path));
    if options.write_baseline && baseline_path.is_none() {
        return Err("--write-baseline needs --baseline FILE".to_owned());
    }
    let files = discover::discover(filesystem, cwd, &options.paths)?;
    let findings = findings(filesystem, glossary, &files)?;
    let errors: Vec<Finding> = findings
        .iter()
        .filter(|finding| finding.severity == Severity::Error)
        .cloned()
        .collect();
    let mut text = String::new();
    for finding in &findings {
        // Writing to a `String` cannot fail.
        let _ = writeln!(text, "{finding}");
    }
    let _ = writeln!(text, "{}", summary(files.len(), &findings));
    let Some(baseline_path) = baseline_path else {
        return Ok(Report {
            text,
            failed: !errors.is_empty(),
        });
    };
    let shown = options.baseline.as_deref().unwrap_or_default();
    if options.write_baseline {
        let rendered = Baseline::render(&errors);
        filesystem.write(&baseline_path, &rendered)?;
        let _ = writeln!(text, "wrote {} accepted errors to {shown}", errors.len());
        return Ok(Report {
            text,
            failed: false,
        });
    }
    if filesystem.kind(&baseline_path) != Some(EntryKind::File) {
        return Err(format!("{shown}: no such baseline file"));
    }
    let accepted = Baseline::parse(&filesystem.read(&baseline_path)?)
        .map_err(|error| format!("{shown}: {error}"))?;
    let ratchet = baseline::ratchet(&accepted, &errors);
    text.push_str(&ratchet_text(&ratchet, errors.len(), shown));
    Ok(Report {
        text,
        failed: !ratchet.is_clean(),
    })
}

/// The closing line of a lint run.
#[must_use]
pub fn summary(files: usize, findings: &[Finding]) -> String {
    let errors = findings
        .iter()
        .filter(|finding| finding.severity == Severity::Error)
        .count();
    let advisories = findings.len() - errors;
    format!(
        "krabka-bdd lint: {files} {}, {errors} {}, {advisories} {}",
        plural(files, "feature file", "feature files"),
        plural(errors, "error", "errors"),
        plural(advisories, "advisory", "advisories"),
    )
}

/// What the ratchet found, as the lines that follow the summary.
#[must_use]
pub fn ratchet_text(ratchet: &Ratchet, errors: usize, baseline: &str) -> String {
    let mut text = String::new();
    if !ratchet.new.is_empty() {
        let _ = writeln!(text, "new:");
        for finding in &ratchet.new {
            let _ = writeln!(text, "  {finding}");
        }
    }
    if !ratchet.gone.is_empty() {
        let _ = writeln!(text, "fixed, remove from the baseline:");
        for entry in &ratchet.gone {
            let _ = writeln!(
                text,
                "  {}: {}: {} ({} fewer)",
                entry.path, entry.rule, entry.message, entry.count
            );
        }
    }
    let gone: usize = ratchet.gone.iter().map(|entry| entry.count).sum();
    let accepted = errors - ratchet.new.len();
    let _ = writeln!(
        text,
        "baseline {baseline}: {accepted} accepted, {} new, {gone} fixed",
        ratchet.new.len()
    );
    text
}

fn plural(count: usize, one: &'static str, many: &'static str) -> &'static str {
    if count == 1 { one } else { many }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::fs::memory::MemoryFs,
        assert2::assert,
        krabka_bdd_model::{baseline::Entry, lint::LintRule},
        rstest::rstest,
    };

    const CLEAN: &str = "\
Feature: Segments roll on size

  Scenario: A full segment rolls
    Given a segment has reached its size limit
    When a batch is appended
    Then a new segment is opened
";

    const TWO_WHENS: &str = "\
Feature: Topics are created

  Scenario: A topic is created twice
    Given no topic exists
    When a topic is created
    When the same topic is created
    Then the second request fails with TOPIC_ALREADY_EXISTS
";

    const DEPENDS: &str = "\
Feature: Compaction keeps the last value
  Depends on: Segments roll - ../roll/roll.feature
  Depends on: Missing - ../gone/gone.feature

  Scenario: An older value is removed
    Given a key has been written twice
    When the cleaner runs
    Then only the newer value remains
";

    const ADVISORY: &str = "\
Feature: Fetches stop at the high watermark

  Scenario: A fetch past the watermark is empty
    Given a partition has records and a high watermark of 3
    When a consumer fetches from offset 3
    Then the fetch returns no records
";

    fn repo(baseline: Option<&str>) -> MemoryFs {
        let mut files = vec![
            ("/repo/Cargo.toml", "[workspace]"),
            ("/repo/crates/log/Cargo.toml", "[package]"),
            ("/repo/crates/log/tests/features/roll/roll.feature", CLEAN),
            (
                "/repo/crates/log/tests/features/compact/compact.feature",
                DEPENDS,
            ),
            (
                "/repo/crates/log/tests/features/fetch/fetch.feature",
                ADVISORY,
            ),
            ("/repo/crates/topics/Cargo.toml", "[package]"),
            (
                "/repo/crates/topics/tests/features/create/create.feature",
                TWO_WHENS,
            ),
        ];
        if let Some(text) = baseline {
            files.push(("/repo/baseline.tsv", text));
        }
        MemoryFs::new(files)
    }

    fn options(baseline: Option<&str>, write_baseline: bool) -> LintOptions {
        LintOptions {
            paths: vec!["crates".to_owned()],
            baseline: baseline.map(str::to_owned),
            write_baseline,
        }
    }

    fn glossary() -> Glossary { Glossary::embedded().unwrap().clone() }

    #[test]
    fn findings_carry_the_shown_path_and_resolve_depends_on_in_the_crate() {
        let filesystem = repo(None);
        let files = discover::discover(&filesystem, "/repo", &["crates".to_owned()]).unwrap();
        let found = findings(&filesystem, &glossary(), &files).unwrap();
        let summary: Vec<(String, LintRule, Severity)> = found
            .iter()
            .map(|finding| (finding.path.clone(), finding.rule, finding.severity))
            .collect();
        let expected = vec![
            (
                "crates/log/tests/features/compact/compact.feature".to_owned(),
                LintRule::DependsOn,
                Severity::Error,
            ),
            (
                "crates/log/tests/features/fetch/fetch.feature".to_owned(),
                LintRule::AndInStep,
                Severity::Advisory,
            ),
            (
                "crates/topics/tests/features/create/create.feature".to_owned(),
                LintRule::OneWhen,
                Severity::Error,
            ),
        ];
        assert!(summary == expected);
        let depends = &found[0];
        assert!(depends.line == 3);
        assert!(
            depends.message
                == "dependency `Missing` at `../gone/gone.feature` does not exist, as \
                    `tests/features/gone/gone.feature`"
        );
    }

    #[test]
    fn a_dependency_that_names_a_directory_does_not_exist() {
        let filesystem = MemoryFs::new(vec![
            ("/repo/Cargo.toml", "[workspace]"),
            ("/repo/crates/log/Cargo.toml", "[package]"),
            ("/repo/crates/log/tests/features/roll/roll.feature", CLEAN),
            (
                "/repo/crates/log/tests/features/compact/compact.feature",
                "Feature: Compaction keeps the last value\n  Depends on: Segments roll - ../roll\n\n  \
                 Scenario: An older value is removed\n    Given a key has been written twice\n    \
                 When the cleaner runs\n    Then only the newer value remains\n",
            ),
        ]);
        let files = discover::discover(&filesystem, "/repo", &["crates".to_owned()]).unwrap();
        let found: Vec<LintRule> = findings(&filesystem, &glossary(), &files)
            .unwrap()
            .iter()
            .map(|finding| finding.rule)
            .collect();
        assert!(found == vec![LintRule::DependsOn]);
    }

    #[rstest]
    #[case::no_findings(0, &[], "krabka-bdd lint: 0 feature files, 0 errors, 0 advisories")]
    #[case::singular(1, &[Severity::Error, Severity::Advisory], "krabka-bdd lint: 1 feature file, 1 error, 1 advisory")]
    #[case::plural(3, &[Severity::Error, Severity::Error, Severity::Advisory, Severity::Advisory], "krabka-bdd lint: 3 feature files, 2 errors, 2 advisories")]
    fn summary_counts_files_errors_and_advisories(
        #[case] files: usize,
        #[case] severities: &[Severity],
        #[case] expected: &str,
    ) {
        let found: Vec<Finding> = severities
            .iter()
            .map(|severity| Finding {
                rule: LintRule::OneWhen,
                severity: *severity,
                path: "a.feature".to_owned(),
                line: 1,
                col: 1,
                message: "m".to_owned(),
            })
            .collect();
        assert!(summary(files, &found) == expected);
    }

    #[test]
    fn without_a_baseline_an_error_fails_the_run() {
        let report = run(&repo(None), "/repo", &glossary(), &options(None, false)).unwrap();
        assert!(report.failed);
        let lines: Vec<&str> = report.text.lines().collect();
        assert!(lines.len() == 4);
        assert!(
            lines[0]
                .starts_with("crates/log/tests/features/compact/compact.feature:3:3: depends-on: ")
        );
        assert!(lines[1].starts_with(
            "crates/log/tests/features/fetch/fetch.feature:4:5: and-in-step (advisory): "
        ));
        assert!(lines[3] == "krabka-bdd lint: 4 feature files, 2 errors, 1 advisory");
    }

    #[test]
    fn advisories_alone_pass() {
        let mut options = options(None, false);
        options.paths = vec!["crates/log/tests/features/fetch".to_owned()];
        let report = run(&repo(None), "/repo", &glossary(), &options).unwrap();
        assert!(!report.failed);
    }

    #[test]
    fn a_written_baseline_round_trips_clean() {
        let filesystem = repo(None);
        let written = run(
            &filesystem,
            "/repo",
            &glossary(),
            &options(Some("baseline.tsv"), true),
        )
        .unwrap();
        assert!(!written.failed);
        assert!(
            written
                .text
                .ends_with("wrote 2 accepted errors to baseline.tsv\n")
        );
        let accepted = Baseline::parse(&filesystem.file("/repo/baseline.tsv").unwrap()).unwrap();
        let paths: Vec<&str> = accepted
            .entries
            .iter()
            .map(|entry| entry.path.as_str())
            .collect();
        assert!(
            paths
                == vec![
                    "crates/log/tests/features/compact/compact.feature",
                    "crates/topics/tests/features/create/create.feature",
                ]
        );
        let checked = run(
            &filesystem,
            "/repo",
            &glossary(),
            &options(Some("baseline.tsv"), false),
        )
        .unwrap();
        assert!(!checked.failed);
        assert!(
            checked
                .text
                .ends_with("baseline baseline.tsv: 2 accepted, 0 new, 0 fixed\n")
        );
    }

    #[test]
    fn a_new_finding_and_a_fixed_entry_fail_the_ratchet() {
        let baseline = "\
crates/topics/tests/features/create/create.feature\tone-when\tscenario `A topic is created twice` has 2 `When` steps, and exactly one is allowed\t1
crates/log/tests/features/roll/roll.feature\tmax-steps\tscenario `A full segment rolls` has 6 steps\t1
";
        let report = run(
            &repo(Some(baseline)),
            "/repo",
            &glossary(),
            &options(Some("baseline.tsv"), false),
        )
        .unwrap();
        assert!(report.failed);
        let tail: Vec<&str> = report.text.lines().skip(4).collect();
        assert!(tail.len() == 5);
        assert!(tail[0] == "new:");
        assert!(
            tail[1].starts_with(
                "  crates/log/tests/features/compact/compact.feature:3:3: depends-on: "
            )
        );
        assert!(tail[2] == "fixed, remove from the baseline:");
        assert!(
            tail[3]
                == "  crates/log/tests/features/roll/roll.feature: max-steps: scenario `A full \
                    segment rolls` has 6 steps (1 fewer)"
        );
        assert!(tail[4] == "baseline baseline.tsv: 1 accepted, 1 new, 1 fixed");
    }

    #[test]
    fn ratchet_text_of_a_clean_ratchet_is_one_line() {
        let ratchet = Ratchet {
            new: Vec::new(),
            gone: Vec::new(),
        };
        assert!(
            ratchet_text(&ratchet, 7, "b.tsv") == "baseline b.tsv: 7 accepted, 0 new, 0 fixed\n"
        );
        let gone = Ratchet {
            new: Vec::new(),
            gone: vec![Entry {
                path: "a.feature".to_owned(),
                rule: "one-when".to_owned(),
                message: "m".to_owned(),
                count: 2,
            }],
        };
        assert!(
            ratchet_text(&gone, 0, "b.tsv")
                == "fixed, remove from the baseline:\n  a.feature: one-when: m (2 fewer)\nbaseline \
                    b.tsv: 0 accepted, 0 new, 2 fixed\n"
        );
    }

    #[rstest]
    #[case::write_without_a_file(
        options(None, true),
        None,
        "--write-baseline needs --baseline FILE"
    )]
    #[case::missing_baseline(
        options(Some("none.tsv"), false),
        None,
        "none.tsv: no such baseline file"
    )]
    #[case::malformed_baseline(
        options(Some("baseline.tsv"), false),
        Some("only\ttwo\n"),
        "baseline.tsv: baseline line 1 is not `path<TAB>rule-id<TAB>message<TAB>count`: only\ttwo"
    )]
    fn usage_and_baseline_errors(
        #[case] options: LintOptions,
        #[case] baseline: Option<&str>,
        #[case] expected: &str,
    ) {
        let result = run(&repo(baseline), "/repo", &glossary(), &options);
        assert!(result == Err(expected.to_owned()));
    }
}
