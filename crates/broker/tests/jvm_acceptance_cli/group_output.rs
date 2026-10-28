//! Reading what `kafka-consumer-groups` printed.
//!
//! Every function here is pure: text in, a comparable value out. That is what
//! lets the oracle differential compare *answers* rather than bytes. The two
//! sides run the same binary from the same image, so their column widths and
//! their header lines are identical and worth nothing; what has to agree is
//! the offset the tool computed, the order it was reached in, and the
//! exception a refusal named.
//!
//! Parsing is deliberately loose about layout and strict about content. The
//! tool pads its table with `%-30s`-style formats that no case should depend
//! on, so a row is whitespace-split; but a row is only a row when its
//! partition and its offset are numbers, which is what stops a header, a
//! warning line or a stack frame from being read as data.

use std::collections::BTreeSet;

/// One row of the `GROUP TOPIC PARTITION NEW-OFFSET` table that
/// `--reset-offsets` prints under both `--dry-run` and `--execute`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ResetRow {
    pub(crate) group: String,
    pub(crate) topic: String,
    pub(crate) partition: i32,
    pub(crate) new_offset: i64,
}

/// Parse the reset table out of a `--reset-offsets` stdout, sorted.
///
/// Sorted because the tool walks a map: the same broker answers the same
/// partitions in a different order between two runs, and a case that compared
/// the raw order would be flaky on one side and correct on neither.
pub fn parse_reset_table(stdout: &str) -> Vec<ResetRow> {
    let mut rows: Vec<ResetRow> = stdout
        .lines()
        .filter_map(|line| {
            let fields: Vec<&str> = line.split_whitespace().collect();
            let [group, topic, partition, new_offset] = fields[..] else {
                return None;
            };
            Some(ResetRow {
                group: group.to_owned(),
                topic: topic.to_owned(),
                partition: partition.parse().ok()?,
                new_offset: new_offset.parse().ok()?,
            })
        })
        .collect();
    rows.sort();
    rows
}

/// One line of the CSV that `--reset-offsets --export` writes and
/// `--from-file` reads back.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ExportRow {
    /// `None` for the three-column form. The tool drops the group column when
    /// exactly one `--group` was named, and keeps it under `--all-groups`, so
    /// both shapes are legitimate output and the parser accepts either.
    pub(crate) group: Option<String>,
    pub(crate) topic: String,
    pub(crate) partition: i32,
    pub(crate) offset: i64,
}

/// Parse an exported offsets CSV, sorted.
///
/// The tool writes this file with a Jackson `CsvMapper`, and
/// apache/kafka:4.3.1 quotes the string columns while leaving the numeric ones
/// bare -- `"orders",0,3`. The quoting is not part of the claim these cases
/// make, so [`unquote`] takes it back off before the fields are compared.
///
/// Splitting on `,` before unquoting would mis-split a field that contained a
/// comma. A Kafka topic name cannot (`[a-zA-Z0-9._-]` only), and the group ids
/// this suite uses cannot either, so the simple split is enough here.
pub fn parse_export_csv(stdout: &str) -> Vec<ExportRow> {
    let mut rows: Vec<ExportRow> = stdout
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .filter_map(|line| {
            let fields: Vec<&str> = line.split(',').map(str::trim).collect();
            let (group, topic, partition, offset) = match fields[..] {
                [topic, partition, offset] => (None, topic, partition, offset),
                [group, topic, partition, offset] => (Some(group), topic, partition, offset),
                _ => return None,
            };
            Some(ExportRow {
                group: group.map(unquote),
                topic: unquote(topic),
                partition: unquote(partition).parse().ok()?,
                offset: unquote(offset).parse().ok()?,
            })
        })
        .collect();
    rows.sort();
    rows
}

/// One CSV field with its surrounding quotes removed, and any doubled quote
/// inside it read back as the single quote it stands for. A field that is not
/// quoted is returned as it came.
fn unquote(field: &str) -> String {
    let Some(inner) = field
        .strip_prefix('"')
        .and_then(|rest| rest.strip_suffix('"'))
    else {
        return field.to_owned();
    };
    inner.replace("\"\"", "\"")
}

/// The fully-qualified Kafka exception class names a tool printed, sorted and
/// deduplicated.
///
/// This is the part of a refusal worth comparing across the two sides. The
/// sentence after the class name is the broker's own and the two brokers word
/// theirs differently; the class is Kafka's, built by `Errors.forCode` from
/// the numeric code the broker sent, so it is exactly the claim a differential
/// about error codes wants to make.
pub fn kafka_exceptions(text: &str) -> BTreeSet<String> {
    const PREFIX: &str = "org.apache.kafka.common.errors.";
    let mut found = BTreeSet::new();
    let mut rest = text;
    while let Some((_, after)) = rest.split_once(PREFIX) {
        // The class name runs to the first character that cannot be in one.
        let (name, tail) = after
            .split_once(|c: char| !c.is_ascii_alphanumeric())
            .unwrap_or((after, ""));
        if !name.is_empty() {
            found.insert(format!("{PREFIX}{name}"));
        }
        rest = tail;
    }
    found
}

/// What `--delete` said about one group.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DeleteOutcome {
    pub(crate) group: String,
    /// The exception class the tool named, or `None` when the delete worked.
    pub(crate) failure: Option<String>,
}

/// Read the per-group outcome of a `kafka-consumer-groups --delete`.
///
/// The tool prints one of two shapes: a single success sentence naming every
/// group it deleted, or `Error: Deletion of some consumer groups failed:`
/// followed by one `* Group 'g' could not be deleted due to: <exception>` line
/// per group. Both are reduced to the same list here, so a case states the
/// rule once for the group that existed and the group that did not.
pub fn parse_delete_groups(text: &str) -> Vec<DeleteOutcome> {
    let mut outcomes: Vec<DeleteOutcome> = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if let Some(group) = quoted_group(line, "Group ") {
            outcomes.push(DeleteOutcome {
                group,
                failure: kafka_exceptions(line).into_iter().next(),
            });
        } else if line.starts_with("Deletion of requested consumer groups") {
            outcomes.extend(quoted_groups(line).into_iter().map(|group| DeleteOutcome {
                group,
                failure: None,
            }));
        }
    }
    outcomes.sort();
    outcomes
}

/// The group named by the first `'...'` after `marker`, when the line has one.
fn quoted_group(line: &str, marker: &str) -> Option<String> {
    let (_, rest) = line.split_once(marker)?;
    let (_, quoted) = rest.split_once('\'')?;
    let (name, _) = quoted.split_once('\'')?;
    Some(name.to_owned())
}

/// Every `'...'`-quoted name on a line.
fn quoted_groups(line: &str) -> Vec<String> {
    let mut names = Vec::new();
    let mut rest = line;
    while let Some((_, quoted)) = rest.split_once('\'') {
        let Some((name, tail)) = quoted.split_once('\'') else {
            break;
        };
        names.push(name.to_owned());
        rest = tail;
    }
    names
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::check,
        rstest::fixture,
        rstest_bdd::Slot,
        rstest_bdd_macros::{given, scenarios, then, when},
    };

    struct GroupToolOutput {
        stdout: String,
        reset_tables: Vec<Vec<ResetRow>>,
        export_tables: Vec<Vec<ExportRow>>,
        exceptions: Slot<BTreeSet<String>>,
        deletion: Vec<DeleteOutcome>,
    }

    #[fixture]
    fn output() -> GroupToolOutput {
        // Start this scenario with fresh inputs and observations.

        GroupToolOutput {
            stdout: String::new(),
            reset_tables: Vec::new(),
            export_tables: Vec::new(),
            exceptions: Slot::new(),
            deletion: Vec::new(),
        }
    }

    #[given("the consumer group tool printed")]
    fn group_stdout(output: &mut GroupToolOutput, docstring: String) { output.stdout = docstring; }

    #[given("the exported CSV consists of two blank lines")]
    fn blank_csv(output: &mut GroupToolOutput) { output.stdout = "\n\n".into(); }

    #[when("reset offsets are read from the table")]
    fn read_reset(output: &mut GroupToolOutput) {
        output.reset_tables = vec![parse_reset_table(&output.stdout)];
    }

    #[when("reset offsets are read with the table lines in either order")]
    fn read_reset_orders(output: &mut GroupToolOutput) {
        let reversed = output.stdout.lines().rev().collect::<Vec<_>>().join("\n");
        output.reset_tables = vec![
            parse_reset_table(&output.stdout),
            parse_reset_table(&reversed),
        ];
    }

    #[then("every reset table contains exactly these sorted offsets")]
    fn reset_rows(output: &GroupToolOutput, datatable: Vec<Vec<String>>) {
        let expected = datatable
            .into_iter()
            .skip(1)
            .map(|row| {
                let [group, topic, partition, new_offset]: [String; 4] =
                    row.try_into().expect("four reset table columns");
                ResetRow {
                    group,
                    topic,
                    partition: partition.parse().expect("partition index"),
                    new_offset: new_offset.parse().expect("new offset"),
                }
            })
            .collect::<Vec<_>>();
        check!(!output.reset_tables.is_empty());
        for actual in &output.reset_tables {
            check!(*actual == expected);
        }
    }

    #[when("exported offsets are read from CSV")]
    fn read_exports(output: &mut GroupToolOutput) {
        output.export_tables = vec![parse_export_csv(&output.stdout)];
    }

    #[when("the quoted and unquoted forms of the exported CSV are read")]
    fn read_csv_quoting(output: &mut GroupToolOutput) {
        output.export_tables = vec![
            parse_export_csv(&output.stdout),
            parse_export_csv(&output.stdout.replace('"', "")),
        ];
    }

    #[then("every exported table contains exactly")]
    fn export_rows(output: &GroupToolOutput, datatable: Vec<Vec<String>>) {
        let expected = datatable
            .into_iter()
            .skip(1)
            .map(|row| {
                let [group, topic, partition, offset]: [String; 4] =
                    row.try_into().expect("four export table columns");
                ExportRow {
                    group: (group != "absent").then_some(group),
                    topic,
                    partition: partition.parse().expect("partition index"),
                    offset: offset.parse().expect("exported offset"),
                }
            })
            .collect::<Vec<_>>();
        check!(!output.export_tables.is_empty());
        for actual in &output.export_tables {
            check!(*actual == expected);
        }
    }

    #[when("Kafka exception classes are read from the output")]
    fn read_exceptions(output: &mut GroupToolOutput) {
        output.exceptions.set(kafka_exceptions(&output.stdout));
    }

    #[then("the unique Kafka exception classes are exactly")]
    fn exception_classes(output: &GroupToolOutput, datatable: Vec<Vec<String>>) {
        let expected = datatable
            .into_iter()
            .skip(1)
            .map(|row| {
                let [class]: [String; 1] = row.try_into().expect("one exception class");
                class
            })
            .collect::<BTreeSet<_>>();
        check!(output.exceptions.get() == Some(expected));
    }

    #[when("consumer group deletion outcomes are read")]
    fn read_deletions(output: &mut GroupToolOutput) {
        output.deletion = parse_delete_groups(&output.stdout);
    }

    #[then("the sorted group deletion outcomes are exactly")]
    fn deletion_outcomes(output: &GroupToolOutput, datatable: Vec<Vec<String>>) {
        let expected = datatable
            .into_iter()
            .skip(1)
            .map(|row| {
                let [group, failure]: [String; 2] =
                    row.try_into().expect("group and deletion failure");
                DeleteOutcome {
                    group,
                    failure: (failure != "success").then_some(failure),
                }
            })
            .collect::<Vec<_>>();
        check!(output.deletion == expected);
    }

    scenarios!("tests/features/migrated/tests/jvm_acceptance_cli/group_output.feature", fixtures = [output: GroupToolOutput]);
}
