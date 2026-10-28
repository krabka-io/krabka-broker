//! The JSON files `kafka-leader-election` and `kafka-reassign-partitions`
//! read, and the output they print.
//!
//! Both tools take a plan in a file and answer in prose, so a suite about them
//! needs the two halves this module holds: builders for the file shapes, and
//! parsers for the answers. Everything here is pure, and unit-tested below, so
//! the part of these cases that is not a container is verified without one.
//!
//! # Why the answers are parsed rather than matched
//!
//! `kafka-leader-election` reports three outcomes per partition, and only one
//! of them is an error line: `ELECTION_NOT_NEEDED` (84) is folded into a
//! `Valid replica already elected for partitions …` line, while
//! `PREFERRED_LEADER_NOT_AVAILABLE` (80) becomes an
//! `Error completing leader election …` line. A case that grepped for a
//! substring would silently pass on a broker that reported the wrong one of
//! the two, which is exactly the divergence this suite exists to catch.
//! [`parse_election`] therefore reduces the whole output to one outcome per
//! partition, and the oracle and krabka are compared on that map.

use {serde_json::json, std::collections::BTreeMap};

/// A partition, in the `topic-index` spelling the tools print.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct TopicPartition {
    pub(crate) topic: String,
    pub(crate) partition: i32,
}

impl TopicPartition {
    pub(crate) fn new(topic: &str, partition: i32) -> Self {
        Self {
            topic: topic.to_owned(),
            partition,
        }
    }

    /// Read one back out of a rendered `TopicPartition`.
    ///
    /// Split from the right: a topic name may contain `-`, and
    /// `orders-eu-3` is partition 3 of `orders-eu`, not partition `eu-3` of
    /// `orders`.
    fn parse(text: &str) -> Option<Self> {
        let (topic, partition) = text.trim().rsplit_once('-')?;
        Some(Self {
            topic: topic.to_owned(),
            partition: partition.parse().ok()?,
        })
    }
}

/// What `kafka-leader-election` said about one partition.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum ElectionOutcome {
    /// The tool moved the leadership.
    Elected,
    /// `ELECTION_NOT_NEEDED` (84): the partition was already led by the
    /// replica the election would have chosen.
    AlreadyElected,
    /// Anything the tool reported as an error, named by the Kafka exception
    /// class it printed -- `PREFERRED_LEADER_NOT_AVAILABLE` (80) becomes
    /// `PreferredLeaderNotAvailableException`.
    Failed(String),
}

/// Reduce a whole `kafka-leader-election` run to one outcome per partition.
pub fn parse_election(text: &str) -> BTreeMap<TopicPartition, ElectionOutcome> {
    let mut outcomes = BTreeMap::new();
    for line in text.lines() {
        let line = line.trim();
        if let Some(list) = after(line, "for partitions ") {
            let outcome = if line.starts_with("Valid replica already elected") {
                ElectionOutcome::AlreadyElected
            } else {
                ElectionOutcome::Elected
            };
            for partition in list.split(',').filter_map(TopicPartition::parse) {
                outcomes.insert(partition, outcome.clone());
            }
        } else if let Some(rest) = after(line, "for partition: ") {
            // `Error completing leader election (PREFERRED) for partition:
            // <tp>: <exception>: <message>`. The partition ends at the colon
            // that introduces the exception.
            let Some((partition, tail)) = rest.split_once(':') else {
                continue;
            };
            let Some(partition) = TopicPartition::parse(partition) else {
                continue;
            };
            outcomes.insert(
                partition,
                ElectionOutcome::Failed(
                    exception_class(tail).unwrap_or_else(|| tail.trim().into()),
                ),
            );
        }
    }
    outcomes
}

/// The partitions a `--cancel` said it cancelled.
///
/// `ReassignPartitionsCommand` agrees the noun with the count, so cancelling
/// one partition prints "reassignment for:" and cancelling several prints
/// "reassignments for:". cp-kafka:7.5.0 emits the singular for a one-partition
/// plan, which is the shape this suite drives, so the marker stops before the
/// plural `s` and the ` for: ` that follows either spelling is what the list
/// is taken after.
pub fn parse_cancelled(text: &str) -> Vec<TopicPartition> {
    let mut cancelled: Vec<TopicPartition> = text
        .lines()
        .filter_map(|line| {
            let tail = after(line.trim(), "Successfully cancelled partition reassignment")?;
            after(tail, " for: ")
        })
        .flat_map(|list| {
            list.split(',')
                .filter_map(TopicPartition::parse)
                .collect::<Vec<_>>()
        })
        .collect();
    cancelled.sort();
    cancelled
}

/// One partition's replica list, as `--generate` proposes it.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Assignment {
    pub(crate) partition: TopicPartition,
    pub(crate) replicas: Vec<i32>,
}

/// The current and the proposed assignment `--generate` printed, in that
/// order.
///
/// The tool writes two labelled JSON documents separated by a blank line. The
/// labels are what tells them apart, so they are what is looked for; the
/// documents themselves are parsed as JSON rather than compared as text,
/// because neither tool promises key order and `--generate` is free to emit
/// the partitions in any order at all.
pub fn parse_generate(stdout: &str) -> Option<(Vec<Assignment>, Vec<Assignment>)> {
    let current = json_after(stdout, "Current partition replica assignment")?;
    let proposed = json_after(stdout, "Proposed partition reassignment configuration")?;
    Some((parse_assignments(&current)?, parse_assignments(&proposed)?))
}

/// The first non-empty line after `label`, which is where each tool puts the
/// document that belongs to it.
fn json_after(stdout: &str, label: &str) -> Option<String> {
    let mut lines = stdout.lines().skip_while(|line| !line.contains(label));
    lines.next()?;
    lines
        .map(str::trim)
        .find(|line| !line.is_empty())
        .map(str::to_owned)
}

/// Read a reassignment document into a sorted assignment list.
pub fn parse_assignments(document: &str) -> Option<Vec<Assignment>> {
    let value: serde_json::Value = serde_json::from_str(document).ok()?;
    let mut assignments: Vec<Assignment> = value
        .get("partitions")?
        .as_array()?
        .iter()
        .filter_map(|entry| {
            Some(Assignment {
                partition: TopicPartition {
                    topic: entry.get("topic")?.as_str()?.to_owned(),
                    partition: i32::try_from(entry.get("partition")?.as_i64()?).ok()?,
                },
                replicas: entry
                    .get("replicas")?
                    .as_array()?
                    .iter()
                    .filter_map(|id| i32::try_from(id.as_i64()?).ok())
                    .collect(),
            })
        })
        .collect();
    assignments.sort();
    Some(assignments)
}

/// The `--path-to-json-file` document `kafka-leader-election` reads.
pub fn election_json(partitions: &[TopicPartition]) -> String {
    json!({
        "partitions": partitions
            .iter()
            .map(|tp| json!({"topic": tp.topic, "partition": tp.partition}))
            .collect::<Vec<_>>(),
    })
    .to_string()
}

/// The `--topics-to-move-json-file` document `--generate` reads.
pub fn topics_to_move_json(topics: &[&str]) -> String {
    json!({
        "topics": topics.iter().map(|topic| json!({"topic": topic})).collect::<Vec<_>>(),
        "version": 1,
    })
    .to_string()
}

/// The `--reassignment-json-file` document `--execute`, `--verify` and
/// `--cancel` read.
pub fn reassignment_json(assignments: &[Assignment]) -> String {
    json!({
        "version": 1,
        "partitions": assignments
            .iter()
            .map(|a| json!({
                "topic": a.partition.topic,
                "partition": a.partition.partition,
                "replicas": a.replicas,
            }))
            .collect::<Vec<_>>(),
    })
    .to_string()
}

/// Everything after `marker` on a line that has it.
fn after<'a>(line: &'a str, marker: &str) -> Option<&'a str> {
    line.split_once(marker).map(|(_, rest)| rest)
}

/// The fully-qualified Kafka exception class named in `text`.
fn exception_class(text: &str) -> Option<String> {
    const PREFIX: &str = "org.apache.kafka.common.errors.";
    let (_, after) = text.split_once(PREFIX)?;
    let name = after
        .split_once(|c: char| !c.is_ascii_alphanumeric())
        .map_or(after, |(name, _)| name);
    Some(format!("{PREFIX}{name}"))
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::check,
        rstest::fixture,
        rstest_bdd::Slot,
        rstest_bdd_macros::{given, scenarios, then, when},
        serde_json::Value,
    };

    struct ElectionListing {
        stdout: String,
        outcomes: BTreeMap<TopicPartition, ElectionOutcome>,
    }

    #[fixture]
    fn election() -> ElectionListing {
        // Start this scenario with fresh inputs and observations.

        ElectionListing {
            stdout: String::new(),
            outcomes: BTreeMap::new(),
        }
    }

    #[given("the leader election tool printed")]
    fn election_stdout(election: &mut ElectionListing, docstring: String) {
        election.stdout = docstring;
    }

    #[when("partition election outcomes are read")]
    fn read_elections(election: &mut ElectionListing) {
        election.outcomes = parse_election(&election.stdout);
    }

    #[then("the partition election outcomes are exactly")]
    fn election_outcomes(election: &ElectionListing, datatable: Vec<Vec<String>>) {
        let expected = datatable
            .into_iter()
            .skip(1)
            .map(|row| {
                let [topic, partition, result]: [String; 3] =
                    row.try_into().expect("topic, partition and outcome");
                let outcome = match result.as_str() {
                    "elected" => ElectionOutcome::Elected,
                    "already elected" => ElectionOutcome::AlreadyElected,
                    class => ElectionOutcome::Failed(class.into()),
                };
                (
                    TopicPartition::new(&topic, partition.parse().expect("partition index")),
                    outcome,
                )
            })
            .collect::<BTreeMap<_, _>>();
        check!(election.outcomes == expected);
    }

    struct PartitionLabel {
        text: String,
        parsed: Slot<Option<TopicPartition>>,
    }

    #[fixture]
    fn label() -> PartitionLabel {
        // Start this scenario with fresh inputs and observations.

        PartitionLabel {
            text: String::new(),
            parsed: Slot::new(),
        }
    }

    #[given("a tool partition label {text}")]
    fn partition_label(label: &mut PartitionLabel, text: String) { label.text = text; }

    #[when("the topic name and partition index are read from the label")]
    fn read_partition_label(label: &mut PartitionLabel) {
        label.parsed.set(TopicPartition::parse(&label.text));
    }

    #[then("the topic-partition label resolves to {result}")]
    fn partition_label_result(label: &PartitionLabel, result: String) {
        let expected = if result == "invalid" {
            None
        } else {
            let (topic, partition) = result
                .split_once(" / ")
                .expect("expected topic / partition");
            Some(TopicPartition::new(
                topic,
                partition.parse().expect("partition index"),
            ))
        };
        check!(label.parsed.get() == Some(expected));
    }

    struct CancellationListing {
        stdout: String,
        partitions: Vec<TopicPartition>,
    }

    #[fixture]
    fn cancellation() -> CancellationListing {
        // Start this scenario with fresh inputs and observations.

        CancellationListing {
            stdout: String::new(),
            partitions: Vec::new(),
        }
    }

    #[given("the reassignment cancellation tool printed")]
    fn cancellation_stdout(cancellation: &mut CancellationListing, docstring: String) {
        cancellation.stdout = docstring;
    }

    #[when("cancelled partitions are read")]
    fn read_cancellations(cancellation: &mut CancellationListing) {
        cancellation.partitions = parse_cancelled(&cancellation.stdout);
    }

    #[then("the cancelled partitions are {partitions}")]
    fn cancelled_partitions(cancellation: &CancellationListing, partitions: String) {
        let expected = if partitions == "none" {
            Vec::new()
        } else {
            partitions
                .split(',')
                .map(|partition| {
                    let (topic, index) = partition.trim().split_once(':').expect("topic:partition");
                    TopicPartition::new(topic, index.parse().expect("partition index"))
                })
                .collect()
        };
        check!(cancellation.partitions == expected);
    }

    struct PlanDocuments {
        stdout: String,
        generated: Slot<Option<(Vec<Assignment>, Vec<Assignment>)>>,
        assignment_documents: Vec<String>,
        parsed_assignments: Vec<Option<Vec<Assignment>>>,
        input_assignments: Vec<Assignment>,
        election_json: String,
        topics_json: String,
    }

    #[fixture]
    fn plans() -> PlanDocuments {
        // Start this scenario with fresh inputs and observations.

        PlanDocuments {
            stdout: String::new(),
            generated: Slot::new(),
            assignment_documents: Vec::new(),
            parsed_assignments: Vec::new(),
            input_assignments: Vec::new(),
            election_json: String::new(),
            topics_json: String::new(),
        }
    }

    fn assignments_from_rows(rows: Vec<Vec<String>>) -> Vec<Assignment> {
        rows.into_iter()
            .map(|row| {
                let [topic, partition, replicas]: [String; 3] =
                    row.try_into().expect("topic, partition, replicas");
                Assignment {
                    partition: TopicPartition::new(
                        &topic,
                        partition.parse().expect("partition index"),
                    ),
                    replicas: replicas
                        .split(',')
                        .map(|id| id.trim().parse().expect("replica id"))
                        .collect(),
                }
            })
            .collect()
    }

    #[given("the reassignment generation tool printed")]
    fn generation_stdout(plans: &mut PlanDocuments, docstring: String) { plans.stdout = docstring; }

    #[when("the current and proposed assignments are read")]
    fn read_generated(plans: &mut PlanDocuments) {
        plans.generated.set(parse_generate(&plans.stdout));
    }

    impl PlanDocuments {
        fn generated_pair(&self) -> &Option<(Vec<Assignment>, Vec<Assignment>)> {
            self.generated.get().expect("the generated plans were read")
        }
    }

    #[then("the current assignment is exactly")]
    fn current_assignment(plans: &PlanDocuments, datatable: Vec<Vec<String>>) {
        let expected = assignments_from_rows(datatable.into_iter().skip(1).collect());
        let generated = plans.generated_pair();
        let (current, _) = generated.as_ref().expect("both generated plans were found");
        check!(*current == expected);
    }

    #[then("the proposed assignment is exactly")]
    fn proposed_assignment(plans: &PlanDocuments, datatable: Vec<Vec<String>>) {
        let expected = assignments_from_rows(datatable.into_iter().skip(1).collect());
        let generated = plans.generated_pair();
        let (_, proposed) = generated.as_ref().expect("both generated plans were found");
        check!(*proposed == expected);
    }

    #[then("no generated assignment is reported")]
    fn no_generated_assignment(plans: &PlanDocuments) {
        check!(plans.generated.get() == Some(None));
    }

    #[given("the same assignment document is printed in opposite partition orders")]
    fn opposite_order(plans: &mut PlanDocuments, docstring: String) {
        let mut reversed: Value = serde_json::from_str(&docstring).expect("assignment JSON");
        reversed
            .get_mut("partitions")
            .and_then(Value::as_array_mut)
            .expect("assignment partitions")
            .reverse();
        plans.assignment_documents = vec![docstring, reversed.to_string()];
    }

    #[when("each assignment document is read")]
    fn read_assignment_documents(plans: &mut PlanDocuments) {
        plans.parsed_assignments = plans
            .assignment_documents
            .iter()
            .map(|document| parse_assignments(document))
            .collect();
    }

    #[then("every document yields the same sorted assignments")]
    fn sorted_assignments(plans: &PlanDocuments, datatable: Vec<Vec<String>>) {
        let expected = assignments_from_rows(datatable.into_iter().skip(1).collect());
        check!(!plans.parsed_assignments.is_empty());
        for actual in &plans.parsed_assignments {
            check!(actual.as_ref() == Some(&expected));
        }
    }

    #[given("the requested partition assignments are")]
    fn requested_assignments(plans: &mut PlanDocuments, datatable: Vec<Vec<String>>) {
        plans.input_assignments = assignments_from_rows(datatable.into_iter().skip(1).collect());
    }

    #[when("reassignment election and topic-move input documents are generated")]
    fn generate_documents(plans: &mut PlanDocuments) {
        plans.assignment_documents = vec![reassignment_json(&plans.input_assignments)];
        read_assignment_documents(plans);
        plans.election_json = election_json(&[TopicPartition::new("orders", 0)]);
        plans.topics_json = topics_to_move_json(&["orders"]);
    }

    #[then("the reassignment document preserves every requested replica")]
    fn assignment_round_trip(plans: &PlanDocuments) {
        check!(plans.parsed_assignments == vec![Some(plans.input_assignments.clone())]);
    }

    #[then("the election input JSON is")]
    fn election_document(plans: &PlanDocuments, docstring: String) {
        check!(plans.election_json == docstring.trim());
    }

    #[then("the topic-move input JSON is")]
    fn topic_document(plans: &PlanDocuments, docstring: String) {
        check!(plans.topics_json == docstring.trim());
    }

    scenarios!("tests/features/migrated/tests/jvm_acceptance_reassign/tool_output.feature", fixtures = [election: ElectionListing, label: PartitionLabel, cancellation: CancellationListing, plans: PlanDocuments]);
}
