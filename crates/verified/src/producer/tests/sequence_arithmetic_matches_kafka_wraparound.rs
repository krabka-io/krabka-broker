use {
    super::*,
    assert2::assert,
    rstest::fixture,
    rstest_bdd_macros::{given, scenarios, then, when},
    std::vec,
};

struct SequenceArithmetic {
    sequence: Option<i32>,
    actual: Option<(i32, i32)>,
}

#[fixture]
fn arithmetic() -> SequenceArithmetic {
    // Start this scenario with fresh inputs and observations.

    SequenceArithmetic {
        sequence: None,
        actual: None,
    }
}

#[given("a producer sequence number {sequence}")]
fn initial_sequence(arithmetic: &mut SequenceArithmetic, sequence: i32) {
    arithmetic.sequence = Some(sequence);
}

#[when("the producer sequence is incremented and decremented by {step}")]
fn advance_sequence(arithmetic: &mut SequenceArithmetic, step: i32) {
    let sequence = arithmetic.sequence.expect("a sequence number was given");
    arithmetic.actual = Some((
        increment_sequence(sequence, step),
        decrement_sequence(sequence, step),
    ));
}

#[then("the incremented sequence is {incremented} and the decremented sequence is {decremented}")]
fn wrapped_sequences(arithmetic: &SequenceArithmetic, incremented: i32, decremented: i32) {
    assert!(arithmetic.actual == Some((incremented, decremented)));
}

struct ProducerAdmission {
    entry: Option<ProducerEntryFacts>,
    retained: Vec<Option<RetainedSequenceRange>>,
    actual: Option<ProducerDecision>,
}

#[fixture]
fn producer() -> ProducerAdmission {
    // Start this scenario with fresh inputs and observations.

    ProducerAdmission {
        entry: None,
        retained: Vec::new(),
        actual: None,
    }
}

#[given("producer snapshot {snapshot} with retained sequence ranges {ranges}")]
fn retained_producer(producer: &mut ProducerAdmission, snapshot: String, ranges: String) {
    producer.entry = if snapshot == "absent" {
        None
    } else {
        let (epoch, last_sequence) = snapshot
            .split_once(':')
            .expect("snapshot is epoch:last-sequence");
        Some(ProducerEntryFacts {
            epoch: epoch.parse().expect("producer epoch is an integer"),
            last_sequence: last_sequence.parse().expect("last sequence is an integer"),
        })
    };
    producer.retained = if ranges == "empty" {
        Vec::new()
    } else {
        ranges
            .split(',')
            .map(|range| {
                if range == "unused" {
                    return None;
                }
                let (base, last) = range.split_once('-').unwrap_or((range, range));
                Some(RetainedSequenceRange {
                    base_sequence: base.parse().expect("base sequence is an integer"),
                    last_sequence: last.parse().expect("last sequence is an integer"),
                })
            })
            .collect()
    };
}

#[when(
    "a batch at epoch {epoch}, base sequence {base_sequence}, and delta {delta} is evaluated on a {log_state} log under {rules} rules"
)]
fn evaluate_batch(
    producer: &mut ProducerAdmission,
    epoch: i16,
    base_sequence: i32,
    delta: i32,
    log_state: String,
    rules: String,
) {
    let log_empty = match log_state.as_str() {
        "empty" => true,
        "nonempty" => false,
        _ => panic!("unknown producer log state: {log_state}"),
    };
    let trunk_rules = match rules.as_str() {
        "trunk" => true,
        "4.3.1" => false,
        _ => panic!("unknown Kafka producer admission rules: {rules}"),
    };
    producer.actual = Some(producer_decision(
        producer.entry,
        &producer.retained,
        epoch,
        base_sequence,
        delta,
        log_empty,
        trunk_rules,
    ));
}

#[then("producer admission is {decision}")]
fn producer_outcome(producer: &ProducerAdmission, decision: String) {
    let expected = match decision.as_str() {
        "append" => ProducerDecision::Append,
        "fenced" => ProducerDecision::Fenced,
        "out-of-order" => ProducerDecision::OutOfOrder,
        _ => {
            let retained = decision
                .strip_prefix("duplicate-slot-")
                .expect("a duplicate names its retained slot")
                .parse()
                .expect("retained slot is an integer");
            ProducerDecision::Duplicate { retained }
        }
    };
    assert!(producer.actual == Some(expected));
}

scenarios!("tests/features/migrated/src/producer/tests/sequence_arithmetic_matches_kafka_wraparound.feature", fixtures = [arithmetic: SequenceArithmetic, producer: ProducerAdmission]);
