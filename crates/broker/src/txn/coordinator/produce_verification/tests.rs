use {super::*, assert2::assert};

/// `transaction.partition.verification.enable` is a dynamic broker config
/// with a static layer: the broker's own dynamic value beats the cluster-wide
/// one, which beats the static value of `server.properties`, which beats
/// Kafka's default `true`. A dynamic value that is not a boolean does not
/// apply.
#[test]
fn the_verification_knob_reads_the_broker_then_the_cluster_then_the_static_value() {
    use {
        crate::config::BrokerConfig,
        krabka_metadata::{
            BrokerConfigRecord, DEFAULT_BROKER_CONFIG_NODE_ID, MetadataImage, MetadataRecord,
            NodeId,
        },
    };

    const NODE: NodeId = NodeId(1);
    /// A dynamic config value set for one broker, or for the cluster.
    struct DynamicValue<'a> {
        node_id: NodeId,
        value: &'a str,
    }
    let image = |records: &[DynamicValue<'_>]| {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        for DynamicValue { node_id, value } in records {
            image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                node_id: *node_id,
                config_name: PARTITION_VERIFICATION_ENABLE.to_owned(),
                config_value: Some((*value).to_owned()),
            }));
        }
        image
    };
    let cluster = |value| DynamicValue {
        node_id: DEFAULT_BROKER_CONFIG_NODE_ID,
        value,
    };
    let broker = |node_id, value| DynamicValue { node_id, value };
    // (label, dynamic records, the static value, the answer)
    let cases = [
        ("no config", image(&[]), true, true),
        ("static off", image(&[]), false, false),
        ("cluster off", image(&[cluster("false")]), true, false),
        (
            "cluster off, case and blanks",
            image(&[cluster(" False ")]),
            true,
            false,
        ),
        (
            "cluster on over static off",
            image(&[cluster("true")]),
            false,
            true,
        ),
        (
            "cluster off over static on",
            image(&[cluster("false")]),
            true,
            false,
        ),
        (
            "broker on over cluster off",
            image(&[cluster("false"), broker(NODE, "true")]),
            true,
            true,
        ),
        (
            "broker off over cluster on",
            image(&[cluster("true"), broker(NODE, "false")]),
            true,
            false,
        ),
        (
            "broker on over static off",
            image(&[broker(NODE, "true")]),
            false,
            true,
        ),
        (
            "broker off over static on",
            image(&[broker(NODE, "false")]),
            true,
            false,
        ),
        (
            "another broker's value",
            image(&[broker(NodeId(2), "false")]),
            true,
            true,
        ),
        (
            "another broker's value, static off",
            image(&[broker(NodeId(2), "true")]),
            false,
            false,
        ),
        (
            "not a boolean falls to static on",
            image(&[broker(NODE, "no")]),
            true,
            true,
        ),
        (
            "not a boolean falls to static off",
            image(&[broker(NODE, "no")]),
            false,
            false,
        ),
    ];
    for (name, image, static_value, want) in cases {
        let config = BrokerConfig {
            node_id: NODE,
            transaction_partition_verification_enable: static_value,
            ..BrokerConfig::default()
        };
        assert!(
            partition_verification_enabled(&image, &config) == want,
            "{name}"
        );
    }
}

/// Only a verify-only operation skips the coordinator, and only with the knob
/// off: an operation that adds the partition still has to reach the
/// coordinator.
#[test]
fn only_a_verify_only_call_skips_the_coordinator_and_only_with_the_knob_off() {
    // (the operation adds the partition, the knob is on, the call is skipped)
    let cases = [
        (false, true, false),
        (false, false, true),
        (true, true, false),
        (true, false, false),
    ];
    for (adds_partition, enabled, skipped) in cases {
        assert!(
            skips_coordinator_verification(adds_partition, enabled) == skipped,
            "adds_partition={adds_partition} enabled={enabled}"
        );
    }
}

#[derive(Default)]
struct TransactionPartitionVerification {
    requested: Option<ProducerIdentity>,
    held: Option<HeldTransaction>,
    contains: Option<bool>,
    code: Option<i16>,
}

#[rstest::fixture]
fn transaction_partition_verification() -> TransactionPartitionVerification {
    // Each producer request is checked against one independent held transaction.
    TransactionPartitionVerification::default()
}

#[rstest_bdd_macros::given("producer 7 at epoch 3 requests transaction partition verification")]
fn requested_transaction_verification(
    transaction_partition_verification: &mut TransactionPartitionVerification,
) {
    transaction_partition_verification.requested = Some(ProducerIdentity {
        producer_id: ProducerId(7),
        producer_epoch: 3,
    });
}

#[rstest_bdd_macros::given(
    "the held transaction has producer {producer}, epoch {epoch}, state {state}, and partition membership {contains}"
)]
fn held_transaction_verification(
    transaction_partition_verification: &mut TransactionPartitionVerification,
    producer: i64,
    epoch: i16,
    state: String,
    contains: bool,
) {
    let state = match state.as_str() {
        "Ongoing" => TxnState::Ongoing,
        "PrepareCommit" => TxnState::PrepareCommit,
        "PrepareAbort" => TxnState::PrepareAbort,
        "CompleteCommit" => TxnState::CompleteCommit,
        _ => panic!("unknown held transaction state {state}"),
    };
    transaction_partition_verification.held = Some(HeldTransaction {
        identity: ProducerIdentity {
            producer_id: ProducerId(producer),
            producer_epoch: epoch,
        },
        state,
    });
    transaction_partition_verification.contains = Some(contains);
}

#[rstest_bdd_macros::when("the transaction coordinator verifies the requested partition")]
fn verify_held_transaction_partition(
    transaction_partition_verification: &mut TransactionPartitionVerification,
) {
    transaction_partition_verification.code = Some(verification_code(
        transaction_partition_verification
            .held
            .take()
            .expect("held transaction was given"),
        transaction_partition_verification
            .contains
            .expect("partition membership was given"),
        transaction_partition_verification
            .requested
            .take()
            .expect("producer request was given"),
    ));
}

#[rstest_bdd_macros::then("transaction partition verification returns Kafka code {code}")]
fn expected_transaction_partition_verification(
    transaction_partition_verification: &TransactionPartitionVerification,
    code: i16,
) {
    assert!(transaction_partition_verification.code == Some(code));
}

rstest_bdd_macros::scenarios!(
    "tests/features/migrated/src/txn/coordinator/produce_verification/tests.feature",
    fixtures = [transaction_partition_verification: TransactionPartitionVerification]
);
