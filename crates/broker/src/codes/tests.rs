//! Guard tests for the wire-level error codes in [`super`], plus the
//! [`super::from_broker_error`] mapping.
//!
//! The guards are total rather than sampled: every constant the module
//! declares is checked against the Apache Kafka table extracted from the
//! pinned container image, and every constant is checked against every other
//! for a collision. A new constant that invents a number therefore fails here
//! rather than reaching a JVM client.
//!
//! The table itself is checked in, so none of this needs a container. What
//! keeps a checked-in oracle honest is
//! [`the_error_table_records_the_image_the_build_pins`], which holds its
//! recorded provenance against the image the build still pins.

mod kafka_error_table;

use {
    super::*,
    crate::error::{BrokerError, ConfigError},
    assert2::assert,
    kafka_error_table::{KAFKA_ERROR_TABLE, KAFKA_ERROR_TABLE_IMAGE_DIGEST},
    std::io::Error,
};

/// The digest `//MODULE.bazel` pins for `apache_kafka_4_3_1`.
///
/// The build supplies it -- `crates/broker/build.rs` under Cargo,
/// `//bazel/images:pinned_digests` under Bazel -- so the comparison below is
/// between two strings this compilation already holds. Reading `MODULE.bazel`
/// here instead would be a test asserting on the text of a source file, and
/// would not survive Bazel's sandbox, which hands a test only its declared
/// inputs.
const PINNED_KAFKA_ORACLE_IMAGE_DIGEST: &str = env!("KRABKA_PINNED_IMAGE_APACHE_KAFKA_4_3_1");

#[test]
fn the_error_table_records_the_image_the_build_pins() {
    assert!(
        KAFKA_ERROR_TABLE_IMAGE_DIGEST == PINNED_KAFKA_ORACLE_IMAGE_DIGEST,
        "the Kafka error table was extracted from an image the build no longer \
         pins; re-derive crates/broker/src/codes/tests/kafka_error_table.rs \
         from {PINNED_KAFKA_ORACLE_IMAGE_DIGEST} and record that digest there",
    );
}

#[derive(Default)]
struct ErrorNamespaces {
    release_ceiling: Option<i16>,
    trunk_ceiling: Option<i16>,
    released: Option<Vec<NamedCode>>,
    private: Option<Vec<NamedCode>>,
}

#[rstest::fixture]
fn errors() -> ErrorNamespaces {
    // Keep errors inputs and observations fresh for each scenario.
    ErrorNamespaces::default()
}

#[rstest_bdd_macros::given("the published Kafka release and trunk error namespaces")]
fn kafka_error_namespaces(errors: &mut ErrorNamespaces) {
    errors.release_ceiling = KAFKA_ERROR_TABLE.iter().map(|error| error.code).max();
    errors.trunk_ceiling = KAFKA_TRUNK_ERROR_TABLE.iter().map(|error| error.code).max();
}

#[rstest_bdd_macros::when("the broker's released and private wire error codes are enumerated")]
fn broker_error_codes(errors: &mut ErrorNamespaces) {
    errors.released = Some(KAFKA_RANGE_CODES.to_vec());
    errors.private = Some(KRABKA_PRIVATE_CODES.to_vec());
}

#[rstest_bdd_macros::then("every released error keeps Kafka's name, numeric wire code and message")]
fn released_error_codes(errors: &ErrorNamespaces) {
    for wire in errors
        .released
        .as_ref()
        .expect("broker errors were enumerated")
    {
        let NamedCode { name, code, .. } = wire;
        let kafka = KAFKA_ERROR_TABLE
            .iter()
            .find(|kafka| kafka.name == wire.name);
        assert!(kafka == Some(wire), "{name} = {code}");
    }
}

#[rstest_bdd_macros::then("every private error is at least 1000 and above both Kafka namespaces")]
fn private_error_codes(errors: &ErrorNamespaces) {
    let highest_kafka = errors
        .release_ceiling
        .expect("the extracted Kafka table is not empty");
    let highest_trunk = errors.trunk_ceiling.expect("the trunk table is not empty");
    for NamedCode { name, code, .. } in errors
        .private
        .as_ref()
        .expect("broker errors were enumerated")
    {
        assert!(*code >= 1000, "{name}");
        assert!(*code > highest_kafka, "{name}");
        assert!(*code > highest_trunk, "{name}");
    }
}

/// The codes Kafka trunk adds after the 4.3.1 table ends, with their default
/// messages, as `org.apache.kafka.common.protocol.Errors` assigns them at trunk
/// `abf522e1ca5d`: 134 is `GROUP_DELETION_FAILED`, which krabka does not
/// return, 135 is KIP-1331's and 136 is KIP-1312's.
const KAFKA_TRUNK_ERROR_TABLE: &[NamedCode] = &[
    NamedCode {
        name: "GROUP_DELETION_FAILED",
        code: 134,
        message: Some(
            "DeleteGroups could not complete; see the error message on the per-group result for details.",
        ),
    },
    NamedCode {
        name: "STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED",
        code: 135,
        message: Some(
            "The broker could not process the topology description update; see the error message for details.",
        ),
    },
    NamedCode {
        name: "CONTROLLER_ID_NOT_REGISTERED",
        code: 136,
        message: Some("The given controller ID was not registered."),
    },
];

#[test]
fn every_trunk_constant_appears_in_the_trunk_table_and_not_in_the_release() {
    for wire in KAFKA_TRUNK_CODES {
        let NamedCode { name, code, .. } = wire;
        let trunk = KAFKA_TRUNK_ERROR_TABLE
            .iter()
            .find(|trunk| trunk.name == wire.name);
        assert!(trunk == Some(wire), "{name} = {code}");
        assert!(
            KAFKA_ERROR_TABLE
                .iter()
                .all(|released| released.code != *code),
            "{name} is in the release table and belongs with the Kafka-range codes"
        );
    }
}

#[test]
fn every_private_constant_declares_no_message() {
    for NamedCode { name, message, .. } in KRABKA_PRIVATE_CODES {
        assert!(message.is_none(), "{name}");
    }
}

#[test]
fn every_declared_kafka_code_answers_its_own_name_and_message() {
    for NamedCode {
        name,
        code,
        message,
    } in KAFKA_RANGE_CODES.iter().chain(KAFKA_TRUNK_CODES)
    {
        assert!(error_name(*code) == *name);
        assert!(Some(default_message(*code)) == *message, "{name}");
    }
}

/// Kafka's `Errors.forCode` reads a code it does not know as
/// `UNKNOWN_SERVER_ERROR`, and so do [`error_name`] and [`default_message`]:
/// for a Kafka code this module does not declare, for a krabka-private code,
/// and for a number no table assigns.
#[yare::parameterized(
    undeclared_kafka_code = { 4 },
    krabka_private_code = { BARRIER_INJECTION_IN_PROGRESS },
    unassigned_code = { i16::MAX },
    below_unknown_server_error = { -2 },
)]
fn an_undeclared_code_reads_as_unknown_server_error(code: i16) {
    assert2::assert!(error_name(code) == "UNKNOWN_SERVER_ERROR");
    assert2::assert!(
        default_message(code)
            == "The server experienced an unexpected error when processing the request."
    );
}

/// The messages the broker's handlers put on the wire, pinned byte for byte.
/// The guard above holds every declared row against Kafka's table; this one
/// holds the rows the handlers send against the literal bytes they sent
/// before the messages had one home.
#[yare::parameterized(
    none = { NONE, "NONE" },
    unknown_server_error = {
        UNKNOWN_SERVER_ERROR,
        "The server experienced an unexpected error when processing the request.",
    },
    unknown_topic_or_partition = {
        UNKNOWN_TOPIC_OR_PARTITION,
        "This server does not host this topic-partition.",
    },
    leader_not_available = {
        LEADER_NOT_AVAILABLE,
        "There is no leader for this topic-partition as we are in the middle of a leadership \
         election.",
    },
    not_leader_or_follower = {
        NOT_LEADER_OR_FOLLOWER,
        "For requests intended only for the leader, this error indicates that the broker is not \
         the current leader. For requests intended for any replica, this error indicates that the \
         broker is not a replica of the topic partition.",
    },
    request_timed_out = { REQUEST_TIMED_OUT, "The request timed out." },
    network_exception = {
        NETWORK_EXCEPTION,
        "The server disconnected before a response was received.",
    },
    coordinator_load_in_progress = {
        COORDINATOR_LOAD_IN_PROGRESS,
        "The coordinator is loading and hence can't process requests.",
    },
    coordinator_not_available = { COORDINATOR_NOT_AVAILABLE, "The coordinator is not available." },
    not_coordinator = { NOT_COORDINATOR, "This is not the correct coordinator." },
    invalid_topic_exception = {
        INVALID_TOPIC_EXCEPTION,
        "The request attempted to perform an operation on an invalid topic.",
    },
    invalid_group_id = { INVALID_GROUP_ID, "The group id is invalid." },
    topic_authorization_failed = { TOPIC_AUTHORIZATION_FAILED, "Topic authorization failed." },
    group_authorization_failed = { GROUP_AUTHORIZATION_FAILED, "Group authorization failed." },
    cluster_authorization_failed = {
        CLUSTER_AUTHORIZATION_FAILED,
        "Cluster authorization failed.",
    },
    illegal_sasl_state = {
        ILLEGAL_SASL_STATE,
        "Request is not valid given the current SASL state.",
    },
    unsupported_version = { UNSUPPORTED_VERSION, "The version of API is not supported." },
    topic_already_exists = { TOPIC_ALREADY_EXISTS, "Topic with this name already exists." },
    invalid_partitions = { INVALID_PARTITIONS, "Number of partitions is below 1." },
    invalid_replication_factor = {
        INVALID_REPLICATION_FACTOR,
        "Replication factor is below 1 or larger than the number of available brokers.",
    },
    invalid_replica_assignment = { INVALID_REPLICA_ASSIGNMENT, "Replica assignment is invalid." },
    invalid_config = { INVALID_CONFIG, "Configuration is invalid." },
    not_controller = { NOT_CONTROLLER, "This is not the correct controller for this cluster." },
    invalid_request = {
        INVALID_REQUEST,
        "This most likely occurs because of a request being malformed by the client library or \
         the message was sent to an incompatible broker. See the broker logs for more details.",
    },
    policy_violation = {
        POLICY_VIOLATION,
        "Request parameters do not satisfy the configured policy.",
    },
    transactional_id_authorization_failed = {
        TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
        "Transactional Id authorization failed.",
    },
    non_empty_group = { NON_EMPTY_GROUP, "The group is not empty." },
    group_id_not_found = { GROUP_ID_NOT_FOUND, "The group id does not exist." },
    kafka_storage_error = {
        KAFKA_STORAGE_ERROR,
        "Disk error when trying to access log file on the disk.",
    },
    fenced_leader_epoch = {
        FENCED_LEADER_EPOCH,
        "The leader epoch in the request is older than the epoch on the broker.",
    },
    unknown_leader_epoch = {
        UNKNOWN_LEADER_EPOCH,
        "The leader epoch in the request is newer than the epoch on the broker.",
    },
    offset_not_available = {
        OFFSET_NOT_AVAILABLE,
        "The leader high watermark has not caught up from a recent leader election so the \
         offsets cannot be guaranteed to be monotonically increasing.",
    },
    preferred_leader_not_available = {
        PREFERRED_LEADER_NOT_AVAILABLE,
        "The preferred leader was not available.",
    },
    eligible_leaders_not_available = {
        ELIGIBLE_LEADERS_NOT_AVAILABLE,
        "Eligible topic partition leaders are not available.",
    },
    election_not_needed = {
        ELECTION_NOT_NEEDED,
        "Leader election not needed for topic partition.",
    },
    no_reassignment_in_progress = {
        NO_REASSIGNMENT_IN_PROGRESS,
        "No partition reassignment is in progress.",
    },
    throttling_quota_exceeded = {
        THROTTLING_QUOTA_EXCEEDED,
        "The throttling quota has been exceeded.",
    },
    unknown_topic_id = { UNKNOWN_TOPIC_ID, "This server does not host this topic ID." },
)]
fn the_messages_the_handlers_send_keep_their_bytes(code: i16, message: &str) {
    assert2::assert!(default_message(code) == message);
}

#[test]
fn no_two_error_code_constants_collide() {
    let all: Vec<NamedCode> = KAFKA_RANGE_CODES
        .iter()
        .chain(KAFKA_TRUNK_CODES)
        .chain(KRABKA_PRIVATE_CODES)
        .copied()
        .collect();
    for (
        index,
        NamedCode {
            name: left_name,
            code: left,
            ..
        },
    ) in all.iter().enumerate()
    {
        for NamedCode {
            name: right_name,
            code: right,
            ..
        } in &all[index + 1..]
        {
            assert!(
                left != right,
                "{left_name} and {right_name} both use {left}"
            );
        }
    }
}

#[yare::parameterized(
    unsupported_api = {
        BrokerError::UnsupportedApi { api_key: 0, version: 99 },
        UNSUPPORTED_VERSION,
    },
    partition_writer_died = {
        BrokerError::PartitionWriterDied { topic: "t".into(), partition: 0 },
        NOT_LEADER_OR_FOLLOWER,
    },
    fenced_log = { BrokerError::Log(krabka_log::LogError::Fenced), NOT_LEADER_OR_FOLLOWER },
    invalid_group_state = {
        BrokerError::GroupInvalidState {
            group_id: "g".into(),
            state: "PreparingRebalance".into(),
        },
        REBALANCE_IN_PROGRESS,
    },
    unknown_member = {
        BrokerError::UnknownMember { group_id: "g".into(), member_id: "m".into() },
        UNKNOWN_MEMBER_ID,
    },
    generation_mismatch = {
        BrokerError::GenerationMismatch { group_id: "g".into(), current: 5, requested: 4 },
        ILLEGAL_GENERATION,
    },
    producer_epoch_fenced = {
        BrokerError::ProducerEpochFenced { producer_id: 1000, current: 2, requested: 1 },
        INVALID_PRODUCER_EPOCH,
    },
    fenced_leader_epoch = {
        BrokerError::FencedLeaderEpoch { have: 0, current: 1 },
        FENCED_LEADER_EPOCH,
    },
    unknown_leader_epoch = { BrokerError::UnknownLeaderEpoch(2), UNKNOWN_LEADER_EPOCH },
    coordinator_write_not_coordinator = {
        BrokerError::CoordinatorWriteUncommitted { partition: 0, code: NOT_COORDINATOR },
        NOT_COORDINATOR,
    },
    coordinator_write_unavailable = {
        BrokerError::CoordinatorWriteUncommitted {
            partition: 0,
            code: COORDINATOR_NOT_AVAILABLE,
        },
        COORDINATOR_NOT_AVAILABLE,
    },
    transaction_state_write_uncommitted = {
        BrokerError::TransactionStateWriteUncommitted {
            partition: 0,
            code: COORDINATOR_LOAD_IN_PROGRESS,
        },
        COORDINATOR_LOAD_IN_PROGRESS,
    },
    log_io_error = {
        BrokerError::Log(krabka_log::LogError::Io(Error::other("EIO"))),
        KAFKA_STORAGE_ERROR,
    },
    catch_all = { BrokerError::Txn("test".into()), UNKNOWN_SERVER_ERROR },
)]
fn from_broker_error_maps_variants_to_wire_codes(err: BrokerError, want: i16) {
    assert2::assert!(from_broker_error(&err) == want, "{err:?}");
}

#[test]
fn policy_violation_is_not_a_code_that_poisons_the_metadata_cache() {
    // KFC-9 chose 44 and rejected both of these, because each one also
    // changes the JVM client's metadata cache. 29 marks the topic
    // unauthorized, and 17 marks the name permanently invalid.
    assert!(POLICY_VIOLATION != TOPIC_AUTHORIZATION_FAILED);
    assert!(POLICY_VIOLATION != INVALID_TOPIC_EXCEPTION);
}

#[test]
fn replica_not_available_is_not_the_controller_moved_code() {
    // A JVM AdminClient turns 11 into StaleControllerEpochException, which
    // makes `kafka-reassign-partitions` report a controller failure for a
    // replica this broker simply does not host.
    let stale_controller_epoch = KAFKA_ERROR_TABLE
        .iter()
        .find(|kafka| kafka.name == "STALE_CONTROLLER_EPOCH")
        .map(|kafka| kafka.code);
    assert!(stale_controller_epoch == Some(11));
    assert!(REPLICA_NOT_AVAILABLE == 9);
}

rstest_bdd_macros::scenarios!("tests/features/migrated/src/codes/tests.feature", fixtures = [errors: ErrorNamespaces]);

// Config validation fails startup and never reaches a request, but its errors
// still have a wire code, and an operator still reads their messages: both
// stay fixed.
#[yare::parameterized(
    listener_conflict = {
        BrokerError::Config(ConfigError::ListenerConflict { a: "A".into(), b: "B".into() }),
        "listener bind conflict: A and B share bind_addr",
    },
    empty_roles = { BrokerError::Config(ConfigError::EmptyRoles), "process.roles must list at least one role" },
    broker_id_node_id_mismatch = {
        BrokerError::Config(ConfigError::BrokerIdNodeIdMismatch { broker_id: 1, node_id: krabka_raft::NodeId(2) }),
        "broker_id 1 must equal node_id 2",
    },
    stretch_requires_rack = {
        BrokerError::Config(ConfigError::StretchRequiresRack),
        "stretch profile is configured but rack is unset: a node in a stretch cluster must name \
         the site it runs in",
    },
    stretch_rack_not_in_profile = {
        BrokerError::Config(ConfigError::StretchRackNotInProfile { rack: "r".into() }),
        "rack r is not one of stretch.sites",
    },
    gssapi_config_missing = {
        BrokerError::Config(ConfigError::GssapiConfigMissing),
        "GSSAPI is an enabled SASL mechanism but gssapi config is missing",
    },
    invalid_leader_rebalance_interval = {
        BrokerError::Config(ConfigError::InvalidLeaderRebalanceInterval { value: 0 }),
        "invalid leader_imbalance_check_interval = 0: must be >= 1",
    },
)]
fn config_errors_keep_their_message_and_map_to_unknown_server_error(
    err: BrokerError,
    message: &str,
) {
    assert2::assert!(err.to_string() == message);
    assert2::assert!(from_broker_error(&err) == UNKNOWN_SERVER_ERROR);
}
