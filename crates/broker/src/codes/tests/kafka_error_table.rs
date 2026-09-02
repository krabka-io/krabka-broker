//! The Apache Kafka error table, extracted from the pinned container image.
//!
//! Every pair below comes from `org.apache.kafka.common.protocol.Errors` in
//! `kafka-clients` inside `mirror.gcr.io/apache/kafka:4.3.1`, the newest image
//! that `//bazel/images` pins. The extraction iterated `Errors.values()` and
//! printed `code()` and `name()`, so the table is the JVM's own view of the
//! wire numbers rather than a transcription of prose.
//!
//! `every_kafka_range_constant_appears_in_the_kafka_table` checks each
//! constant in [`crate::codes`] against this table, which is what stops a
//! later addition inventing a number.
//!
//! Re-deriving it needs the image the digest below names, and nothing else:
//!
//! ```text
//! image=mirror.gcr.io/apache/kafka@<KAFKA_ERROR_TABLE_IMAGE_DIGEST>
//! id=$(docker create "${image}" true)
//! docker cp "${id}:/opt/kafka/libs/kafka-clients-4.3.1.jar" .
//! docker cp "${id}:/opt/kafka/libs/slf4j-api-1.7.36.jar" .
//! # Dump.java: for (Errors e : Errors.values()) print e.name() and e.code().
//! java -cp 'kafka-clients-4.3.1.jar:slf4j-api-1.7.36.jar' Dump.java
//! ```
//!
//! The image ships a JRE rather than a JDK, so the dump runs on the host's
//! `java` against the jars copied out of it.

/// The image digest [`KAFKA_ERROR_TABLE`] was extracted from.
///
/// `the_error_table_records_the_image_the_build_pins` compares this with the
/// digest `//MODULE.bazel` pins for `apache_kafka_4_3_1`, which the build hands
/// the compilation rather than the test reading it from the tree. The table is
/// hermetic on purpose -- nothing here needs a container -- and this is what
/// keeps it honest: bumping the pin fails that guard until the table above is
/// re-derived from the new image.
///
/// The jar the recorded extraction read is `kafka-clients-4.3.1.jar`, sha256
/// `dc3d65e3ac811a446184ea1dca0fe9cf957c2d8984dcb4668d01f4b77fc8f50e`.
pub(super) const KAFKA_ERROR_TABLE_IMAGE_DIGEST: &str =
    "sha256:ccd1314e47ec76909e01f86308b4dcf2064f19f7c89759234322314b0e319e26";

/// Name and wire code of every variant of the Apache Kafka `Errors` enum.
pub(super) const KAFKA_ERROR_TABLE: &[(&str, i16)] = &[
    ("UNKNOWN_SERVER_ERROR", -1),
    ("NONE", 0),
    ("OFFSET_OUT_OF_RANGE", 1),
    ("CORRUPT_MESSAGE", 2),
    ("UNKNOWN_TOPIC_OR_PARTITION", 3),
    ("INVALID_FETCH_SIZE", 4),
    ("LEADER_NOT_AVAILABLE", 5),
    ("NOT_LEADER_OR_FOLLOWER", 6),
    ("REQUEST_TIMED_OUT", 7),
    ("BROKER_NOT_AVAILABLE", 8),
    ("REPLICA_NOT_AVAILABLE", 9),
    ("MESSAGE_TOO_LARGE", 10),
    ("STALE_CONTROLLER_EPOCH", 11),
    ("OFFSET_METADATA_TOO_LARGE", 12),
    ("NETWORK_EXCEPTION", 13),
    ("COORDINATOR_LOAD_IN_PROGRESS", 14),
    ("COORDINATOR_NOT_AVAILABLE", 15),
    ("NOT_COORDINATOR", 16),
    ("INVALID_TOPIC_EXCEPTION", 17),
    ("RECORD_LIST_TOO_LARGE", 18),
    ("NOT_ENOUGH_REPLICAS", 19),
    ("NOT_ENOUGH_REPLICAS_AFTER_APPEND", 20),
    ("INVALID_REQUIRED_ACKS", 21),
    ("ILLEGAL_GENERATION", 22),
    ("INCONSISTENT_GROUP_PROTOCOL", 23),
    ("INVALID_GROUP_ID", 24),
    ("UNKNOWN_MEMBER_ID", 25),
    ("INVALID_SESSION_TIMEOUT", 26),
    ("REBALANCE_IN_PROGRESS", 27),
    ("INVALID_COMMIT_OFFSET_SIZE", 28),
    ("TOPIC_AUTHORIZATION_FAILED", 29),
    ("GROUP_AUTHORIZATION_FAILED", 30),
    ("CLUSTER_AUTHORIZATION_FAILED", 31),
    ("INVALID_TIMESTAMP", 32),
    ("UNSUPPORTED_SASL_MECHANISM", 33),
    ("ILLEGAL_SASL_STATE", 34),
    ("UNSUPPORTED_VERSION", 35),
    ("TOPIC_ALREADY_EXISTS", 36),
    ("INVALID_PARTITIONS", 37),
    ("INVALID_REPLICATION_FACTOR", 38),
    ("INVALID_REPLICA_ASSIGNMENT", 39),
    ("INVALID_CONFIG", 40),
    ("NOT_CONTROLLER", 41),
    ("INVALID_REQUEST", 42),
    ("UNSUPPORTED_FOR_MESSAGE_FORMAT", 43),
    ("POLICY_VIOLATION", 44),
    ("OUT_OF_ORDER_SEQUENCE_NUMBER", 45),
    ("DUPLICATE_SEQUENCE_NUMBER", 46),
    ("INVALID_PRODUCER_EPOCH", 47),
    ("INVALID_TXN_STATE", 48),
    ("INVALID_PRODUCER_ID_MAPPING", 49),
    ("INVALID_TRANSACTION_TIMEOUT", 50),
    ("CONCURRENT_TRANSACTIONS", 51),
    ("TRANSACTION_COORDINATOR_FENCED", 52),
    ("TRANSACTIONAL_ID_AUTHORIZATION_FAILED", 53),
    ("SECURITY_DISABLED", 54),
    ("OPERATION_NOT_ATTEMPTED", 55),
    ("KAFKA_STORAGE_ERROR", 56),
    ("LOG_DIR_NOT_FOUND", 57),
    ("SASL_AUTHENTICATION_FAILED", 58),
    ("UNKNOWN_PRODUCER_ID", 59),
    ("REASSIGNMENT_IN_PROGRESS", 60),
    ("DELEGATION_TOKEN_AUTH_DISABLED", 61),
    ("DELEGATION_TOKEN_NOT_FOUND", 62),
    ("DELEGATION_TOKEN_OWNER_MISMATCH", 63),
    ("DELEGATION_TOKEN_REQUEST_NOT_ALLOWED", 64),
    ("DELEGATION_TOKEN_AUTHORIZATION_FAILED", 65),
    ("DELEGATION_TOKEN_EXPIRED", 66),
    ("INVALID_PRINCIPAL_TYPE", 67),
    ("NON_EMPTY_GROUP", 68),
    ("GROUP_ID_NOT_FOUND", 69),
    ("FETCH_SESSION_ID_NOT_FOUND", 70),
    ("INVALID_FETCH_SESSION_EPOCH", 71),
    ("LISTENER_NOT_FOUND", 72),
    ("TOPIC_DELETION_DISABLED", 73),
    ("FENCED_LEADER_EPOCH", 74),
    ("UNKNOWN_LEADER_EPOCH", 75),
    ("UNSUPPORTED_COMPRESSION_TYPE", 76),
    ("STALE_BROKER_EPOCH", 77),
    ("OFFSET_NOT_AVAILABLE", 78),
    ("MEMBER_ID_REQUIRED", 79),
    ("PREFERRED_LEADER_NOT_AVAILABLE", 80),
    ("GROUP_MAX_SIZE_REACHED", 81),
    ("FENCED_INSTANCE_ID", 82),
    ("ELIGIBLE_LEADERS_NOT_AVAILABLE", 83),
    ("ELECTION_NOT_NEEDED", 84),
    ("NO_REASSIGNMENT_IN_PROGRESS", 85),
    ("GROUP_SUBSCRIBED_TO_TOPIC", 86),
    ("INVALID_RECORD", 87),
    ("UNSTABLE_OFFSET_COMMIT", 88),
    ("THROTTLING_QUOTA_EXCEEDED", 89),
    ("PRODUCER_FENCED", 90),
    ("RESOURCE_NOT_FOUND", 91),
    ("DUPLICATE_RESOURCE", 92),
    ("UNACCEPTABLE_CREDENTIAL", 93),
    ("INCONSISTENT_VOTER_SET", 94),
    ("INVALID_UPDATE_VERSION", 95),
    ("FEATURE_UPDATE_FAILED", 96),
    ("PRINCIPAL_DESERIALIZATION_FAILURE", 97),
    ("SNAPSHOT_NOT_FOUND", 98),
    ("POSITION_OUT_OF_RANGE", 99),
    ("UNKNOWN_TOPIC_ID", 100),
    ("DUPLICATE_BROKER_REGISTRATION", 101),
    ("BROKER_ID_NOT_REGISTERED", 102),
    ("INCONSISTENT_TOPIC_ID", 103),
    ("INCONSISTENT_CLUSTER_ID", 104),
    ("TRANSACTIONAL_ID_NOT_FOUND", 105),
    ("FETCH_SESSION_TOPIC_ID_ERROR", 106),
    ("INELIGIBLE_REPLICA", 107),
    ("NEW_LEADER_ELECTED", 108),
    ("OFFSET_MOVED_TO_TIERED_STORAGE", 109),
    ("FENCED_MEMBER_EPOCH", 110),
    ("UNRELEASED_INSTANCE_ID", 111),
    ("UNSUPPORTED_ASSIGNOR", 112),
    ("STALE_MEMBER_EPOCH", 113),
    ("MISMATCHED_ENDPOINT_TYPE", 114),
    ("UNSUPPORTED_ENDPOINT_TYPE", 115),
    ("UNKNOWN_CONTROLLER_ID", 116),
    ("UNKNOWN_SUBSCRIPTION_ID", 117),
    ("TELEMETRY_TOO_LARGE", 118),
    ("INVALID_REGISTRATION", 119),
    ("TRANSACTION_ABORTABLE", 120),
    ("INVALID_RECORD_STATE", 121),
    ("SHARE_SESSION_NOT_FOUND", 122),
    ("INVALID_SHARE_SESSION_EPOCH", 123),
    ("FENCED_STATE_EPOCH", 124),
    ("INVALID_VOTER_KEY", 125),
    ("DUPLICATE_VOTER", 126),
    ("VOTER_NOT_FOUND", 127),
    ("INVALID_REGULAR_EXPRESSION", 128),
    ("REBOOTSTRAP_REQUIRED", 129),
    ("STREAMS_INVALID_TOPOLOGY", 130),
    ("STREAMS_INVALID_TOPOLOGY_EPOCH", 131),
    ("STREAMS_TOPOLOGY_FENCED", 132),
    ("SHARE_SESSION_LIMIT_REACHED", 133),
];
