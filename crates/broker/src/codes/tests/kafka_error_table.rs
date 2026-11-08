//! The Apache Kafka error table, extracted from the pinned container image.
//!
//! Every row below comes from `org.apache.kafka.common.protocol.Errors` in
//! `kafka-clients` inside `mirror.gcr.io/apache/kafka:4.3.1`, the newest image
//! that `//bazel/images` pins. The extraction iterated `Errors.values()` and
//! printed `code()`, `name()` and `message()`, so the table is the JVM's own
//! view of the wire numbers and default messages rather than a transcription
//! of prose.
//!
//! The messages were added from the same iteration run against
//! `kafka-clients-4.3.1.jar` from Maven Central (sha256
//! `52501b7b47510c66f898871adaf6d2968ab7246561d44ced43643a8a587f0b36`). Its
//! names and codes match the image's row for row.
//!
//! The released-code guard checks each constant in [`crate::codes`], with its
//! message, against this table, which is what stops a later addition inventing
//! a number or rewording a message a client reads.
//!
//! Re-deriving it needs the image the digest below names, and nothing else:
//!
//! ```text
//! image=mirror.gcr.io/apache/kafka@<KAFKA_ERROR_TABLE_IMAGE_DIGEST>
//! id=$(docker create "${image}" true)
//! docker cp "${id}:/opt/kafka/libs/kafka-clients-4.3.1.jar" .
//! docker cp "${id}:/opt/kafka/libs/slf4j-api-1.7.36.jar" .
//! # Dump.java: for (Errors e : Errors.values()) print e.code(), e.name()
//! # and e.message().
//! java -cp 'kafka-clients-4.3.1.jar:slf4j-api-1.7.36.jar' Dump.java
//! ```
//!
//! The image ships a JRE rather than a JDK, so the dump runs on the host's
//! `java` against the jars copied out of it.

use crate::codes::NamedCode;

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

/// Name, wire code and default message of every variant of the Apache Kafka
/// `Errors` enum.
pub(super) const KAFKA_ERROR_TABLE: &[NamedCode] = &[
    NamedCode {
        name: "UNKNOWN_SERVER_ERROR",
        code: -1,
        message: Some("The server experienced an unexpected error when processing the request."),
    },
    NamedCode {
        name: "NONE",
        code: 0,
        message: Some("NONE"),
    },
    NamedCode {
        name: "OFFSET_OUT_OF_RANGE",
        code: 1,
        message: Some(
            "The requested offset is not within the range of offsets maintained by the server.",
        ),
    },
    NamedCode {
        name: "CORRUPT_MESSAGE",
        code: 2,
        message: Some(
            "This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, or is otherwise corrupt.",
        ),
    },
    NamedCode {
        name: "UNKNOWN_TOPIC_OR_PARTITION",
        code: 3,
        message: Some("This server does not host this topic-partition."),
    },
    NamedCode {
        name: "INVALID_FETCH_SIZE",
        code: 4,
        message: Some("The requested fetch size is invalid."),
    },
    NamedCode {
        name: "LEADER_NOT_AVAILABLE",
        code: 5,
        message: Some(
            "There is no leader for this topic-partition as we are in the middle of a leadership election.",
        ),
    },
    NamedCode {
        name: "NOT_LEADER_OR_FOLLOWER",
        code: 6,
        message: Some(
            "For requests intended only for the leader, this error indicates that the broker is not the current leader. For requests intended for any replica, this error indicates that the broker is not a replica of the topic partition.",
        ),
    },
    NamedCode {
        name: "REQUEST_TIMED_OUT",
        code: 7,
        message: Some("The request timed out."),
    },
    NamedCode {
        name: "BROKER_NOT_AVAILABLE",
        code: 8,
        message: Some("The broker is not available."),
    },
    NamedCode {
        name: "REPLICA_NOT_AVAILABLE",
        code: 9,
        message: Some(
            "The replica is not available for the requested topic-partition. Produce/Fetch requests and other requests intended only for the leader or follower return NOT_LEADER_OR_FOLLOWER if the broker is not a replica of the topic-partition.",
        ),
    },
    NamedCode {
        name: "MESSAGE_TOO_LARGE",
        code: 10,
        message: Some(
            "The request included a message larger than the max message size the server will accept.",
        ),
    },
    NamedCode {
        name: "STALE_CONTROLLER_EPOCH",
        code: 11,
        message: Some("The controller moved to another broker."),
    },
    NamedCode {
        name: "OFFSET_METADATA_TOO_LARGE",
        code: 12,
        message: Some("The metadata field of the offset request was too large."),
    },
    NamedCode {
        name: "NETWORK_EXCEPTION",
        code: 13,
        message: Some("The server disconnected before a response was received."),
    },
    NamedCode {
        name: "COORDINATOR_LOAD_IN_PROGRESS",
        code: 14,
        message: Some("The coordinator is loading and hence can't process requests."),
    },
    NamedCode {
        name: "COORDINATOR_NOT_AVAILABLE",
        code: 15,
        message: Some("The coordinator is not available."),
    },
    NamedCode {
        name: "NOT_COORDINATOR",
        code: 16,
        message: Some("This is not the correct coordinator."),
    },
    NamedCode {
        name: "INVALID_TOPIC_EXCEPTION",
        code: 17,
        message: Some("The request attempted to perform an operation on an invalid topic."),
    },
    NamedCode {
        name: "RECORD_LIST_TOO_LARGE",
        code: 18,
        message: Some(
            "The request included message batch larger than the configured segment size on the server.",
        ),
    },
    NamedCode {
        name: "NOT_ENOUGH_REPLICAS",
        code: 19,
        message: Some(
            "Messages are rejected since there are fewer in-sync replicas than required.",
        ),
    },
    NamedCode {
        name: "NOT_ENOUGH_REPLICAS_AFTER_APPEND",
        code: 20,
        message: Some(
            "Messages are written to the log, but to fewer in-sync replicas than required.",
        ),
    },
    NamedCode {
        name: "INVALID_REQUIRED_ACKS",
        code: 21,
        message: Some("Produce request specified an invalid value for required acks."),
    },
    NamedCode {
        name: "ILLEGAL_GENERATION",
        code: 22,
        message: Some("Specified group generation id is not valid."),
    },
    NamedCode {
        name: "INCONSISTENT_GROUP_PROTOCOL",
        code: 23,
        message: Some(
            "The group member's supported protocols are incompatible with those of existing members or first group member tried to join with empty protocol type or empty protocol list.",
        ),
    },
    NamedCode {
        name: "INVALID_GROUP_ID",
        code: 24,
        message: Some("The group id is invalid."),
    },
    NamedCode {
        name: "UNKNOWN_MEMBER_ID",
        code: 25,
        message: Some("The coordinator is not aware of this member."),
    },
    NamedCode {
        name: "INVALID_SESSION_TIMEOUT",
        code: 26,
        message: Some(
            "The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).",
        ),
    },
    NamedCode {
        name: "REBALANCE_IN_PROGRESS",
        code: 27,
        message: Some("The group is rebalancing, so a rejoin is needed."),
    },
    NamedCode {
        name: "INVALID_COMMIT_OFFSET_SIZE",
        code: 28,
        message: Some("The committing offset data size is not valid."),
    },
    NamedCode {
        name: "TOPIC_AUTHORIZATION_FAILED",
        code: 29,
        message: Some("Topic authorization failed."),
    },
    NamedCode {
        name: "GROUP_AUTHORIZATION_FAILED",
        code: 30,
        message: Some("Group authorization failed."),
    },
    NamedCode {
        name: "CLUSTER_AUTHORIZATION_FAILED",
        code: 31,
        message: Some("Cluster authorization failed."),
    },
    NamedCode {
        name: "INVALID_TIMESTAMP",
        code: 32,
        message: Some("The timestamp of the message is out of acceptable range."),
    },
    NamedCode {
        name: "UNSUPPORTED_SASL_MECHANISM",
        code: 33,
        message: Some("The broker does not support the requested SASL mechanism."),
    },
    NamedCode {
        name: "ILLEGAL_SASL_STATE",
        code: 34,
        message: Some("Request is not valid given the current SASL state."),
    },
    NamedCode {
        name: "UNSUPPORTED_VERSION",
        code: 35,
        message: Some("The version of API is not supported."),
    },
    NamedCode {
        name: "TOPIC_ALREADY_EXISTS",
        code: 36,
        message: Some("Topic with this name already exists."),
    },
    NamedCode {
        name: "INVALID_PARTITIONS",
        code: 37,
        message: Some("Number of partitions is below 1."),
    },
    NamedCode {
        name: "INVALID_REPLICATION_FACTOR",
        code: 38,
        message: Some(
            "Replication factor is below 1 or larger than the number of available brokers.",
        ),
    },
    NamedCode {
        name: "INVALID_REPLICA_ASSIGNMENT",
        code: 39,
        message: Some("Replica assignment is invalid."),
    },
    NamedCode {
        name: "INVALID_CONFIG",
        code: 40,
        message: Some("Configuration is invalid."),
    },
    NamedCode {
        name: "NOT_CONTROLLER",
        code: 41,
        message: Some("This is not the correct controller for this cluster."),
    },
    NamedCode {
        name: "INVALID_REQUEST",
        code: 42,
        message: Some(
            "This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details.",
        ),
    },
    NamedCode {
        name: "UNSUPPORTED_FOR_MESSAGE_FORMAT",
        code: 43,
        message: Some("The message format version on the broker does not support the request."),
    },
    NamedCode {
        name: "POLICY_VIOLATION",
        code: 44,
        message: Some("Request parameters do not satisfy the configured policy."),
    },
    NamedCode {
        name: "OUT_OF_ORDER_SEQUENCE_NUMBER",
        code: 45,
        message: Some("The broker received an out of order sequence number."),
    },
    NamedCode {
        name: "DUPLICATE_SEQUENCE_NUMBER",
        code: 46,
        message: Some("The broker received a duplicate sequence number."),
    },
    NamedCode {
        name: "INVALID_PRODUCER_EPOCH",
        code: 47,
        message: Some("Producer attempted to produce with an old epoch."),
    },
    NamedCode {
        name: "INVALID_TXN_STATE",
        code: 48,
        message: Some("The producer attempted a transactional operation in an invalid state."),
    },
    NamedCode {
        name: "INVALID_PRODUCER_ID_MAPPING",
        code: 49,
        message: Some(
            "The producer attempted to use a producer id which is not currently assigned to its transactional id.",
        ),
    },
    NamedCode {
        name: "INVALID_TRANSACTION_TIMEOUT",
        code: 50,
        message: Some(
            "The transaction timeout is larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms).",
        ),
    },
    NamedCode {
        name: "CONCURRENT_TRANSACTIONS",
        code: 51,
        message: Some(
            "The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing.",
        ),
    },
    NamedCode {
        name: "TRANSACTION_COORDINATOR_FENCED",
        code: 52,
        message: Some(
            "Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer.",
        ),
    },
    NamedCode {
        name: "TRANSACTIONAL_ID_AUTHORIZATION_FAILED",
        code: 53,
        message: Some("Transactional Id authorization failed."),
    },
    NamedCode {
        name: "SECURITY_DISABLED",
        code: 54,
        message: Some("Security features are disabled."),
    },
    NamedCode {
        name: "OPERATION_NOT_ATTEMPTED",
        code: 55,
        message: Some(
            "The broker did not attempt to execute this operation. This may happen for batched RPCs where some operations in the batch failed, causing the broker to respond without trying the rest.",
        ),
    },
    NamedCode {
        name: "KAFKA_STORAGE_ERROR",
        code: 56,
        message: Some("Disk error when trying to access log file on the disk."),
    },
    NamedCode {
        name: "LOG_DIR_NOT_FOUND",
        code: 57,
        message: Some("The user-specified log directory is not found in the broker config."),
    },
    NamedCode {
        name: "SASL_AUTHENTICATION_FAILED",
        code: 58,
        message: Some("SASL Authentication failed."),
    },
    NamedCode {
        name: "UNKNOWN_PRODUCER_ID",
        code: 59,
        message: Some(
            "This exception is raised by the broker if it could not locate the producer metadata associated with the producerId in question. This could happen if, for instance, the producer's records were deleted because their retention time had elapsed. Once the last records of the producerId are removed, the producer's metadata is removed from the broker, and future appends by the producer will return this exception.",
        ),
    },
    NamedCode {
        name: "REASSIGNMENT_IN_PROGRESS",
        code: 60,
        message: Some("A partition reassignment is in progress."),
    },
    NamedCode {
        name: "DELEGATION_TOKEN_AUTH_DISABLED",
        code: 61,
        message: Some("Delegation Token feature is not enabled."),
    },
    NamedCode {
        name: "DELEGATION_TOKEN_NOT_FOUND",
        code: 62,
        message: Some("Delegation Token is not found on server."),
    },
    NamedCode {
        name: "DELEGATION_TOKEN_OWNER_MISMATCH",
        code: 63,
        message: Some("Specified Principal is not valid Owner/Renewer."),
    },
    NamedCode {
        name: "DELEGATION_TOKEN_REQUEST_NOT_ALLOWED",
        code: 64,
        message: Some(
            "Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels.",
        ),
    },
    NamedCode {
        name: "DELEGATION_TOKEN_AUTHORIZATION_FAILED",
        code: 65,
        message: Some("Delegation Token authorization failed."),
    },
    NamedCode {
        name: "DELEGATION_TOKEN_EXPIRED",
        code: 66,
        message: Some("Delegation Token is expired."),
    },
    NamedCode {
        name: "INVALID_PRINCIPAL_TYPE",
        code: 67,
        message: Some("Supplied principalType is not supported."),
    },
    NamedCode {
        name: "NON_EMPTY_GROUP",
        code: 68,
        message: Some("The group is not empty."),
    },
    NamedCode {
        name: "GROUP_ID_NOT_FOUND",
        code: 69,
        message: Some("The group id does not exist."),
    },
    NamedCode {
        name: "FETCH_SESSION_ID_NOT_FOUND",
        code: 70,
        message: Some("The fetch session ID was not found."),
    },
    NamedCode {
        name: "INVALID_FETCH_SESSION_EPOCH",
        code: 71,
        message: Some("The fetch session epoch is invalid."),
    },
    NamedCode {
        name: "LISTENER_NOT_FOUND",
        code: 72,
        message: Some(
            "There is no listener on the leader broker that matches the listener on which metadata request was processed.",
        ),
    },
    NamedCode {
        name: "TOPIC_DELETION_DISABLED",
        code: 73,
        message: Some("Topic deletion is disabled."),
    },
    NamedCode {
        name: "FENCED_LEADER_EPOCH",
        code: 74,
        message: Some("The leader epoch in the request is older than the epoch on the broker."),
    },
    NamedCode {
        name: "UNKNOWN_LEADER_EPOCH",
        code: 75,
        message: Some("The leader epoch in the request is newer than the epoch on the broker."),
    },
    NamedCode {
        name: "UNSUPPORTED_COMPRESSION_TYPE",
        code: 76,
        message: Some(
            "The requesting client does not support the compression type of given partition.",
        ),
    },
    NamedCode {
        name: "STALE_BROKER_EPOCH",
        code: 77,
        message: Some("Broker epoch has changed."),
    },
    NamedCode {
        name: "OFFSET_NOT_AVAILABLE",
        code: 78,
        message: Some(
            "The leader high watermark has not caught up from a recent leader election so the offsets cannot be guaranteed to be monotonically increasing.",
        ),
    },
    NamedCode {
        name: "MEMBER_ID_REQUIRED",
        code: 79,
        message: Some(
            "The group member needs to have a valid member id before actually entering a consumer group.",
        ),
    },
    NamedCode {
        name: "PREFERRED_LEADER_NOT_AVAILABLE",
        code: 80,
        message: Some("The preferred leader was not available."),
    },
    NamedCode {
        name: "GROUP_MAX_SIZE_REACHED",
        code: 81,
        message: Some("The group has reached its maximum size."),
    },
    NamedCode {
        name: "FENCED_INSTANCE_ID",
        code: 82,
        message: Some(
            "The broker rejected this static consumer since another consumer with the same group.instance.id has registered with a different member.id.",
        ),
    },
    NamedCode {
        name: "ELIGIBLE_LEADERS_NOT_AVAILABLE",
        code: 83,
        message: Some("Eligible topic partition leaders are not available."),
    },
    NamedCode {
        name: "ELECTION_NOT_NEEDED",
        code: 84,
        message: Some("Leader election not needed for topic partition."),
    },
    NamedCode {
        name: "NO_REASSIGNMENT_IN_PROGRESS",
        code: 85,
        message: Some("No partition reassignment is in progress."),
    },
    NamedCode {
        name: "GROUP_SUBSCRIBED_TO_TOPIC",
        code: 86,
        message: Some(
            "Deleting offsets of a topic is forbidden while the consumer group is actively subscribed to it.",
        ),
    },
    NamedCode {
        name: "INVALID_RECORD",
        code: 87,
        message: Some(
            "This record has failed the validation on broker and hence will be rejected.",
        ),
    },
    NamedCode {
        name: "UNSTABLE_OFFSET_COMMIT",
        code: 88,
        message: Some("There are unstable offsets that need to be cleared."),
    },
    NamedCode {
        name: "THROTTLING_QUOTA_EXCEEDED",
        code: 89,
        message: Some("The throttling quota has been exceeded."),
    },
    NamedCode {
        name: "PRODUCER_FENCED",
        code: 90,
        message: Some(
            "There is a newer producer with the same transactionalId which fences the current one.",
        ),
    },
    NamedCode {
        name: "RESOURCE_NOT_FOUND",
        code: 91,
        message: Some("A request illegally referred to a resource that does not exist."),
    },
    NamedCode {
        name: "DUPLICATE_RESOURCE",
        code: 92,
        message: Some("A request illegally referred to the same resource twice."),
    },
    NamedCode {
        name: "UNACCEPTABLE_CREDENTIAL",
        code: 93,
        message: Some("Requested credential would not meet criteria for acceptability."),
    },
    NamedCode {
        name: "INCONSISTENT_VOTER_SET",
        code: 94,
        message: Some(
            "Indicates that the either the sender or recipient of a voter-only request is not one of the expected voters.",
        ),
    },
    NamedCode {
        name: "INVALID_UPDATE_VERSION",
        code: 95,
        message: Some("The given update version was invalid."),
    },
    NamedCode {
        name: "FEATURE_UPDATE_FAILED",
        code: 96,
        message: Some("Unable to update finalized features due to an unexpected server error."),
    },
    NamedCode {
        name: "PRINCIPAL_DESERIALIZATION_FAILURE",
        code: 97,
        message: Some(
            "Request principal deserialization failed during forwarding. This indicates an internal error on the broker cluster security setup.",
        ),
    },
    NamedCode {
        name: "SNAPSHOT_NOT_FOUND",
        code: 98,
        message: Some("Requested snapshot was not found."),
    },
    NamedCode {
        name: "POSITION_OUT_OF_RANGE",
        code: 99,
        message: Some(
            "Requested position is not greater than or equal to zero, and less than the size of the snapshot.",
        ),
    },
    NamedCode {
        name: "UNKNOWN_TOPIC_ID",
        code: 100,
        message: Some("This server does not host this topic ID."),
    },
    NamedCode {
        name: "DUPLICATE_BROKER_REGISTRATION",
        code: 101,
        message: Some("This broker ID is already in use."),
    },
    NamedCode {
        name: "BROKER_ID_NOT_REGISTERED",
        code: 102,
        message: Some("The given broker ID was not registered."),
    },
    NamedCode {
        name: "INCONSISTENT_TOPIC_ID",
        code: 103,
        message: Some("The log's topic ID did not match the topic ID in the request."),
    },
    NamedCode {
        name: "INCONSISTENT_CLUSTER_ID",
        code: 104,
        message: Some("The clusterId in the request does not match that found on the server."),
    },
    NamedCode {
        name: "TRANSACTIONAL_ID_NOT_FOUND",
        code: 105,
        message: Some("The transactionalId could not be found."),
    },
    NamedCode {
        name: "FETCH_SESSION_TOPIC_ID_ERROR",
        code: 106,
        message: Some("The fetch session encountered inconsistent topic ID usage."),
    },
    NamedCode {
        name: "INELIGIBLE_REPLICA",
        code: 107,
        message: Some("The new ISR contains at least one ineligible replica."),
    },
    NamedCode {
        name: "NEW_LEADER_ELECTED",
        code: 108,
        message: Some(
            "The AlterPartition request successfully updated the partition state but the leader has changed.",
        ),
    },
    NamedCode {
        name: "OFFSET_MOVED_TO_TIERED_STORAGE",
        code: 109,
        message: Some("The requested offset is moved to tiered storage."),
    },
    NamedCode {
        name: "FENCED_MEMBER_EPOCH",
        code: 110,
        message: Some(
            "The member epoch is fenced by the group coordinator. The member must abandon all its partitions and rejoin.",
        ),
    },
    NamedCode {
        name: "UNRELEASED_INSTANCE_ID",
        code: 111,
        message: Some(
            "The instance ID is still used by another member in the consumer group. That member must leave first.",
        ),
    },
    NamedCode {
        name: "UNSUPPORTED_ASSIGNOR",
        code: 112,
        message: Some("The assignor or its version range is not supported by the consumer group."),
    },
    NamedCode {
        name: "STALE_MEMBER_EPOCH",
        code: 113,
        message: Some(
            "The member epoch is stale. The member must retry after receiving its updated member epoch via the ConsumerGroupHeartbeat API.",
        ),
    },
    NamedCode {
        name: "MISMATCHED_ENDPOINT_TYPE",
        code: 114,
        message: Some("The request was sent to an endpoint of the wrong type."),
    },
    NamedCode {
        name: "UNSUPPORTED_ENDPOINT_TYPE",
        code: 115,
        message: Some("This endpoint type is not supported yet."),
    },
    NamedCode {
        name: "UNKNOWN_CONTROLLER_ID",
        code: 116,
        message: Some("This controller ID is not known."),
    },
    NamedCode {
        name: "UNKNOWN_SUBSCRIPTION_ID",
        code: 117,
        message: Some(
            "Client sent a push telemetry request with an invalid or outdated subscription ID.",
        ),
    },
    NamedCode {
        name: "TELEMETRY_TOO_LARGE",
        code: 118,
        message: Some(
            "Client sent a push telemetry request larger than the maximum size the broker will accept.",
        ),
    },
    NamedCode {
        name: "INVALID_REGISTRATION",
        code: 119,
        message: Some("The controller has considered the broker registration to be invalid."),
    },
    NamedCode {
        name: "TRANSACTION_ABORTABLE",
        code: 120,
        message: Some(
            "The server encountered an error with the transaction. The client can abort the transaction to continue using this transactional ID.",
        ),
    },
    NamedCode {
        name: "INVALID_RECORD_STATE",
        code: 121,
        message: Some(
            "The record state is invalid. The acknowledgement of delivery could not be completed.",
        ),
    },
    NamedCode {
        name: "SHARE_SESSION_NOT_FOUND",
        code: 122,
        message: Some("The share session was not found."),
    },
    NamedCode {
        name: "INVALID_SHARE_SESSION_EPOCH",
        code: 123,
        message: Some("The share session epoch is invalid."),
    },
    NamedCode {
        name: "FENCED_STATE_EPOCH",
        code: 124,
        message: Some(
            "The share coordinator rejected the request because the share-group state epoch did not match.",
        ),
    },
    NamedCode {
        name: "INVALID_VOTER_KEY",
        code: 125,
        message: Some("The voter key doesn't match the receiving replica's key."),
    },
    NamedCode {
        name: "DUPLICATE_VOTER",
        code: 126,
        message: Some("The voter is already part of the set of voters."),
    },
    NamedCode {
        name: "VOTER_NOT_FOUND",
        code: 127,
        message: Some("The voter is not part of the set of voters."),
    },
    NamedCode {
        name: "INVALID_REGULAR_EXPRESSION",
        code: 128,
        message: Some("The regular expression is not valid."),
    },
    NamedCode {
        name: "REBOOTSTRAP_REQUIRED",
        code: 129,
        message: Some(
            "Client metadata is stale. The client should rebootstrap to obtain new metadata.",
        ),
    },
    NamedCode {
        name: "STREAMS_INVALID_TOPOLOGY",
        code: 130,
        message: Some("The supplied topology is invalid."),
    },
    NamedCode {
        name: "STREAMS_INVALID_TOPOLOGY_EPOCH",
        code: 131,
        message: Some("The supplied topology epoch is invalid."),
    },
    NamedCode {
        name: "STREAMS_TOPOLOGY_FENCED",
        code: 132,
        message: Some("The supplied topology epoch is outdated."),
    },
    NamedCode {
        name: "SHARE_SESSION_LIMIT_REACHED",
        code: 133,
        message: Some("The limit of share sessions has been reached."),
    },
];
