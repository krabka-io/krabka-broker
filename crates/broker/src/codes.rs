//! Kafka wire-level error codes used by the broker.
//!
//! Per-(topic, partition) response fields use these `i16` values. JVM clients
//! react to specific codes, so a substitution changes client behavior. Every
//! constant in the Kafka range comes from `Errors.values()` in the
//! `kafka-clients` jar of the pinned `apache/kafka:4.3.1` image, and a guard
//! test in the private `tests` module re-checks each one against that extracted
//! table.
//!
//! The private section of the declaration holds the krabka-private codes at
//! 1000 and above. A broker returns those only on a krabka-private api key.

use crate::error::BrokerError;

#[cfg(test)]
mod tests;

/// A wire code, the name a guard-test failure reports for it, and the message
/// its row declares. Kafka-range and trunk rows carry Kafka's
/// `Errors.message()`; krabka-private rows carry none.
#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct NamedCode {
    name: &'static str,
    code: i16,
    message: Option<&'static str>,
}

/// One of Kafka's `Errors` entries: `Errors.name()` and `Errors.message()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct KafkaError {
    name: &'static str,
    message: &'static str,
}

// Declares the constants that carry a wire code, and collects them into the
// named table for the guard test: `KAFKA_RANGE_CODES` for the Apache Kafka
// codes, `KAFKA_TRUNK_CODES` for the codes Kafka trunk assigns and no Kafka
// release has yet, and `KRABKA_PRIVATE_CODES` for the krabka-private ones.
//
// Each Kafka-range and trunk row also names Kafka's default message for the
// code after `=>`. A client reads those bytes as `error_message`, so
// `default_message` answers them from here rather than from copies.
//
// Declaring through the macro is what makes the guard total: a constant that
// is not written here is not a wire code, and a constant that is written here
// cannot escape the check against the extracted Kafka table, message
// included.
krabka_broker_macros::wire_codes! {
    release KAFKA_RANGE_CODES {
        NONE = 0 => "NONE";
        UNKNOWN_SERVER_ERROR = -1
            => "The server experienced an unexpected error when processing the request.";
        OFFSET_OUT_OF_RANGE = 1
            => "The requested offset is not within the range of offsets maintained by the server.";
        /// `CORRUPT_MESSAGE` (2): the broker received a record batch whose bytes
        /// are malformed, or whose CRC or magic does not match any supported
        /// format.
        CORRUPT_MESSAGE = 2
            => "This message has failed its CRC checksum, exceeds the valid size, has a null key for a compacted topic, or is otherwise corrupt.";
        UNKNOWN_TOPIC_OR_PARTITION = 3 => "This server does not host this topic-partition.";
        /// `LEADER_NOT_AVAILABLE` (5): the partition has no leader this broker can
        /// name. Kafka's `KRaftMetadataCache` sets it on a `Metadata` partition row
        /// whose `leader_id` it is answering as `-1`; a JVM client reads it as
        /// retriable and refreshes its metadata.
        LEADER_NOT_AVAILABLE = 5
            => "There is no leader for this topic-partition as we are in the middle of a leadership election.";
        NOT_LEADER_OR_FOLLOWER = 6
            => "For requests intended only for the leader, this error indicates that the broker is not the current leader. For requests intended for any replica, this error indicates that the broker is not a replica of the topic partition.";
        REQUEST_TIMED_OUT = 7 => "The request timed out.";
        /// `REPLICA_NOT_AVAILABLE` (9, KIP-113): this broker does not host the
        /// targeted replica. `AlterReplicaLogDirs` and `DescribeLogDirs` return it
        /// when the client names a `(topic, partition)` that the broker does not
        /// own.
        ///
        /// 11 is `STALE_CONTROLLER_EPOCH`, which a JVM `AdminClient` turns into
        /// `ControllerMovedException`, so a replica miss must not use it.
        REPLICA_NOT_AVAILABLE = 9
            => "The replica is not available for the requested topic-partition. Produce/Fetch requests and other requests intended only for the leader or follower return NOT_LEADER_OR_FOLLOWER if the broker is not a replica of the topic-partition.";
        /// `MESSAGE_TOO_LARGE` (10): a produced record or batch is larger than
        /// `max.message.bytes` for the topic, or larger than the broker's
        /// `message.max.bytes`. The JVM maps it to `RecordTooLargeException`,
        /// which no producer retries.
        MESSAGE_TOO_LARGE = 10
            => "The request included a message larger than the max message size the server will accept.";
        /// `RECORD_LIST_TOO_LARGE` (18): a produced record set is larger than the
        /// topic's `segment.bytes`. Kafka's `RecordBatchTooLargeException`, which a
        /// JVM producer does not retry.
        RECORD_LIST_TOO_LARGE = 18
            => "The request included message batch larger than the configured segment size on the server.";
        /// `OFFSET_METADATA_TOO_LARGE` (12): an `OffsetCommit` partition row
        /// carries committed metadata longer than `offset.metadata.max.bytes`.
        OFFSET_METADATA_TOO_LARGE = 12 => "The metadata field of the offset request was too large.";
        /// `KAFKA_STORAGE_ERROR` (56, KIP-113): a log-dir-level I/O failure on
        /// open, rename, remove, append, or read, or a concurrent move with a
        /// conflicting target.
        KAFKA_STORAGE_ERROR = 56 => "Disk error when trying to access log file on the disk.";
        /// `LOG_DIR_NOT_FOUND` (57, KIP-113): the destination directory in an
        /// `AlterReplicaLogDirs` request is not one of this broker's configured
        /// `log.dirs`.
        LOG_DIR_NOT_FOUND = 57
            => "The user-specified log directory is not found in the broker config.";
        /// `NETWORK_EXCEPTION` (13): the server disconnected before a response
        /// arrived. A broker reports it for an inter-broker call it could not
        /// complete, as Kafka's `AddPartitionsToTxnManager` does.
        NETWORK_EXCEPTION = 13 => "The server disconnected before a response was received.";
        COORDINATOR_NOT_AVAILABLE = 15 => "The coordinator is not available.";
        /// `COORDINATOR_LOAD_IN_PROGRESS` (14, KIP-848): the group coordinator is
        /// still loading state from `__consumer_offsets`. Clients should retry
        /// after a brief back-off.
        COORDINATOR_LOAD_IN_PROGRESS = 14
            => "The coordinator is loading and hence can't process requests.";
        NOT_COORDINATOR = 16 => "This is not the correct coordinator.";
        INVALID_TOPIC_EXCEPTION = 17
            => "The request attempted to perform an operation on an invalid topic.";
        /// `ILLEGAL_SASL_STATE` (34): the broker received a request on a SASL
        /// listener before the connection completed `SaslHandshake` and
        /// `SaslAuthenticate`, or in the wrong order. The broker closes the
        /// connection after it emits this code.
        ILLEGAL_SASL_STATE = 34 => "Request is not valid given the current SASL state.";
        /// `UNSUPPORTED_SASL_MECHANISM` (33): the client requested a SASL
        /// mechanism the broker does not offer on this listener.
        UNSUPPORTED_SASL_MECHANISM = 33
            => "The broker does not support the requested SASL mechanism.";
        UNSUPPORTED_VERSION = 35 => "The version of API is not supported.";
        /// `SASL_AUTHENTICATION_FAILED` (58) — the supplied credentials were
        /// rejected.
        SASL_AUTHENTICATION_FAILED = 58 => "SASL Authentication failed.";
        STALE_BROKER_EPOCH = 77 => "Broker epoch has changed.";
        BROKER_ID_NOT_REGISTERED = 102 => "The given broker ID was not registered.";
        DUPLICATE_BROKER_REGISTRATION = 101 => "This broker ID is already in use.";
        TOPIC_ALREADY_EXISTS = 36 => "Topic with this name already exists.";
        INVALID_PARTITIONS = 37 => "Number of partitions is below 1.";
        INVALID_REPLICATION_FACTOR = 38
            => "Replication factor is below 1 or larger than the number of available brokers.";
        NOT_CONTROLLER = 41 => "This is not the correct controller for this cluster.";
        /// `LISTENER_NOT_FOUND` (72): the leader of a partition is registered but
        /// has no endpoint on the listener the request arrived on. `Metadata` v6
        /// and later answer it beside a `-1` leader; earlier versions answer
        /// `LEADER_NOT_AVAILABLE`.
        LISTENER_NOT_FOUND = 72
            => "There is no listener on the leader broker that matches the listener on which metadata request was processed.";
        /// `TOPIC_DELETION_DISABLED` (73): `delete.topic.enable` is `false`, so
        /// `DeleteTopics` v3 and later refuse every topic.
        TOPIC_DELETION_DISABLED = 73 => "Topic deletion is disabled.";
        /// `INVALID_REQUEST` (42): the request is structurally or semantically
        /// unacceptable. It is also the code Kafka returns for a resource type
        /// that an `AlterConfigs` or `IncrementalAlterConfigs` broker does not
        /// support, because the protocol assigns no distinct code for that.
        INVALID_REQUEST = 42
            => "This most likely occurs because of a request being malformed by the client library or the message was sent to an incompatible broker. See the broker logs for more details.";
        /// `INVALID_REGULAR_EXPRESSION` (128): a request contains a malformed
        /// regex.
        INVALID_REGULAR_EXPRESSION = 128 => "The regular expression is not valid.";
        /// `REBOOTSTRAP_REQUIRED` (129, KIP-1242): the client connected using
        /// stale cluster or node metadata and must bootstrap again.
        REBOOTSTRAP_REQUIRED = 129
            => "Client metadata is stale. The client should rebootstrap to obtain new metadata.";
        /// `INVALID_RECORD` (87): a record-bearing payload is structurally
        /// malformed. Examples include an invalid Produce `MessageSet` and a
        /// `PushTelemetry` payload that cannot be decompressed or decoded as OTLP
        /// metrics.
        INVALID_RECORD = 87
            => "This record has failed the validation on broker and hence will be rejected.";

        // Group coordinator codes.
        ILLEGAL_GENERATION = 22 => "Specified group generation id is not valid.";
        INCONSISTENT_GROUP_PROTOCOL = 23
            => "The group member's supported protocols are incompatible with those of existing members or first group member tried to join with empty protocol type or empty protocol list.";
        /// `INVALID_GROUP_ID` (24): the request named the empty string as its
        /// `group_id`. Kafka's `GroupCoordinatorService` checks this before any
        /// group lookup.
        INVALID_GROUP_ID = 24 => "The group id is invalid.";
        UNKNOWN_MEMBER_ID = 25 => "The coordinator is not aware of this member.";
        /// `INVALID_SESSION_TIMEOUT` (26): a `JoinGroup` session timeout outside
        /// `group.min.session.timeout.ms` and `group.max.session.timeout.ms`.
        INVALID_SESSION_TIMEOUT = 26
            => "The session timeout is not within the range allowed by the broker (as configured by group.min.session.timeout.ms and group.max.session.timeout.ms).";
        REBALANCE_IN_PROGRESS = 27 => "The group is rebalancing, so a rejoin is needed.";
        /// `INVALID_TIMESTAMP` (32): a producer supplied a timestamp type or value
        /// that the broker cannot accept for the target topic.
        INVALID_TIMESTAMP = 32 => "The timestamp of the message is out of acceptable range.";
        MEMBER_ID_REQUIRED = 79
            => "The group member needs to have a valid member id before actually entering a consumer group.";
        /// `GROUP_MAX_SIZE_REACHED` (81): a new member cannot join because the
        /// group already has its configured maximum number of members.
        GROUP_MAX_SIZE_REACHED = 81 => "The group has reached its maximum size.";
        /// `UNSTABLE_OFFSET_COMMIT` (88, KIP-447): `OffsetFetch` with
        /// `require_stable` found an offset that an in-flight transaction has not
        /// yet resolved. The JVM consumer retries.
        UNSTABLE_OFFSET_COMMIT = 88 => "There are unstable offsets that need to be cleared.";

        // Idempotent-producer codes.
        OUT_OF_ORDER_SEQUENCE_NUMBER = 45 => "The broker received an out of order sequence number.";
        /// `INVALID_PRODUCER_EPOCH` (47): the producer's epoch does not match the
        /// coordinator's current epoch, or no transaction state exists for the
        /// given (`transactional_id`, `producer_id`) pair. The Rust producer
        /// client maps this code to `ProducerError::FencedProducer`.
        INVALID_PRODUCER_EPOCH = 47 => "Producer attempted to produce with an old epoch.";
        /// `INVALID_PRODUCER_ID_MAPPING` (49) — the transactional id is not mapped
        /// to the supplied producer id.
        INVALID_PRODUCER_ID_MAPPING = 49
            => "The producer attempted to use a producer id which is not currently assigned to its transactional id.";
        TRANSACTIONAL_ID_AUTHORIZATION_FAILED = 53 => "Transactional Id authorization failed.";

        // Transactional protocol codes.
        INVALID_TXN_STATE = 48
            => "The producer attempted a transactional operation in an invalid state.";
        /// `INVALID_TRANSACTION_TIMEOUT` (50): the `InitProducerId` request asked
        /// for a transaction timeout that is not positive, or that is above
        /// `transaction.max.timeout.ms`.
        INVALID_TRANSACTION_TIMEOUT = 50
            => "The transaction timeout is larger than the maximum value allowed by the broker (as configured by transaction.max.timeout.ms).";
        CONCURRENT_TRANSACTIONS = 51
            => "The producer attempted to update a transaction while another concurrent operation on the same transaction was ongoing.";
        /// `TRANSACTION_COORDINATOR_FENCED` (52): the marker came from an older
        /// transaction-coordinator generation than the partition has observed.
        TRANSACTION_COORDINATOR_FENCED = 52
            => "Indicates that the transaction coordinator sending a WriteTxnMarker is no longer the current coordinator for a given producer.";
        /// `PRODUCER_FENCED` (90, KIP-360): the request carried a
        /// `(producer_id, producer_epoch)` pair that the transaction coordinator
        /// no longer recognises as the transactional id's live producer. A JVM
        /// client raises `ProducerFencedException`, which is fatal to that
        /// producer instance.
        PRODUCER_FENCED = 90
            => "There is a newer producer with the same transactionalId which fences the current one.";
        /// `TRANSACTION_ABORTABLE` (120, KIP-890) — the operation failed but the
        /// transaction can still be aborted by the client; e.g.
        /// `AddPartitionsToTxn` verify-only found a partition that is not part of
        /// the ongoing transaction.
        TRANSACTION_ABORTABLE = 120
            => "The server encountered an error with the transaction. The client can abort the transaction to continue using this transactional ID.";

        /// `FENCED_INSTANCE_ID` (82, KIP-345): another client is currently pinned
        /// to the same `group.instance.id`. The losing client must exit. The
        /// broker fences it across `JoinGroup`, `SyncGroup`, `Heartbeat`,
        /// `OffsetCommit`, `TxnOffsetCommit`, and `LeaveGroup`.
        FENCED_INSTANCE_ID = 82
            => "The broker rejected this static consumer since another consumer with the same group.instance.id has registered with a different member.id.";

        /// `STALE_MEMBER_EPOCH` (113, KIP-848): the supplied member epoch is older
        /// than the coordinator's current epoch for the consumer group member.
        STALE_MEMBER_EPOCH = 113
            => "The member epoch is stale. The member must retry after receiving its updated member epoch via the ConsumerGroupHeartbeat API.";
        /// `FENCED_MEMBER_EPOCH` (110, KIP-848): the supplied member epoch is
        /// newer than the coordinator's. The consumer must rejoin from epoch 0.
        FENCED_MEMBER_EPOCH = 110
            => "The member epoch is fenced by the group coordinator. The member must abandon all its partitions and rejoin.";
        /// `UNSUPPORTED_ASSIGNOR` (112, KIP-848): the requested `server_assignor`
        /// is not enabled on this broker.
        UNSUPPORTED_ASSIGNOR = 112
            => "The assignor or its version range is not supported by the consumer group.";
        /// `UNRELEASED_INSTANCE_ID` (111, KIP-848 + KIP-345): the static
        /// `instance_id` is still bound to a live member of the group.
        UNRELEASED_INSTANCE_ID = 111
            => "The instance ID is still used by another member in the consumer group. That member must leave first.";
        /// `MISMATCHED_ENDPOINT_TYPE` (114, KIP-919): the request reached a broker
        /// endpoint while asking for controllers, or vice versa.
        MISMATCHED_ENDPOINT_TYPE = 114 => "The request was sent to an endpoint of the wrong type.";
        /// `UNSUPPORTED_ENDPOINT_TYPE` (115, KIP-919): the request named an
        /// `endpoint_type` this broker does not recognize (neither `BROKER` nor
        /// `CONTROLLER`).
        UNSUPPORTED_ENDPOINT_TYPE = 115 => "This endpoint type is not supported yet.";
        /// `UNKNOWN_SUBSCRIPTION_ID` (117, KIP-848): the coordinator did not find
        /// the consumer's persisted subscription identifier.
        UNKNOWN_SUBSCRIPTION_ID = 117
            => "Client sent a push telemetry request with an invalid or outdated subscription ID.";

        /// `INVALID_RECORD_STATE` (121, KIP-932): an acknowledgement targeted a
        /// record no longer Acquired.
        INVALID_RECORD_STATE = 121
            => "The record state is invalid. The acknowledgement of delivery could not be completed.";
        /// `SHARE_SESSION_NOT_FOUND` (122, KIP-932): the named share session does
        /// not exist.
        SHARE_SESSION_NOT_FOUND = 122 => "The share session was not found.";
        /// `INVALID_SHARE_SESSION_EPOCH` (123, KIP-932): the share session epoch
        /// did not match the broker's expectation.
        INVALID_SHARE_SESSION_EPOCH = 123 => "The share session epoch is invalid.";
        /// `FENCED_STATE_EPOCH` (124, KIP-932): the share coordinator fenced a
        /// write on a stale state epoch.
        FENCED_STATE_EPOCH = 124
            => "The share coordinator rejected the request because the share-group state epoch did not match.";
        /// `STREAMS_INVALID_TOPOLOGY` (130, KIP-1071): the topology of a
        /// `StreamsGroupHeartbeat` cannot be configured, for example because a
        /// repartition source topic is never a sink topic.
        STREAMS_INVALID_TOPOLOGY = 130 => "The supplied topology is invalid.";
        /// `STREAMS_INVALID_TOPOLOGY_EPOCH` (131, KIP-1071): the topology epoch of
        /// a `StreamsGroupHeartbeat` is not valid.
        STREAMS_INVALID_TOPOLOGY_EPOCH = 131 => "The supplied topology epoch is invalid.";
        /// `STREAMS_TOPOLOGY_FENCED` (132, KIP-1071): the topology of the member
        /// is older than the topology of the group.
        STREAMS_TOPOLOGY_FENCED = 132 => "The supplied topology epoch is outdated.";
        /// `SHARE_SESSION_LIMIT_REACHED` (133, KIP-932): the per-broker share
        /// session cache is full. 133 is the last code the pinned image assigns.
        SHARE_SESSION_LIMIT_REACHED = 133 => "The limit of share sessions has been reached.";

        // Admin handler codes.
        /// `INVALID_CONFIG` (40): a config key/value pair is invalid or unknown.
        INVALID_CONFIG = 40 => "Configuration is invalid.";
        /// `NON_EMPTY_GROUP` (68): the group still has live members, so the broker
        /// cannot delete it.
        NON_EMPTY_GROUP = 68 => "The group is not empty.";
        /// `GROUP_ID_NOT_FOUND` (69): no group with the given id exists.
        GROUP_ID_NOT_FOUND = 69 => "The group id does not exist.";
        /// `GROUP_SUBSCRIBED_TO_TOPIC` (86, KIP-496): `OffsetDelete` refused the
        /// request because the live consumer-protocol group still subscribes to
        /// the topic. The operator must stop the consumers first.
        GROUP_SUBSCRIBED_TO_TOPIC = 86
            => "Deleting offsets of a topic is forbidden while the consumer group is actively subscribed to it.";

        // `AlterUserScramCredentials` (KIP-554) result codes.
        /// `CLUSTER_AUTHORIZATION_FAILED` (31): the principal has no cluster-level
        /// authorization. The `AlterUserScramCredentials` handler returns it in
        /// place of an ACL check when the request principal is not the configured
        /// super-user.
        CLUSTER_AUTHORIZATION_FAILED = 31 => "Cluster authorization failed.";
        /// `RESOURCE_NOT_FOUND` (91): the resource that the request names does not
        /// exist. KIP-554 uses it for missing SCRAM credentials, in both Alter
        /// deletion rows and Describe per-user result rows.
        RESOURCE_NOT_FOUND = 91
            => "A request illegally referred to a resource that does not exist.";
        /// `UNACCEPTABLE_CREDENTIAL` (93): per-user error for an upsertion that
        /// carries invalid SCRAM parameters, such as iterations below 4096, too
        /// many iterations, or an empty username.
        UNACCEPTABLE_CREDENTIAL = 93
            => "Requested credential would not meet criteria for acceptability.";
        /// `DUPLICATE_RESOURCE` (92): per-user error when the same user appears
        /// twice in one `AlterUserScramCredentials` or
        /// `DescribeUserScramCredentials` request.
        DUPLICATE_RESOURCE = 92 => "A request illegally referred to the same resource twice.";

        /// `INVALID_UPDATE_VERSION` (95, KIP-584): a feature-level update in
        /// `UpdateFeatures` is outside the broker's supported range, or it tries
        /// an unguarded downgrade or deletion of a finalized feature.
        INVALID_UPDATE_VERSION = 95 => "The given update version was invalid.";

        /// `FEATURE_UPDATE_FAILED` (96, KIP-584): the cluster failed to persist a
        /// validated feature update. For example, Raft rejected the metadata
        /// write, or the write timed out.
        FEATURE_UPDATE_FAILED = 96
            => "Unable to update finalized features due to an unexpected server error.";

        /// `PRINCIPAL_DESERIALIZATION_FAILURE` (97, KIP-590): the controller could
        /// not read the `request_principal` a forwarding node put in an
        /// `Envelope`. `kafka.server.EnvelopeUtils` raises it whenever the
        /// configured `KafkaPrincipalSerde` throws, which covers an absent
        /// principal, a schema version outside the supported range, and a
        /// truncated `DefaultPrincipalData` body.
        PRINCIPAL_DESERIALIZATION_FAILURE = 97
            => "Request principal deserialization failed during forwarding. This indicates an internal error on the broker cluster security setup.";

        // KIP-853 voter-reconfiguration codes. Kafka assigns exactly three, at
        // 125 to 127; there is no separate code for a malformed voter update.
        /// `INVALID_VOTER_KEY` (125, KIP-853): a raft RPC carried a voter key
        /// whose replica id or directory id does not match the local replica.
        /// `KafkaRaftClient` returns it from `Vote`, `BeginQuorumEpoch`, and
        /// `EndQuorumEpoch`, which krabka's raft layer serves without it today.
        INVALID_VOTER_KEY = 125 => "The voter key doesn't match the receiving replica's key.";
        /// `DUPLICATE_VOTER` (126, KIP-853): the requested node id is already
        /// present in the voter set.
        DUPLICATE_VOTER = 126 => "The voter is already part of the set of voters.";
        /// `VOTER_NOT_FOUND` (127, KIP-853): the exact node/directory voter
        /// identity was not found.
        VOTER_NOT_FOUND = 127 => "The voter is not part of the set of voters.";

        // ACL authorization codes.
        /// `TOPIC_AUTHORIZATION_FAILED` (29): principal lacks permission on the
        /// topic.
        TOPIC_AUTHORIZATION_FAILED = 29 => "Topic authorization failed.";
        /// `GROUP_AUTHORIZATION_FAILED` (30): principal lacks permission on the
        /// group.
        GROUP_AUTHORIZATION_FAILED = 30 => "Group authorization failed.";
        /// `SECURITY_DISABLED` (54): the cluster runs without an authorizer, so
        /// the ACL administration RPCs -- `DescribeAcls`, `CreateAcls` and
        /// `DeleteAcls` -- have nothing to read or write. Kafka's `KafkaApis`
        /// answers all three with this code whenever `authorizer.class.name` is
        /// unset, rather than with an empty listing or a durable binding no
        /// decision point will ever consult.
        SECURITY_DISABLED = 54 => "Security features are disabled.";
        /// `OPERATION_NOT_ATTEMPTED` (55): a batch failed on another item, so
        /// this one was not tried. `AddPartitionsToTxn` puts it on every valid
        /// partition of a transaction that names an unauthorized or unknown
        /// partition.
        OPERATION_NOT_ATTEMPTED = 55
            => "The broker did not attempt to execute this operation. This may happen for batched RPCs where some operations in the batch failed, causing the broker to respond without trying the rest.";

        // Bulletproof EOS / acks=all codes.
        /// `NOT_ENOUGH_REPLICAS` (19): per-partition error that `acks=all` Produce
        /// returns when the request completes without enough in-sync replicas that
        /// confirm the write. The record is durably on the leader's log. The
        /// producer should retry.
        NOT_ENOUGH_REPLICAS = 19
            => "Messages are rejected since there are fewer in-sync replicas than required.";

        /// `NOT_ENOUGH_REPLICAS_AFTER_APPEND` (20): per-partition error that
        /// `acks=all` Produce returns when the append succeeded on the leader but
        /// the HW timeout elapsed before enough in-sync replicas confirmed the
        /// write. The record is durably on the leader's log, but not yet on every
        /// ISR follower.
        NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20
            => "Messages are written to the log, but to fewer in-sync replicas than required.";

        /// `INVALID_REQUIRED_ACKS` (21): a Produce request's `acks` field held a
        /// value other than 0, 1 or -1. `ReplicaManager.isValidRequiredAcks`
        /// admits only those three; every other value refuses every partition of
        /// the request with this code, and appends nothing.
        INVALID_REQUIRED_ACKS = 21
            => "Produce request specified an invalid value for required acks.";

        /// `FENCED_LEADER_EPOCH` (74, KIP-101): caller's `current_leader_epoch` is
        /// older than the partition's current `leader_epoch`. The caller should
        /// re-fetch metadata, or call `OffsetForLeaderEpoch` to learn the
        /// truncation point.
        FENCED_LEADER_EPOCH = 74
            => "The leader epoch in the request is older than the epoch on the broker.";

        /// `UNKNOWN_LEADER_EPOCH` (75, KIP-101): caller's `current_leader_epoch`
        /// is newer than the broker's view. This is metadata propagation lag. The
        /// caller retries after a brief wait.
        UNKNOWN_LEADER_EPOCH = 75
            => "The leader epoch in the request is newer than the epoch on the broker.";

        /// `OFFSET_NOT_AVAILABLE` (78, KIP-207): a `ListOffsets` v5-and-up request
        /// asked for `LATEST`, or resolved a sentinel to an offset at or above the
        /// bound, while the new leader's high watermark has not yet caught up to
        /// the start offset of its own epoch. Answering with a real offset here
        /// could later move backwards once the watermark advances, which would
        /// let a client observe a non-monotonic end of partition.
        /// `Partition.maybeOffsetsError` raises it; a v1-v4 request gets
        /// `LEADER_NOT_AVAILABLE` (5) for the same condition instead, because the
        /// dedicated code did not exist yet.
        OFFSET_NOT_AVAILABLE = 78
            => "The leader high watermark has not caught up from a recent leader election so the offsets cannot be guaranteed to be monotonically increasing.";

        /// `TRANSACTIONAL_ID_NOT_FOUND` (105, KIP-664): `DescribeTransactions`
        /// named a transactional id this broker's coordinator holds no entry
        /// for, or one whose entry is `Dead`.
        TRANSACTIONAL_ID_NOT_FOUND = 105 => "The transactionalId could not be found.";

        /// `INELIGIBLE_REPLICA` (107, KIP-903): an `AlterPartition` proposed a new
        /// ISR that holds at least one ineligible replica. Such a replica is a
        /// broker that is not currently registered, or one whose stamped broker
        /// epoch is stale against the controller's registration epoch. The
        /// partition's ISR does not change.
        INELIGIBLE_REPLICA = 107 => "The new ISR contains at least one ineligible replica.";

        /// `OFFSET_MOVED_TO_TIERED_STORAGE` (109, KIP-405): a *follower* asked its
        /// leader for an offset that the leader's local log no longer holds but
        /// the remote tier does. Kafka's
        /// `ReplicaManager.handleOffsetOutOfRangeError` answers a follower fetch
        /// in `[logStartOffset, localLogStartOffset)` with this code instead of
        /// streaming the whole archive back down the replication path. The
        /// follower reads the leader's local log start with
        /// `ListOffsets(EARLIEST_LOCAL_TIMESTAMP)` and restarts its log there.
        ///
        /// A consumer never sees it: the same band is served to a consumer out of
        /// the tier, so only a fetch that carries a replica id is answered this
        /// way.
        OFFSET_MOVED_TO_TIERED_STORAGE = 109 => "The requested offset is moved to tiered storage.";

        // Leader election codes.
        PREFERRED_LEADER_NOT_AVAILABLE = 80 => "The preferred leader was not available.";
        ELIGIBLE_LEADERS_NOT_AVAILABLE = 83
            => "Eligible topic partition leaders are not available.";
        ELECTION_NOT_NEEDED = 84 => "Leader election not needed for topic partition.";

        // Partition reassignment codes (KIP-455).
        INVALID_REPLICA_ASSIGNMENT = 39 => "Replica assignment is invalid.";
        NO_REASSIGNMENT_IN_PROGRESS = 85 => "No partition reassignment is in progress.";

        // KIP-227 incremental-fetch-session codes.
        /// `FETCH_SESSION_ID_NOT_FOUND` (70): the broker returns this at the top
        /// level of a `FetchResponse` when the request carried a non-zero
        /// `session_id` that the broker's session cache does not hold. The session
        /// was evicted, never existed, or is already closed.
        FETCH_SESSION_ID_NOT_FOUND = 70 => "The fetch session ID was not found.";
        /// `INVALID_FETCH_SESSION_EPOCH` (71): the broker returns this at the top
        /// level of a `FetchResponse` when the request's `session_epoch` does not
        /// match the cached session's current epoch. It also returns it when
        /// `session_id == 0` and `session_epoch` is neither `0` (new session) nor
        /// `-1` (sessionless full fetch).
        INVALID_FETCH_SESSION_EPOCH = 71 => "The fetch session epoch is invalid.";
        /// `FETCH_SESSION_TOPIC_ID_ERROR` (106): the broker returns this at the
        /// top level of a `FetchResponse` when an incremental request's version
        /// does not match the topic-id use of its session: v12 or earlier on a
        /// session a v13+ request created, or the other way round.
        FETCH_SESSION_TOPIC_ID_ERROR = 106
            => "The fetch session encountered inconsistent topic ID usage.";

        // KIP-48 delegation-token codes.
        DELEGATION_TOKEN_AUTH_DISABLED = 61 => "Delegation Token feature is not enabled.";
        DELEGATION_TOKEN_NOT_FOUND = 62 => "Delegation Token is not found on server.";
        DELEGATION_TOKEN_OWNER_MISMATCH = 63 => "Specified Principal is not valid Owner/Renewer.";
        DELEGATION_TOKEN_REQUEST_NOT_ALLOWED = 64
            => "Delegation Token requests are not allowed on PLAINTEXT/1-way SSL channels and on delegation token authenticated channels.";
        DELEGATION_TOKEN_AUTHORIZATION_FAILED = 65 => "Delegation Token authorization failed.";
        DELEGATION_TOKEN_EXPIRED = 66 => "Delegation Token is expired.";
        /// `INVALID_PRINCIPAL_TYPE` (67): a `CreateDelegationToken` renewer whose
        /// principal type is not `User`.
        INVALID_PRINCIPAL_TYPE = 67 => "Supplied principalType is not supported.";

        // KIP-630 FetchSnapshot (api_key 59) codes.
        /// `SNAPSHOT_NOT_FOUND` (98): the requested `__cluster_metadata` snapshot
        /// does not exist, because the controller has not generated one yet.
        SNAPSHOT_NOT_FOUND = 98 => "Requested snapshot was not found.";
        /// `POSITION_OUT_OF_RANGE` (99): the requested `position` is past the end
        /// of the `__cluster_metadata` snapshot.
        POSITION_OUT_OF_RANGE = 99
            => "Requested position is not greater than or equal to zero, and less than the size of the snapshot.";
        /// `INCONSISTENT_CLUSTER_ID` (104): the request's `cluster_id` does not
        /// match this cluster's id.
        INCONSISTENT_CLUSTER_ID = 104
            => "The clusterId in the request does not match that found on the server.";
        /// `UNKNOWN_CONTROLLER_ID` (116): Kafka's "This controller ID is not
        /// known."
        UNKNOWN_CONTROLLER_ID = 116 => "This controller ID is not known.";
        /// `INVALID_REGISTRATION` (119): a broker/controller registration is
        /// malformed.
        INVALID_REGISTRATION = 119
            => "The controller has considered the broker registration to be invalid.";
        /// `UNKNOWN_TOPIC_ID` (100): a request referenced a topic by UUID that
        /// this cluster does not know about (KIP-516).
        UNKNOWN_TOPIC_ID = 100 => "This server does not host this topic ID.";
        /// `INCONSISTENT_TOPIC_ID` (103): a request supplied a topic UUID that
        /// does not match the UUID stored for the named topic (KIP-516).
        INCONSISTENT_TOPIC_ID = 103
            => "The log's topic ID did not match the topic ID in the request.";

        /// `UNSUPPORTED_COMPRESSION_TYPE` (76): a KIP-714 `PushTelemetry` carried
        /// a `compression_type` that the broker cannot decompress, or a `Produce`
        /// batch below v7 used zstd, which `ProduceRequest.validateRecords`
        /// refuses because a client that old is not assumed to be able to decode
        /// zstd back out of a fetch.
        UNSUPPORTED_COMPRESSION_TYPE = 76
            => "The requesting client does not support the compression type of given partition.";

        /// `THROTTLING_QUOTA_EXCEEDED` (89): a KIP-714 client pushed or fetched
        /// telemetry faster than the configured interval allows.
        THROTTLING_QUOTA_EXCEEDED = 89 => "The throttling quota has been exceeded.";

        /// `TELEMETRY_TOO_LARGE` (118): KIP-714 `PushTelemetry` payload exceeded
        /// `telemetry.max.bytes`.
        TELEMETRY_TOO_LARGE = 118
            => "Client sent a push telemetry request larger than the maximum size the broker will accept.";

        /// `POLICY_VIOLATION` (44): the request parameters do not satisfy the
        /// configured policy. Apache Kafka returns it from `CreateTopicPolicy` and
        /// `AlterConfigPolicy`.
        ///
        /// The JVM maps 44 to `PolicyViolationException`, which extends
        /// `ApiException` and not `RetriableException`, so no client retries it.
        ///
        /// KFC-9 returns this code for a produce to a frozen topic, and for a
        /// privileged transition that carries no break-glass approval.
        POLICY_VIOLATION = 44 => "Request parameters do not satisfy the configured policy.";
    }

    // ---------------------------------------------------------------------------
    // Kafka trunk codes.
    //
    // Kafka trunk assigns these above the 133 the pinned `apache/kafka:4.3.1`
    // image ends at. They serve the trunk-only api keys krabka implements because
    // krabka-protocol vendors their schemas, and a guard test checks them against
    // the trunk `Errors` table recorded in the private `tests` module.
    // ---------------------------------------------------------------------------

    trunk KAFKA_TRUNK_CODES {
        /// KIP-1331: the description backend could not delete group state.
        GROUP_DELETION_FAILED = 134
            => "DeleteGroups could not complete; see the error message on the per-group result for details.";
        /// `STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED` (135, KIP-1331): the
        /// broker could not store a `StreamsGroupTopologyDescriptionUpdate`.
        STREAMS_TOPOLOGY_DESCRIPTION_UPDATE_FAILED = 135
            => "The broker could not process the topology description update; see the error message for details.";
        /// `CONTROLLER_ID_NOT_REGISTERED` (136, KIP-1312): `UnregisterController`
        /// named a controller id that has no registration.
        CONTROLLER_ID_NOT_REGISTERED = 136 => "The given controller ID was not registered.";
    }

    // ---------------------------------------------------------------------------
    // krabka-private error codes.
    //
    // The Apache Kafka table in the pinned image ends at 133
    // (`SHARE_SESSION_LIMIT_REACHED`), Kafka trunk's at 136, and Kafka assigns
    // codes upward from 0, so krabka reserves 1000 and above for errors that no
    // Kafka error table names. The gap below 1000 is what keeps the two ranges
    // apart as Kafka adds codes. A broker returns a code in this range only on a krabka-private api
    // key, which sits at or above `crate::handlers::KRABKA_PRIVATE_API_KEY_FLOOR`.
    // A JVM client cannot negotiate such an api key, so it never receives one of
    // these codes.
    //
    // The private error-code range and the private api-key range are separate
    // namespaces that share the floor 1000 and nothing else. One number can be a
    // legal error code and a legal api key at the same time. 1010 is
    // `OPERATOR_SIGNATURE_REQUIRED` here and `AlterBarrierGroups` in
    // `crate::handlers`. That is not a collision, because an error code and an api
    // key are different fields on the wire.
    // ---------------------------------------------------------------------------

    private KRABKA_PRIVATE_CODES {
        /// `BARRIER_INJECTION_IN_PROGRESS` (1000): an injection for this barrier
        /// group is already in flight. The caller should retry after a brief
        /// back-off.
        ///
        /// The barrier coordinator runs one injection per group at a time, because
        /// an injection freezes the target set and consumes an epoch before it
        /// appends the first marker. A second `TriggerBarrier` for the same group
        /// gets this code instead of a second epoch.
        ///
        /// Kafka has no generic operation-in-progress code.
        /// `CONCURRENT_TRANSACTIONS` (51) and `REBALANCE_IN_PROGRESS` (27) both
        /// drive JVM client state machines, so neither one carries this meaning.
        /// The broker returns this code only on a krabka-private api key, and it
        /// never reaches a JVM client.
        BARRIER_INJECTION_IN_PROGRESS = 1000;

        /// `BREAK_GLASS_APPROVAL_REQUIRED` (1006): the request names a privileged
        /// transition that needs an approved break-glass proposal, and no such
        /// proposal exists.
        ///
        /// KFC-9 gates these transitions on an approved proposal: a thaw of a
        /// topic write-freeze, a forced leader epoch bump, a broker
        /// unregistration, a reassignment cancel, and a delete of a topic or of
        /// records. The operator should collect the approvals first, then send the
        /// request again.
        BREAK_GLASS_APPROVAL_REQUIRED = 1006;

        /// `BREAK_GLASS_DUPLICATE_APPROVER` (1007): the principal on the
        /// connection already approved this break-glass proposal.
        ///
        /// A two-person rule counts distinct principals. A second approval from
        /// one principal does not move the proposal closer to its threshold, so
        /// the broker refuses it instead of storing it.
        BREAK_GLASS_DUPLICATE_APPROVER = 1007;

        /// `BREAK_GLASS_NOT_AN_APPROVER` (1008): the principal on the connection
        /// is not in the configured break-glass approver set.
        ///
        /// The approver set comes from `broker.toml` and not from the metadata
        /// log, so a principal who can write the metadata log cannot add an
        /// approver. The ACL store keeps `super_users` out for the same reason.
        BREAK_GLASS_NOT_AN_APPROVER = 1008;

        /// `OPERATOR_SIGNATURE_INVALID` (1009): the broker refused the detached
        /// operator signature on the request.
        ///
        /// One code covers five distinct failures:
        ///
        /// - The signature does not verify against the named key.
        /// - The `key_id` names no configured operator key.
        /// - The `set_by` principal is not the principal bound to that key.
        /// - The timestamp sits outside the configured skew window.
        /// - The timestamp repeats one the broker already accepted.
        ///
        /// The response message names which check failed. The code does not,
        /// because an error code that separates the five tells an attacker which
        /// check they failed.
        ///
        /// The freeze path and the break-glass path share this code, because both
        /// verify against one operator key set with one set of rules.
        OPERATOR_SIGNATURE_INVALID = 1009;

        /// `OPERATOR_SIGNATURE_REQUIRED` (1010): the request needs a detached
        /// operator signature and carries none.
        ///
        /// A thaw of a topic write-freeze always needs one. A freeze needs one
        /// when `freeze.require_signature` is `true`. The freeze path and the
        /// break-glass path share this code.
        OPERATOR_SIGNATURE_REQUIRED = 1010;

        /// `FREEZE_SCOPE_INVALID` (1011): the freeze scope in the request is not
        /// one the broker accepts.
        ///
        /// The scope is empty, it is not a legal topic name, or it names an
        /// internal topic. The broker never freezes a `__` name, because a prefix
        /// scope of `""` would otherwise freeze `__consumer_offsets` and stop the
        /// cluster.
        FREEZE_SCOPE_INVALID = 1011;

        /// `FREEZE_LIMIT_EXCEEDED` (1012): the freeze registry already holds
        /// `freeze.max_entries` entries.
        ///
        /// The produce path resolves a prefix scope with a reverse walk over the
        /// prefixed entries, so a bounded registry bounds that walk. The operator
        /// should remove an entry before they add another one.
        FREEZE_LIMIT_EXCEEDED = 1012;
    }
}

/// The entry `Errors.forCode` falls back to for a code it does not know.
const UNKNOWN_SERVER_ERROR_ENTRY: KafkaError = match declared_kafka_error(UNKNOWN_SERVER_ERROR) {
    Some(entry) => entry,
    None => panic!("UNKNOWN_SERVER_ERROR is declared with its message"),
};

/// Kafka's `Errors.forCode(code)`, over the codes this module declares: the
/// entry of `code`, or that of `UNKNOWN_SERVER_ERROR` for any other code,
/// krabka-private codes included.
const fn kafka_error(code: i16) -> KafkaError {
    match declared_kafka_error(code) {
        Some(entry) => entry,
        None => UNKNOWN_SERVER_ERROR_ENTRY,
    }
}

/// Kafka's `Errors.forCode(code).name()`. A code this module does not declare
/// as a Kafka-range or trunk code reads as `"UNKNOWN_SERVER_ERROR"`.
#[must_use]
pub const fn error_name(code: i16) -> &'static str { kafka_error(code).name }

/// Kafka's `Errors.forCode(code).message()`.
///
/// It is the default `error_message` that `new ApiError(error)` and the
/// request classes' `getErrorResponse` put on a response. `NONE` answers
/// `"NONE"`, as Kafka's does, and a code this module does not declare as a
/// Kafka-range or trunk code answers the `UNKNOWN_SERVER_ERROR` message.
#[must_use]
pub const fn default_message(code: i16) -> &'static str { kafka_error(code).message }

/// Kafka's `CoordinatorOperationExceptionHelper.handleOperationException`:
/// a verification that could not reach the coordinator is a retriable load
/// (`NETWORK_EXCEPTION` becomes `COORDINATOR_LOAD_IN_PROGRESS`, which asks
/// the client to retry without a coordinator lookup), an unavailable topic
/// or replica set is `COORDINATOR_NOT_AVAILABLE`, and a lost leadership is
/// `NOT_COORDINATOR`.
#[must_use]
pub(crate) fn coordinator_operation_code(code: i16) -> i16 {
    match code {
        NETWORK_EXCEPTION => COORDINATOR_LOAD_IN_PROGRESS,
        UNKNOWN_TOPIC_OR_PARTITION | NOT_ENOUGH_REPLICAS | REQUEST_TIMED_OUT => {
            COORDINATOR_NOT_AVAILABLE
        }
        NOT_LEADER_OR_FOLLOWER | KAFKA_STORAGE_ERROR => NOT_COORDINATOR,
        other => other,
    }
}

/// Maps an internal [`crate::error::BrokerError`] to a wire-level code. Most
/// internal errors map to `UNKNOWN_SERVER_ERROR`. Specific variants map to
/// more meaningful codes.
#[must_use]
pub fn from_broker_error(err: &BrokerError) -> i16 {
    use crate::error::BrokerError;
    match err {
        // KFC-1 `delivery.schedule.monotonic`. The log refuses a batch whose
        // delivery time precedes one the partition already holds, and the
        // refusal is a statement about the batch's timestamp, not a server
        // fault: the produce row carries the same code the produce-side
        // `delivery.max.delay.ms` bound answers with.
        BrokerError::Log(krabka_log::LogError::ScheduleRunsBackwards { .. }) => INVALID_TIMESTAMP,
        // Kafka trunk's `max.decompressed.message.bytes`: the
        // `InvalidRecordException` a decompressing read throws for a record
        // above the limit is `INVALID_RECORD` under `Errors.forException`.
        BrokerError::Log(krabka_log::LogError::RecordTooLarge { .. }) => INVALID_RECORD,
        // Kafka's `LocalLog.maybeHandleIOException` turns every `IOException`
        // of an append or a read into `KafkaStorageException`, which
        // `Errors.forException` answers with `KAFKA_STORAGE_ERROR`: retriable,
        // and it makes the client refresh its metadata. Left to the catch-all
        // below, a disk fault is `UNKNOWN_SERVER_ERROR`, which the JVM
        // producer never retries.
        BrokerError::Log(krabka_log::LogError::Io(_)) => KAFKA_STORAGE_ERROR,
        BrokerError::UnsupportedApi { .. } => UNSUPPORTED_VERSION,
        // A raft write that reached a node which is no longer the leader is
        // the same answer to a client as a partition whose writer is gone:
        // retry, after refreshing metadata.
        // An append that raced the removal of its partition finds the log
        // fenced, which is the same answer as a writer that is gone.
        BrokerError::PartitionWriterDied { .. }
        | BrokerError::Log(krabka_log::LogError::Fenced)
        | BrokerError::Raft(krabka_raft::RaftError::NotLeader { .. }) => NOT_LEADER_OR_FOLLOWER,
        BrokerError::Raft(krabka_raft::RaftError::Network(_)) => REQUEST_TIMED_OUT,
        BrokerError::GroupInvalidState { .. } => REBALANCE_IN_PROGRESS,
        BrokerError::UnknownMember { .. } => UNKNOWN_MEMBER_ID,
        BrokerError::GenerationMismatch { .. } => ILLEGAL_GENERATION,
        BrokerError::ProducerEpochFenced { .. }
        | BrokerError::TransactionAppend(
            krabka_log::TransactionAppendRefusal::StaleProducerEpoch,
        ) => INVALID_PRODUCER_EPOCH,
        BrokerError::TransactionAppend(
            krabka_log::TransactionAppendRefusal::InvalidTransactionState,
        ) => INVALID_TXN_STATE,
        BrokerError::TransactionAppend(
            krabka_log::TransactionAppendRefusal::OutOfOrderSequence,
        ) => OUT_OF_ORDER_SEQUENCE_NUMBER,
        BrokerError::CoordinatorEpochFenced { .. } => TRANSACTION_COORDINATOR_FENCED,
        BrokerError::ProducerIdBlockUnavailable(_) => COORDINATOR_LOAD_IN_PROGRESS,
        BrokerError::FencedLeaderEpoch { .. } => FENCED_LEADER_EPOCH,
        BrokerError::UnknownLeaderEpoch(_) => UNKNOWN_LEADER_EPOCH,
        BrokerError::CoordinatorWriteUncommitted { code, .. }
        | BrokerError::TransactionStateWriteUncommitted { code, .. } => *code,
        BrokerError::Replication(_)
        | BrokerError::Shutdown
        | BrokerError::Io(_)
        | BrokerError::Log(_)
        | BrokerError::Protocol(_)
        | BrokerError::Startup(_)
        | BrokerError::ListenerBind { .. }
        | BrokerError::FatalFault(_)
        | BrokerError::Txn(_)
        | BrokerError::MarkerWriteRefused { .. }
        | BrokerError::Share(_)
        | BrokerError::SharePartitionState { .. }
        | BrokerError::Config(_)
        | BrokerError::Tls(_)
        | BrokerError::BootstrapFile { .. }
        | BrokerError::InvalidRuntimeConfig(_)
        | BrokerError::AuditSigningKey { .. }
        | BrokerError::AuditSigningKeyRequired
        | BrokerError::ShutdownTimeout(_)
        | BrokerError::Raft(_)
        | BrokerError::StreamsTaskAssignment { .. } => UNKNOWN_SERVER_ERROR,
    }
}

/// Kafka coordinator response mapping for a failed metadata append.
pub(crate) fn coordinator_append_error(code: i16) -> i16 {
    match code {
        UNKNOWN_TOPIC_OR_PARTITION | NOT_ENOUGH_REPLICAS | REQUEST_TIMED_OUT => {
            COORDINATOR_NOT_AVAILABLE
        }
        NOT_LEADER_OR_FOLLOWER | KAFKA_STORAGE_ERROR => NOT_COORDINATOR,
        MESSAGE_TOO_LARGE => UNKNOWN_SERVER_ERROR,
        other => other,
    }
}
