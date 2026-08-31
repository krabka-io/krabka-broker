//! The one answer to "does the broker own this topic".
//!
//! `Metadata` and `DescribeTopicPartitions` both fill the `is_internal` flag of
//! a topic row from [`is_internal_topic`], so a client sees the same answer
//! whichever RPC it asks with. The flag is what hides a topic from
//! `AdminClient.listTopics()`, from `kafka-topics --list --exclude-internal`,
//! and from a consumer that subscribes by pattern with the default
//! `exclude.internal.topics=true`. A broker-owned topic that answers `false` is
//! one a `.*` mirroring or audit sink silently starts consuming.
//!
//! # The set is the subsystems' own constants
//!
//! Every entry of [`INTERNAL_TOPICS`] is the constant the owning subsystem
//! already declares for its topic, never a second spelling of the name. A new
//! internal topic is covered the day its constant lands, by naming that
//! constant here.
//!
//! # What Kafka reports as internal
//!
//! Kafka's own set is a fixed three names — `__consumer_offsets`,
//! `__transaction_state` and `__share_group_state` — and it does **not**
//! include `__remote_log_metadata`, whose tiered-storage records are
//! broker-owned in spirit but ordinary on the wire. That was settled against
//! the pinned images rather than the wiki, two ways that agree: the
//! `INTERNAL_TOPICS` set in `org.apache.kafka.common.internals.Topic` inside
//! each image's `kafka-clients` jar, and creating all four names on a running
//! broker and diffing `kafka-topics --list` against
//! `kafka-topics --list --exclude-internal`. `apache/kafka:4.3.1` and
//! `apache/kafka:4.0.0` report the three; `confluentinc/cp-kafka:7.5.0`
//! predates KIP-932 and reports the first two, leaving `__share_group_state`
//! ordinary because it has no share coordinator to own it. Krabka does have
//! one, so it follows the 4.x answer.
//!
//! The remaining entries are Krabka's own: the audit log, the barrier state
//! topic, and the diskless WAL index. Kafka has no name for them and so no
//! opinion about them, and each carries broker state that no application should
//! read as a data topic.

/// The topics the broker owns, each named by the constant its subsystem
/// declares.
///
/// [`is_internal_topic`] is the only reader. The constant is `pub(crate)`
/// rather than private so that tests elsewhere in the crate — the freeze
/// registry's, in particular — can hold their own rules against the whole set
/// instead of against a copy of it.
pub(crate) const INTERNAL_TOPICS: [&str; 6] = [
    crate::coordinator::bootstrap::OFFSETS_TOPIC,
    crate::txn::bootstrap::TOPIC,
    crate::share_coordinator::bootstrap::TOPIC,
    crate::coordinator::bootstrap::AUDIT_TOPIC,
    crate::barrier::STATE_TOPIC,
    crate::diskless::index_log::DISKLESS_WAL_INDEX_TOPIC,
];

/// The name prefix that marks a broker-owned topic.
///
/// Every name in [`INTERNAL_TOPICS`] carries it, which
/// `every_internal_topic_carries_the_internal_prefix` holds them to.
/// [`crate::freeze::scope_covers_internal_topic`] tests for the prefix rather
/// than for set membership, because a freeze has to be refused on a scope that
/// merely *reaches* an internal topic, not only on one that names it.
pub(crate) const INTERNAL_TOPIC_PREFIX: &str = "__";

/// Whether Kafka clients should treat this topic as owned by the broker.
pub(crate) fn is_internal_topic(name: &str) -> bool {
    INTERNAL_TOPICS.contains(&name)
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    /// Kafka's three names answer as Kafka answers, Krabka's own topics answer
    /// `true`, and nothing else does. `__remote_log_metadata` is the row that
    /// is easy to guess wrong: broker-owned in spirit, ordinary to Kafka.
    #[test]
    fn a_broker_owned_topic_is_internal_and_nothing_else_is() {
        for (label, name, expected) in [
            ("the offsets topic", "__consumer_offsets", true),
            ("the transaction state topic", "__transaction_state", true),
            ("the share group state topic", "__share_group_state", true),
            ("the audit log", "__krabka_audit", true),
            ("the barrier state topic", "__barrier_state", true),
            ("the diskless WAL index", "__diskless_wal_index", true),
            (
                "the tiered-storage metadata topic, ordinary on Kafka",
                "__remote_log_metadata",
                false,
            ),
            (
                "the raft metadata log, which is never a metadata topic row",
                "__cluster_metadata",
                false,
            ),
            ("an ordinary topic", "orders", false),
            ("a single-underscore name", "_foo", false),
            (
                "an application topic that borrows the prefix",
                "__user_topic",
                false,
            ),
            (
                "a name that only extends an internal one",
                "__consumer_offsets-2",
                false,
            ),
            (
                "a name that is only a prefix of an internal one",
                "__consumer_offset",
                false,
            ),
        ] {
            check!(is_internal_topic(name) == expected, "{label}");
        }
    }

    /// The exact-name set and the `__` convention the freeze registry tests for
    /// have to agree, or a topic could be internal and freezable at once.
    #[test]
    fn every_internal_topic_carries_the_internal_prefix() {
        for name in INTERNAL_TOPICS {
            check!(name.starts_with(INTERNAL_TOPIC_PREFIX), "{name}");
        }
    }

    /// Two subsystems naming the same topic would make the set's length lie
    /// about how many topics the broker owns.
    #[test]
    fn no_two_subsystems_claim_the_same_topic_name() {
        let mut seen = INTERNAL_TOPICS.to_vec();
        seen.sort_unstable();
        seen.dedup();
        check!(seen.len() == INTERNAL_TOPICS.len());
    }
}
