//! The label sets the broker attaches to its metric families, the label
//! values that are closed enums rather than free strings, and the sentinel
//! value that keeps an unbounded input from becoming an unbounded label. They
//! live together because cardinality is a property of the whole set, not of
//! any one family that uses it.

use std::{
    fmt,
    hash::{Hash, Hasher},
    sync::Arc,
};

use krabka_metadata::BreakGlassAction as GatedAction;
use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue, LabelValueEncoder};

/// Sentinel label value that folds unbounded inputs (unrecognised
/// `api_key`s, `SaslAuthenticate` without a prior handshake) into one
/// series, keeping label cardinality bounded.
pub(crate) const UNKNOWN_LABEL: &str = "Unknown";

/// Per-topic label set. `EncodeLabelSet` is the prometheus-client
/// derive that produces the `topic="<name>"` label on emitted samples.
///
/// `topic` is an `Arc<str>` and not a `String` because the data path builds
/// one of these per topic per request and then throws it away: `get_or_create`
/// only needs to hash the label to find the counter, and the copy it clones on
/// a first sighting is kept forever. An `Arc<str>` makes the common case a
/// hash plus a refcount bump. The registry hands out the shared name —
/// see `PartitionRegistry::shared_topic_name`. Rendering is unchanged:
/// prometheus-client encodes an `Arc<str>` through the same `&str` impl a
/// `String` reaches.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct TopicLabel {
    pub topic: Arc<str>,
}

/// Per-partition label set, paired with the `partition_*` and `delivery_*`
/// metric families. Consumed by the rebalancer's metric scraper. Cardinality is
/// bounded by the number of partitions this broker hosts, because both fields
/// come from the metadata image and never from client input.
///
/// `topic` is an `Arc<str>` for the reason [`TopicLabel::topic`] is: this is
/// the label set the produce, fetch and replication paths build once per
/// partition per request, so the allocation it used to cost was O(partitions)
/// on exactly the path the partition registry was made allocation-free for.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct PartitionLabel {
    pub topic: Arc<str>,
    pub partition: i32,
}

/// Fleet-complete KIP-932 backlog for one share-group partition.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ShareGroupLabel {
    pub group_id: String,
    pub topic: String,
    pub partition: i32,
}

/// KIP-511 client software fingerprint, attached to the
/// `client_software_versions_total` counter on every accepted v3+
/// `ApiVersions` handshake.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ClientSoftwareLabel {
    pub software_name: String,
    pub software_version: String,
}

/// Per-API request fingerprint, paired with the
/// `api_requests` counter family. `api_key` is the
/// `ApiKey::IntoStaticStr`-derived variant name (e.g. `"Produce"`,
/// `"DescribeQuorum"`) so operators see human-readable api-name
/// labels. A krabka-private api key carries its own RPC name instead
/// (e.g. `"TriggerBarrier"`), because the generated enum does not
/// know that range. Cardinality is bounded by `ApiKey::ALL.len()`
/// (~80 entries) plus one label per krabka-private RPC; requests for
/// unknown api keys land under the `"Unknown"` sentinel label.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ApiKeyLabel {
    pub api_key: String,
}

/// Per-barrier-group label set, paired with the `barrier_*` families.
/// `group` is the barrier group name. Cardinality is bounded by
/// `BrokerConfig::barrier_max_groups`, because the coordinator rejects a new
/// group past that cap.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct BarrierGroupLabel {
    pub group: String,
}

/// Controller directory identity attached to the current vote.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct DirectoryLabel {
    pub directory_id: String,
}

/// SASL mechanism fingerprint, paired with the
/// `{successful,failed}_authentication_total` counter families.
/// `mechanism` is the canonical Kafka wire name from
/// [`krabka_security::SaslMechanism::wire_name`] (`"PLAIN"`,
/// `"SCRAM-SHA-256"`, `"SCRAM-SHA-512"`, `"OAUTHBEARER"`) when the
/// `SaslAuthenticate` frame arrived in a valid sequence; the
/// `"Unknown"` sentinel covers `ILLEGAL_SASL_STATE` rejects where
/// no prior `SaslHandshake` ran and the mechanism is unset.
/// Cardinality is bounded by `SaslMechanism::*` + 1 — a tight set
/// regardless of client population.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct SaslMechanismLabel {
    pub mechanism: String,
}

/// KFC-7 rejection fingerprint, paired with the
/// `schema_validation_rejections` counter family. `reason` is one of the five
/// fixed values `unframed`, `unknown_id`, `wrong_subject`, `body_mismatch`,
/// and `registry_unavailable`, so a topic with schema validation on adds at
/// most five series. No schema id, subject, or client string reaches this
/// label set.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct SchemaRejectionLabel {
    pub topic: String,
    pub reason: String,
}

/// KFC-9 state of a break-glass proposal.
///
/// The four states are the whole lifecycle of a proposal, so a closed enum
/// bounds the `break_glass_proposals` label set at four series.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum BreakGlassState {
    /// The proposal has fewer approvals than the broker needs.
    Pending,
    /// The proposal has every approval it needs and no transition used it yet.
    Approved,
    /// The proposal passed its expiry time and no transition used it.
    Expired,
    /// A privileged transition used the proposal.
    Consumed,
}

impl BreakGlassState {
    /// Every state, in lifecycle order.
    pub const ALL: [Self; 4] = [Self::Pending, Self::Approved, Self::Expired, Self::Consumed];

    /// The `state` label value this variant renders as.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Approved => "approved",
            Self::Expired => "expired",
            Self::Consumed => "consumed",
        }
    }
}

impl EncodeLabelValue for BreakGlassState {
    fn encode(&self, encoder: &mut LabelValueEncoder) -> Result<(), fmt::Error> {
        EncodeLabelValue::encode(&self.as_str(), encoder)
    }
}

/// KFC-9 privileged transition that a break-glass proposal authorizes, as a
/// metric label.
///
/// [`krabka_metadata::BreakGlassAction`] is the one definition of the gated
/// set. This newtype is only what lets that definition *be* a label value:
/// both the enum and `EncodeLabelValue` are foreign to this crate, so the
/// orphan rule forbids implementing the trait on the enum directly.
///
/// The label set stays as closed as a broker-local enum made it. The wrapped
/// enum is itself closed, so the `break_glass_refusals` and
/// `break_glass_bypassed` families still carry one series per gated operation
/// and no caller can name an eighth action. What the newtype adds is that
/// there is now one enum rather than two: an action added to the metadata
/// definition cannot be forgotten here, and `break_glass::action_name` is the
/// one spelling that the configuration, the audit event and the metric label
/// all share.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BreakGlassAction(pub GatedAction);

impl BreakGlassAction {
    /// The `action` label value this transition renders as.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        crate::break_glass::action_name(self.0)
    }
}

/// Hashing the label text rather than the wrapped enum, which carries no
/// `Hash` of its own. Two actions are equal exactly when their names are, so
/// the hash agrees with `Eq`.
impl Hash for BreakGlassAction {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.as_str().hash(state);
    }
}

impl EncodeLabelValue for BreakGlassAction {
    fn encode(&self, encoder: &mut LabelValueEncoder) -> Result<(), fmt::Error> {
        EncodeLabelValue::encode(&self.as_str(), encoder)
    }
}

/// KFC-9 proposal-state label set, paired with the `break_glass_proposals`
/// gauge family. Cardinality is bounded at four, because the field is the
/// closed [`BreakGlassState`] enum and no caller can name a fifth state.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct BreakGlassStateLabel {
    pub state: BreakGlassState,
}

/// KFC-9 privileged-transition label set, paired with the
/// `break_glass_refusals` and `break_glass_bypassed` counter families.
/// Cardinality is bounded at seven, because the field wraps the closed
/// [`krabka_metadata::BreakGlassAction`] enum and no caller can name an eighth
/// action.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct BreakGlassActionLabel {
    pub action: BreakGlassAction,
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use prometheus_client::{
        encoding::{EncodeLabelSet, text::encode},
        metrics::{counter::Counter, family::Family},
        registry::Registry,
    };

    use super::{PartitionLabel, TopicLabel};

    /// [`TopicLabel`] exactly as it read before its `topic` became an
    /// `Arc<str>`.
    ///
    /// What an operator scrapes must not move, so the previous definition is
    /// kept here as the thing the current one is measured against. It is not
    /// redundant with the frozen bytes in [`CAPTURED`]: prometheus-client
    /// reaches a `String` and an `Arc<str>` through different
    /// `EncodeLabelValue` impls, and this is what says the two agree, on this
    /// version of the crate and on the next one.
    #[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
    struct OwnedTopicLabel {
        topic: String,
    }

    /// [`PartitionLabel`] exactly as it read before its `topic` became an
    /// `Arc<str>`. See [`OwnedTopicLabel`].
    #[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
    struct OwnedPartitionLabel {
        topic: String,
        partition: i32,
    }

    /// One sample per case, as (topic, partition, count).
    ///
    /// The names are deliberately not all well-behaved. The text exposition
    /// format gives `"`, `\` and a newline inside a label value a meaning of
    /// their own, so a rendering difference between the two encodings would
    /// show up there first if it showed up anywhere.
    const SAMPLES: [(&str, i32, u64); 4] = [
        ("orders", 0, 3),
        ("orders", 7, 11),
        ("payments.eu-west-1", 2, 5),
        ("quote\"back\\slash\nnewline", 1, 1),
    ];

    /// What the `String`-labelled families rendered, sample by sample, before
    /// `topic` became an `Arc<str>`. Captured from a run of
    /// [`owned_labels_still_render_the_captured_bytes`] against the previous
    /// definitions, which [`OwnedTopicLabel`] and [`OwnedPartitionLabel`]
    /// preserve.
    const CAPTURED: [&str; 4] = [
        "# HELP topic_produce_requests Produce requests per topic.\n\
         # TYPE topic_produce_requests counter\n\
         topic_produce_requests_total{topic=\"orders\"} 3\n\
         # HELP partition_bytes_in Bytes appended per partition.\n\
         # TYPE partition_bytes_in counter\n\
         partition_bytes_in_total{topic=\"orders\",partition=\"0\"} 3\n\
         # EOF\n",
        "# HELP topic_produce_requests Produce requests per topic.\n\
         # TYPE topic_produce_requests counter\n\
         topic_produce_requests_total{topic=\"orders\"} 11\n\
         # HELP partition_bytes_in Bytes appended per partition.\n\
         # TYPE partition_bytes_in counter\n\
         partition_bytes_in_total{topic=\"orders\",partition=\"7\"} 11\n\
         # EOF\n",
        "# HELP topic_produce_requests Produce requests per topic.\n\
         # TYPE topic_produce_requests counter\n\
         topic_produce_requests_total{topic=\"payments.eu-west-1\"} 5\n\
         # HELP partition_bytes_in Bytes appended per partition.\n\
         # TYPE partition_bytes_in counter\n\
         partition_bytes_in_total{topic=\"payments.eu-west-1\",partition=\"2\"} 5\n\
         # EOF\n",
        "# HELP topic_produce_requests Produce requests per topic.\n\
         # TYPE topic_produce_requests counter\n\
         topic_produce_requests_total{topic=\"quote\"back\\slash\nnewline\"} 1\n\
         # HELP partition_bytes_in Bytes appended per partition.\n\
         # TYPE partition_bytes_in counter\n\
         partition_bytes_in_total{topic=\"quote\"back\\slash\nnewline\",partition=\"1\"} 1\n\
         # EOF\n",
    ];

    /// Register a topic-labelled and a partition-labelled counter family
    /// under a fresh registry, record one sample against each, and encode.
    ///
    /// One sample per rendering, because a `Family` iterates in hash order
    /// and a two-series rendering is only equal to another up to that order —
    /// which is exactly the kind of "equal enough" this test must not accept.
    fn render<T, P>(
        topic_label: impl Fn(&str) -> T,
        partition_label: impl Fn(&str, i32) -> P,
        sample: (&str, i32, u64),
    ) -> String
    where
        T: EncodeLabelSet + Clone + Eq + std::fmt::Debug + std::hash::Hash + Send + Sync + 'static,
        P: EncodeLabelSet + Clone + Eq + std::fmt::Debug + std::hash::Hash + Send + Sync + 'static,
    {
        let (topic, partition, count) = sample;
        let mut registry = Registry::default();
        let topics = Family::<T, Counter>::default();
        let partitions = Family::<P, Counter>::default();
        registry.register(
            "topic_produce_requests",
            "Produce requests per topic",
            topics.clone(),
        );
        registry.register(
            "partition_bytes_in",
            "Bytes appended per partition",
            partitions.clone(),
        );
        topics.get_or_create(&topic_label(topic)).inc_by(count);
        partitions
            .get_or_create(&partition_label(topic, partition))
            .inc_by(count);
        let mut buf = String::new();
        encode(&mut buf, &registry).expect("encoding into a String cannot fail");
        buf
    }

    /// Render one sample through the current, `Arc<str>`-labelled sets.
    fn render_shared(sample: (&str, i32, u64)) -> String {
        render(
            |topic| TopicLabel {
                topic: topic.into(),
            },
            |topic, partition| PartitionLabel {
                topic: topic.into(),
                partition,
            },
            sample,
        )
    }

    /// Render one sample through the previous, `String`-labelled sets.
    fn render_owned(sample: (&str, i32, u64)) -> String {
        render(
            |topic| OwnedTopicLabel {
                topic: topic.to_owned(),
            },
            |topic, partition| OwnedPartitionLabel {
                topic: topic.to_owned(),
                partition,
            },
            sample,
        )
    }

    /// The bytes an operator scrapes are the contract, and this change was
    /// meant to move none of them.
    #[test]
    fn shared_topic_labels_render_exactly_as_owned_ones_did() {
        for sample in SAMPLES {
            assert!(render_shared(sample) == render_owned(sample));
        }
    }

    /// [`CAPTURED`] is the previous implementation's output, so it is only
    /// worth comparing against for as long as it stays that. This is what
    /// says it does.
    #[test]
    fn owned_labels_still_render_the_captured_bytes() {
        for (sample, captured) in SAMPLES.into_iter().zip(CAPTURED) {
            assert!(render_owned(sample) == captured);
        }
    }

    /// The acceptance the change was asked for: today's scrape, byte for
    /// byte, out of the `Arc<str>` label sets.
    #[test]
    fn shared_labels_render_the_captured_bytes() {
        for (sample, captured) in SAMPLES.into_iter().zip(CAPTURED) {
            assert!(render_shared(sample) == captured);
        }
    }
}
