//! The label sets the broker attaches to its metric families, and the
//! sentinel value that keeps an unbounded input from becoming an unbounded
//! label. They live together because cardinality is a property of the whole
//! set, not of any one family that uses it.

use prometheus_client::encoding::EncodeLabelSet;

/// Sentinel label value that folds unbounded inputs (unrecognised
/// `api_key`s, `SaslAuthenticate` without a prior handshake) into one
/// series, keeping label cardinality bounded.
pub(crate) const UNKNOWN_LABEL: &str = "Unknown";

/// Per-topic label set. `EncodeLabelSet` is the prometheus-client
/// derive that produces the `topic="<name>"` label on emitted samples.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct TopicLabel {
    pub topic: String,
}

/// Per-partition label set, paired with the `partition_*` and `delivery_*`
/// metric families. Consumed by the rebalancer's metric scraper. Cardinality is
/// bounded by the number of partitions this broker hosts, because both fields
/// come from the metadata image and never from client input.
#[derive(Debug, Clone, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct PartitionLabel {
    pub topic: String,
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
