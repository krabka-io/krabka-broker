//! The label sets the broker attaches to its metric families, the label
//! values that are closed enums rather than free strings, and the sentinel
//! value that keeps an unbounded input from becoming an unbounded label. They
//! live together because cardinality is a property of the whole set, not of
//! any one family that uses it.

use std::{
    fmt,
    hash::{Hash, Hasher},
};

use krabka_metadata::BreakGlassAction as GatedAction;
use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue, LabelValueEncoder};

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

/// The drain path a fetch response's records regions took to the socket.
///
/// Which one a fetch takes is the whole return on the zero-copy fetch writer,
/// and a regression that quietly routes every fetch onto a copy path moves no
/// other series. These three are the complete set, so a closed enum bounds the
/// `fetch_response_drain` family at three series and no connection can invent
/// a fourth.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum FetchDrainPath {
    /// The kernel `sendfile(2)` moved at least one records region from the
    /// page cache to the socket with no userspace copy. A plaintext or kTLS
    /// connection takes this path for a records run at or above
    /// `sendfile_min`.
    Sendfile,
    /// Every records region left as userspace bytes beside the response
    /// metadata, the portable Increment-C path. Userspace TLS, Windows, a
    /// records run below `sendfile_min`, and a `read_committed` read all land
    /// here.
    Vectored,
    /// The response carried a file-backed records region that the drain had to
    /// `pread` into a buffer, because the stream declared itself
    /// sendfile-capable but would not hand out a socket. It is the fallback
    /// arm *inside* the drain, distinct from [`Self::Vectored`], where the
    /// bytes were already in userspace before the plan was built.
    Pread,
}

impl FetchDrainPath {
    /// Every path. Registration creates all three series at zero from it, so a
    /// dashboard finds them on a broker that has served no fetch yet, and on a
    /// target where one of them is unreachable.
    pub const ALL: [Self; 3] = [Self::Sendfile, Self::Vectored, Self::Pread];

    /// The `path` label value this variant renders as.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Sendfile => "sendfile",
            Self::Vectored => "vectored",
            Self::Pread => "pread",
        }
    }
}

impl EncodeLabelValue for FetchDrainPath {
    fn encode(&self, encoder: &mut LabelValueEncoder) -> Result<(), fmt::Error> {
        EncodeLabelValue::encode(&self.as_str(), encoder)
    }
}

/// Drain-path label set, paired with the `fetch_response_drain` counter
/// family. Cardinality is bounded at three, because the field is the closed
/// [`FetchDrainPath`] enum.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct FetchDrainPathLabel {
    pub path: FetchDrainPath,
}
