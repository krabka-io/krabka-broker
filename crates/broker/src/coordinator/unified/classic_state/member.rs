//! The classic-protocol [`Member`] record and the [`AddMemberOutcome`] verdict
//! that `ClassicGroup::add_member` returns.
//!
//! A member carries the KIP-345 static-membership pin, the protocol proposals
//! it offered in its `JoinGroup`, and the assignment the group leader installed
//! for it in `SyncGroup`.

use std::time::{Duration, Instant};

use bytes::Bytes;

/// One member of a [`Group`].
#[derive(Debug, Clone)]
pub struct Member {
    pub id: String,
    /// KIP-345 static-membership pin. When `Some`, the broker keeps this
    /// member's slot across session timeouts, and matches a reconnecting
    /// client by `group_instance_id` instead of creating a fresh
    /// `member_id`.
    pub group_instance_id: Option<String>,
    pub client_id: String,
    pub host: String,
    pub session_timeout: Duration,
    pub rebalance_timeout: Duration,
    pub last_heartbeat: Instant,
    /// Encoded `ConsumerProtocolSubscription` bytes, from a `subscription`
    /// field in `JoinGroupRequest`. The broker does not read them. These are
    /// the metadata for the selected protocol.
    /// [`Group::resolve_selected_protocol_metadata`] fills the field after
    /// `select_protocol` picks a winner.
    pub protocol_metadata: Bytes,
    /// Full list of `(protocol_name, metadata)` pairs the member proposed in
    /// its `JoinGroupRequest`. The broker uses them to negotiate the group
    /// protocol.
    pub protocols: Vec<(String, Bytes)>,
    /// Encoded `ConsumerProtocolAssignment` bytes. The leader fills them in
    /// `SyncGroup`. The value is `None` until then.
    pub assignment: Option<Bytes>,
}

impl Member {
    #[must_use]
    pub fn new(
        member_id: impl Into<String>,
        client_id: impl Into<String>,
        host: impl Into<String>,
        session_timeout: Duration,
        rebalance_timeout: Duration,
        protocols: Vec<(String, Bytes)>,
    ) -> Self {
        let protocol_metadata = protocols
            .first()
            .map(|(_, b)| b.clone())
            .unwrap_or_default();
        Self {
            id: member_id.into(),
            group_instance_id: None,
            client_id: client_id.into(),
            host: host.into(),
            session_timeout,
            rebalance_timeout,
            last_heartbeat: Instant::now(),
            protocol_metadata,
            protocols,
            assignment: None,
        }
    }

    /// Builder-style: pin a `group.instance.id` (KIP-345 static membership).
    #[must_use]
    pub fn with_instance_id(mut self, instance_id: Option<String>) -> Self {
        self.group_instance_id = instance_id;
        self
    }

    #[must_use]
    pub fn is_static(&self) -> bool {
        self.group_instance_id.is_some()
    }
}

/// Outcome of [`Group::add_member`]. It drives the `JoinGroup` handler's
/// fast-path decisions for a KIP-345 static rejoin.
#[derive(Debug, PartialEq, Eq)]
pub enum AddMemberOutcome {
    /// The group added a new member. The group moved to `PreparingRebalance`
    /// if it was `Empty` or `Stable` before.
    NewMember,
    /// A static member with this `group.instance.id` already existed, and the
    /// group replaced it in place. This variant returns the prior
    /// `member_id`, which the new session can match or not match. If the group
    /// was `Stable`, the state does not change, and the new session reuses the
    /// cached assignment without a forced rebalance.
    StaticRejoin { prior_member_id: String },
    /// A *different* live `member_id` is currently pinned to this
    /// `group.instance.id`. The caller must reject the request with
    /// `FENCED_INSTANCE_ID`. Nothing changed.
    Fenced { live_member_id: String },
}
