//! The [`MemberState`] record for one KIP-848 consumer-group member, the
//! [`CompiledRegex`] cache that backs its subscription pattern, and the
//! [`ClassicMemberFacade`] a classic member carries inside an upgraded group.
//!
//! A member is pure data plus the accessors that keep the compiled pattern in
//! step with the pattern string. The group-level transitions that add, remove,
//! and reconcile members live in the `group` and `reconcile` siblings.

use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use bytes::Bytes;
use krabka_protocol::primitives::uuid::Uuid;
use regex::Regex;

use crate::coordinator::unified::persistence_next_gen::MemberAssignmentState;

/// Classic-protocol state for a member hosted inside an *upgraded* consumer
/// group during a KIP-848 upgrade.
///
/// The value is `None` on a native consumer-protocol member. It is `Some` once
/// the broker upgraded a classic member's group, and when a classic member
/// joins an already-upgraded group.
///
/// The member keeps speaking the classic `JoinGroup`, `SyncGroup`, and
/// `Heartbeat` protocol. The coordinator serves it by mapping onto the
/// consumer-group machinery, and by translating its target into a
/// `ConsumerProtocolAssignment` blob on `SyncGroup`.
#[derive(Debug, Clone)]
pub struct ClassicMemberFacade {
    /// Classic generation that the coordinator echoes to the member. It
    /// advances with the group epoch.
    pub generation_id: i32,
    /// `(protocol_name, metadata)` pairs the member proposed in `JoinGroup`.
    /// The state keeps them so that a downgrade restores the classic member
    /// with no loss.
    pub supported_protocols: Vec<(String, Bytes)>,
    /// The member's classic `session.timeout.ms`.
    pub session_timeout: Duration,
    /// The last `ConsumerProtocolAssignment` blob that `SyncGroup` returned.
    pub last_synced_assignment: Bytes,
    /// `true` once the member must send `SyncGroup` again to pick up a changed
    /// target.
    pub awaiting_sync: bool,
}

#[derive(Debug, Clone)]
pub struct MemberState {
    pub member_id: String,
    pub instance_id: Option<String>,
    pub rack_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub subscribed_topic_names: HashSet<String>,
    /// KIP-848 v1+ `subscribed_topic_regex`. When set, the reconciler resolves
    /// it against the metadata image and unions the match with
    /// `subscribed_topic_names`. `None` means "no regex", that is an
    /// exact-name subscription only.
    pub subscribed_topic_regex: Option<String>,
    /// Compiled form of `subscribed_topic_regex`. The cache stops the
    /// reconciler from compiling the pattern for this member again on every
    /// recompute. It separates three cases: a compiled pattern, a cached
    /// compilation failure, and an absent pattern. Always keep it in step
    /// through [`MemberState::set_regex`]. Never set `subscribed_topic_regex`
    /// directly.
    ///
    /// `Regex` is `Clone` and `Debug`, but NOT `PartialEq` or `Eq`.
    /// `MemberState` derives only `Clone` and `Debug`, with no `PartialEq`, so
    /// this field needs no special handling. If someone adds `PartialEq`,
    /// compare on the pattern string instead and skip this cached field.
    ///
    /// This field is public only so that cross-module struct literals can
    /// initialize it to its default. Treat it as private, and change it only
    /// through [`MemberState::set_regex`] and
    /// [`MemberState::sync_regex_cache`].
    pub compiled_regex: CompiledRegex,
    pub server_assignor: Option<String>,
    pub rebalance_timeout: Duration,
    pub member_epoch: i32,
    pub previous_member_epoch: i32,
    pub assignment_state: MemberAssignmentState,
    pub assigned_partitions: HashMap<Uuid, Vec<i32>>,
    pub partitions_pending_revocation: HashMap<Uuid, Vec<i32>>,
    pub last_seen: Instant,
    /// Set if and only if this is a classic member hosted in an upgraded
    /// group.
    pub classic: Option<ClassicMemberFacade>,
}

#[derive(Debug, Clone, Default)]
pub enum CompiledRegex {
    #[default]
    Absent,
    /// A pattern that did not compile. Only replay can produce this: the
    /// heartbeat path rejects a bad pattern with
    /// `INVALID_REGULAR_EXPRESSION` before it is stored.
    Invalid,
    Valid(Regex),
}

impl MemberState {
    /// `true` if this member speaks the classic protocol inside an upgraded
    /// group. Its RPCs are then `JoinGroup`, `SyncGroup`, and `Heartbeat`, not
    /// `ConsumerGroupHeartbeat`.
    #[must_use]
    pub fn is_classic(&self) -> bool {
        self.classic.is_some()
    }

    /// Sets `subscribed_topic_regex` and compiles the cached `Regex` again.
    ///
    /// The compile runs exactly once per distinct pattern, so call this method
    /// only when the pattern changes. An invalid pattern goes into the cache
    /// as [`CompiledRegex::Invalid`], with one warning. The reconciler then
    /// neither retries the compile nor treats the pattern as "match
    /// everything".
    ///
    /// The live heartbeat path never reaches that arm: a
    /// `ConsumerGroupHeartbeat` whose `SubscribedTopicRegex` does not compile
    /// is answered `INVALID_REGULAR_EXPRESSION` (128) before any member state
    /// is touched, exactly as Kafka does. [`CompiledRegex::Invalid`] is
    /// therefore reachable only from replay, where a persisted pattern is
    /// restored without going back through that gate.
    pub fn set_regex(&mut self, pattern: Option<String>) {
        self.compiled_regex = match pattern.as_deref() {
            None => CompiledRegex::Absent,
            Some(pat) => match Regex::new(pat) {
                Ok(regex) => CompiledRegex::Valid(regex),
                Err(e) => {
                    tracing::warn!(
                        pattern = %pat, error = %e,
                        "consumer-group: subscribed_topic_regex failed to compile; ignored"
                    );
                    CompiledRegex::Invalid
                }
            },
        };
        self.subscribed_topic_regex = pattern;
    }

    /// Compiles the cache again from the current value of
    /// `subscribed_topic_regex`.
    ///
    /// This method exists for construction sites that set the pattern field
    /// through a struct literal. Those sites are in other modules, so they
    /// cannot call the setter inline. Call this method once afterwards to fill
    /// the cache.
    pub fn sync_regex_cache(&mut self) {
        let pattern = self.subscribed_topic_regex.take();
        self.set_regex(pattern);
    }

    /// The subscription regex that compiled successfully, if there is one. It
    /// returns `None` when there is no pattern, and when the pattern failed to
    /// compile.
    #[must_use]
    pub fn compiled_regex(&self) -> Option<&Regex> {
        match &self.compiled_regex {
            CompiledRegex::Valid(regex) => Some(regex),
            CompiledRegex::Absent | CompiledRegex::Invalid => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use crate::coordinator::unified::consumer_state::test_support::member;

    #[test]
    fn set_regex_compiles_and_caches() {
        let mut m = member("m1");
        m.set_regex(Some("^orders-.*".into()));
        assert!(m.subscribed_topic_regex.as_deref() == Some("^orders-.*"));
        let re = m.compiled_regex().expect("valid regex must compile");
        assert!(re.is_match("orders-eu"));
        assert!(!re.is_match("shipments"));
    }

    #[test]
    fn set_regex_caches_invalid_as_none() {
        let mut m = member("m1");
        m.set_regex(Some("*invalid".into()));
        // Pattern string is retained, but no compiled regex is exposed —
        // the reconciler treats this as names-only, not match-everything.
        assert!(m.subscribed_topic_regex.as_deref() == Some("*invalid"));
        assert!(m.compiled_regex().is_none());
    }

    #[test]
    fn set_regex_none_clears_cache() {
        let mut m = member("m1");
        m.set_regex(Some("^a".into()));
        assert!(m.compiled_regex().is_some());
        m.set_regex(None);
        assert!(m.subscribed_topic_regex.is_none());
        assert!(m.compiled_regex().is_none());
    }

    #[test]
    fn sync_regex_cache_populates_from_literal_field() {
        // Mimics a cross-module struct literal: pattern set, cache left None.
        let mut m = member("m1");
        m.subscribed_topic_regex = Some("^a".into());
        m.compiled_regex = crate::coordinator::unified::consumer_state::CompiledRegex::Absent;
        m.sync_regex_cache();
        assert!(m.subscribed_topic_regex.as_deref() == Some("^a"));
        assert!(m.compiled_regex().expect("synced").is_match("apple"));
    }
}
