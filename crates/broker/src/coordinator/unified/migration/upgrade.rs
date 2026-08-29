//! The classic-to-next-gen direction of a KIP-848 live migration.
//!
//! It holds the subscription decoder, the admission predicate a classic group
//! must pass, the state translation that re-expresses its members as hosted
//! classic members of a consumer group, and the atomic record batch that makes
//! the flip durable.

use std::{
    collections::{HashMap, HashSet},
    time::Instant,
};

use krabka_protocol::{
    Decode, owned::consumer_protocol_subscription::ConsumerProtocolSubscription,
};

use crate::coordinator::unified::{
    actor::{PendingRecords, full_pending_records},
    classic_state::ClassicGroup as ClassicState,
    consumer_state::{ClassicMemberFacade, GroupState as ConsumerState, MemberState},
    persistence_next_gen::MemberAssignmentState,
};

/// Decodes a classic member's `protocol_metadata` blob as a
/// `ConsumerProtocolSubscription`.
///
/// The blob carries a leading `i16` version, then the schema body. That
/// version is the "consumer" embedded-protocol version negotiation, which is
/// separate from the per-field version gates in the
/// `ConsumerProtocolSubscription` schema.
///
/// This function returns `None` on any decode error, and on an unknown
/// version. Such a member's subscription cannot survive translation to the
/// server-side consumer model. It mirrors
/// `offset_delete::decode_subscribed_topics`.
pub(crate) fn decode_consumer_subscription(
    metadata: &[u8],
) -> Option<ConsumerProtocolSubscription> {
    use bytes::Buf;
    if metadata.len() < 2 {
        return None;
    }
    let mut cur = metadata;
    let version = cur.get_i16();
    if !(0..=3).contains(&version) {
        return None;
    }
    ConsumerProtocolSubscription::decode(&mut cur, version).ok()
}

/// Can this classic group be upgraded to a next-gen consumer group?
///
/// This mirrors the admission rule in Apache Kafka's
/// `ConsumerGroup.fromClassicGroup`. The group must use the `"consumer"`
/// protocol type. **Every** current member's selected `protocol_metadata` must
/// decode as a valid `ConsumerProtocolSubscription`, so that each subscription
/// survives translation. An empty group is always convertible.
pub(crate) fn classic_is_convertible(state: &ClassicState) -> bool {
    if state.protocol_type.as_deref() != Some("consumer") {
        return false;
    }
    state
        .members
        .values()
        .all(|m| decode_consumer_subscription(&m.protocol_metadata).is_some())
}

/// Converts a classic group into a consumer group that **hosts its classic
/// members** during a KIP-848 upgrade.
///
/// Each classic member becomes a [`MemberState`] that carries a
/// [`ClassicMemberFacade`]. This function decodes the member's subscription
/// from its `ConsumerProtocolSubscription` metadata, which holds topic names.
/// The reconciler resolves those names to topic IDs against the metadata
/// image. The function marks the group dirty, so the next reconcile computes
/// the unified target.
///
/// Precondition: the caller has checked [`classic_is_convertible`]. Committed
/// offsets live on the kind-agnostic `Group` container, and this function does
/// not change them.
pub(crate) fn convert_classic_to_consumer(classic: &ClassicState) -> ConsumerState {
    let mut state = ConsumerState::new(classic.group_id.clone());
    // Seed the group epoch from the classic generation so epochs stay
    // monotonic across the flip; the first reconcile bumps it.
    state.group_epoch = classic.generation_id.max(0);
    for m in classic.members.values() {
        let names: HashSet<String> = decode_consumer_subscription(&m.protocol_metadata)
            .map(|s| s.topics.into_iter().collect())
            .unwrap_or_default();
        let facade = ClassicMemberFacade {
            generation_id: classic.generation_id,
            supported_protocols: m.protocols.clone(),
            session_timeout: m.session_timeout,
            last_synced_assignment: m.assignment.clone().unwrap_or_default(),
            awaiting_sync: true,
        };
        state.add_or_update_member(MemberState {
            member_id: m.id.clone(),
            instance_id: m.group_instance_id.clone(),
            rack_id: None,
            client_id: m.client_id.clone(),
            client_host: m.host.clone(),
            subscribed_topic_names: names,
            subscribed_topic_regex: None,
            compiled_regex: crate::coordinator::unified::consumer_state::CompiledRegex::Absent,
            server_assignor: None,
            rebalance_timeout: m.rebalance_timeout,
            member_epoch: state.group_epoch,
            previous_member_epoch: 0,
            assignment_state: MemberAssignmentState::Stable,
            assigned_partitions: HashMap::new(),
            partitions_pending_revocation: HashMap::new(),
            last_seen: Instant::now(),
            classic: Some(facade),
        });
    }
    state.dirty = true;
    state
}

/// The atomic record batch for an upgrade. It tombstones the classic k2
/// `GroupMetadata` and writes the full next-gen record set for the converted
/// group. Both go into one batch, so the flip is all-or-nothing.
pub(crate) fn upgrade_pending_records(state: &ConsumerState) -> PendingRecords {
    let mut pending = full_pending_records(state);
    pending.classic_group_metadata_tombstone = true;
    pending
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::assert;
    use bytes::{BufMut, Bytes, BytesMut};
    use krabka_protocol::Encode;

    use super::*;
    use crate::coordinator::unified::classic_state::{ClassicGroup, Member};

    /// Encodes a `ConsumerProtocolSubscription` with the leading version
    /// prefix, as a real classic consumer client sends it in its `JoinGroup`
    /// protocol metadata.
    fn subscription_blob(topics: &[&str]) -> Bytes {
        let sub = ConsumerProtocolSubscription {
            topics: topics.iter().map(|s| (*s).to_string()).collect(),
            ..Default::default()
        };
        let mut out = BytesMut::new();
        out.put_i16(0); // protocol version-negotiation prefix
        sub.encode(&mut out, 0).unwrap();
        out.freeze()
    }

    fn consumer_member(id: &str, metadata: Bytes) -> Member {
        let mut m = Member::new(
            id,
            "client",
            "127.0.0.1",
            Duration::from_secs(30),
            Duration::from_mins(1),
            vec![("range".into(), metadata.clone())],
        );
        m.protocol_metadata = metadata;
        m
    }

    #[test]
    fn empty_consumer_group_is_convertible() {
        let mut g = ClassicGroup::new("g");
        g.protocol_type = Some("consumer".into());
        assert!(classic_is_convertible(&g));
    }

    #[test]
    fn non_consumer_protocol_type_is_not_convertible() {
        let mut g = ClassicGroup::new("g");
        g.protocol_type = Some("connect".into());
        assert!(!classic_is_convertible(&g));
        // None protocol_type (never joined) is also not convertible.
        let g2 = ClassicGroup::new("g2");
        assert!(!classic_is_convertible(&g2));
    }

    #[test]
    fn group_of_valid_consumer_members_is_convertible() {
        let mut g = ClassicGroup::new("g");
        g.protocol_type = Some("consumer".into());
        g.add_member(consumer_member("m1", subscription_blob(&["t1"])));
        g.add_member(consumer_member("m2", subscription_blob(&["t1", "t2"])));
        assert!(classic_is_convertible(&g));
    }

    #[test]
    fn member_with_undecodable_metadata_blocks_conversion() {
        let mut g = ClassicGroup::new("g");
        g.protocol_type = Some("consumer".into());
        g.add_member(consumer_member("ok", subscription_blob(&["t1"])));
        // Garbage metadata that is not a ConsumerProtocolSubscription.
        g.add_member(consumer_member(
            "bad",
            Bytes::from_static(&[0xff, 0xff, 0x01]),
        ));
        assert!(!classic_is_convertible(&g));
    }

    #[test]
    fn decode_rejects_short_and_bad_version() {
        // Too short to hold a version, or (version 99) out of the supported
        // 0..=3 range.
        for input in [&[][..], &[0][..], &[0, 99][..]] {
            assert!(decode_consumer_subscription(input).is_none());
        }
    }

    #[test]
    fn convert_preserves_members_subscriptions_and_facade() {
        let mut g = ClassicGroup::new("g");
        g.protocol_type = Some("consumer".into());
        g.generation_id = 3;
        g.add_member(consumer_member("m1", subscription_blob(&["t1"])));
        g.add_member(consumer_member("m2", subscription_blob(&["t1", "t2"])));

        let state = convert_classic_to_consumer(&g);
        assert!(state.group_id == "g");
        assert!(state.group_epoch == 3); // seeded from classic generation
        assert!(state.members.len() == 2);
        let m1 = &state.members["m1"];
        assert!(m1.is_classic());
        assert!(m1.subscribed_topic_names.contains("t1"));
        let facade = m1.classic.as_ref().unwrap();
        assert!(facade.generation_id == 3);
        assert!(facade.awaiting_sync);
        // m2 subscribed to both topics.
        let m2 = &state.members["m2"];
        assert!(m2.subscribed_topic_names.len() == 2);
        // Marked dirty so the next reconcile computes the unified target.
        assert!(state.dirty);
    }
}
