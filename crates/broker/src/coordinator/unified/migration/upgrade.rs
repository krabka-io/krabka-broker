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
use krabka_verified::{
    GroupMigrationDirection, GroupMigrationRecordAction, classic_upgrade_epoch,
    group_migration_record_plan,
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
/// survives translation. An empty group with the consumer protocol type is
/// convertible.
pub(crate) fn classic_is_convertible(state: &ClassicState) -> bool {
    let every_subscription_decodable = state
        .members
        .values()
        .all(|m| decode_consumer_subscription(&m.protocol_metadata).is_some());
    classic_upgrade_epoch(
        state.protocol_type.as_deref() == Some("consumer"),
        every_subscription_decodable,
        state.generation_id,
    )
    .is_some()
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
    state.group_epoch = classic_upgrade_epoch(
        classic.protocol_type.as_deref() == Some("consumer"),
        classic
            .members
            .values()
            .all(|member| decode_consumer_subscription(&member.protocol_metadata).is_some()),
        classic.generation_id,
    )
    .expect("upgrade precondition: classic group is representable");
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
    let plan = group_migration_record_plan(GroupMigrationDirection::Upgrade, state.members.len());
    let mut pending = full_pending_records(state);
    pending.classic_group_metadata_tombstone =
        plan.classic_group == GroupMigrationRecordAction::Tombstone;
    assert2::debug_assert!(pending.group_metadata.is_some());
    assert2::debug_assert!(pending.target_metadata.is_some());
    assert2::debug_assert!(pending.member_metadata.len() == plan.member_count);
    assert2::debug_assert!(pending.target_per_member.len() == plan.member_count);
    assert2::debug_assert!(pending.current_per_member.len() == plan.member_count);
    pending
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::{assert, check};
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
        let mut source_m1 = consumer_member("m1", subscription_blob(&["t1"]));
        source_m1.group_instance_id = Some("instance-1".into());
        source_m1.assignment = Some(Bytes::from_static(b"last-assignment"));
        g.add_member(source_m1.clone());
        g.add_member(consumer_member("m2", subscription_blob(&["t1", "t2"])));

        let state = convert_classic_to_consumer(&g);
        assert!(state.group_id == "g");
        assert!(state.group_epoch == 3); // seeded from classic generation
        assert!(state.members.len() == 2);
        let m1 = &state.members["m1"];
        assert!(m1.is_classic());
        assert!(m1.subscribed_topic_names.contains("t1"));
        assert!(m1.instance_id == source_m1.group_instance_id);
        assert!(m1.client_id == source_m1.client_id);
        assert!(m1.client_host == source_m1.host);
        assert!(m1.rebalance_timeout == source_m1.rebalance_timeout);
        let facade = m1.classic.as_ref().unwrap();
        assert!(facade.generation_id == 3);
        assert!(facade.supported_protocols == source_m1.protocols);
        assert!(facade.session_timeout == source_m1.session_timeout);
        assert!(facade.last_synced_assignment == source_m1.assignment.unwrap());
        assert!(facade.awaiting_sync);
        // m2 subscribed to both topics.
        let m2 = &state.members["m2"];
        assert!(m2.subscribed_topic_names.len() == 2);
        // Marked dirty so the next reconcile computes the unified target.
        assert!(state.dirty);
    }

    #[test]
    fn conversion_clamps_only_negative_epochs_and_retry_is_byte_identical() {
        let mut g = ClassicGroup::new("g");
        g.protocol_type = Some("consumer".into());
        g.generation_id = -1;
        g.add_member(consumer_member("m1", subscription_blob(&["t1"])));

        let first = convert_classic_to_consumer(&g);
        let second = convert_classic_to_consumer(&g);
        check!(first.group_epoch == 0);
        let first_batch = upgrade_pending_records(&first).to_batch("g", 7);
        let second_batch = upgrade_pending_records(&second).to_batch("g", 7);
        check!(first_batch.records.len() == 6);
        assert!(first_batch.records == second_batch.records);

        g.generation_id = i32::MAX;
        assert!(convert_classic_to_consumer(&g).group_epoch == i32::MAX);
    }
}
