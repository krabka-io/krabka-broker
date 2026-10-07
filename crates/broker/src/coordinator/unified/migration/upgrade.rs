//! The classic-to-next-gen direction of a KIP-848 live migration.
//!
//! It holds the subscription decoder, the admission predicate a classic group
//! must pass, the state translation that re-expresses its members as hosted
//! classic members of a consumer group, and the atomic record batch that makes
//! the flip durable.

use std::{collections::HashMap, time::Instant};

use krabka_protocol::{
    Decode,
    owned::{
        consumer_protocol_assignment::ConsumerProtocolAssignment,
        consumer_protocol_subscription::ConsumerProtocolSubscription,
    },
    primitives::uuid::Uuid,
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
    reconciler::{self, ReconcileInput},
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

/// Kafka's `GroupMetadataManager.validateOnlineUpgrade` for a non-empty
/// classic group that a `ConsumerGroupHeartbeat` joins, followed by the
/// subscription check of [`classic_is_convertible`].
///
/// An empty classic group never reaches this: Kafka's
/// `maybeDeleteEmptyClassicGroup` replaces it whatever the policy and the
/// protocol type. The error is the `GROUP_ID_NOT_FOUND` message Kafka sends.
pub(crate) fn validate_online_upgrade(
    state: &ClassicState,
    upgrade_enabled: bool,
    consumer_max_size: usize,
) -> Result<(), String> {
    let group_id = &state.group_id;
    if !upgrade_enabled {
        return Err(format!(
            "Cannot upgrade classic group {group_id} to consumer group because online upgrade is \
             disabled."
        ));
    }
    if state.protocol_type.as_deref() != Some("consumer") || !classic_is_convertible(state) {
        return Err(format!(
            "Cannot upgrade classic group {group_id} to consumer group because the group does \
             not use the consumer embedded protocol."
        ));
    }
    if state.members.len() > consumer_max_size {
        return Err(format!(
            "Cannot upgrade classic group {group_id} to consumer group because the group size \
             exceeds the consumer group maximum size."
        ));
    }
    Ok(())
}

/// Converts a classic group into a consumer group that **hosts its classic
/// members** during a KIP-848 upgrade, as Kafka's
/// `ConsumerGroup.fromClassicGroup` does.
///
/// The group epoch and the target assignment epoch are the classic
/// generation, with an unknown assignment time (0). Each classic member
/// becomes a stable [`MemberState`] at the generation, previous epoch
/// included, with a [`ClassicMemberFacade`], the topic names and rack of its
/// `ConsumerProtocolSubscription`, and its last assignment, read from its
/// `ConsumerProtocolAssignment` against `image`, as both its assignment (at
/// the generation) and its target. The group records the metadata hash of its
/// subscribed topics.
///
/// Precondition: the caller has checked [`classic_is_convertible`]. Committed
/// offsets live on the kind-agnostic `Group` container, and this function does
/// not change them.
pub(crate) fn convert_classic_to_consumer(
    classic: &ClassicState,
    image: &ReconcileInput,
) -> ConsumerState {
    let mut state = ConsumerState::new(classic.group_id.clone());
    let generation = classic_upgrade_epoch(
        classic.protocol_type.as_deref() == Some("consumer"),
        classic
            .members
            .values()
            .all(|member| decode_consumer_subscription(&member.protocol_metadata).is_some()),
        classic.generation_id,
    )
    .expect("upgrade precondition: classic group is representable");
    state.group_epoch = generation;
    state.target.epoch = generation;
    for m in classic.members.values() {
        let subscription = decode_consumer_subscription(&m.protocol_metadata).unwrap_or_default();
        let assigned = m
            .assignment
            .as_ref()
            .map(|blob| decode_consumer_assignment(blob, image))
            .unwrap_or_default();
        let facade = ClassicMemberFacade {
            generation_id: classic.generation_id,
            supported_protocols: m.protocols.clone(),
            session_timeout: m.session_timeout,
            last_synced_assignment: m.assignment.clone().unwrap_or_default(),
            awaiting_sync: true,
        };
        let assignment_epochs = assigned
            .iter()
            .map(|(topic_id, partitions)| {
                (
                    *topic_id,
                    partitions.iter().map(|p| (*p, generation)).collect(),
                )
            })
            .collect();
        state
            .target
            .per_member
            .insert(m.id.clone(), assigned.clone());
        state.add_or_update_member(MemberState {
            member_id: m.id.clone(),
            instance_id: m.group_instance_id.clone(),
            rack_id: subscription.rack_id.filter(|rack| !rack.is_empty()),
            client_id: m.client_id.clone(),
            client_host: m.host.clone(),
            subscribed_topic_names: subscription.topics.into_iter().collect(),
            subscribed_topic_regex: None,
            server_assignor: None,
            rebalance_timeout: m.rebalance_timeout,
            member_epoch: generation,
            previous_member_epoch: generation,
            assignment_state: MemberAssignmentState::Stable,
            assigned_partitions: assigned,
            partitions_pending_revocation: HashMap::new(),
            assignment_epochs,
            last_seen: Instant::now(),
            classic: Some(facade),
        });
    }
    let hash = reconciler::metadata_hash(&state, image);
    state.record_metadata_hash(hash);
    // A converted group's metadata is fresh, as Kafka's `fromClassicGroup`
    // computes the hash from the current image; the heartbeat that converts
    // it refreshes nothing more.
    state
}

/// Kafka's `toTopicPartitionMap` of a classic member's
/// `ConsumerProtocolAssignment`: the partitions of each topic that `image`
/// knows, by topic id. An empty or undecodable blob assigns nothing.
fn decode_consumer_assignment(blob: &[u8], image: &ReconcileInput) -> HashMap<Uuid, Vec<i32>> {
    use bytes::Buf;
    let mut assigned: HashMap<Uuid, Vec<i32>> = HashMap::new();
    if blob.len() < 2 {
        return assigned;
    }
    let mut cur = blob;
    let version = cur.get_i16();
    let Ok(assignment) = ConsumerProtocolAssignment::decode(&mut cur, version) else {
        return assigned;
    };
    for topic in assignment.assigned_partitions {
        if let Some(topic_id) = image.topic_id_by_name.get(&topic.topic) {
            let partitions = assigned.entry(*topic_id).or_default();
            partitions.extend(topic.partitions);
            partitions.sort_unstable();
            partitions.dedup();
        }
    }
    assigned
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
    super::super::persistence::assert_member_record_count!(pending, plan.member_count);
    pending
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::{assert, check};
    use bytes::Bytes;

    use super::*;
    use crate::coordinator::unified::{
        actor::test_support::subscription_blob,
        classic_state::{ClassicGroup, Member},
    };

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

        let state = convert_classic_to_consumer(&g, &ReconcileInput::default());
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
        // Kafka's `fromClassicGroup`: the target is the last assignment, at
        // the generation.
        assert!((state.target.epoch, state.target_is_stale()) == (3, false));
    }

    #[test]
    fn conversion_clamps_only_negative_epochs_and_retry_is_byte_identical() {
        let mut g = ClassicGroup::new("g");
        g.protocol_type = Some("consumer".into());
        g.generation_id = -1;
        g.add_member(consumer_member("m1", subscription_blob(&["t1"])));

        let first = convert_classic_to_consumer(&g, &ReconcileInput::default());
        let second = convert_classic_to_consumer(&g, &ReconcileInput::default());
        check!(first.group_epoch == 0);
        let first_batch = upgrade_pending_records(&first).to_batch("g", 7).unwrap();
        let second_batch = upgrade_pending_records(&second).to_batch("g", 7).unwrap();
        check!(first_batch.records.len() == 6);
        assert!(first_batch.records == second_batch.records);

        g.generation_id = i32::MAX;
        assert!(
            convert_classic_to_consumer(&g, &ReconcileInput::default()).group_epoch == i32::MAX
        );
    }

    /// Kafka's `validateOnlineUpgrade`, row by row: (label, protocol type,
    /// member count, upgrade enabled, consumer max size, expected result).
    #[test]
    fn validate_online_upgrade_follows_kafka() {
        let disabled = "Cannot upgrade classic group g to consumer group because online upgrade \
                        is disabled.";
        let protocol = "Cannot upgrade classic group g to consumer group because the group does \
                        not use the consumer embedded protocol.";
        let size = "Cannot upgrade classic group g to consumer group because the group size \
                    exceeds the consumer group maximum size.";
        let rows = [
            ("upgradable", Some("consumer"), 2, true, 2, Ok(())),
            ("policy off", Some("consumer"), 2, false, 2, Err(disabled)),
            ("connect group", Some("connect"), 2, true, 2, Err(protocol)),
            ("too many members", Some("consumer"), 3, true, 2, Err(size)),
        ];
        for (label, protocol_type, members, enabled, max_size, want) in rows {
            let mut g = ClassicGroup::new("g");
            g.protocol_type = protocol_type.map(String::from);
            for index in 0..members {
                g.add_member(consumer_member(
                    &format!("m{index}"),
                    subscription_blob(&["t"]),
                ));
            }
            let got = validate_online_upgrade(&g, enabled, max_size);
            check!(got == want.map_err(String::from), "{label}");
        }
    }
}
