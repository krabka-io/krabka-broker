//! Reading a group's offset state out of its coordinator actor.
//!
//! Both `OffsetFetch` request shapes need the same name-keyed offset map, and
//! both reach it the same way: find the group's actor and ask it for its
//! offsets. A group with no actor, or an actor that has gone away, yields an
//! empty view rather than an error, which is the "no committed offsets" answer
//! the response already encodes. The read never creates a group: Kafka's
//! `OffsetMetadataManager.fetchOffsets` answers -1 rows for a group it does
//! not know and creates nothing.
//!
//! The reply carries the stable offsets and the `(topic, partition)` keys an
//! unresolved transaction has written, because KIP-447's `require_stable`
//! decides between the two per partition and must see one consistent snapshot
//! of both.

use {
    crate::{
        broker::Broker,
        coordinator::unified::{
            actor::GroupActorMessage,
            group::{GroupOffsets, OffsetEntry, OffsetFetchMember},
            streams::actor::validate_streams_group_offset_fetch,
        },
        task_util::ask,
    },
    krabka_protocol::owned::offset_fetch_response as wire,
};

/// Fetches the group's committed offsets, keyed by topic name and partition,
/// together with the keys its open transactions have not resolved yet.
///
/// `member_id` and `member_epoch` are the v9+ request fields (`None` and -1
/// before v9). A consumer or streams group checks them first, and a refusal is
/// the group error code Kafka's `ConsumerGroup.validateOffsetFetch` or
/// `StreamsGroup.validateOffsetFetch` answers. A streams group's members live
/// in its streams actor, so it is checked there, before and whether or not
/// the group has committed an offset. A fetch with no member id and a
/// negative epoch is allowed without asking, and an actor that exited after a
/// failed write is respawned from the group's replayed state first, so a
/// stopped actor never refuses a fetch with `UNKNOWN_SERVER_ERROR`.
pub(super) async fn fetch_offsets(
    broker: &Broker,
    group_id: &str,
    member_id: Option<&str>,
    member_epoch: i32,
) -> Result<GroupOffsets, i16> {
    let names_member = member_id.is_some() || member_epoch >= 0;
    if names_member && broker.group_coordinator.find_streams(group_id).is_some() {
        let streams = broker.group_coordinator.get_or_create_streams(group_id);
        if let Some(error_code) =
            validate_streams_group_offset_fetch(&streams, member_id, member_epoch).await
        {
            return Err(error_code);
        }
    }
    let Some(handle) = broker.group_coordinator.find(group_id) else {
        return Ok(GroupOffsets::default());
    };
    ask(&handle.tx, |reply| {
        GroupActorMessage::FetchOffsetsForMember {
            member: OffsetFetchMember {
                member_id: member_id.map(str::to_string),
                member_epoch,
            },
            reply,
        }
    })
    .await
    .unwrap_or_else(|_| Ok(GroupOffsets::default()))
}

/// Build the topic-name ordered fetch-all view using either wire row shape.
pub(super) fn committed_topics<T>(
    offsets: &GroupOffsets,
    mut row: impl FnMut(&str, i32) -> T,
) -> std::collections::BTreeMap<&str, Vec<T>> {
    let mut by_topic = std::collections::BTreeMap::<&str, Vec<T>>::new();
    for (topic, partition) in offsets.committed.keys() {
        by_topic
            .entry(topic.as_str())
            .or_default()
            .push(row(topic, *partition));
    }
    by_topic
}

// The two wire shapes carry identical committed and invalid-offset fields.
// Missing rows carry offset/epoch -1 and an empty metadata string, never null.
krabka_macros::offset_fetch_rows! { wire::OffsetFetchResponsePartition { missing: missing_legacy_row, stable: stable_legacy_row }
wire::OffsetFetchResponsePartitions { missing: missing_group_row, stable: stable_group_row } }
