//! The group side of `OffsetDelete` (KIP-496).
//!
//! Kafka's `OffsetMetadataManager.deleteOffsets` first runs the group's
//! `validateOffsetDelete`, then answers `GROUP_SUBSCRIBED_TO_TOPIC` for every
//! partition of a topic for which `isSubscribedToTopic` holds. This module
//! computes both from the live group: the group-level error, or the set of
//! topics that the group subscribes to.
//!
//! It also does the write. Kafka tombstones a partition only when the group
//! holds a committed or a pending transactional offset for it, and this
//! module decides that and appends in the same actor turn.

use {
    super::{ErrorCode, chrono_now_ms, retention::append_tombstones},
    crate::{
        codes,
        coordinator::unified::{
            classic_state::{ClassicGroup as ClassicState, GroupState as ClassicGroupState},
            consumer_state::GroupState as ConsumerState,
            group::{CoordinatorGroup, GroupKind},
            offsets_log::OffsetsLog,
        },
    },
    bytes::Buf as _,
    krabka_protocol::{
        Decode, owned::consumer_protocol_subscription::ConsumerProtocolSubscription,
    },
    std::collections::HashSet,
};

/// The topics whose offsets `OffsetDelete` must not remove.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscribedTopics {
    /// Every topic. A classic consumer group whose subscriptions Kafka cannot
    /// read (`ClassicGroup.isSubscribedToTopic` falls back to
    /// `usesConsumerGroupProtocol()`), or a KIP-848 group with a regex
    /// subscription that replay restored and no heartbeat has resolved since.
    All,
    /// Exactly these topics.
    Named(HashSet<String>),
}

impl SubscribedTopics {
    /// Kafka's `Group.isSubscribedToTopic`.
    #[must_use]
    pub fn contains(&self, topic: &str) -> bool {
        match self {
            Self::All => true,
            Self::Named(topics) => topics.contains(topic),
        }
    }
}

/// Kafka's `validateOffsetDelete` followed by the group's subscribed topics.
pub(super) fn offset_delete_guard(group: &CoordinatorGroup) -> Result<SubscribedTopics, ErrorCode> {
    match &group.kind {
        GroupKind::Classic(state) => classic_guard(state),
        // `ConsumerGroup.validateOffsetDelete` accepts every state.
        GroupKind::Consumer(state) => Ok(consumer_subscribed_topics(state)),
    }
}

/// Kafka's write half of `OffsetMetadataManager.deleteOffsets`: tombstones
/// the keys of `requested` for which the group holds a committed offset
/// (`hasCommittedOffset`) or an unresolved transactional one
/// (`hasPendingTransactionalOffsets`), in request order, and then drops them
/// from the group. A commit still waiting for the high watermark counts as
/// committed, as Kafka's write operations read the newest state, and the
/// tombstone supersedes it.
///
/// Reading the group and appending in one actor turn keeps a concurrent
/// commit from landing between the check and the write. An unresolved
/// transactional key counts whether its records are durable yet or only
/// reserved, as for `DeleteGroups`. When no key is held nothing is appended.
///
/// # Errors
///
/// Returns the code of the failed append, which leaves the group as it was.
pub(super) async fn delete_held_offsets(
    group: &mut CoordinatorGroup,
    offsets_log: &dyn OffsetsLog,
    requested: &[(String, i32)],
) -> Result<(), ErrorCode> {
    let pending = group.unresolved_txn_keys();
    let latest = group.latest_offsets();
    let held: Vec<(String, i32)> = requested
        .iter()
        .filter(|key| latest.contains_key(*key) || pending.contains(*key))
        .cloned()
        .collect();
    if held.is_empty() {
        return Ok(());
    }
    if let Err(error) =
        append_tombstones(offsets_log, &group.group_id, &held, None, chrono_now_ms()).await
    {
        tracing::warn!(
            group_id = %group.group_id,
            %error,
            "OffsetDelete: the tombstone write failed",
        );
        return Err(codes::from_broker_error(&error));
    }
    group.drop_offsets(&held);
    Ok(())
}

/// `ClassicGroup.validateOffsetDelete` and `ClassicGroup.isSubscribedToTopic`.
///
/// A non-empty group that does not use the consumer protocol (Connect, for
/// example) is refused with `NON_EMPTY_GROUP`. A consumer group subscribes to
/// the topics in its members' metadata for the selected protocol, read with
/// the v0 schema that prefixes every `ConsumerProtocolSubscription` version.
/// When no protocol is selected yet or a member's metadata does not decode,
/// Kafka cannot tell, and treats the group as subscribed to every topic.
fn classic_guard(state: &ClassicState) -> Result<SubscribedTopics, ErrorCode> {
    if state.state == ClassicGroupState::Empty || state.members.is_empty() {
        return Ok(SubscribedTopics::Named(HashSet::new()));
    }
    if state.protocol_type.as_deref() != Some("consumer") {
        return Err(codes::NON_EMPTY_GROUP);
    }
    if state.protocol_name.is_none() {
        return Ok(SubscribedTopics::All);
    }
    let mut topics = HashSet::new();
    for member in state.members.values() {
        let Some(member_topics) = decode_subscribed_topics(&member.protocol_metadata) else {
            return Ok(SubscribedTopics::All);
        };
        topics.extend(member_topics);
    }
    Ok(SubscribedTopics::Named(topics))
}

/// `ModernGroup.isSubscribedToTopic`: the names the members subscribe to,
/// plus the topics their regular expressions resolved to in the group.
///
/// A regex that the group has not resolved yet subscribes the group to topics
/// it cannot name. Until it is resolved the group counts as subscribed to
/// every topic, so neither `OffsetDelete` nor offset expiration removes an
/// offset the regex may still cover.
fn consumer_subscribed_topics(state: &ConsumerState) -> SubscribedTopics {
    if state.has_unresolved_regex() {
        return SubscribedTopics::All;
    }
    let mut topics = HashSet::new();
    for member in state.members.values() {
        topics.extend(member.subscribed_topic_names.iter().cloned());
        topics.extend(state.regex_topics(member).cloned());
    }
    SubscribedTopics::Named(topics)
}

/// Kafka's `ConsumerProtocol.deserializeVersion` and
/// `deserializeConsumerProtocolSubscription(buffer, 0)`: skip the `i16`
/// version, then read the v0 body. `None` when the blob does not decode.
fn decode_subscribed_topics(metadata: &[u8]) -> Option<Vec<String>> {
    if metadata.len() < 2 {
        return None;
    }
    let mut cur = metadata;
    let _version = cur.get_i16();
    ConsumerProtocolSubscription::decode(&mut cur, 0)
        .ok()
        .map(|subscription| subscription.topics)
}

#[cfg(test)]
mod held_offsets_tests;

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::coordinator::unified::{
            actor::test_support::subscription_blob_at as subscription,
            classic_state::Member as ClassicMember,
            consumer_state::{ResolvedRegularExpression, test_support::member},
        },
        assert2::check,
        bytes::Bytes,
        std::time::Duration,
    };

    fn named(topics: &[&str]) -> SubscribedTopics {
        SubscribedTopics::Named(topics.iter().map(|s| (*s).to_owned()).collect())
    }

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct ClassicOffsetSetup<'a> {
        #[default(ClassicGroupState::Empty)]
        state: ClassicGroupState,
        #[default(Some("consumer"))]
        protocol_type: Option<&'a str>,
        protocol_name: Option<&'a str>,
        metadata: &'a [Bytes],
    }

    #[bon::builder]
    fn classic(
        #[builder(default = ClassicGroupState :: Empty)] state: ClassicGroupState,
        #[builder(required, default = Some ("consumer"))] protocol_type: Option<&'_ str>,
        protocol_name: Option<&'_ str>,
        #[builder(default)] metadata: &'_ [Bytes],
    ) -> ClassicState {
        let setup = ClassicOffsetSetup {
            state,
            protocol_type,
            protocol_name,
            metadata,
        };
        let ClassicOffsetSetup {
            state,
            protocol_type,
            protocol_name,
            metadata,
        } = setup;
        let mut g = ClassicState::new("g");
        g.state = state;
        g.protocol_type = protocol_type.map(String::from);
        g.protocol_name = protocol_name.map(String::from);
        for (i, blob) in metadata.iter().enumerate() {
            let id = format!("m{i}");
            let m = ClassicMember::new(
                id.clone(),
                "client",
                "host",
                Duration::from_secs(30),
                Duration::from_mins(1),
                vec![("range".into(), blob.clone())],
            );
            g.members.insert(id, m);
        }
        g
    }

    /// `ClassicGroup.validateOffsetDelete` and `isSubscribedToTopic`, row by
    /// row.
    /// `ModernGroup.isSubscribedToTopic`: the names, and the topics that the
    /// group resolved each regex to. A regex the group has not resolved makes
    /// the group subscribed to every topic.
    #[test]
    fn consumer_group_subscribes_to_names_and_resolved_regex() {
        let mut g = ConsumerState::new("g");
        let mut by_name = member("m1");
        by_name.subscribed_topic_names = HashSet::from(["orders".to_owned()]);
        let mut by_regex = member("m2");
        by_regex.subscribed_topic_regex = Some("pay.*".into());
        g.add_or_update_member(by_name);
        g.add_or_update_member(by_regex);

        check!(consumer_subscribed_topics(&g) == SubscribedTopics::All);

        g.set_resolved_regex("pay.*".into(), ResolvedRegularExpression {
            topics: ["payments".to_owned()].into(),
            version: 1,
            timestamp_ms: 1,
        });
        check!(consumer_subscribed_topics(&g) == named(&["orders", "payments"]));
    }
    use {
        rstest_bdd::datatable::Rows,
        rstest_bdd_macros::{DataTableRow, given, scenarios, then, when},
    };
    #[derive(Default)]
    struct OffsetDeletionGuard {
        group: Option<ClassicState>,
        decision: Option<Result<SubscribedTopics, i16>>,
    }
    #[derive(DataTableRow)]
    struct SubscriptionMetadata {
        version: String,
        topics: String,
    }
    #[rstest::fixture]
    fn deletion_guard() -> OffsetDeletionGuard {
        // Start this scenario with fresh inputs and observations.
        OffsetDeletionGuard::default()
    }
    #[given(
        "a classic group is {state} with protocol type {kind} selected protocol {selected} and member metadata"
    )]
    fn deletion_group_metadata(
        deletion_guard: &mut OffsetDeletionGuard,
        state: String,
        kind: String,
        selected: String,
        #[datatable] rows: Rows<SubscriptionMetadata>,
    ) {
        let state = match state.as_str() {
            "empty" => ClassicGroupState::Empty,
            "stable" => ClassicGroupState::Stable,
            "preparing" => ClassicGroupState::PreparingRebalance,
            "completing" => ClassicGroupState::CompletingRebalance,
            _ => panic!("unknown classic group state"),
        };
        let kind = if kind == "absent" {
            None
        } else {
            Some(kind.as_str())
        };
        let selected = if selected == "absent" {
            None
        } else {
            Some(selected.as_str())
        };
        let metadata = rows
            .into_iter()
            .map(|row| match row.version.as_str() {
                "empty" => Bytes::new(),
                "opaque" => Bytes::from_static(b"x"),
                "truncated" => Bytes::from_static(b"\x00"),
                version => {
                    let topics = row.topics.split(',').collect::<Vec<_>>();
                    subscription(version.parse().expect("subscription version"), &topics)
                }
            })
            .collect::<Vec<_>>();
        deletion_guard.group = Some(
            classic()
                .state(state)
                .protocol_type(kind)
                .maybe_protocol_name(selected)
                .metadata(&metadata)
                .call(),
        );
    }
    #[when("the coordinator checks which topics a classic OffsetDelete must protect")]
    fn classic_deletion_guard(deletion_guard: &mut OffsetDeletionGuard) {
        deletion_guard.decision = Some(classic_guard(
            deletion_guard
                .group
                .as_ref()
                .expect("classic group prepared"),
        ));
    }
    #[then("the deletion guard protects {topics}")]
    fn classic_protected_topics(deletion_guard: &OffsetDeletionGuard, topics: String) {
        let expected = match topics.as_str() {
            "none" => Ok(named(&[])),
            "all" => Ok(SubscribedTopics::All),
            "nonempty-group-error" => Err(codes::NON_EMPTY_GROUP),
            _ => {
                let names = topics.split(',').collect::<Vec<_>>();
                Ok(named(&names))
            }
        };
        check!(deletion_guard.decision.as_ref() == Some(&expected));
    }
    scenarios!("tests/features/migrated/src/coordinator/unified/actor/offset_delete.feature",fixtures=[deletion_guard:OffsetDeletionGuard]);
}
