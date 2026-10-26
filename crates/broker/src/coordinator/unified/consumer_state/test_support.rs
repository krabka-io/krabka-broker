//! The member fixture the `consumer_state` submodule tests share.

use {
    super::{group::GroupState, member::MemberState, regex::ResolvedRegularExpression},
    crate::coordinator::unified::{actor::MetadataProvider, reconciler::ReconcileInput},
    krabka_protocol::primitives::uuid::Uuid,
    std::{
        sync::Arc,
        time::{Duration, Instant},
    },
};

pub fn member(id: &str) -> MemberState {
    MemberState {
        client_id: "c".into(),
        client_host: "/127.0.0.1".into(),
        rebalance_timeout: Duration::from_mins(1),
        ..MemberState::empty(id, Instant::now())
    }
}

/// `member(id)` subscribed to the topics `names`.
pub fn subscribed_member(id: &str, names: &[&str]) -> MemberState {
    MemberState {
        subscribed_topic_names: names.iter().map(|name| (*name).to_owned()).collect(),
        ..member(id)
    }
}

pub fn regex_group(subscriptions: &[(&str, Option<&str>)]) -> GroupState {
    let mut group = GroupState::new("g");
    for (member_id, regex) in subscriptions {
        let mut m = member(member_id);
        m.subscribed_topic_regex = regex.map(str::to_owned);
        group.add_or_update_member(m);
    }
    group
}

pub fn resolved_regex(
    topics: &[&str],
    version: i64,
    timestamp_ms: i64,
) -> ResolvedRegularExpression {
    ResolvedRegularExpression {
        topics: topics.iter().map(|topic| (*topic).to_owned()).collect(),
        version,
        timestamp_ms,
    }
}

/// A topic that [`Topics`] holds.
#[derive(Debug, Clone, Copy)]
pub struct NamedTopic {
    pub(crate) name: &'static str,
    pub(crate) id: Uuid,
}

/// A metadata provider that holds the topics it was given, each with two
/// partitions.
#[derive(Debug)]
pub struct Topics(pub(crate) Vec<NamedTopic>);

impl MetadataProvider for Topics {
    fn snapshot(&self) -> Arc<ReconcileInput> {
        Arc::new(ReconcileInput {
            topic_id_by_name: self
                .0
                .iter()
                .map(|topic| (topic.name.to_owned(), topic.id))
                .collect(),
            partitions_per_topic: self.0.iter().map(|topic| (topic.id, 2)).collect(),
            ..Default::default()
        })
    }
}
