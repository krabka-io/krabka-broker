//! The member fixture the `consumer_state` submodule tests share.

use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use krabka_protocol::primitives::uuid::Uuid;

use super::member::MemberState;
use crate::coordinator::unified::{
    actor::MetadataProvider, persistence_next_gen::MemberAssignmentState,
    reconciler::ReconcileInput,
};

pub(crate) fn member(id: &str) -> MemberState {
    MemberState {
        member_id: id.into(),
        instance_id: None,
        rack_id: None,
        client_id: "c".into(),
        client_host: "/127.0.0.1".into(),
        subscribed_topic_names: HashSet::new(),
        subscribed_topic_regex: None,
        server_assignor: None,
        rebalance_timeout: Duration::from_mins(1),
        member_epoch: 0,
        previous_member_epoch: 0,
        assignment_state: MemberAssignmentState::Stable,
        assigned_partitions: HashMap::new(),
        partitions_pending_revocation: HashMap::new(),
        assignment_epochs: HashMap::new(),
        last_seen: Instant::now(),
        classic: None,
    }
}

/// `member(id)` subscribed to the topics `names`.
pub(crate) fn subscribed_member(id: &str, names: &[&str]) -> MemberState {
    MemberState {
        subscribed_topic_names: names.iter().map(|name| (*name).to_owned()).collect(),
        ..member(id)
    }
}

/// A metadata provider that holds the topics it was given, each with two
/// partitions.
#[derive(Debug)]
pub(crate) struct Topics(pub(crate) Vec<(&'static str, Uuid)>);

impl MetadataProvider for Topics {
    fn snapshot(&self) -> ReconcileInput {
        ReconcileInput {
            topic_id_by_name: self
                .0
                .iter()
                .map(|(name, id)| ((*name).to_owned(), *id))
                .collect(),
            partitions_per_topic: self.0.iter().map(|(_, id)| (*id, 2)).collect(),
            ..Default::default()
        }
    }
}
