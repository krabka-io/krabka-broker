//! The member fixture the `consumer_state` submodule tests share.

use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use super::member::MemberState;
use crate::coordinator::unified::persistence_next_gen::MemberAssignmentState;

pub(super) fn member(id: &str) -> MemberState {
    MemberState {
        member_id: id.into(),
        instance_id: None,
        rack_id: None,
        client_id: "c".into(),
        client_host: "/127.0.0.1".into(),
        subscribed_topic_names: HashSet::new(),
        subscribed_topic_regex: None,
        compiled_regex: crate::coordinator::unified::consumer_state::CompiledRegex::Absent,
        server_assignor: None,
        rebalance_timeout: Duration::from_mins(1),
        member_epoch: 0,
        previous_member_epoch: 0,
        assignment_state: MemberAssignmentState::Stable,
        assigned_partitions: HashMap::new(),
        partitions_pending_revocation: HashMap::new(),
        last_seen: Instant::now(),
        classic: None,
    }
}
