//! The read-only projection of a share group that the `ShareGroupDescribe`
//! handler consumes, together with its rendering into the wire row. It is its
//! own file because presenting group state is separate from the state machine
//! that maintains it.

use std::collections::HashMap;

use krabka_protocol::{
    owned::share_group_describe_response::{DescribedGroup, Member as DescribeMember},
    primitives::uuid::Uuid,
};

use crate::{
    codes,
    coordinator::unified::share::{assignor::ShareGroupAssignor, state::ShareGroupState},
};

/// Read-only projection of [`ShareGroupState`], consumed by the
/// `ShareGroupDescribe` handler (a later dispatch).
#[derive(Debug, Clone)]
pub struct ShareDescribeView {
    pub group_id: String,
    pub group_epoch: i32,
    pub assignment_epoch: i32,
    pub group_state: String,
    pub assignor_name: String,
    pub members: Vec<ShareDescribeMember>,
}

#[derive(Debug, Clone)]
pub struct ShareDescribeMember {
    pub member_id: String,
    pub member_epoch: i32,
    pub rack_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub subscribed_topic_names: Vec<String>,
    pub assigned_partitions: HashMap<Uuid, Vec<i32>>,
}

impl ShareDescribeView {
    /// Render this view into a `ShareGroupDescribe` `DescribedGroup` wire row.
    /// `error_code` and `authorized_operations` keep their defaults, because
    /// the handler owns the ACL outcome.
    #[must_use]
    pub fn into_described_group(self) -> DescribedGroup {
        use krabka_protocol::owned::common::share_group_describe_response::{
            assignment::Assignment, topic_partitions::TopicPartitions,
        };

        let members = self
            .members
            .into_iter()
            .map(|m| {
                let topic_partitions = m
                    .assigned_partitions
                    .into_iter()
                    .map(|(tid, parts)| TopicPartitions {
                        topic_id: tid,
                        partitions: parts,
                        ..Default::default()
                    })
                    .collect();
                DescribeMember {
                    member_id: m.member_id,
                    rack_id: m.rack_id,
                    member_epoch: m.member_epoch,
                    client_id: m.client_id,
                    client_host: m.client_host,
                    subscribed_topic_names: m.subscribed_topic_names,
                    assignment: Assignment {
                        topic_partitions,
                        ..Default::default()
                    },
                    ..Default::default()
                }
            })
            .collect();
        DescribedGroup {
            group_id: self.group_id,
            group_state: self.group_state,
            group_epoch: self.group_epoch,
            assignment_epoch: self.assignment_epoch,
            assignor_name: self.assignor_name,
            members,
            error_code: codes::NONE,
            ..Default::default()
        }
    }
}

pub(super) fn build_describe(state: &ShareGroupState) -> ShareDescribeView {
    let group_state = if state.members.is_empty() {
        "Empty"
    } else {
        "Stable"
    };
    ShareDescribeView {
        group_id: state.group_id.clone(),
        group_epoch: state.group_epoch,
        assignment_epoch: state.target.epoch,
        group_state: group_state.into(),
        assignor_name: ShareGroupAssignor.name().into(),
        members: state
            .members
            .values()
            .map(|m| ShareDescribeMember {
                member_id: m.member_id.clone(),
                member_epoch: m.member_epoch,
                rack_id: m.rack_id.clone(),
                client_id: m.client_id.clone(),
                client_host: m.client_host.clone(),
                subscribed_topic_names: m.subscribed_topic_names.iter().cloned().collect(),
                assigned_partitions: m.assigned_partitions.clone(),
            })
            .collect(),
    }
}
