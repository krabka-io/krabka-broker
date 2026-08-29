//! The record keys of the KIP-1071 streams-group records, and their codec.
//!
//! Every key starts with an `i16` key-version discriminator in the range 15 to
//! 21, followed by the group id and, for the per-member records, the member
//! id. [`StreamsGroupKey`] is the parsed form that the `__consumer_offsets`
//! replay path dispatches on.

use bytes::{BufMut, Bytes, BytesMut};
use krabka_protocol::ProtocolError;

use crate::{
    coordinator::unified::persistence::{get_string, put_string},
    error::BrokerError,
};

pub const KEY_STREAMS_GROUP_METADATA: i16 = 15;
pub const KEY_STREAMS_MEMBER_METADATA: i16 = 16;
pub const KEY_STREAMS_TOPOLOGY: i16 = 17;
pub const KEY_STREAMS_PARTITION_METADATA: i16 = 18;
pub const KEY_STREAMS_TARGET_ASSIGNMENT_METADATA: i16 = 19;
pub const KEY_STREAMS_TARGET_ASSIGNMENT_MEMBER: i16 = 20;
pub const KEY_STREAMS_CURRENT_MEMBER_ASSIGNMENT: i16 = 21;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StreamsGroupKey {
    GroupMetadata { group_id: String },
    MemberMetadata { group_id: String, member_id: String },
    Topology { group_id: String },
    PartitionMetadata { group_id: String },
    TargetAssignmentMetadata { group_id: String },
    TargetAssignmentMember { group_id: String, member_id: String },
    CurrentMemberAssignment { group_id: String, member_id: String },
}

#[must_use]
pub fn encode_group_metadata_key(group_id: &str) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_i16(KEY_STREAMS_GROUP_METADATA);
    put_string(&mut buf, group_id);
    buf.freeze()
}

#[must_use]
pub fn encode_member_metadata_key(group_id: &str, member_id: &str) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_i16(KEY_STREAMS_MEMBER_METADATA);
    put_string(&mut buf, group_id);
    put_string(&mut buf, member_id);
    buf.freeze()
}

#[must_use]
pub fn encode_topology_key(group_id: &str) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_i16(KEY_STREAMS_TOPOLOGY);
    put_string(&mut buf, group_id);
    buf.freeze()
}

#[must_use]
pub fn encode_partition_metadata_key(group_id: &str) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_i16(KEY_STREAMS_PARTITION_METADATA);
    put_string(&mut buf, group_id);
    buf.freeze()
}

#[must_use]
pub fn encode_target_assignment_metadata_key(group_id: &str) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_i16(KEY_STREAMS_TARGET_ASSIGNMENT_METADATA);
    put_string(&mut buf, group_id);
    buf.freeze()
}

#[must_use]
pub fn encode_target_assignment_member_key(group_id: &str, member_id: &str) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_i16(KEY_STREAMS_TARGET_ASSIGNMENT_MEMBER);
    put_string(&mut buf, group_id);
    put_string(&mut buf, member_id);
    buf.freeze()
}

#[must_use]
pub fn encode_current_member_assignment_key(group_id: &str, member_id: &str) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_i16(KEY_STREAMS_CURRENT_MEMBER_ASSIGNMENT);
    put_string(&mut buf, group_id);
    put_string(&mut buf, member_id);
    buf.freeze()
}

/// Encodes a [`StreamsGroupKey`] for dispatch, with a leading `i16` key
/// version.
#[must_use]
pub fn encode_streams_key(key: &StreamsGroupKey) -> Bytes {
    match key {
        StreamsGroupKey::GroupMetadata { group_id } => encode_group_metadata_key(group_id),
        StreamsGroupKey::MemberMetadata {
            group_id,
            member_id,
        } => encode_member_metadata_key(group_id, member_id),
        StreamsGroupKey::Topology { group_id } => encode_topology_key(group_id),
        StreamsGroupKey::PartitionMetadata { group_id } => encode_partition_metadata_key(group_id),
        StreamsGroupKey::TargetAssignmentMetadata { group_id } => {
            encode_target_assignment_metadata_key(group_id)
        }
        StreamsGroupKey::TargetAssignmentMember {
            group_id,
            member_id,
        } => encode_target_assignment_member_key(group_id, member_id),
        StreamsGroupKey::CurrentMemberAssignment {
            group_id,
            member_id,
        } => encode_current_member_assignment_key(group_id, member_id),
    }
}

/// # Errors
/// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
pub fn parse_streams_key(version: i16, mut buf: &[u8]) -> Result<StreamsGroupKey, BrokerError> {
    let key = match version {
        KEY_STREAMS_GROUP_METADATA => StreamsGroupKey::GroupMetadata {
            group_id: get_string(&mut buf)?,
        },
        KEY_STREAMS_MEMBER_METADATA => StreamsGroupKey::MemberMetadata {
            group_id: get_string(&mut buf)?,
            member_id: get_string(&mut buf)?,
        },
        KEY_STREAMS_TOPOLOGY => StreamsGroupKey::Topology {
            group_id: get_string(&mut buf)?,
        },
        KEY_STREAMS_PARTITION_METADATA => StreamsGroupKey::PartitionMetadata {
            group_id: get_string(&mut buf)?,
        },
        KEY_STREAMS_TARGET_ASSIGNMENT_METADATA => StreamsGroupKey::TargetAssignmentMetadata {
            group_id: get_string(&mut buf)?,
        },
        KEY_STREAMS_TARGET_ASSIGNMENT_MEMBER => StreamsGroupKey::TargetAssignmentMember {
            group_id: get_string(&mut buf)?,
            member_id: get_string(&mut buf)?,
        },
        KEY_STREAMS_CURRENT_MEMBER_ASSIGNMENT => StreamsGroupKey::CurrentMemberAssignment {
            group_id: get_string(&mut buf)?,
            member_id: get_string(&mut buf)?,
        },
        _ => {
            return Err(BrokerError::Protocol(ProtocolError::InvalidValue(
                "unknown streams-group key version",
            )));
        }
    };
    Ok(key)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn unknown_key_version_rejected() {
        assert!(parse_streams_key(99, &[]).is_err());
    }
}
