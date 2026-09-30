//! The bound on the strings a group request hands to a coordinator record.
//!
//! Kafka's generated request readers refuse a string field longer than
//! `0x7fff` bytes: `MessageDataGenerator.generateVariableLengthReader` throws
//! `string field <name> had invalid length <n>`, `RequestContext.parseRequest`
//! wraps it in an `InvalidRequestException`, and
//! `SocketServer.Processor.processCompletedReceives` closes the connection
//! without a response. Only a flexible version can carry such a string, since
//! an older one has an `INT16` length.
//!
//! krabka's decoder accepts a longer compact string, and the group
//! coordinator writes every record key, and the classic group value, with an
//! `INT16` string length
//! ([`put_string`](crate::coordinator::unified::persistence::put_string)). A
//! group id, member id, instance id, or regular expression that long would
//! therefore panic the group actor when it wrote the record.
//! [`decode_group_request`] refuses it at the handler boundary as Kafka's
//! reader does, before the request reaches any coordinator.
//!
//! The strings that a record can carry are listed once, per request type, in
//! the [`RecordStrings`] impls below. A request that is not listed reads or
//! writes only state that a listed request created, or has no flexible
//! version (`OffsetDelete`), so its strings cannot pass the `INT16` bound.

use krabka_protocol::{
    Decode,
    owned::{
        alter_share_group_offsets_request::AlterShareGroupOffsetsRequest,
        consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
        delete_groups_request::DeleteGroupsRequest,
        delete_share_group_offsets_request::DeleteShareGroupOffsetsRequest,
        heartbeat_request::HeartbeatRequest, join_group_request::JoinGroupRequest,
        leave_group_request::LeaveGroupRequest, offset_commit_request::OffsetCommitRequest,
        share_group_heartbeat_request::ShareGroupHeartbeatRequest,
        streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
        sync_group_request::SyncGroupRequest, txn_offset_commit_request::TxnOffsetCommitRequest,
    },
};

use crate::error::BrokerError;

/// The longest string Kafka reads: `Short.MAX_VALUE`, the largest `INT16`
/// length.
const MAX_STRING_BYTES: usize = 0x7fff;

/// A group request whose strings a coordinator record can carry.
pub(crate) trait RecordStrings {
    /// The length in bytes of the longest string of the request that a
    /// coordinator record can carry: a group, member or instance id, a
    /// regular expression, a protocol type or name, a topic name, or offset
    /// metadata.
    fn longest_record_string(&self) -> usize;
}

/// Decode a group request, refusing one that carries a string Kafka's reader
/// would refuse.
///
/// The refusal is a protocol error, which closes the connection as Kafka does.
///
/// # Errors
///
/// Returns [`BrokerError::Protocol`] when the request does not decode, or when
/// one of its record strings is longer than 32767 bytes.
pub(crate) fn decode_group_request<R>(buf: &mut &[u8], version: i16) -> Result<R, BrokerError>
where
    R: for<'de> Decode<'de> + RecordStrings,
{
    let request = R::decode(buf, version)?;
    if request.longest_record_string() > MAX_STRING_BYTES {
        return Err(BrokerError::Protocol(
            krabka_protocol::ProtocolError::InvalidValue("string field had invalid length"),
        ));
    }
    Ok(request)
}

/// The length of the longest of `strings`.
fn longest<'a>(strings: impl IntoIterator<Item = &'a str>) -> usize {
    strings.into_iter().map(str::len).max().unwrap_or(0)
}

impl RecordStrings for JoinGroupRequest {
    fn longest_record_string(&self) -> usize {
        longest(
            [
                self.group_id.as_str(),
                self.member_id.as_str(),
                self.protocol_type.as_str(),
            ]
            .into_iter()
            .chain(self.group_instance_id.as_deref())
            .chain(self.reason.as_deref())
            .chain(self.protocols.iter().map(|protocol| protocol.name.as_str())),
        )
    }
}

impl RecordStrings for SyncGroupRequest {
    fn longest_record_string(&self) -> usize {
        longest(
            [self.group_id.as_str(), self.member_id.as_str()]
                .into_iter()
                .chain(self.group_instance_id.as_deref())
                .chain(self.protocol_type.as_deref())
                .chain(self.protocol_name.as_deref())
                .chain(
                    self.assignments
                        .iter()
                        .map(|assignment| assignment.member_id.as_str()),
                ),
        )
    }
}

impl RecordStrings for HeartbeatRequest {
    fn longest_record_string(&self) -> usize {
        longest(
            [self.group_id.as_str(), self.member_id.as_str()]
                .into_iter()
                .chain(self.group_instance_id.as_deref()),
        )
    }
}

impl RecordStrings for LeaveGroupRequest {
    fn longest_record_string(&self) -> usize {
        longest(
            [self.group_id.as_str(), self.member_id.as_str()]
                .into_iter()
                .chain(self.members.iter().flat_map(|member| {
                    std::iter::once(member.member_id.as_str())
                        .chain(member.group_instance_id.as_deref())
                        .chain(member.reason.as_deref())
                })),
        )
    }
}

impl RecordStrings for OffsetCommitRequest {
    fn longest_record_string(&self) -> usize {
        longest(
            [self.group_id.as_str(), self.member_id.as_str()]
                .into_iter()
                .chain(self.group_instance_id.as_deref())
                .chain(self.topics.iter().flat_map(|topic| {
                    std::iter::once(topic.name.as_str()).chain(
                        topic
                            .partitions
                            .iter()
                            .filter_map(|partition| partition.committed_metadata.as_deref()),
                    )
                })),
        )
    }
}

impl RecordStrings for TxnOffsetCommitRequest {
    fn longest_record_string(&self) -> usize {
        longest(
            [
                self.transactional_id.as_str(),
                self.group_id.as_str(),
                self.member_id.as_str(),
            ]
            .into_iter()
            .chain(self.group_instance_id.as_deref())
            .chain(self.topics.iter().flat_map(|topic| {
                std::iter::once(topic.name.as_str()).chain(
                    topic
                        .partitions
                        .iter()
                        .filter_map(|partition| partition.committed_metadata.as_deref()),
                )
            })),
        )
    }
}

impl RecordStrings for ConsumerGroupHeartbeatRequest {
    fn longest_record_string(&self) -> usize {
        longest(
            [self.group_id.as_str(), self.member_id.as_str()]
                .into_iter()
                .chain(self.instance_id.as_deref())
                .chain(self.rack_id.as_deref())
                .chain(self.subscribed_topic_regex.as_deref())
                .chain(self.server_assignor.as_deref())
                .chain(
                    self.subscribed_topic_names
                        .iter()
                        .flatten()
                        .map(String::as_str),
                ),
        )
    }
}

impl RecordStrings for ShareGroupHeartbeatRequest {
    fn longest_record_string(&self) -> usize {
        longest(
            [self.group_id.as_str(), self.member_id.as_str()]
                .into_iter()
                .chain(self.rack_id.as_deref())
                .chain(
                    self.subscribed_topic_names
                        .iter()
                        .flatten()
                        .map(String::as_str),
                ),
        )
    }
}

impl RecordStrings for StreamsGroupHeartbeatRequest {
    fn longest_record_string(&self) -> usize {
        longest(
            [self.group_id.as_str(), self.member_id.as_str()]
                .into_iter()
                .chain(self.instance_id.as_deref())
                .chain(self.rack_id.as_deref())
                .chain(self.process_id.as_deref()),
        )
    }
}

impl RecordStrings for DeleteGroupsRequest {
    fn longest_record_string(&self) -> usize {
        longest(self.groups_names.iter().map(String::as_str))
    }
}

impl RecordStrings for AlterShareGroupOffsetsRequest {
    fn longest_record_string(&self) -> usize {
        longest(
            std::iter::once(self.group_id.as_str())
                .chain(self.topics.iter().map(|topic| topic.topic_name.as_str())),
        )
    }
}

impl RecordStrings for DeleteShareGroupOffsetsRequest {
    fn longest_record_string(&self) -> usize {
        longest(
            std::iter::once(self.group_id.as_str())
                .chain(self.topics.iter().map(|topic| topic.topic_name.as_str())),
        )
    }
}

#[cfg(test)]
mod handler_tests;
#[cfg(test)]
mod tests;
