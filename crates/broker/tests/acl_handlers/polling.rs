//! Bounded polls that re-drive a request until the broker's authorization
//! decision changes. A seeded ACL record commits through raft and is then
//! applied into the metadata image the request path reads, so a test that
//! asserts on the decision immediately after the seed would race that gap;
//! these helpers absorb it and return the final response.

use std::{io, net::SocketAddr};

use krabka_protocol::owned::{
    join_group_response::JoinGroupResponse, metadata_response::MetadataResponse,
    produce_response::ProduceResponse,
};

use crate::{
    ERR_GROUP_AUTHORIZATION_FAILED, ERR_TOPIC_AUTHORIZATION_FAILED,
    client_api::{
        drive_join_group_as_plain, drive_metadata_as_plain, drive_produce_as_plain,
        join_group_request, single_record_produce_request,
    },
    support::{discovery::topic_metadata_request, topics::metadata_topic},
};

/// Retry `drive_produce_as_plain` against `topic`/partition-0 until the
/// per-partition `error_code` is no longer `TOPIC_AUTHORIZATION_FAILED`,
/// that is, until the ACL submit reaches the metadata image, or until a
/// 10 s deadline elapses. The happy-path Produce test uses this to absorb
/// the raft commit-then-apply gap. It returns the final response.
pub async fn retry_produce_until_allowed(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    topic: &str,
) -> Result<ProduceResponse, io::Error> {
    crate::support::poll::retry_response(
        std::time::Duration::from_secs(10),
        std::time::Duration::from_millis(50),
        || {
            drive_produce_as_plain(
                addr,
                user,
                password,
                single_record_produce_request(topic, 0, b"hello"),
            )
        },
        |response| {
            response
                .responses
                .first()
                .and_then(|topic| topic.partition_responses.first())
                .is_some_and(|partition| partition.error_code != ERR_TOPIC_AUTHORIZATION_FAILED)
        },
    )
    .await
}

/// Retry `drive_metadata_as_plain` until `topic` appears in the
/// response, that is, until the Allow Describe ACL is applied, or until a
/// 10 s deadline elapses. This helper forwards `req_topics` unchanged to the
/// inner `MetadataRequest::topics`, so callers can poll either the fetch-all
/// path or the named-topic path.
pub async fn retry_metadata_until_topic_visible(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    topic: &str,
    req_topics: Option<Vec<String>>,
) -> Result<MetadataResponse, io::Error> {
    let req = topic_metadata_request(req_topics.as_ref().map(|names| {
        names
            .iter()
            .map(|n| {
                metadata_topic(
                    Some(n.clone()),
                    krabka_protocol::primitives::uuid::Uuid::default(),
                )
            })
            .collect()
    }));
    crate::support::poll::retry_response(
        std::time::Duration::from_secs(10),
        std::time::Duration::from_millis(50),
        || drive_metadata_as_plain(addr, user, password, req.clone()),
        |response| {
            response
                .topics
                .iter()
                .any(|entry| entry.name.as_deref() == Some(topic))
        },
    )
    .await
}

/// Retry `drive_join_group_as_plain` against `group_id` with an empty
/// `member_id` until the response is no longer `GROUP_AUTHORIZATION_FAILED`,
/// that is, until the Allow Read ACL is applied, or until a 10 s deadline
/// elapses. The next code in the success ladder is
/// `MEMBER_ID_REQUIRED (79)`. The caller then sends the generated
/// `member_id` to complete the join.
pub async fn retry_join_group_until_allowed(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    group_id: &str,
) -> Result<JoinGroupResponse, io::Error> {
    crate::support::poll::retry_response(
        std::time::Duration::from_secs(10),
        std::time::Duration::from_millis(50),
        || drive_join_group_as_plain(addr, user, password, join_group_request(group_id)),
        |response| response.error_code != ERR_GROUP_AUTHORIZATION_FAILED,
    )
    .await
}
