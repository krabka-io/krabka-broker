//! Bounded polls that re-drive a request until the broker's authorization
//! decision changes. A seeded ACL record commits through raft and is then
//! applied into the metadata image the request path reads, so a test that
//! asserts on the decision immediately after the seed would race that gap;
//! these helpers absorb it and return the first response that carries the
//! decision. A poll that never sees it panics at its deadline.

use {
    crate::{
        ERR_GROUP_AUTHORIZATION_FAILED, ERR_TOPIC_AUTHORIZATION_FAILED,
        client_api::{
            drive_join_group_as_plain, drive_metadata_as_plain, drive_produce_as_plain,
            join_group_request, single_record_produce_request,
        },
        framing::CLIENT_ID,
        kafka_wire::PlainAuthenticationSetup,
        support::{
            discovery::topic_metadata_request, poll::retry_response, topics::metadata_topic,
        },
    },
    krabka_protocol::{
        owned::{
            join_group_response::JoinGroupResponse, metadata_response::MetadataResponse,
            produce_response::ProduceResponse,
        },
        primitives::uuid::Uuid,
    },
    std::{io, net::SocketAddr},
};

/// Retry `drive_produce_as_plain` against `topic`/partition-0 until the
/// per-partition `error_code` is no longer `TOPIC_AUTHORIZATION_FAILED`,
/// that is, until the ACL submit reaches the metadata image. It panics if a
/// 10 s deadline elapses first. The happy-path Produce test uses this to
/// absorb the raft commit-then-apply gap.
pub async fn retry_produce_until_allowed(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    topic: &str,
) -> Result<ProduceResponse, io::Error> {
    retry_response()
        .request(|| async {
            drive_produce_as_plain(
                addr,
                single_record_produce_request(topic, 0, b"hello"),
                &PlainAuthenticationSetup::builder()
                    .client_id(CLIENT_ID)
                    .user(user)
                    .password(password)
                    .build(),
            )
            .await
        })
        .ready(|response| {
            response
                .responses
                .first()
                .and_then(|topic| topic.partition_responses.first())
                .is_some_and(|partition| partition.error_code != ERR_TOPIC_AUTHORIZATION_FAILED)
        })
        .call()
        .await
}

/// Whether `response` has a row for `topic` that carries `error_code`.
///
/// A row's presence alone is not the decision: a named request returns a
/// denied topic by name with `TOPIC_AUTHORIZATION_FAILED`.
pub fn topic_row_reports(response: &MetadataResponse, topic: &str, error_code: i16) -> bool {
    response
        .topics
        .iter()
        .any(|entry| entry.name.as_deref() == Some(topic) && entry.error_code == error_code)
}

/// Retry `drive_metadata_as_plain` until the response has a row for `topic`
/// with `error_code`, that is, until the ACL decision the caller expects is
/// applied. It panics if a 10 s deadline elapses first. This helper forwards
/// `req_topics` unchanged to the inner `MetadataRequest::topics`, so callers
/// can poll either the fetch-all path or the named-topic path.
pub async fn retry_metadata_until_topic_row(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    topic: &str,
    req_topics: Option<Vec<String>>,
    error_code: i16,
) -> Result<MetadataResponse, io::Error> {
    let req = topic_metadata_request(req_topics.as_ref().map(|names| {
        names
            .iter()
            .map(|n| metadata_topic(Some(n.clone()), Uuid::default()))
            .collect()
    }));
    retry_response()
        .request(|| async {
            drive_metadata_as_plain(
                addr,
                req.clone(),
                &PlainAuthenticationSetup::builder()
                    .client_id(CLIENT_ID)
                    .user(user)
                    .password(password)
                    .build(),
            )
            .await
        })
        .ready(|response| topic_row_reports(response, topic, error_code))
        .call()
        .await
}

/// Ask for one named topic as Alice until its row carries `error_code`.
pub async fn named_metadata_as_alice(
    addr: SocketAddr,
    topic: &str,
    error_code: i16,
) -> MetadataResponse {
    retry_metadata_until_topic_row(
        addr,
        "alice",
        b"wonderland",
        topic,
        Some(vec![topic.to_owned()]),
        error_code,
    )
    .await
    .expect("Metadata must round-trip")
}

/// Retry `drive_join_group_as_plain` against `group_id` with an empty
/// `member_id` until the response is no longer `GROUP_AUTHORIZATION_FAILED`,
/// that is, until the Allow Read ACL is applied. It panics if a 10 s deadline
/// elapses first. The next code in the success ladder is
/// `MEMBER_ID_REQUIRED (79)`. The caller then sends the generated
/// `member_id` to complete the join.
pub async fn retry_join_group_until_allowed(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    group_id: &str,
) -> Result<JoinGroupResponse, io::Error> {
    retry_response()
        .request(|| async {
            drive_join_group_as_plain(
                addr,
                join_group_request(group_id),
                &PlainAuthenticationSetup::builder()
                    .client_id(CLIENT_ID)
                    .user(user)
                    .password(password)
                    .build(),
            )
            .await
        })
        .ready(|response| response.error_code != ERR_GROUP_AUTHORIZATION_FAILED)
        .call()
        .await
}
