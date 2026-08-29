//! Bounded polls that re-drive a request until the broker's authorization
//! decision changes. A seeded ACL record commits through raft and is then
//! applied into the metadata image the request path reads, so a test that
//! asserts on the decision immediately after the seed would race that gap;
//! these helpers absorb it and return the final response.

use std::{io, net::SocketAddr};

use krabka_protocol::owned::{
    join_group_response::JoinGroupResponse,
    metadata_request::{MetadataRequest, MetadataRequestTopic},
    metadata_response::MetadataResponse,
    produce_response::ProduceResponse,
};

use crate::{
    ERR_GROUP_AUTHORIZATION_FAILED, ERR_TOPIC_AUTHORIZATION_FAILED,
    client_api::{
        drive_join_group_as_plain, drive_metadata_as_plain, drive_produce_as_plain,
        join_group_request, single_record_produce_request,
    },
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
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let resp = drive_produce_as_plain(
            addr,
            user,
            password,
            single_record_produce_request(topic, 0, b"hello"),
        )
        .await?;
        let part = resp
            .responses
            .first()
            .and_then(|t| t.partition_responses.first());
        if part.is_some_and(|p| p.error_code != ERR_TOPIC_AUTHORIZATION_FAILED) {
            return Ok(resp);
        }
        if std::time::Instant::now() > deadline {
            return Ok(resp);
        }
        // intentional: bounded RPC-response poll. The ground truth is the
        // broker's authorization decision (and partition-writer readiness for
        // acks=-1), observed by re-driving Produce — not an image/metric an
        // awaiter exposes. wait_for_image watches the controller's committed
        // image, which can lead the request path's applied copy, so an image
        // wait would race; re-driving absorbs the commit-then-apply gap.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
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
    let req = MetadataRequest {
        topics: req_topics.as_ref().map(|names| {
            names
                .iter()
                .map(|n| MetadataRequestTopic {
                    name: Some(n.clone()),
                    ..Default::default()
                })
                .collect()
        }),
        ..Default::default()
    };
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let resp = drive_metadata_as_plain(addr, user, password, req.clone()).await?;
        let visible = resp.topics.iter().any(|t| t.name.as_deref() == Some(topic));
        if visible {
            return Ok(resp);
        }
        if std::time::Instant::now() > deadline {
            return Ok(resp);
        }
        // intentional: bounded RPC-response poll. The awaited signal is the
        // broker's authorization decision (topic visible to alice), observed by
        // re-driving Metadata — not an image/metric an awaiter exposes.
        // wait_for_image watches the controller's committed image, which can
        // lead the request path's applied copy, so an image wait would race;
        // re-driving absorbs the raft commit-then-apply gap.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
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
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    loop {
        let resp =
            drive_join_group_as_plain(addr, user, password, join_group_request(group_id)).await?;
        if resp.error_code != ERR_GROUP_AUTHORIZATION_FAILED {
            return Ok(resp);
        }
        if std::time::Instant::now() > deadline {
            return Ok(resp);
        }
        // intentional: bounded RPC-response poll. The awaited signal is the
        // broker's authorization decision (JoinGroup no longer denied),
        // observed by re-driving JoinGroup — not an image/metric an awaiter
        // exposes. wait_for_image watches the controller's committed image,
        // which can lead the request path's applied copy, so an image wait
        // would race; re-driving absorbs the raft commit-then-apply gap.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}
