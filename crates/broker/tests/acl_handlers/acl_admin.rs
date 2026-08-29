//! Request builders and drivers for the admin APIs a super-user drives in
//! this suite: `CreateTopics`, which materialises the partitions the
//! enforcement tests then read and write, and the `CreateAcls` /
//! `DescribeAcls` / `DeleteAcls` trio.
//!
//! Same shape as `drive_alter_user_scram_credentials_as_plain`: one
//! `ApiVersions` warm-up, one `SaslHandshake`, one `SaslAuthenticate`, then
//! the typed request. Each helper authenticates fresh on a new TCP stream
//! because that is the simplest model for "a client doing one admin
//! action"; reuse is unnecessary for these tests.

use std::{io, net::SocketAddr};

use assert2::assert;
use bytes::BytesMut;
use krabka_protocol::{
    Decode, Encode,
    owned::{
        create_acls_request::{AclCreation, CreateAclsRequest},
        create_acls_response::CreateAclsResponse,
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        create_topics_response::CreateTopicsResponse,
        delete_acls_request::DeleteAclsRequest,
        delete_acls_response::DeleteAclsResponse,
        describe_acls_request::DescribeAclsRequest,
        describe_acls_response::DescribeAclsResponse,
    },
};

use crate::{
    CREATE_ACLS_VERSION, CREATE_TOPICS_VERSION, DELETE_ACLS_VERSION, DESCRIBE_ACLS_VERSION,
    OPERATION_ANY, PATTERN_TYPE_ANY, PATTERN_TYPE_LITERAL, PERMISSION_ALLOW, PERMISSION_ANY,
    RESOURCE_TYPE_TOPIC,
    framing::{round_trip, sasl_plain_authenticate},
};

/// Shorthand for `Allow <op> on Topic LITERAL <name> for <principal> from *`.
/// Every test in this file uses literal Topic ACLs with host `*`, so the only
/// dimensions that vary per binding are `resource_name`, `principal`, and
/// `operation`. This helper wraps them and keeps the test bodies short.
pub fn topic_allow_creation(name: &str, principal: &str, operation: i8) -> AclCreation {
    AclCreation {
        resource_type: RESOURCE_TYPE_TOPIC,
        resource_name: name.to_string(),
        resource_pattern_type: PATTERN_TYPE_LITERAL,
        principal: principal.to_string(),
        host: "*".to_string(),
        operation,
        permission_type: PERMISSION_ALLOW,
        ..Default::default()
    }
}

/// Permissive `DescribeAclsRequest` for `Topic`. Every other axis is wildcard.
pub fn describe_all_topic_acls() -> DescribeAclsRequest {
    DescribeAclsRequest {
        resource_type_filter: RESOURCE_TYPE_TOPIC,
        resource_name_filter: None,
        pattern_type_filter: PATTERN_TYPE_ANY,
        principal_filter: None,
        host_filter: None,
        operation: OPERATION_ANY,
        permission_type: PERMISSION_ANY,
        ..Default::default()
    }
}

/// Drive a single `CreateTopics` against `addr` authenticated as
/// `admin` / `admin-secret`. Asserts the response has `error_code=0`
/// for the requested topic. The T23 tests use it to materialise a
/// partition before they produce to it or fetch from it.
pub async fn create_topic_as_admin(addr: SocketAddr, name: &str, partitions: i32) {
    let req = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: name.to_string(),
            num_partitions: partitions,
            replication_factor: 1,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let resp = drive_create_topics_as_plain(addr, "admin", b"admin-secret", req)
        .await
        .expect("CreateTopics as super-user must round-trip");
    assert!(resp.topics.len() == 1, "one topic in response");
    assert!(
        resp.topics[0].error_code == 0,
        "CreateTopics({name}) must succeed: {:?}",
        resp.topics[0].error_message
    );
}

async fn drive_create_topics_as_plain(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    req: CreateTopicsRequest,
) -> Result<CreateTopicsResponse, io::Error> {
    let mut stream = sasl_plain_authenticate(addr, user, password).await?;
    let mut body = BytesMut::new();
    req.encode(&mut body, CREATE_TOPICS_VERSION)
        .map_err(|e| io::Error::other(format!("CreateTopics encode: {e}")))?;
    let resp_bytes = round_trip(&mut stream, 19, CREATE_TOPICS_VERSION, 4, true, &body).await?;
    let mut cur: &[u8] = &resp_bytes;
    CreateTopicsResponse::decode(&mut cur, CREATE_TOPICS_VERSION)
        .map_err(|e| io::Error::other(format!("CreateTopics decode: {e}")))
}

pub async fn drive_create_acls_as_plain(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    req: CreateAclsRequest,
) -> Result<CreateAclsResponse, io::Error> {
    let mut stream = sasl_plain_authenticate(addr, user, password).await?;
    let mut body = BytesMut::new();
    req.encode(&mut body, CREATE_ACLS_VERSION)
        .map_err(|e| io::Error::other(format!("CreateAcls encode: {e}")))?;
    let resp_bytes = round_trip(&mut stream, 30, CREATE_ACLS_VERSION, 4, true, &body).await?;
    let mut cur: &[u8] = &resp_bytes;
    CreateAclsResponse::decode(&mut cur, CREATE_ACLS_VERSION)
        .map_err(|e| io::Error::other(format!("CreateAcls decode: {e}")))
}

pub async fn drive_describe_acls_as_plain(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    req: DescribeAclsRequest,
) -> Result<DescribeAclsResponse, io::Error> {
    let mut stream = sasl_plain_authenticate(addr, user, password).await?;
    let mut body = BytesMut::new();
    req.encode(&mut body, DESCRIBE_ACLS_VERSION)
        .map_err(|e| io::Error::other(format!("DescribeAcls encode: {e}")))?;
    let resp_bytes = round_trip(&mut stream, 29, DESCRIBE_ACLS_VERSION, 4, true, &body).await?;
    let mut cur: &[u8] = &resp_bytes;
    DescribeAclsResponse::decode(&mut cur, DESCRIBE_ACLS_VERSION)
        .map_err(|e| io::Error::other(format!("DescribeAcls decode: {e}")))
}

pub async fn drive_delete_acls_as_plain(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    req: DeleteAclsRequest,
) -> Result<DeleteAclsResponse, io::Error> {
    let mut stream = sasl_plain_authenticate(addr, user, password).await?;
    let mut body = BytesMut::new();
    req.encode(&mut body, DELETE_ACLS_VERSION)
        .map_err(|e| io::Error::other(format!("DeleteAcls encode: {e}")))?;
    let resp_bytes = round_trip(&mut stream, 31, DELETE_ACLS_VERSION, 4, true, &body).await?;
    let mut cur: &[u8] = &resp_bytes;
    DeleteAclsResponse::decode(&mut cur, DELETE_ACLS_VERSION)
        .map_err(|e| io::Error::other(format!("DeleteAcls decode: {e}")))
}
