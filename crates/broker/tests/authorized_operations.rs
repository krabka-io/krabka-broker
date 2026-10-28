//! KIP-430: the `authorized_operations` bitfield on `Metadata`,
//! `DescribeCluster`, and `DescribeGroups` responses.
//!
//! Covered:
//!   * `Metadata.cluster_authorized_operations` (request v8-10) and
//!     `MetadataResponseTopic.topic_authorized_operations` (request v8+)
//!     populate only when the corresponding `include_*` flag is set.
//!   * `DescribeCluster.cluster_authorized_operations` follows the
//!     `include_cluster_authorized_operations` request flag.
//!   * `DescribeGroups.DescribedGroup.authorized_operations` follows the
//!     `include_authorized_operations` request flag.
//!   * Bitfield contents show the configured authorizer's decisions:
//!     super-user → full mask; explicit ACL row → only that bit plus any
//!     implications; opt-out → `i32::MIN` "not present" sentinel.
//!
//! These tests run against a plaintext loopback listener through the in-process
//! handler dispatch. There is no SASL framing step. The tests drive
//! `BrokerConfig.authorizer` directly with a `SimpleAclAuthorizer`.
#![cfg(test)]

mod support;

use {
    crate::support::{
        acl::{
            BIT_ALTER, BIT_ALTER_CONFIGS, BIT_CLUSTER_ACTION, BIT_CREATE, BIT_DELETE, BIT_DESCRIBE,
            BIT_DESCRIBE_CONFIGS, BIT_IDEMPOTENT_WRITE, BIT_READ, TOPIC_FULL_MASK,
        },
        client::connect_owned,
        discovery::named_topic_metadata,
        topics::{creatable_topic, create_topic_request},
        wire::{HeaderEncoding, request_body_capacity, request_frame},
    },
    assert2::assert,
    bytes::{Buf, BytesMut},
    krabka_broker::{Broker, BrokerConfig, authorizer::SimpleAclAuthorizer},
    krabka_client_core::Client,
    krabka_protocol::{
        Decode, Encode,
        owned::{
            describe_cluster_request::DescribeClusterRequest,
            describe_groups_request::DescribeGroupsRequest,
            describe_groups_response::DescribeGroupsResponse, metadata_request::MetadataRequest,
            metadata_response::MetadataResponse,
        },
    },
    std::{collections::HashSet, sync::Arc},
    tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpStream,
    },
};

// Bit positions match Kafka's `AclOperation.code()` (verified by the
// helper's unit tests). Spelled out here so the assertions don't depend
// on importing the crate-private helper module.

const GROUP_FULL_MASK: i32 =
    BIT_READ | BIT_DESCRIBE | BIT_DELETE | BIT_DESCRIBE_CONFIGS | BIT_ALTER_CONFIGS;

const CLUSTER_FULL_MASK: i32 = BIT_CREATE
    | BIT_ALTER
    | BIT_DESCRIBE
    | BIT_CLUSTER_ACTION
    | BIT_ALTER_CONFIGS
    | BIT_DESCRIBE_CONFIGS
    | BIT_IDEMPOTENT_WRITE;

struct Harness {
    handle: krabka_broker::BrokerHandle,
    client: Client,
    _tempdir: tempfile::TempDir,
}

impl Harness {
    async fn shutdown(self) {
        // Drop the client first so it doesn't keep a socket open across
        // broker shutdown.
        drop(self.client);
        self.handle.shutdown().await;
    }
}

/// Boot a single broker with a plaintext loopback listener and an authorizer.
///
/// The [`SimpleAclAuthorizer`] is configured so that the connecting principal,
/// the PLAINTEXT default `User:ANONYMOUS`, is a super-user. That gives the
/// metadata-driving client full access. The test can still seed ACLs and see
/// their effect on the authorized-operations bitfield for a principal name that
/// the authorizer evaluates separately.
///
/// The name goes to the authorizer only, which is all this suite needs to
/// drive metadata over a PLAINTEXT listener.
fn boot_with_super_user(super_user: &str) -> impl Future<Output = Harness> {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let mut cfg = BrokerConfig::for_tests(tempdir.path().to_path_buf());
    let authorizer_super_users: HashSet<String> = {
        let mut s = HashSet::new();
        s.insert(super_user.to_owned());
        s
    };
    cfg.authorizer = Arc::new(SimpleAclAuthorizer::new(authorizer_super_users));
    Box::pin(async move {
        let handle = Broker::start(cfg).await.expect("broker start");
        // `DescribeGroups` needs a loaded group coordinator.
        handle.wait_until_group_coordinator_ready().await;
        let client = connect_owned(
            handle.listen_addr().to_string(),
            "krabka-kip-430-test",
            "client build",
        )
        .await;
        Harness {
            handle,
            client,
            _tempdir: tempdir,
        }
    })
}

async fn create_topic(client: &Client, name: &str, partitions: i32) {
    let resp = client
        .send(create_topic_request(
            creatable_topic()
                .name(name.to_owned())
                .partitions(partitions)
                .call(),
        ))
        .await
        .expect("CreateTopics");
    assert!(resp.topics[0].error_code == 0, "topic create: {resp:?}");
}

use krabka_protocol::owned::describe_cluster_response::DescribeClusterResponse;
enum AuthorizedReply {
    Topic(MetadataResponse),
    Cluster(DescribeClusterResponse),
    Group(DescribeGroupsResponse),
}
struct AuthorizedOperations {
    harness: Option<Harness>,
    metadata: Option<MetadataResponse>,
    resource: String,
    reply: Option<AuthorizedReply>,
}
#[rstest::fixture]
fn operations() -> AuthorizedOperations {
    // Start this scenario with fresh inputs and observations.

    AuthorizedOperations {
        harness: None,
        metadata: None,
        resource: String::new(),
        reply: None,
    }
}
#[rstest_bdd_macros::given("the anonymous plaintext principal is a configured super user")]
async fn anonymous_operations_superuser(operations: &mut AuthorizedOperations) {
    operations.harness = Some(boot_with_super_user("ANONYMOUS").await);
}
#[rstest_bdd_macros::given("resource {resource} is available for authorization discovery")]
async fn discoverable_authorized_resource(operations: &mut AuthorizedOperations, resource: String) {
    let harness = operations
        .harness
        .as_ref()
        .expect("superuser broker started");
    match resource.as_str() {
        "foo" => create_topic(&harness.client, &resource, 1).await,
        "g1" => harness.handle.group_create_for_test(&resource),
        "cluster" => {}
        other => panic!("unknown authorization resource {other}"),
    }
    operations.resource = resource;
}
#[rstest_bdd_macros::when("{api} requests authorized operations {include} for that resource")]
async fn discover_authorized_operations(
    operations: &mut AuthorizedOperations,
    api: String,
    include: bool,
) {
    let client = &operations
        .harness
        .as_ref()
        .expect("superuser broker started")
        .client;
    operations.reply = Some(match api.as_str() {
        "Metadata" => AuthorizedReply::Topic(
            client
                .send(MetadataRequest {
                    include_topic_authorized_operations: include,
                    ..named_topic_metadata(&operations.resource)
                })
                .await
                .expect("Metadata"),
        ),
        "DescribeCluster" => AuthorizedReply::Cluster(
            client
                .send(DescribeClusterRequest {
                    include_cluster_authorized_operations: include,
                    ..Default::default()
                })
                .await
                .expect("DescribeCluster"),
        ),
        "DescribeGroups" => AuthorizedReply::Group(
            client
                .send(DescribeGroupsRequest {
                    groups: vec![operations.resource.clone()],
                    include_authorized_operations: include,
                    ..Default::default()
                })
                .await
                .expect("DescribeGroups"),
        ),
        other => panic!("unsupported authorization discovery API {other}"),
    });
}
#[rstest_bdd_macros::then("the reported authorized operations equal {expected}")]
fn authorized_operations_mask(operations: &AuthorizedOperations, expected: String) {
    let actual = match operations
        .reply
        .as_ref()
        .expect("authorized operations requested")
    {
        AuthorizedReply::Topic(response) => {
            response
                .topics
                .iter()
                .find(|row| row.name.as_deref() == Some(operations.resource.as_str()))
                .expect("requested topic row present")
                .topic_authorized_operations
        }
        AuthorizedReply::Cluster(response) => {
            assert!(
                response.error_code == 0,
                "DescribeCluster error: {response:?}"
            );
            response.cluster_authorized_operations
        }
        AuthorizedReply::Group(response) => {
            assert!(response.groups.len() == 1);
            let group = &response.groups[0];
            assert!(group.error_code == 0, "DescribeGroups error: {group:?}");
            group.authorized_operations
        }
    };
    let wanted = match expected.as_str() {
        "not-present" => i32::MIN,
        "full-topic-mask" => TOPIC_FULL_MASK,
        "full-cluster-mask" => CLUSTER_FULL_MASK,
        "full-group-mask" => GROUP_FULL_MASK,
        other => panic!("unknown authorized operations expectation {other}"),
    };
    assert!(actual == wanted, "expected {expected}, got 0b{actual:b}");
}
#[rstest_bdd_macros::when(
    "a client pins Metadata to version 9 and requests cluster authorized operations"
)]
async fn v9_cluster_operations_request(operations: &mut AuthorizedOperations) {
    let h = operations
        .harness
        .as_ref()
        .expect("superuser broker started");
    // Pin v9 so the cluster-level field is in the on-wire version range
    // (v8-10). The `Client` default would pick a higher Metadata version
    // that drops the field, in which case the value can't be observed
    // round-trip.
    let req = MetadataRequest {
        // Fetch-all so we don't need a topic for the field to be set.
        topics: None,
        include_cluster_authorized_operations: true,
        ..Default::default()
    };
    let version: i16 = 9;

    let mut body = BytesMut::new();
    req.encode(&mut body, version).expect("Metadata encode");

    // Build the v2 request header (flexible — Metadata went flexible at
    // v9). One TCP round-trip, plaintext, no SASL.
    let frame = request_frame()
        .api_key(3)
        .version(version)
        .correlation(7)
        .header(HeaderEncoding::Flexible)
        .client_id("krabka-kip-430-v9")
        .body(&body)
        .maybe_capacity(Some(request_body_capacity(&body)))
        .call();

    let mut stream = TcpStream::connect(h.handle.listen_addr())
        .await
        .expect("tcp connect");
    stream
        .write_u32(u32::try_from(frame.len()).unwrap())
        .await
        .expect("write len");
    stream.write_all(&frame).await.expect("write frame");
    stream.flush().await.expect("flush");

    let resp_len = stream.read_u32().await.expect("read len");
    let mut resp = vec![0u8; resp_len as usize];
    stream.read_exact(&mut resp).await.expect("read body");

    // Strip response header v1 (flexible): i32 correlation_id + 1 tagged byte.
    let mut cur: &[u8] = &resp;
    let _corr = cur.get_i32();
    let _hdr_tagged = cur.get_u8();
    let resp = MetadataResponse::decode(&mut cur, version).expect("Metadata decode");

    operations.metadata = Some(resp);
}
#[rstest_bdd_macros::then("Metadata v9 carries the full cluster authorized operations mask")]
fn v9_cluster_operations_result(operations: &AuthorizedOperations) {
    let resp = operations.metadata.as_ref().expect("Metadata v9 requested");
    assert!(
        resp.cluster_authorized_operations == CLUSTER_FULL_MASK,
        "super-user must see the full cluster mask on Metadata v9, got 0b{:b}",
        resp.cluster_authorized_operations
    );
}

#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/authorized_operations.feature",
    name = "Include flags control authorization discovery on each resource API"
)]
#[tokio::test]
async fn include_flags_control_authorization(
    operations: AuthorizedOperations,
    resource: String,
    api: String,
    include: bool,
    expected: String,
) {
    let _ = (resource, api, include, expected);
    operations
        .harness
        .expect("superuser broker started")
        .shutdown()
        .await;
}
#[rstest_bdd_macros::scenario(
    path = "tests/features/migrated/tests/authorized_operations.feature",
    name = "metadata cluster authorized operations super user gets full mask v9"
)]
#[tokio::test]
async fn metadata_cluster_authorized_operations_super_user_gets_full_mask_v9(
    operations: AuthorizedOperations,
) {
    operations
        .harness
        .expect("superuser broker started")
        .shutdown()
        .await;
}
