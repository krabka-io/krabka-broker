//! Shared in-crate scaffolding for the per-handler `#[cfg(test)] mod tests`
//! modules.
//!
//! The mutant-hardening pass (#713) copied the same helper set into ~40
//! handler test modules: a deny-everything authorizer, principal, peer, and
//! request-context builders, wire codec helpers, and a temp-dir broker
//! launcher. This module holds one copy of each. Handlers keep only thin,
//! behaviour-specific facades over them. Their own principal name, client id,
//! `BrokerConfig` tweaks, and negotiated wire version live at the call site, so
//! this module centralises no behaviour that a handler needs to vary.
//!
//! It also holds [`FakeMetadataSource`], the one metadata authority every
//! suite in this crate fakes with.
//!
//! [`BrokenTimer`] is the one fixture here that is not handler scaffolding.
//! Every broker cadence loop takes its ticker through an injectable field, and
//! every one of them stops when that ticker gives out, so the fake that makes a
//! ticker give out is shared rather than copied into each of their test
//! modules.
//!
//! `LogCapture` records what one piece of work logs, for the tests that check
//! the level of a log line.

use std::{
    collections::BTreeSet,
    io,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{self, AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::{Bytes, BytesMut};
use krabka_metadata::{MetadataImage, MetadataRecord};
use krabka_protocol::{Decode, Encode};
use krabka_raft::{
    AddVoter, Node, NodeId, QuorumState, RaftError, ReconfigOutcome, RemoveVoter, SnapshotRange,
    SubmitChangeResult, UpdateVoter,
};
use krabka_security::{AuthMethod, Principal};
use qubit_clock::{
    MonotonicClock, MonotonicInstant, StdMonotonicClock, TimeError, Timer, TimerFuture,
    TimerUnavailableError,
};
use tokio::sync::watch;
use tracing_subscriber::{
    Layer,
    layer::{Context, SubscriberExt as _},
    registry::LookupSpan,
};

use crate::{
    broker::{Broker, BrokerHandle},
    config::BrokerConfig,
    handlers::RequestContext,
    metadata_source::MetadataSource,
};

/// Authorizer that denies every request. It drives the authorization-failure
/// path in every handler that consults the cluster authorizer.
#[derive(Debug)]
pub(crate) struct DenyAll;

impl crate::authorizer::Authorizer for DenyAll {
    fn authorize(
        &self,
        _source: &dyn krabka_authz::AclSource,
        _req: &crate::authorizer::AuthorizationRequest<'_>,
    ) -> crate::authorizer::AuthorizationResult {
        crate::authorizer::AuthorizationResult::Deny
    }
}

/// An authorizer that lets the broker's own controller requests through and
/// asks the wrapped one about everything else.
///
/// A test broker talks to its own controller over a plaintext controller
/// listener, as the `ANONYMOUS` principal. A new registration stays fenced
/// until a heartbeat unfences it, and the heartbeat needs `ClusterAction` on
/// the `Cluster`. The auto topic creation of a coordinator topic sends
/// `CreateTopics` with no client principal, and the controller checks
/// `Create` on the `Cluster` for it. A Kafka operator grants the inter-broker
/// principal `ClusterAction` and `Create`, or makes it a super user, for the
/// same reasons. Tests that exercise a restrictive authorizer on client
/// requests wrap it in this, so the broker still unfences and still creates
/// its coordinator topics.
#[derive(Debug)]
pub(crate) struct ControllerPeerAllowed<A>(pub(crate) A);

impl<A: crate::authorizer::Authorizer> crate::authorizer::Authorizer for ControllerPeerAllowed<A> {
    fn authorize(
        &self,
        source: &dyn crate::authorizer::AclSource,
        request: &crate::authorizer::AuthorizationRequest<'_>,
    ) -> crate::authorizer::AuthorizationResult {
        if request.principal.name == "ANONYMOUS"
            && request.resource_type == krabka_metadata::ResourceType::Cluster
            && matches!(
                request.operation,
                krabka_metadata::AclOperation::ClusterAction
                    | krabka_metadata::AclOperation::Create
            )
        {
            crate::authorizer::AuthorizationResult::Allow
        } else {
            self.0.authorize(source, request)
        }
    }

    fn is_configured(&self) -> bool {
        self.0.is_configured()
    }

    fn decision_ttl(&self) -> Option<std::time::Duration> {
        self.0.decision_ttl()
    }

    fn authorize_by_resource_type(
        &self,
        source: &dyn crate::authorizer::AclSource,
        principal: &krabka_security::Principal,
        host: &std::net::SocketAddr,
        resource_type: krabka_metadata::ResourceType,
        operation: krabka_metadata::AclOperation,
    ) -> crate::authorizer::AuthorizationResult {
        self.0
            .authorize_by_resource_type(source, principal, host, resource_type, operation)
    }
}

/// An authorizer that a fixture holds as a trait object, so that it can go
/// inside [`ControllerPeerAllowed`].
#[derive(Debug)]
struct SharedAuthorizer(std::sync::Arc<dyn crate::authorizer::Authorizer>);

impl crate::authorizer::Authorizer for SharedAuthorizer {
    fn authorize(
        &self,
        source: &dyn crate::authorizer::AclSource,
        request: &crate::authorizer::AuthorizationRequest<'_>,
    ) -> crate::authorizer::AuthorizationResult {
        self.0.authorize(source, request)
    }

    fn is_configured(&self) -> bool {
        self.0.is_configured()
    }

    fn decision_ttl(&self) -> Option<std::time::Duration> {
        self.0.decision_ttl()
    }

    fn authorize_by_resource_type(
        &self,
        source: &dyn crate::authorizer::AclSource,
        principal: &krabka_security::Principal,
        host: &std::net::SocketAddr,
        resource_type: krabka_metadata::ResourceType,
        operation: krabka_metadata::AclOperation,
    ) -> crate::authorizer::AuthorizationResult {
        self.0
            .authorize_by_resource_type(source, principal, host, resource_type, operation)
    }
}

/// Wraps `authorizer` in [`ControllerPeerAllowed`].
///
/// A fixture that waits for a coordinator needs this: the broker places a
/// coordinator topic only on an unfenced broker, as Kafka's
/// `ReplicaPlacer` does.
pub(crate) fn controller_peer_allowed(
    authorizer: std::sync::Arc<dyn crate::authorizer::Authorizer>,
) -> std::sync::Arc<dyn crate::authorizer::Authorizer> {
    std::sync::Arc::new(ControllerPeerAllowed(SharedAuthorizer(authorizer)))
}

/// Finalize `eligible.leader.replicas.version` at 1 in `image`.
///
/// KIP-966 ELR maintenance is gated on the feature, and its release default is
/// 0 at every `metadata.version` krabka supports, so a fixture that wants the
/// controller to publish an ELR has to finalize it the way an operator's
/// `kafka-features upgrade` would. The tests that assert the feature is *off*
/// build their image without calling this.
pub(crate) fn finalize_elr_version(image: &mut MetadataImage) {
    image.apply(&MetadataRecord::V1FeatureLevel(
        krabka_metadata::FeatureLevelRecord {
            name: crate::features::ELR_VERSION.into(),
            level: 1,
        },
    ));
}

/// Finalize `eligible.leader.replicas.version` at 1 on a running broker, and
/// wait until its own image reports the level.
///
/// The image-level [`finalize_elr_version`] cannot serve a test that boots a
/// broker: the level has to arrive as a committed record, the way an
/// operator's `kafka-features upgrade --feature
/// eligible.leader.replicas.version=1` delivers it.
pub(crate) async fn finalize_elr_version_on(broker: &crate::Broker) {
    broker
        .controller
        .submit_change(vec![MetadataRecord::V1FeatureLevel(
            krabka_metadata::FeatureLevelRecord {
                name: crate::features::ELR_VERSION.into(),
                level: 1,
            },
        )])
        .await
        .expect("submit eligible.leader.replicas.version");

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if broker
                .controller
                .current_image()
                .finalized_feature(crate::features::ELR_VERSION)
                == Some(1)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("eligible.leader.replicas.version visible");
}

/// The record that finalizes `metadata.version` 33 (`4.4-IV2`), one of trunk's
/// unstable levels. Kafka 4.3 supports levels up to 30, so a controller with
/// the unstable flag off that replays this record stops over a fatal fault.
pub(crate) fn finalize_unstable_metadata_version() -> MetadataRecord {
    MetadataRecord::V1FeatureLevel(krabka_metadata::FeatureLevelRecord {
        name: "metadata.version".into(),
        level: 33,
    })
}

/// The fault that replaying [`finalize_unstable_metadata_version`] raises.
pub(crate) const UNSUPPORTED_LEVEL_FAULT: &str = "Tried to apply FeatureLevelRecord \
    FeatureLevelRecord(name='metadata.version', featureLevel=33), \
    but this controller only supports versions 7-30";

/// Writes `records` as the length-prefixed frames of `bootstrap.records.bin`,
/// the file `krabka format` leaves for the first start to submit.
pub(crate) fn write_bootstrap_records(log_dir: &std::path::Path, records: &[MetadataRecord]) {
    use serde_wincode::SerdeCompat;
    use wincode::Serialize as _;

    let mut bytes = Vec::new();
    for record in records {
        let frame =
            <SerdeCompat<MetadataRecord>>::serialize(record).expect("serialize a bootstrap record");
        bytes.extend_from_slice(
            &u32::try_from(frame.len())
                .expect("bootstrap frame fits in u32")
                .to_le_bytes(),
        );
        bytes.extend_from_slice(&frame);
    }
    std::fs::write(log_dir.join("bootstrap.records.bin"), bytes).expect("write bootstrap records");
}

/// Initialize the share state of `(group, topic_id, partition)` at state
/// epoch 1 with no start offset, as the group coordinator does when it
/// assigns the partition to a member (Kafka's Initialize-first flow). The
/// share coordinator refuses a read of a key with no state, so a handler test
/// that fetches or acknowledges without a share group heartbeat calls this
/// first.
pub(crate) async fn initialize_share_state(
    broker: &crate::broker::BrokerHandle,
    group: &str,
    topic_id: uuid::Uuid,
    partition: i32,
) {
    broker
        .broker_arc_for_test()
        .group_coordinator
        .share_persister()
        .expect("share persister")
        .initialize(
            group,
            topic_id,
            partition,
            1,
            krabka_log::Offset(crate::share_coordinator::coordinator::UNINITIALIZED_START_OFFSET),
        )
        .await
        .expect("initialize the share state");
}

/// End the heartbeat session of `broker_id` on the controller `broker`, as if
/// that broker stopped and its session expired.
///
/// It first waits for a liveness tick of the current controller term to
/// finish. The first tick of a term seeds every registered broker with a
/// session, so a session ended before that tick would be opened again.
pub(crate) async fn end_heartbeat_session(broker: &crate::Broker, broker_id: u64) {
    let ticks = broker.metrics.controller_fencing_publications_total.get();
    tokio::time::timeout(Duration::from_secs(10), async {
        while broker.metrics.controller_fencing_publications_total.get() <= ticks {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("a liveness tick of this controller term finished");
    broker.liveness.end_session(broker_id).await;
}

/// Build an anonymous-auth [`Principal`] with the given name and no groups.
///
/// The name matters. Authorization decisions and audit records key on this
/// subject, so each handler passes the identity its scenario expects, such as
/// `"alice"`, `"admin"`, or `"ANONYMOUS"`.
pub(crate) fn principal(name: &str) -> Principal {
    Principal {
        name: name.into(),
        auth_method: AuthMethod::Anonymous,
        groups: Vec::new(),
    }
}

/// Build a SASL/PLAIN-authenticated [`Principal`] with the given name and no
/// groups, as a client that logged in over a `SASL_PLAINTEXT` listener is.
pub(crate) fn sasl_principal(name: &str) -> Principal {
    Principal {
        name: name.into(),
        auth_method: AuthMethod::SaslPlain,
        groups: Vec::new(),
    }
}

/// The loopback peer address (`127.0.0.1:9092`) that handler tests attribute
/// requests to.
pub(crate) fn peer() -> SocketAddr {
    "127.0.0.1:9092".parse().unwrap()
}

/// Make this broker the leader of each `__transaction_state` partition in
/// `partitions`, as the controller does when it creates the topic, and wait
/// until the transaction coordinator loads each one.
///
/// The image holds only these partitions of the topic, so this broker leads no
/// other partition of it. The replica reconcile then materializes each
/// partition and installs its leadership, and the coordinator loads it, as in
/// production. A `__transaction_state` write then passes the leadership and
/// ISR checks of its commit rule.
pub(crate) async fn lead_transaction_state_partitions(
    handle: &BrokerHandle,
    partitions: &[krabka_ids::PartitionIndex],
) {
    let broker = handle.broker_arc_for_test();
    let node = broker.config.node_id;
    let topic = crate::txn::bootstrap::TOPIC;
    // The controller translates a partition record against the topic that the
    // image already holds, so the topic goes first.
    broker
        .controller
        .submit_change(vec![MetadataRecord::V1Topic(
            krabka_metadata::TopicRecord {
                name: topic.into(),
                topic_id: uuid::Uuid::from_u128(0x7472_616e_7361_6374_696f_6e73),
                partitions: 0,
                replication_factor: 1,
            },
        )])
        .await
        .expect("create __transaction_state");
    let led: BTreeSet<i32> = partitions.iter().map(|partition| partition.get()).collect();
    broker
        .controller
        .submit_change(
            led.iter()
                .map(|&partition| {
                    MetadataRecord::V1Partition(krabka_metadata::PartitionRecord {
                        topic: topic.into(),
                        partition,
                        leader: node,
                        replicas: vec![node],
                        isr: vec![node],
                        ..Default::default()
                    })
                })
                .collect(),
        )
        .await
        .expect("lead the __transaction_state partitions");
    let loaded = tokio::time::timeout(Duration::from_secs(30), async {
        for &partition in &led {
            while broker
                .txn_coordinator
                .load_status(krabka_ids::PartitionIndex(partition))
                .await
                != Some(crate::txn::coordinator::leadership::LoadStatus::Loaded)
            {
                // intentional: a load has no awaiter that a test can reach.
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
    })
    .await;
    assert2::assert!(
        loaded.is_ok(),
        "the transaction coordinator did not load {led:?}"
    );
}

/// The registration of an unfenced broker `node_id` at `127.0.0.1:9092`, at
/// broker epoch 0, with a nil incarnation id and no rack, log directories,
/// endpoints, or supported features.
///
/// A test spells out each field its scenario depends on and takes the rest
/// from here: `BrokerRegistrationRecord { fenced: true,
/// ..broker_registration(2) }`.
pub(crate) fn broker_registration(node_id: u64) -> krabka_metadata::BrokerRegistrationRecord {
    krabka_metadata::BrokerRegistrationRecord {
        fenced: false,
        in_controlled_shutdown: false,
        cordoned_log_dirs: None,
        node_id: krabka_raft::NodeId(node_id),
        broker_epoch: 0,
        incarnation_id: uuid::Uuid::nil(),
        host: "127.0.0.1".into(),
        port: 9092,
        rack: None,
        log_dirs: vec![],
        endpoints: vec![],
        features: std::collections::BTreeMap::new(),
    }
}

/// Wait up to five seconds for `broker` to become the controller leader.
pub(crate) async fn wait_for_controller_leader(broker: &Broker) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while !broker
        .controller
        .watch_leader()
        .borrow()
        .is_some_and(|node| node == broker.config.node_id)
    {
        assert2::assert!(
            std::time::Instant::now() <= deadline,
            "broker did not become controller leader"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Register `node_id` as a remote broker in the controller's image.
pub(crate) async fn seed_remote_broker(handle: &BrokerHandle, node_id: u64) {
    handle
        .broker_arc_for_test()
        .controller
        .submit_change(vec![MetadataRecord::V1BrokerRegistration(
            krabka_metadata::BrokerRegistrationRecord {
                broker_epoch: -1,
                ..broker_registration(node_id)
            },
        )])
        .await
        .expect("seed broker registration");
}

/// Fence `node_id` the way the controller does: its heartbeat session is
/// fenced, and its registration says so, which is what every node reads.
pub(crate) async fn fence_remote_broker(handle: &BrokerHandle, node_id: u64) {
    let broker = handle.broker_arc_for_test();
    broker.liveness.record_fenced_heartbeat(node_id).await;
    let fence = crate::heartbeat::fencing::registration_change(
        &broker.controller.current_image(),
        krabka_raft::NodeId(node_id),
        crate::heartbeat::fencing::RegistrationChange::FENCE,
    );
    broker
        .controller
        .submit_change(fence.into_iter().collect())
        .await
        .expect("publish broker fencing");
}

/// Give `node_id` the witness role, as the controller-managed
/// `broker.witness` broker config does.
pub(crate) async fn make_witness(handle: &BrokerHandle, node_id: u64) {
    handle
        .broker_arc_for_test()
        .controller
        .submit_change(vec![MetadataRecord::V1BrokerConfig(
            krabka_metadata::BrokerConfigRecord {
                node_id: krabka_raft::NodeId(node_id),
                config_name: crate::config_keys::BROKER_WITNESS.to_string(),
                config_value: Some(crate::config_keys::WITNESS_TRUE.to_string()),
            },
        )])
        .await
        .expect("publish the witness role");
}

/// Put `node_id` in controlled shutdown, the way the controller's heartbeat
/// state machine does: its registration says so, and it stays unfenced.
pub(crate) async fn begin_controlled_shutdown(handle: &BrokerHandle, node_id: u64) {
    let broker = handle.broker_arc_for_test();
    let change = crate::heartbeat::fencing::registration_change(
        &broker.controller.current_image(),
        krabka_raft::NodeId(node_id),
        crate::heartbeat::fencing::RegistrationChange::CONTROLLED_SHUTDOWN,
    );
    broker
        .controller
        .submit_change(change.into_iter().collect())
        .await
        .expect("publish controlled shutdown");
}

/// Build a [`RequestContext`] over the given principal, peer, and client id.
///
/// The remaining fields are the plaintext, non-sendfile defaults that every
/// handler test shares. `client_id` is a parameter because it feeds
/// client-quota lookups and therefore varies per handler.
pub(crate) fn request_context<'a>(
    principal: &'a Principal,
    peer: &'a SocketAddr,
    client_id: &'a str,
) -> RequestContext<'a> {
    RequestContext::new(
        principal,
        peer,
        client_id,
        "test-connection",
        false,
        "PLAINTEXT",
    )
}

/// Encode a request to wire bytes at `version`.
pub(crate) fn encode_request<T: Encode>(req: &T, version: i16) -> Bytes {
    let mut buf = BytesMut::with_capacity(req.encoded_len(version));
    req.encode(&mut buf, version).expect("encode request");
    buf.freeze()
}

/// Decode a response from `bytes` at `version`, and assert that the decoder
/// consumed every byte.
pub(crate) fn decode_response<T: Decode<'static>>(bytes: &Bytes, version: i16) -> T {
    let mut cur: &[u8] = bytes.as_ref();
    let resp = T::decode(&mut cur, version).expect("decode response");
    assert2::assert!(cur.is_empty(), "response decoder consumed all bytes");
    resp
}

/// An authorizer that reads the grants from the principal name.
///
/// The name is a `+`-separated list of `<ResourceType>:<AclOperation>` grants,
/// in the `Debug` spelling, for example `Cluster:ClusterAction+Group:Read`. A
/// grant allows the operation on every resource of that type. Every other
/// request is denied, so the name `none` holds no grant.
#[derive(Debug)]
pub(crate) struct GrantsInPrincipalName;

impl crate::authorizer::Authorizer for GrantsInPrincipalName {
    fn authorize(
        &self,
        _source: &dyn crate::authorizer::AclSource,
        request: &crate::authorizer::AuthorizationRequest<'_>,
    ) -> crate::authorizer::AuthorizationResult {
        let wanted = format!("{:?}:{:?}", request.resource_type, request.operation);
        if request
            .principal
            .name
            .split('+')
            .any(|grant| grant == wanted)
        {
            crate::authorizer::AuthorizationResult::Allow
        } else {
            crate::authorizer::AuthorizationResult::Deny
        }
    }
}

/// A literal `Allow` ACL for `principal` (such as `User:alice`) from any host,
/// to `operation` on the `resource_type` resource named `resource_name`.
pub(crate) fn allow_acl(
    resource_type: krabka_metadata::ResourceType,
    resource_name: &str,
    principal: &str,
    operation: krabka_metadata::AclOperation,
) -> krabka_metadata::AclEntry {
    krabka_metadata::AclEntry {
        resource_type,
        resource_name: resource_name.to_string(),
        pattern_type: krabka_metadata::PatternType::Literal,
        principal: principal.to_string(),
        host: "*".to_string(),
        operation,
        permission_type: krabka_metadata::PermissionType::Allow,
    }
}

/// Commit one literal `Allow` ACL for `User:<user>` on the cluster resource.
///
/// A test that starts its broker with a [`crate::authorizer::SimpleAclAuthorizer`]
/// uses it to check a handler against Kafka's operation-implication table
/// (for example, `AlterConfigs` implies `DescribeConfigs`, `Alter` does not).
pub(crate) async fn grant_cluster_operation(
    handle: &BrokerHandle,
    user: &str,
    operation: krabka_metadata::AclOperation,
) {
    handle
        .broker_arc_for_test()
        .controller
        .submit_change(vec![MetadataRecord::V1AccessControlEntry(allow_acl(
            krabka_metadata::ResourceType::Cluster,
            crate::handlers::acl_wire::CLUSTER_RESOURCE_NAME,
            &format!("User:{user}"),
            operation,
        ))])
        .await
        .expect("commit cluster acl");
}

/// Commit one literal `Allow` ACL for `User:<user>` on a `Topic` resource.
///
/// `topic` is the literal resource name; pass `"*"` for Kafka's
/// grant-on-every-topic wildcard. A test that starts its broker with a
/// [`crate::authorizer::SimpleAclAuthorizer`] uses it to check a handler
/// against Kafka's per-topic ACL filtering, including the operation-implication
/// table (for example, `Read` implies `Describe`).
pub(crate) async fn grant_topic_operation(
    handle: &BrokerHandle,
    user: &str,
    topic: &str,
    operation: krabka_metadata::AclOperation,
) {
    handle
        .broker_arc_for_test()
        .controller
        .submit_change(vec![MetadataRecord::V1AccessControlEntry(allow_acl(
            krabka_metadata::ResourceType::Topic,
            topic,
            &format!("User:{user}"),
            operation,
        ))])
        .await
        .expect("commit topic acl");
}

/// Serve one request through the broker's dispatch registry, as the connection
/// loop does for a [`crate::handlers::DispatchKind::Context`] entry.
///
/// It panics when `api_key` is not registered as a context dispatch. An
/// authorization test that calls it therefore also fails when its api goes
/// back to a dispatch kind that gets no principal.
pub(crate) async fn dispatch_context(
    broker: &crate::broker::Broker,
    api_key: i16,
    version: i16,
    body: &[u8],
    ctx: &RequestContext<'_>,
) -> Bytes {
    try_dispatch_context(broker, api_key, version, body, ctx)
        .await
        .unwrap_or_else(|error| panic!("api_key {api_key} handler: {error}"))
}

/// [`dispatch_context`], returning the handler's error instead of panicking
/// on it: a test of a request the adapter or the handler refuses.
pub(crate) async fn try_dispatch_context(
    broker: &crate::broker::Broker,
    api_key: i16,
    version: i16,
    body: &[u8],
    ctx: &RequestContext<'_>,
) -> Result<Bytes, crate::error::BrokerError> {
    let entry = broker
        .handlers()
        .get(api_key)
        .unwrap_or_else(|| panic!("api_key {api_key} is registered"));
    let crate::handlers::DispatchKind::Context(handler) = entry.kind() else {
        panic!("api_key {api_key} is a context dispatch, so its handler gets the principal");
    };
    handler(broker, version, 1, body, ctx).await
}

/// Serve `req` through the broker's dispatch registry as wire bytes at
/// `version`, and decode the response at the same version.
///
/// A `typed` handler takes and returns structs, so its unit tests call it
/// directly. This is the path for a test about the wire itself: a field that
/// an older version drops, or the encoding of the response.
pub(crate) async fn dispatch_wire<Resp: Decode<'static>>(
    broker: &crate::broker::Broker,
    api_key: i16,
    version: i16,
    req: &impl Encode,
    ctx: &RequestContext<'_>,
) -> Resp {
    let bytes =
        dispatch_context(broker, api_key, version, &encode_request(req, version), ctx).await;
    decode_response(&bytes, version)
}

/// Start an in-process broker over a fresh temp dir. It applies `configure` to
/// the [`BrokerConfig::for_tests`] baseline before start.
///
/// Each handler passes a closure with exactly the config tweaks it needs, such
/// as an authorizer to install, an `audit_enabled` toggle, or share and streams
/// groups to enable. The returned [`tempfile::TempDir`] must outlive the
/// broker.
pub(crate) fn start_broker_with(
    configure: impl FnOnce(&mut BrokerConfig),
) -> impl std::future::Future<Output = (BrokerHandle, tempfile::TempDir)> {
    let dir = tempfile::TempDir::new().expect("tempdir");
    let mut cfg = BrokerConfig::for_tests(dir.path().to_path_buf());
    configure(&mut cfg);
    Box::pin(async move {
        let handle = Broker::start(cfg).await.expect("start broker");
        (handle, dir)
    })
}

/// Finalizes `share.version` at `level` on a started broker: the feature that
/// turns the share-group APIs on (1) or off (0), as Kafka's
/// `isShareGroupProtocolEnabled` reads it.
pub(crate) async fn finalize_share_version(broker: &Broker, level: i16) {
    broker
        .controller
        .submit_change(vec![MetadataRecord::V1FeatureLevel(
            krabka_metadata::FeatureLevelRecord {
                name: krabka_metadata::metadata_version::SHARE_VERSION_FEATURE.into(),
                level,
            },
        )])
        .await
        .expect("finalize share.version");
}

/// Start an in-process broker with only its authorizer swapped in.
///
/// This is the most common `start_broker` shape across handler test modules;
/// see [`wire_helpers`]. A handler test drives an authorization-failure
/// path with [`DenyAll`] or a custom authorizer, and it otherwise takes
/// the `for_tests` defaults.
pub(crate) async fn start_broker_with_authorizer(
    authorizer: std::sync::Arc<dyn crate::authorizer::Authorizer>,
) -> (BrokerHandle, tempfile::TempDir) {
    start_broker_with(|cfg| cfg.authorizer = authorizer).await
}

/// Like [`start_broker_with`], but it turns `audit_enabled` off before it
/// applies `configure`.
///
/// Most handler tests do not exercise the audit path and turn it off, so that
/// audit-log assertions elsewhere in the suite stay stable.
pub(crate) fn start_broker_no_audit_with(
    configure: impl FnOnce(&mut BrokerConfig),
) -> impl std::future::Future<Output = (BrokerHandle, tempfile::TempDir)> {
    start_broker_with(|cfg| {
        cfg.audit_enabled = false;
        configure(cfg);
    })
}

/// Start an in-process broker on the [`BrokerConfig::for_tests`] baseline with
/// audit logging off.
pub(crate) fn start_broker_no_audit()
-> impl std::future::Future<Output = (BrokerHandle, tempfile::TempDir)> {
    start_broker_no_audit_with(|_| {})
}

/// Like [`start_broker_with_authorizer`], but it also disables audit logging.
///
/// This is the second most common `start_broker` shape. Admin-handler tests
/// that do not exercise the audit path swap the authorizer and turn
/// `audit_enabled` off, so that audit-log assertions elsewhere in the suite
/// stay stable.
pub(crate) async fn start_broker_with_authorizer_no_audit(
    authorizer: std::sync::Arc<dyn crate::authorizer::Authorizer>,
) -> (BrokerHandle, tempfile::TempDir) {
    start_broker_no_audit_with(|cfg| cfg.authorizer = authorizer).await
}

/// Generate the `encode_request` / `decode_response` / `test_context`
/// wrapper trio that every handler's `#[cfg(test)] mod handler_tests` binds
/// over [`encode_request`], [`decode_response`], and [`request_context`].
///
/// `wire_helpers!(ReqTy, RespTy, version = V, client_id = "id")` pins one
/// wire version; leaving out `version = V` makes `encode_request` and
/// `decode_response` take the version per call instead, for
/// version-negotiation tests. A leading visibility, as in
/// `wire_helpers!(pub(super) ReqTy, ...)`, lets a shared `test_support`
/// module hand the helpers to its sibling test modules.
macro_rules! wire_helpers {
    ($vis:vis $req:ty, $resp:ty, $(version = $version:expr,)? client_id = $client_id:expr) => {
        crate::test_support::encode_helper!($vis $req $(, version = $version)?);
        crate::test_support::response_helpers!($vis $resp, $(version = $version,)? client_id = $client_id);
    };
}
pub(crate) use wire_helpers;

/// Like [`wire_helpers`], but for handlers whose `handle()` takes an
/// already-typed request, so there is nothing to encode, and returns wire
/// `Bytes`. Only `decode_response` and `test_context` are needed.
macro_rules! response_helpers {
    ($vis:vis $resp:ty, $(version = $version:expr,)? client_id = $client_id:expr) => {
        crate::test_support::decode_helper!($vis $resp $(, version = $version)?);
        crate::test_support::context_helper!($vis client_id = $client_id);
    };
}
pub(crate) use response_helpers;

/// Like [`wire_helpers`], but for handlers that take no [`RequestContext`],
/// so there is no `test_context` to generate. It generates only
/// `encode_request` and `decode_response`.
macro_rules! codec_helpers {
    ($vis:vis $req:ty, $resp:ty $(, version = $version:expr)?) => {
        crate::test_support::encode_helper!($vis $req $(, version = $version)?);
        crate::test_support::decode_helper!($vis $resp $(, version = $version)?);
    };
}
pub(crate) use codec_helpers;

/// The `encode_request` that [`wire_helpers`] and [`codec_helpers`] generate.
macro_rules! encode_helper {
    ($vis:vis $req:ty, version = $version:expr) => {
        $vis fn encode_request(req: &$req) -> ::bytes::Bytes {
            crate::test_support::encode_request(req, $version)
        }
    };
    ($vis:vis $req:ty) => {
        $vis fn encode_request(req: &$req, version: i16) -> ::bytes::Bytes {
            crate::test_support::encode_request(req, version)
        }
    };
}
pub(crate) use encode_helper;

/// The `decode_response` that the other wire macros generate, usable on its
/// own by a test module that decodes but never encodes or builds a context.
macro_rules! decode_helper {
    ($vis:vis $resp:ty, version = $version:expr) => {
        $vis fn decode_response(bytes: &::bytes::Bytes) -> $resp {
            crate::test_support::decode_response(bytes, $version)
        }
    };
    ($vis:vis $resp:ty) => {
        $vis fn decode_response(bytes: &::bytes::Bytes, version: i16) -> $resp {
            crate::test_support::decode_response(bytes, version)
        }
    };
}
pub(crate) use decode_helper;

/// The `test_context` that [`wire_helpers`] and [`response_helpers`]
/// generate, usable on its own by a test module that needs only the context.
macro_rules! context_helper {
    ($vis:vis client_id = $client_id:expr) => {
        $vis fn test_context<'a>(
            principal: &'a krabka_security::Principal,
            peer: &'a ::std::net::SocketAddr,
        ) -> crate::handlers::RequestContext<'a> {
            crate::test_support::request_context(principal, peer, $client_id)
        }
    };
}
pub(crate) use context_helper;

/// The outcome a [`FakeMetadataSource`] returns from `submit_change`, as a
/// function of the batch it was handed.
type SubmitOutcome =
    Box<dyn Fn(&[MetadataRecord]) -> Result<SubmitChangeResult, RaftError> + Send + Sync>;

/// The metadata authority that this crate's unit tests read from and write
/// through.
///
/// One image, one leader, and one capture buffer stand in for the whole
/// controller. Every [`MetadataSource`] method has a behaving default, so a
/// method added to the trait reaches every suite that fakes metadata at once,
/// rather than arriving as another `unimplemented!()` in another hand-rolled
/// double.
///
/// The image and the leader live in `watch` channels whose senders the fake
/// keeps, so [`FakeMetadataSource::set_image`] and
/// [`FakeMetadataSource::set_leader`] push a change through to whatever the
/// code under test watches, and a watcher of an unchanged fake waits rather
/// than seeing the channel close. Every batch that reaches `submit_change` is
/// captured in order and readable with [`FakeMetadataSource::submitted`].
///
/// Behaviour that genuinely varies per test stays at the call site:
/// [`FakeMetadataSourceBuilder::on_submit`] installs a different write
/// outcome, [`FakeMetadataSourceBuilder::stall_submits`] models a raft commit
/// that never returns, and
/// [`FakeMetadataSourceBuilder::controller_bound_addr`] sets the listener
/// address a test dials or asserts on, and
/// [`FakeMetadataSourceBuilder::term`] with
/// [`FakeMetadataSourceBuilder::without_controller_epoch`] set the controller
/// epoch a caller fences its writes against.
pub(crate) struct FakeMetadataSource {
    image_tx: watch::Sender<Arc<MetadataImage>>,
    leader_tx: watch::Sender<Option<NodeId>>,
    fatal_tx: watch::Sender<Option<String>>,
    controller_bound_addr: SocketAddr,
    term: u64,
    owns_controller_epoch: bool,
    submitted: Mutex<Vec<Vec<MetadataRecord>>>,
    on_submit: SubmitOutcome,
    stall_submits: bool,
    current_image_calls: AtomicUsize,
    controller_bound_addr_calls: AtomicUsize,
}

impl FakeMetadataSource {
    /// A builder over an empty image with no elected leader, no committed
    /// metadata, and an unspecified controller address. Every seam is a
    /// method on the returned builder.
    pub(crate) fn builder() -> FakeMetadataSourceBuilder {
        FakeMetadataSourceBuilder {
            image: Arc::new(MetadataImage::new(uuid::Uuid::nil())),
            leader: None,
            controller_bound_addr: SocketAddr::from(([0, 0, 0, 0], 0)),
            term: 0,
            owns_controller_epoch: true,
            on_submit: None,
            stall_submits: false,
        }
    }

    /// Publish `image` as the current metadata, as an applied change does.
    /// Every `watch_image` receiver observes it.
    pub(crate) fn set_image(&self, image: impl Into<Arc<MetadataImage>>) {
        self.image_tx.send_replace(image.into());
    }

    /// Publish the image that `records` build, under the nil cluster id.
    pub(crate) fn set_records(&self, records: &[MetadataRecord]) {
        self.set_image(MetadataImage::from_records(uuid::Uuid::nil(), records));
    }

    /// Publish a controller-leader change. Every `watch_leader` receiver
    /// observes it, and `quorum_state` reports it as `current_leader`.
    pub(crate) fn set_leader(&self, leader: Option<NodeId>) {
        self.leader_tx.send_replace(leader);
    }

    /// Publish a fatal controller fault, as a controller does before it stops
    /// itself. Every `watch_fatal` receiver observes it. A fake that never
    /// calls this reports no fault, and its watchers wait rather than seeing
    /// the channel close.
    pub(crate) fn set_fatal(&self, fault: &str) {
        self.fatal_tx.send_replace(Some(fault.to_owned()));
    }

    /// The leader channel's sender, for a test that drives a spawned watcher
    /// and wants a push with no receiver to fail rather than pass silently:
    /// `send` errors when nothing is watching, where
    /// [`FakeMetadataSource::set_leader`] would not.
    pub(crate) fn leader_tx(&self) -> &watch::Sender<Option<NodeId>> {
        &self.leader_tx
    }

    /// Every batch handed to `submit_change`, in call order, one entry per
    /// call. A test that asks what the code under test appended reads this
    /// rather than a success flag.
    pub(crate) fn submitted(&self) -> Vec<Vec<MetadataRecord>> {
        self.submitted
            .lock()
            .expect("the submitted batches are not poisoned")
            .clone()
    }

    /// [`FakeMetadataSource::submitted`] flattened, for a test that cares
    /// which records arrived but not how they were batched.
    pub(crate) fn submitted_records(&self) -> Vec<MetadataRecord> {
        self.submitted().concat()
    }

    /// How many times `current_image` was called.
    pub(crate) fn current_image_calls(&self) -> usize {
        self.current_image_calls.load(atomic::Ordering::Relaxed)
    }

    /// How many times `controller_bound_addr` was called.
    pub(crate) fn controller_bound_addr_calls(&self) -> usize {
        self.controller_bound_addr_calls
            .load(atomic::Ordering::Relaxed)
    }
}

/// Builder for [`FakeMetadataSource`]; see that type for what each seam is
/// for.
pub(crate) struct FakeMetadataSourceBuilder {
    image: Arc<MetadataImage>,
    leader: Option<NodeId>,
    controller_bound_addr: SocketAddr,
    term: u64,
    owns_controller_epoch: bool,
    on_submit: Option<SubmitOutcome>,
    stall_submits: bool,
}

impl FakeMetadataSourceBuilder {
    /// Serve `image` as the current metadata.
    pub(crate) fn image(mut self, image: impl Into<Arc<MetadataImage>>) -> Self {
        self.image = image.into();
        self
    }

    /// Serve the image that `records` build, under the nil cluster id.
    pub(crate) fn records(self, records: &[MetadataRecord]) -> Self {
        self.image(MetadataImage::from_records(uuid::Uuid::nil(), records))
    }

    /// Report `leader` as the controller leader, from `watch_leader` and as
    /// `quorum_state().current_leader`.
    pub(crate) fn leader(mut self, leader: Option<NodeId>) -> Self {
        self.leader = leader;
        self
    }

    /// Report `addr` as the controller listener's bound address.
    pub(crate) fn controller_bound_addr(mut self, addr: SocketAddr) -> Self {
        self.controller_bound_addr = addr;
        self
    }

    /// Report `term` as the quorum's current term, and -- unless
    /// [`FakeMetadataSourceBuilder::without_controller_epoch`] is set -- as
    /// the current controller epoch, which is what the trait's own default
    /// does with the term.
    pub(crate) fn term(mut self, term: u64) -> Self {
        self.term = term;
        self
    }

    /// Report no controller epoch at all, as a broker-only observer does: it
    /// tracks the leader id but does not own the controller's term state, so
    /// `current_controller_epoch` is `None` however the quorum's term reads.
    pub(crate) fn without_controller_epoch(mut self) -> Self {
        self.owns_controller_epoch = false;
        self
    }

    /// Decide the result of each `submit_change` from the batch it was
    /// handed. The batch is captured either way; only the outcome the caller
    /// sees changes.
    pub(crate) fn on_submit(
        mut self,
        outcome: impl Fn(&[MetadataRecord]) -> Result<SubmitChangeResult, RaftError>
        + Send
        + Sync
        + 'static,
    ) -> Self {
        self.on_submit = Some(Box::new(outcome));
        self
    }

    /// Never complete a `submit_change`. This models a raft commit that
    /// stalls, so that the caller's own timeout or cancellation path runs.
    pub(crate) fn stall_submits(mut self) -> Self {
        self.stall_submits = true;
        self
    }

    pub(crate) fn build(self) -> FakeMetadataSource {
        let (image_tx, _) = watch::channel(self.image);
        let (leader_tx, _) = watch::channel(self.leader);
        let (fatal_tx, _) = watch::channel(None);
        FakeMetadataSource {
            image_tx,
            leader_tx,
            fatal_tx,
            controller_bound_addr: self.controller_bound_addr,
            term: self.term,
            owns_controller_epoch: self.owns_controller_epoch,
            submitted: Mutex::new(Vec::new()),
            on_submit: self
                .on_submit
                .unwrap_or_else(|| Box::new(|_| Ok(SubmitChangeResult::default()))),
            stall_submits: self.stall_submits,
            current_image_calls: AtomicUsize::new(0),
            controller_bound_addr_calls: AtomicUsize::new(0),
        }
    }
}

/// Every reconfiguration path rejects with this. The fake has no raft log to
/// reconfigure, and reconfiguration is covered against the real controller.
fn unsupported() -> RaftError {
    RaftError::Unsupported("fake metadata source")
}

#[async_trait::async_trait]
impl MetadataSource for FakeMetadataSource {
    fn current_image(&self) -> Arc<MetadataImage> {
        self.current_image_calls
            .fetch_add(1, atomic::Ordering::Relaxed);
        self.image_tx.borrow().clone()
    }

    fn watch_image(&self) -> watch::Receiver<Arc<MetadataImage>> {
        self.image_tx.subscribe()
    }

    fn watch_leader(&self) -> watch::Receiver<Option<NodeId>> {
        self.leader_tx.subscribe()
    }

    fn watch_fatal(&self) -> watch::Receiver<Option<String>> {
        self.fatal_tx.subscribe()
    }

    /// A quorum that has committed nothing and knows no voters. Its leader
    /// comes from the fake's leader channel, so `watch_leader` and
    /// `quorum_state` cannot disagree, and its term is
    /// [`FakeMetadataSourceBuilder::term`].
    fn quorum_state(&self) -> QuorumState {
        QuorumState {
            current_term: self.term,
            last_applied_index: 0,
            current_leader: *self.leader_tx.borrow(),
            voters: Vec::new(),
            voter_nodes: std::collections::BTreeMap::new(),
            per_voter_matched_index: std::collections::BTreeMap::new(),
            per_replica_last_fetch_ms: std::collections::BTreeMap::new(),
            per_replica_last_caught_up_ms: std::collections::BTreeMap::new(),
            observer_directory_ids: std::collections::BTreeMap::new(),
            is_leader: false,
        }
    }

    /// The quorum's term, unless the fake stands in for a source that owns no
    /// quorum view -- see
    /// [`FakeMetadataSourceBuilder::without_controller_epoch`].
    fn current_controller_epoch(&self) -> Option<u64> {
        self.owns_controller_epoch.then_some(self.term)
    }

    /// No fake has voted in a controller election.
    fn voted_directory_id(&self) -> Option<uuid::Uuid> {
        None
    }

    async fn submit_change(
        &self,
        records: Vec<MetadataRecord>,
    ) -> Result<SubmitChangeResult, RaftError> {
        if self.stall_submits {
            std::future::pending::<()>().await;
        }
        let outcome = (self.on_submit)(&records);
        self.submitted
            .lock()
            .expect("the submitted batches are not poisoned")
            .push(records);
        outcome
    }

    async fn change_membership(&self, _new_voters: BTreeSet<NodeId>) -> Result<(), RaftError> {
        Err(unsupported())
    }

    async fn add_learner(&self, _node_id: NodeId, _node: Node) -> Result<(), RaftError> {
        Err(unsupported())
    }

    fn controller_bound_addr(&self) -> SocketAddr {
        self.controller_bound_addr_calls
            .fetch_add(1, atomic::Ordering::Relaxed);
        self.controller_bound_addr
    }

    fn read_snapshot_range(&self, _position: i64, _max_bytes: i32) -> SnapshotRange {
        SnapshotRange::NoSnapshot
    }

    async fn trigger_snapshot(&self) -> Result<(), RaftError> {
        Err(unsupported())
    }

    async fn add_voter(&self, _req: AddVoter) -> Result<ReconfigOutcome, RaftError> {
        Err(unsupported())
    }

    async fn remove_voter(&self, _req: RemoveVoter) -> Result<ReconfigOutcome, RaftError> {
        Err(unsupported())
    }

    async fn update_voter(&self, _req: UpdateVoter) -> Result<ReconfigOutcome, RaftError> {
        Err(unsupported())
    }

    /// Finalizing `kraft.version` is a reconfiguration too, so it rejects with
    /// the others rather than falling through to the trait's `NotLeader`,
    /// which would send a caller down a leadership branch the fake never
    /// models.
    async fn finalize_kraft_version(&self, _version: u16) -> Result<ReconfigOutcome, RaftError> {
        Err(unsupported())
    }

    async fn cancel(&self) {}
}

/// The point in a deadline's life at which a [`BrokenTimer`] fails.
///
/// [`Timer`] reports the two separately — the outer `Result` of `at` covers
/// registration, and the [`TimerFuture`] it hands back covers everything after
/// — and so do [`crate::time_util::arm`] and [`crate::time_util::fired`], which
/// is why a cadence loop has two ways to lose its ticker rather than one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TimerFailure {
    /// The registration is refused outright, so `arm` reports `None`.
    Registration,
    /// The registration is accepted and the deadline it yields then resolves
    /// to an error, so `fired` reports `false`.
    Completion,
}

/// A timer whose backend gives out, for the tests that assert a cadence loop
/// stops instead of spinning once it has no ticker left.
///
/// The first `healthy` deadlines are honoured, and each of them completes the
/// moment it is armed whatever duration it was asked for, so a loop takes that
/// many ticks and no more real time passes than the test needs. Every deadline
/// after them fails, at [`TimerFailure`].
///
/// This is hand-rolled rather than taken from
/// `qubit_clock::test_util::FaultInjectingTimer`, because that fixture honours
/// a deadline that is already due, and the start-up deadline of every cadence
/// loop here but the group actor's is `Duration::ZERO` — exactly the
/// registration these tests need to see refused.
pub(crate) struct BrokenTimer {
    /// The domain every deadline handed to [`Self::at`] is validated against.
    /// Nothing here reads the time; the clock exists to give the timer a
    /// domain, as the [`Timer`] contract requires.
    clock: StdMonotonicClock,
    /// Where in a deadline's life the failure surfaces.
    failure: TimerFailure,
    /// How many leading deadlines are honoured before the failures start.
    healthy: usize,
    /// How many deadlines have been asked for so far.
    registrations: AtomicUsize,
}

impl BrokenTimer {
    /// A timer that fails every deadline, including the start-up one.
    pub(crate) fn dead(failure: TimerFailure) -> Arc<Self> {
        Self::dead_after(0, failure)
    }

    /// A timer that honours `healthy` deadlines — each completing at once, so
    /// the loop takes that many ticks — and fails every deadline after them.
    pub(crate) fn dead_after(healthy: usize, failure: TimerFailure) -> Arc<Self> {
        Arc::new(Self {
            clock: StdMonotonicClock::new(),
            failure,
            healthy,
            registrations: AtomicUsize::new(0),
        })
    }

    /// This timer as the trait object a cadence loop's config holds, leaving
    /// the caller its own handle to read [`Self::registrations`] from.
    pub(crate) fn injectable(self: &Arc<Self>) -> Arc<dyn Timer> {
        Arc::clone(self) as Arc<dyn Timer>
    }

    /// How many deadlines the loop under test has asked this timer for.
    ///
    /// A loop that stopped asked for exactly one more than it was given; a
    /// loop that re-armed through the failure keeps climbing.
    pub(crate) fn registrations(&self) -> usize {
        self.registrations.load(Ordering::Relaxed)
    }
}

impl Timer for BrokenTimer {
    fn clock(&self) -> &dyn MonotonicClock {
        &self.clock
    }

    fn at(&self, _deadline: MonotonicInstant) -> Result<TimerFuture, TimeError> {
        let nth = self.registrations.fetch_add(1, Ordering::Relaxed);
        if nth < self.healthy {
            return Ok(Box::pin(std::future::ready(Ok(()))));
        }
        let error = TimeError::TimerUnavailable {
            source: TimerUnavailableError::BackendUnavailable {
                backend: "krabka-broker test",
                source: Box::new(io::Error::other("the timer backend is gone")),
            },
        };
        match self.failure {
            TimerFailure::Registration => Err(error),
            TimerFailure::Completion => Ok(Box::pin(std::future::ready(Err(error)))),
        }
    }
}

// One `tracing` event that a `LogCapture` recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LoggedEvent {
    pub(crate) level: tracing::Level,
    pub(crate) target: String,
    pub(crate) message: String,
}

// Records the `tracing` events logged inside a `LogCapture::span`, so a test
// sees what one piece of work logged and not what the rest of a running broker
// logged at the same time.
#[derive(Clone, Default)]
pub(crate) struct LogCapture {
    events: Arc<Mutex<Vec<LoggedEvent>>>,
}

impl LogCapture {
    // The name `LogCapture::span` gives its span.
    const SPAN_NAME: &str = "log_capture";

    // A dispatcher that records into this capture. Install it with
    // `tracing::dispatcher::set_default` or `with_default` on the thread that
    // does the work.
    pub(crate) fn dispatch(&self) -> tracing::Dispatch {
        tracing::Dispatch::new(tracing_subscriber::registry().with(self.clone()))
    }

    // The span whose events the capture records. Create it while the
    // capture's dispatcher is the default one.
    pub(crate) fn span() -> tracing::Span {
        tracing::info_span!("log_capture")
    }

    // The events recorded so far, oldest first.
    pub(crate) fn events(&self) -> Vec<LoggedEvent> {
        self.events
            .lock()
            .expect("the captured events are not poisoned")
            .clone()
    }
}

impl<S> Layer<S> for LogCapture
where
    S: tracing::Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        let inside = ctx
            .event_scope(event)
            .is_some_and(|mut scope| scope.any(|span| span.name() == Self::SPAN_NAME));
        if !inside {
            return;
        }
        let mut message = MessageField::default();
        event.record(&mut message);
        self.events
            .lock()
            .expect("the captured events are not poisoned")
            .push(LoggedEvent {
                level: *event.metadata().level(),
                target: event.metadata().target().to_owned(),
                message: message.0,
            });
    }
}

// The `message` of an event, as `format_args!` renders it.
#[derive(Default)]
struct MessageField(String);

impl tracing::field::Visit for MessageField {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.0 = format!("{value:?}");
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use assert2::assert;
    use krabka_metadata::{
        AclOperation, KRaftVersionRange, MetadataImage, MetadataRecord, ResourceType, Voter,
    };
    use krabka_raft::{
        AddVoter, Node, NodeId, QuorumState, RaftError, RemoveVoter, SnapshotRange,
        SubmitChangeResult, UpdateVoter,
    };

    use super::FakeMetadataSource;
    use crate::{
        authorizer::{
            AuthorizationRequest,
            AuthorizationResult::{Allow, Deny},
        },
        metadata_source::MetadataSource,
    };

    /// An authorizer with a decision cache of its own and a real
    /// `authorize_by_resource_type` scan, so that forwarding is visible.
    #[derive(Debug)]
    struct Cached;

    impl crate::authorizer::Authorizer for Cached {
        fn authorize(
            &self,
            _source: &dyn crate::authorizer::AclSource,
            _request: &crate::authorizer::AuthorizationRequest<'_>,
        ) -> crate::authorizer::AuthorizationResult {
            crate::authorizer::AuthorizationResult::Deny
        }

        fn decision_ttl(&self) -> Option<std::time::Duration> {
            Some(std::time::Duration::from_secs(7))
        }

        fn authorize_by_resource_type(
            &self,
            _source: &dyn crate::authorizer::AclSource,
            _principal: &krabka_security::Principal,
            _host: &std::net::SocketAddr,
            _resource_type: krabka_metadata::ResourceType,
            _operation: krabka_metadata::AclOperation,
        ) -> crate::authorizer::AuthorizationResult {
            crate::authorizer::AuthorizationResult::Allow
        }
    }

    /// `controller_peer_allowed` allows only `ClusterAction` and `Create` on
    /// the `Cluster` for the `ANONYMOUS` controller peer, and answers
    /// everything else as the wrapped authorizer does.
    #[test]
    fn controller_peer_allowed_adds_only_the_controller_peer_grant() {
        let image = MetadataImage::new(uuid::Uuid::nil());
        let peer = super::peer();
        let alice = super::principal("alice");
        let authorizer = super::controller_peer_allowed(std::sync::Arc::new(Cached));
        let cases = [
            (
                "ANONYMOUS",
                ResourceType::Cluster,
                AclOperation::ClusterAction,
                Allow,
            ),
            (
                "alice",
                ResourceType::Cluster,
                AclOperation::ClusterAction,
                Deny,
            ),
            (
                "ANONYMOUS",
                ResourceType::Cluster,
                AclOperation::Create,
                Allow,
            ),
            ("alice", ResourceType::Cluster, AclOperation::Create, Deny),
            (
                "ANONYMOUS",
                ResourceType::Cluster,
                AclOperation::Alter,
                Deny,
            ),
            ("ANONYMOUS", ResourceType::Topic, AclOperation::Create, Deny),
            (
                "ANONYMOUS",
                ResourceType::Topic,
                AclOperation::ClusterAction,
                Deny,
            ),
        ];
        for (name, resource_type, operation, expected) in cases {
            let principal = super::principal(name);
            let request = AuthorizationRequest {
                principal: &principal,
                host: &peer,
                resource_type,
                resource_name: "kafka-cluster",
                operation,
            };
            assert!(
                authorizer.authorize(&image, &request) == expected,
                "{name} {resource_type:?} {operation:?}"
            );
        }
        assert!(
            (
                authorizer.is_configured(),
                authorizer.decision_ttl(),
                authorizer.authorize_by_resource_type(
                    &image,
                    &alice,
                    &peer,
                    ResourceType::Topic,
                    AclOperation::Write,
                ),
            ) == (true, Some(std::time::Duration::from_secs(7)), Allow)
        );
        let unconfigured = super::controller_peer_allowed(std::sync::Arc::new(
            crate::authorizer::AllowAllAuthorizer,
        ));
        assert!(!unconfigured.is_configured());
    }

    fn voter() -> Voter {
        Voter {
            id: NodeId(1),
            directory_id: uuid::Uuid::from_u128(1),
            endpoints: Vec::new(),
            kraft_version: KRaftVersionRange::default(),
        }
    }

    /// Every reconfiguration path rejects the same way, `finalize_kraft_version`
    /// included: the fake has no raft log to reconfigure, and a caller that
    /// reached one should see that rather than a leadership error the fake
    /// never models.
    #[tokio::test]
    async fn every_reconfiguration_path_rejects_as_unsupported() {
        let source = FakeMetadataSource::builder().build();

        assert!(let Err(RaftError::Unsupported(_)) =
            source.change_membership(BTreeSet::from([NodeId(1)])).await);
        assert!(let Err(RaftError::Unsupported(_)) =
            source.add_learner(NodeId(1), Node::default()).await);
        assert!(let Err(RaftError::Unsupported(_)) = source.trigger_snapshot().await);
        assert!(let Err(RaftError::Unsupported(_)) = source
            .add_voter(AddVoter {
                voter: voter(),
                ack_when_committed: true,
            })
            .await);
        assert!(let Err(RaftError::Unsupported(_)) = source
            .remove_voter(RemoveVoter {
                id: NodeId(1),
                directory_id: uuid::Uuid::from_u128(1),
            })
            .await);
        assert!(let Err(RaftError::Unsupported(_)) =
            source.update_voter(UpdateVoter { voter: voter() }).await);
        assert!(let Err(RaftError::Unsupported(_)) = source.finalize_kraft_version(1).await);
    }

    /// The controller epoch is the quorum's term by default, as the trait's
    /// own default makes it, and is `None` for a source that owns no quorum
    /// view -- which is what a broker-only observer reports.
    #[test]
    fn the_controller_epoch_follows_the_term_unless_the_fake_owns_no_quorum() {
        let controller = FakeMetadataSource::builder().term(7).build();
        assert!(controller.quorum_state().current_term == 7);
        assert!(controller.current_controller_epoch() == Some(7));

        let observer = FakeMetadataSource::builder()
            .term(7)
            .without_controller_epoch()
            .build();
        assert!(observer.quorum_state().current_term == 7);
        assert!(observer.current_controller_epoch().is_none());
    }

    /// The quorum has committed nothing and knows no voters, and its leader is
    /// whatever the leader channel last published, so `watch_leader` and
    /// `quorum_state` cannot disagree.
    #[test]
    fn quorum_state_reports_the_leader_channel_over_an_empty_quorum() {
        let source = FakeMetadataSource::builder()
            .leader(Some(NodeId(2)))
            .build();

        let QuorumState {
            current_term,
            last_applied_index,
            current_leader,
            voters,
            voter_nodes,
            per_voter_matched_index,
            ..
        } = source.quorum_state();
        assert!(current_term == 0);
        assert!(last_applied_index == 0);
        assert!(current_leader == Some(NodeId(2)));
        assert!(voters.is_empty());
        assert!(voter_nodes.is_empty());
        assert!(per_voter_matched_index.is_empty());
        assert!(source.current_metadata_offset() == -1);

        source.set_leader(None);
        assert!(source.quorum_state().current_leader.is_none());
    }

    /// The fake keeps no checkpoint to serve and has cast no vote.
    #[test]
    fn the_fake_serves_no_snapshot_and_records_no_vote() {
        let source = FakeMetadataSource::builder().build();

        assert!(matches!(
            source.read_snapshot_range(0, 1024),
            SnapshotRange::NoSnapshot
        ));
        assert!(source.voted_directory_id().is_none());
    }

    /// `cancel` has no background work to stop, and leaves the source serving.
    #[tokio::test]
    async fn cancel_leaves_the_source_serving() {
        let source = FakeMetadataSource::builder().build();

        source.cancel().await;

        assert!(let Ok(result) = source.submit_change(Vec::new()).await);
        assert!(result == SubmitChangeResult::default());
        assert!(source.submitted() == vec![Vec::<MetadataRecord>::new()]);
    }
}
