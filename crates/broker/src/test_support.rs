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

use std::{
    collections::BTreeSet,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{self, AtomicUsize},
    },
};

use bytes::{Bytes, BytesMut};
use krabka_metadata::{MetadataImage, MetadataRecord};
use krabka_protocol::{Decode, Encode};
use krabka_raft::{
    AddVoter, Node, NodeId, QuorumState, RaftError, ReconfigOutcome, RemoveVoter, SnapshotRange,
    SubmitChangeResult, UpdateVoter,
};
use krabka_security::{AuthMethod, Principal};
use tokio::sync::watch;

use crate::{
    broker::{Broker, BrokerHandle},
    config::BrokerConfig,
    handlers::RequestContext,
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

/// The loopback peer address (`127.0.0.1:9092`) that handler tests attribute
/// requests to.
pub(crate) fn peer() -> SocketAddr {
    "127.0.0.1:9092".parse().unwrap()
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
    RequestContext {
        principal,
        peer,
        client_id,
        connection_id: "test-connection",
        sendfile_capable: false,
        connection_listener_name: "PLAINTEXT",
    }
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

/// Like [`start_broker_with_authorizer`], but it also disables audit logging.
///
/// This is the second most common `start_broker` shape. Admin-handler tests
/// that do not exercise the audit path swap the authorizer and turn
/// `audit_enabled` off, so that audit-log assertions elsewhere in the suite
/// stay stable.
pub(crate) async fn start_broker_with_authorizer_no_audit(
    authorizer: std::sync::Arc<dyn crate::authorizer::Authorizer>,
) -> (BrokerHandle, tempfile::TempDir) {
    start_broker_with(|cfg| {
        cfg.audit_enabled = false;
        cfg.authorizer = authorizer;
    })
    .await
}

/// Generate the `encode_request` / `decode_response` / `test_context`
/// wrapper trio that every handler's `#[cfg(test)] mod handler_tests` binds
/// over [`encode_request`], [`decode_response`], and [`request_context`].
///
/// Two forms:
///
/// - `wire_helpers!(ReqTy, RespTy, version = V, client_id = "id")`: for
///   handlers that always drive one fixed wire version.
/// - `wire_helpers!(ReqTy, RespTy, client_id = "id")`: for handlers whose
///   tests vary `version` per call, for version-negotiation behaviour.
macro_rules! wire_helpers {
    ($req:ty, $resp:ty, version = $version:expr, client_id = $client_id:expr) => {
        fn encode_request(req: &$req) -> ::bytes::Bytes {
            crate::test_support::encode_request(req, $version)
        }

        fn decode_response(bytes: &::bytes::Bytes) -> $resp {
            crate::test_support::decode_response(bytes, $version)
        }

        fn test_context<'a>(
            principal: &'a krabka_security::Principal,
            peer: &'a ::std::net::SocketAddr,
        ) -> crate::handlers::RequestContext<'a> {
            crate::test_support::request_context(principal, peer, $client_id)
        }
    };
    ($req:ty, $resp:ty, client_id = $client_id:expr) => {
        fn encode_request(req: &$req, version: i16) -> ::bytes::Bytes {
            crate::test_support::encode_request(req, version)
        }

        fn decode_response(bytes: &::bytes::Bytes, version: i16) -> $resp {
            crate::test_support::decode_response(bytes, version)
        }

        fn test_context<'a>(
            principal: &'a krabka_security::Principal,
            peer: &'a ::std::net::SocketAddr,
        ) -> crate::handlers::RequestContext<'a> {
            crate::test_support::request_context(principal, peer, $client_id)
        }
    };
}
pub(crate) use wire_helpers;

/// Like [`wire_helpers`], but for handlers whose `handle()` takes an
/// already-typed request, so there is nothing to encode, and returns wire
/// `Bytes`. Only `decode_response` and `test_context` are needed.
macro_rules! response_helpers {
    ($resp:ty, version = $version:expr, client_id = $client_id:expr) => {
        fn decode_response(bytes: &::bytes::Bytes) -> $resp {
            crate::test_support::decode_response(bytes, $version)
        }

        fn test_context<'a>(
            principal: &'a krabka_security::Principal,
            peer: &'a ::std::net::SocketAddr,
        ) -> crate::handlers::RequestContext<'a> {
            crate::test_support::request_context(principal, peer, $client_id)
        }
    };
    ($resp:ty, client_id = $client_id:expr) => {
        fn decode_response(bytes: &::bytes::Bytes, version: i16) -> $resp {
            crate::test_support::decode_response(bytes, version)
        }

        fn test_context<'a>(
            principal: &'a krabka_security::Principal,
            peer: &'a ::std::net::SocketAddr,
        ) -> crate::handlers::RequestContext<'a> {
            crate::test_support::request_context(principal, peer, $client_id)
        }
    };
}
pub(crate) use response_helpers;

/// Like [`wire_helpers`], but for handlers that take no [`RequestContext`],
/// so there is no `test_context` to generate. It generates only
/// `encode_request` and `decode_response`.
macro_rules! codec_helpers {
    ($req:ty, $resp:ty, version = $version:expr) => {
        fn encode_request(req: &$req) -> ::bytes::Bytes {
            crate::test_support::encode_request(req, $version)
        }

        fn decode_response(bytes: &::bytes::Bytes) -> $resp {
            crate::test_support::decode_response(bytes, $version)
        }
    };
    ($req:ty, $resp:ty) => {
        fn encode_request(req: &$req, version: i16) -> ::bytes::Bytes {
            crate::test_support::encode_request(req, version)
        }

        fn decode_response(bytes: &::bytes::Bytes, version: i16) -> $resp {
            crate::test_support::decode_response(bytes, version)
        }
    };
}
pub(crate) use codec_helpers;

/// The outcome a [`FakeMetadataSource`] returns from `submit_change`, as a
/// function of the batch it was handed.
type SubmitOutcome =
    Box<dyn Fn(&[MetadataRecord]) -> Result<SubmitChangeResult, RaftError> + Send + Sync>;

/// The metadata authority that this crate's unit tests read from and write
/// through.
///
/// One image, one leader, and one capture buffer stand in for the whole
/// controller. Every [`crate::metadata_source::MetadataSource`] method has a
/// behaving default, so a method added to the trait reaches every suite that
/// fakes metadata at once, rather than arriving as another `unimplemented!()`
/// in another hand-rolled double.
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
/// address a test dials or asserts on.
pub(crate) struct FakeMetadataSource {
    image_tx: watch::Sender<Arc<MetadataImage>>,
    leader_tx: watch::Sender<Option<NodeId>>,
    controller_bound_addr: SocketAddr,
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
        FakeMetadataSource {
            image_tx,
            leader_tx,
            controller_bound_addr: self.controller_bound_addr,
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
impl crate::metadata_source::MetadataSource for FakeMetadataSource {
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

    /// A quorum that has committed nothing and knows no voters. Its leader
    /// comes from the fake's leader channel, so `watch_leader` and
    /// `quorum_state` cannot disagree.
    fn quorum_state(&self) -> QuorumState {
        QuorumState {
            current_term: 0,
            last_applied_index: 0,
            current_leader: *self.leader_tx.borrow(),
            voters: Vec::new(),
            voter_nodes: std::collections::BTreeMap::new(),
            per_voter_matched_index: std::collections::BTreeMap::new(),
        }
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

    async fn cancel(&self) {}
}
