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

use std::{
    collections::BTreeSet,
    io,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{self, AtomicUsize, Ordering},
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
use qubit_clock::{
    MonotonicClock, MonotonicInstant, StdMonotonicClock, TimeError, Timer, TimerFuture,
    TimerUnavailableError,
};
use tokio::sync::watch;

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
        throttle: crate::quota::ThrottleSlot::default(),
        listener_authorized_cluster_action: false,
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
        FakeMetadataSource {
            image_tx,
            leader_tx,
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use assert2::assert;
    use krabka_metadata::{KRaftVersionRange, MetadataRecord, Voter};
    use krabka_raft::{
        AddVoter, Node, NodeId, QuorumState, RaftError, RemoveVoter, SnapshotRange,
        SubmitChangeResult, UpdateVoter,
    };

    use super::FakeMetadataSource;
    use crate::metadata_source::MetadataSource;

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
