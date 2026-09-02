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
//! [`BrokenTimer`] is the one fixture here that is not handler scaffolding.
//! Every broker cadence loop takes its ticker through an injectable field, and
//! every one of them stops when that ticker gives out, so the fake that makes a
//! ticker give out is shared rather than copied into each of their test
//! modules.

use std::{
    io,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use bytes::{Bytes, BytesMut};
use krabka_protocol::{Decode, Encode};
use krabka_security::{AuthMethod, Principal};
use qubit_clock::{
    MonotonicClock, MonotonicInstant, StdMonotonicClock, TimeError, Timer, TimerFuture,
    TimerUnavailableError,
};

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
