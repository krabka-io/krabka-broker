//! `Fetch` (`api_key=1`) with long-poll support through per-partition
//! `Notify::notified()` futures.
//!
//! The handler returns records as verbatim `RecordsPayload::Raw` bytes. These
//! are the on-disk `.log` bytes for whole v2 batches. `Log::read_raw` reads
//! them decode-free, and the handler clamps them at the visibility window:
//! the high watermark for `read_uncommitted` consumer fetches, `lso.min(hw)`
//! for `read_committed`, and the log-end offset (LEO) for follower fetches.
//!
//! KFC-1 adds one more cap to a consumer fetch, and only to a consumer fetch:
//! the partition's delivery watermark, which is the first offset whose batch
//! has not reached its activation time. The handler recomputes it under the log
//! mutex it already holds, so it can never serve a record before it is due. A
//! topic that delivers immediately answers the log end offset for it with no
//! I/O, so that cap never binds and the zero-copy path is untouched.
//!
//! `read_committed` does NO server-side batch filtering. Aborted batches and
//! control batches stay in the byte stream, and the consumer drops them on
//! the client side with the `aborted_transactions` list. This matches Apache
//! Kafka.

use bytes::BytesMut;
use krabka_log::Offset;
use krabka_protocol::{
    Decode, Encode,
    owned::{fetch_request::FetchRequest, fetch_response::FetchResponse},
};

mod plan;
mod read;
mod read_loop;
mod remote;
mod request;
mod response;
mod session;
mod throttle;

pub(crate) use self::plan::PendingRead;
use self::{
    plan::{PendingPlanContext, build_pending_reads},
    read_loop::execute_pending_reads,
    request::{FetchPreparation, prepare_fetch},
    response::{downconvert_legacy_responses, record_fetch_metrics},
    session::finalize_fetch_session,
    throttle::{apply_consumer_fetch_quota, throttle_follower_responses},
};
use crate::{
    broker::Broker, error::BrokerError, fetch_session::INVALID_SESSION_ID,
    handlers::cluster_action_denied,
};

/// Handle a `Fetch` request and return the not-yet-encoded response
/// **struct** with the negotiated `version`.
///
/// The dispatch layer turns the struct into a zero-copy write-plan for v4+
/// with the canonical codec, or into a legacy copy-encoded frame for v0–v3.
/// The function returns the struct and not `Bytes`, so the connection writer
/// can split out each partition's records region as a separate write segment.
/// It does not need to materialize the whole body.
#[tracing::instrument(
    name = "handle_fetch",
    level = "info",
    skip_all,
    fields(api = "Fetch", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<(FetchResponse, i16), BrokerError> {
    // KIP-124 request_percentage meters server-side handler time; capture the
    // start so the request throttle can be combined with the consumer
    // byte-rate throttle below (KIP-219).
    let handler_start = std::time::Instant::now();
    let mut cur: &[u8] = req_bytes;
    let req: FetchRequest = if version < 4 {
        krabka_protocol::kafka_3_6_2::owned::fetch_request::FetchRequest::decode(&mut cur, version)?
            .into()
    } else {
        FetchRequest::decode(&mut cur, version)?
    };

    let preparation = match prepare_fetch(broker, &req, ctx) {
        Ok(preparation) => preparation,
        Err(code) => {
            let resp = FetchResponse {
                error_code: code,
                session_id: INVALID_SESSION_ID,
                responses: Vec::new(),
                ..Default::default()
            };
            return Ok((resp, version));
        }
    };
    let FetchPreparation {
        decision,
        effective_topics,
        image,
        denied_topics,
        effective_replica_id,
        is_follower_fetch,
        read_committed,
    } = preparation;

    if version >= crate::wal::quorum::wire::KIP_595_FETCH_VERSION
        && crate::wal::quorum::wire::decode_fetch_request(&req).is_some()
    {
        if cluster_action_denied(broker.config.authorizer.as_ref(), &image, ctx) {
            return Ok((cluster_authorization_failed(), version));
        }
        let Some(authenticated_node) = broker.wal_shards.authenticated_node_id(ctx.principal)
        else {
            return Ok((cluster_authorization_failed(), version));
        };
        if denied_topics.is_empty()
            && let Some(response) = broker
                .wal_shards
                .route_fetch_request(&req, authenticated_node)
        {
            return Ok((response?, version));
        }
    }

    let plan_context = PendingPlanContext {
        broker,
        image: &image,
        denied_topics: &denied_topics,
        rack_id: &req.rack_id,
        mode: (read_committed, is_follower_fetch),
        follower_id: effective_replica_id,
    };
    let pending = build_pending_reads(&plan_context, &effective_topics).await;

    let (mut responses, cpu_micros_by_idx) = execute_pending_reads(
        broker,
        pending,
        req.min_bytes,
        req.max_wait_ms,
        ctx.sendfile_capable,
    )
    .await?;

    downconvert_legacy_responses(broker, version, &mut responses);

    if is_follower_fetch {
        throttle_follower_responses(broker, &image, effective_replica_id, &mut responses);
    }

    let throttle_time_ms_val = if is_follower_fetch {
        0
    } else {
        apply_consumer_fetch_quota(broker, &image, ctx, handler_start, &responses).await
    };

    record_fetch_metrics(broker, &responses, &cpu_micros_by_idx, is_follower_fetch);

    let response_session_id = finalize_fetch_session(
        broker,
        &decision,
        &effective_topics,
        &mut responses,
        is_follower_fetch,
        &ctx.principal.name,
    );

    let resp = FetchResponse {
        throttle_time_ms: throttle_time_ms_val,
        error_code: 0,
        session_id: response_session_id,
        responses,
        ..Default::default()
    };
    Ok((resp, version))
}

fn cluster_authorization_failed() -> FetchResponse {
    FetchResponse {
        error_code: crate::codes::CLUSTER_AUTHORIZATION_FAILED,
        session_id: INVALID_SESSION_ID,
        ..Default::default()
    }
}

/// The pure read-path visibility decision.
///
/// From a partition's watermarks and a fetch's parameters, it gives the
/// offsets that the fetch may expose and the HW and LSO that it reports. It
/// sits apart from [`do_read`], so it is the single source of truth for the
/// response fields on both the `OFFSET_OUT_OF_RANGE` path and the success
/// path. `fetch_visibility_model.rs` covers it with exhaustive tests and
/// property tests in isolation.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct VisibilityWindow {
    /// `fetch_offset < log_start`. The caller returns `OFFSET_OUT_OF_RANGE`.
    pub out_of_range: bool,
    /// `fetch_offset >= upper_bound`. There is nothing to read, and no bytes
    /// go out.
    pub empty: bool,
    /// Exclusive upper offset the raw read may expose: `[fetch_offset, limit_offset)`.
    ///
    /// On a consumer fetch this is capped at the delivery watermark as well
    /// (KFC-1), so a batch that has not reached its activation time never
    /// leaves the broker. Every path that turns bytes into a response honours
    /// it, including the diskless hot-tail cache.
    pub limit_offset: Offset,
    /// `read_committed` aborted-txn scan ceiling. It is `lso.min(hw)` for a
    /// `read_committed` consumer, and `lso` in every other case.
    pub effective_lso: Offset,
    /// Whether to fill `aborted_transactions`, which happens for a
    /// `read_committed` consumer.
    pub read_committed_aborts: bool,
    /// `out.high_watermark` to report.
    pub response_hw: Offset,
    /// `out.last_stable_offset` to report.
    pub response_lso: Offset,
}

/// The partition offsets a fetch reads to decide what it may expose.
///
/// Kafka invariants that the caller upholds: `0 <= log_start <= hw <= log_end`
/// and `lso <= hw`. KFC-1 adds `log_start <= deliverable <= hw`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct FetchWatermarks {
    pub log_start: Offset,
    pub hw: Offset,
    pub lso: Offset,
    pub log_end: Offset,
    /// KFC-1's delivery watermark: the first offset that is not due yet. On a
    /// topic that delivers immediately it is the high watermark, so the window
    /// is the one Kafka computes and the zero-copy read path is unchanged.
    pub deliverable: Offset,
}

/// The caller sets `read_committed` for consumer fetches only, so
/// `read_committed` implies `!is_follower`.
pub(crate) fn compute_visibility_window(
    is_follower: bool,
    read_committed: bool,
    watermarks: FetchWatermarks,
    fetch_offset: Offset,
) -> VisibilityWindow {
    let verified = krabka_verified::fetch_visibility(
        is_follower,
        read_committed,
        krabka_verified::FetchWatermarks {
            log_start: watermarks.log_start.0,
            hw: watermarks.hw.0,
            lso: watermarks.lso.0,
            log_end: watermarks.log_end.0,
            deliverable: watermarks.deliverable.0,
        },
        fetch_offset.0,
    );
    VisibilityWindow {
        out_of_range: verified.out_of_range,
        empty: verified.empty,
        limit_offset: Offset(verified.limit_offset),
        effective_lso: Offset(verified.effective_lso),
        read_committed_aborts: verified.read_committed_aborts,
        response_hw: Offset(verified.response_hw),
        response_lso: Offset(verified.response_lso),
    }
}

/// Encode a `FetchResponse` into a `BytesMut`.
///
/// The function chooses the legacy `kafka_3_6_2` codec for Fetch v0-3 and the
/// current canonical codec for v4+. This version boundary matches the
/// request-decode boundary.
pub(crate) fn encode_fetch_response(
    resp: FetchResponse,
    version: i16,
) -> Result<BytesMut, crate::error::BrokerError> {
    if version < 4 {
        let legacy: krabka_protocol::kafka_3_6_2::owned::fetch_response::FetchResponse =
            resp.into();
        let mut buf = BytesMut::with_capacity(legacy.encoded_len(version));
        legacy.encode(&mut buf, version)?;
        Ok(buf)
    } else {
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Mutex};

    use assert2::assert;
    use bytes::{Bytes, BytesMut};
    use krabka_ids::PartitionIndex;
    use krabka_log::{Log, LogConfig, Offset};
    use krabka_metadata::AclOperation;
    use krabka_protocol::{
        Encode as _,
        records::{Record, RecordBatch, RecordsPayload},
    };
    use krabka_security::{AuthMethod, Principal};

    use crate::{
        authorizer::{AuthorizationRequest, AuthorizationResult, Authorizer},
        broker::Broker,
        handlers::RequestContext,
        wal::quorum::{
            engine::WalShardEngine,
            registry::ShardId,
            wire::{KIP_595_FETCH_VERSION, QuorumGroup, fetch_request},
        },
    };

    #[derive(Debug)]
    struct TopicReadOnly;

    impl Authorizer for TopicReadOnly {
        fn authorize(
            &self,
            _source: &dyn crate::authorizer::AclSource,
            request: &AuthorizationRequest<'_>,
        ) -> AuthorizationResult {
            if request.operation == AclOperation::Read {
                AuthorizationResult::Allow
            } else {
                AuthorizationResult::Deny
            }
        }
    }

    #[tokio::test]
    async fn handle_routes_discriminated_wal_fetch_on_broker_listener() {
        let dir = tempfile::tempdir().expect("tempdir");
        let broker_handle = Broker::start(crate::config::BrokerConfig::for_tests(
            dir.path().to_path_buf(),
        ))
        .await
        .expect("start broker");
        let broker = broker_handle.broker_arc_for_test();
        let source = std::sync::Arc::new(Mutex::new(
            Log::open(dir.path().join("wal-source"), LogConfig::default()).expect("open log"),
        ));
        let mut batch = RecordBatch {
            records: vec![Record {
                attributes: 0,
                offset_delta: 0,
                timestamp_delta: 0,
                key: None,
                value: Some(Bytes::from_static(b"wal")),
                headers: vec![],
            }],
            ..Default::default()
        };
        source
            .lock()
            .expect("log")
            .append_at(&mut batch, Offset(0))
            .expect("append");
        source.lock().expect("log").sync().expect("sync");
        let topic_id = uuid::Uuid::from_u128(0xD15C);
        let local_node_id = broker.config.node_id;
        broker.wal_shards.insert(
            ShardId {
                topic_id,
                partition: PartitionIndex(0),
            },
            std::sync::Arc::new(WalShardEngine::for_logs(
                maplit::btreemap! {krabka_raft::NodeId(1) => source},
            )),
        );
        broker
            .wal_shards
            .replace_placements(&maplit::hashmap! {ShardId {
                topic_id,
                partition: PartitionIndex(0),
            } => vec![local_node_id, krabka_raft::NodeId(2)]});
        let principal = Principal {
            name: "broker-2".into(),
            auth_method: AuthMethod::SaslPlain,
            groups: Vec::new(),
        };
        let peer = SocketAddr::from(([127, 0, 0, 1], 9092));
        let context = RequestContext::new(&principal, &peer, "wal-fetch", "test", false, "");

        for version in [KIP_595_FETCH_VERSION, 18] {
            let request = fetch_request(
                QuorumGroup::diskless_wal(topic_id, PartitionIndex(0)),
                krabka_raft::NodeId(2),
                0,
                0,
                krabka_units::mebibytes(1),
            );
            let mut encoded = BytesMut::new();
            request
                .encode(&mut encoded, version)
                .expect("encode WAL fetch");
            let (response, response_version) =
                super::handle(&broker, version, 1, &encoded, &context)
                    .await
                    .expect("route WAL fetch");

            assert!(response_version == version);
            let partition = &response.responses[0].partitions[0];
            assert!(partition.high_watermark == 1);
            assert!(matches!(
                partition.records,
                Some(RecordsPayload::Raw(ref records)) if !records.is_empty()
            ));
        }
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn wal_fetch_requires_cluster_action() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut config = crate::config::BrokerConfig::for_tests(dir.path().to_path_buf());
        config.authorizer = std::sync::Arc::new(TopicReadOnly);
        let broker_handle = Broker::start(config).await.expect("start broker");
        let broker = broker_handle.broker_arc_for_test();
        let request = fetch_request(
            QuorumGroup::diskless_wal(uuid::Uuid::from_u128(0xD15C), PartitionIndex(0)),
            krabka_raft::NodeId(2),
            0,
            0,
            krabka_units::mebibytes(1),
        );
        let mut encoded = BytesMut::new();
        request
            .encode(&mut encoded, KIP_595_FETCH_VERSION)
            .expect("encode WAL fetch");
        let principal = Principal {
            name: "broker-2".into(),
            auth_method: AuthMethod::SaslPlain,
            groups: Vec::new(),
        };
        let peer = SocketAddr::from(([127, 0, 0, 1], 9092));
        let context = RequestContext::new(&principal, &peer, "wal-fetch", "test", false, "");

        let (response, _) = super::handle(&broker, KIP_595_FETCH_VERSION, 1, &encoded, &context)
            .await
            .expect("deny WAL fetch");

        assert!(response.error_code == crate::codes::CLUSTER_AUTHORIZATION_FAILED);
        assert!(response.responses.is_empty());
        broker_handle.shutdown().await;
    }
}

#[cfg(test)]
#[path = "fetch_visibility_model.rs"]
mod fetch_visibility_model;

#[cfg(test)]
mod visibility_fuzz;
