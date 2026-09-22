//! `StreamsGroupHeartbeat` (`api_key` 88), the KIP-1071 streams rebalance
//! protocol. The handler routes the request to the per-group streams actor in
//! `GroupCoordinator`.
//!
//! It mirrors the KIP-932 share-group heartbeat handler
//! ([`super::share_group_heartbeat`]): decode, gate, `mark_streams` and
//! `get_or_create_streams`, send a `Heartbeat` actor message, await the
//! oneshot, then encode.
//!
//! Two gates gate it: the finalized `streams.version >= 1` feature, which is
//! KIP-1071 early access, AND the `streams_group.enable` config kill-switch.
//! BOTH must allow the request.

use bytes::Bytes;
use krabka_protocol::{
    Decode,
    owned::{
        streams_group_heartbeat_request::StreamsGroupHeartbeatRequest,
        streams_group_heartbeat_response::StreamsGroupHeartbeatResponse,
    },
};
use tokio::sync::oneshot;

use crate::{
    broker::Broker, codes, coordinator::unified::streams::actor::StreamsGroupActorMessage,
    error::BrokerError, handlers::group_read_denied, time_util::now_ms,
};

mod validation;

#[tracing::instrument(
    name = "handle_streams_group_heartbeat",
    level = "info",
    skip_all,
    fields(api = "StreamsGroupHeartbeat", version, req_bytes = req_bytes.len()),
    err,
)]
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let streams_enabled = broker.config.streams_group.enable;
    let image = broker.controller.current_image();
    let ng = broker.group_coordinator.clone();
    {
        let mut cur: &[u8] = req_bytes;
        let req = StreamsGroupHeartbeatRequest::decode(&mut cur, version)?;

        // KafkaApis answers UNSUPPORTED_VERSION before the group ACL when the
        // streams protocol is off: KIP-1071 gates it on a finalized
        // streams.version >= 1 (early access, default-disabled), and krabka
        // also on the `streams_group.enable` config kill-switch.
        if !crate::features::feature_enabled(&image, crate::features::STREAMS_VERSION, 1)
            || !streams_enabled
        {
            return crate::handlers::encode_response(&error(codes::UNSUPPORTED_VERSION), version);
        }

        // ── ACL preamble ────────────────────────────────────────────
        // `Read` on `Group(group_id)`. On Deny → whole-response
        // `error_code = GROUP_AUTHORIZATION_FAILED (30)`. Topology/topic ACLs
        // are not evaluated by this handler.
        if group_read_denied(
            broker.config.authorizer.as_ref(),
            &image,
            ctx,
            &req.group_id,
        ) {
            return crate::handlers::encode_response(
                &error(codes::GROUP_AUTHORIZATION_FAILED),
                version,
            );
        }

        // Kafka's `GroupCoordinatorService` checks the request before it
        // schedules the write on the coordinator, so a refused request changes
        // no group and never gets NOT_COORDINATOR.
        if let Some((error_code, message)) = validation::request_error(&req) {
            return crate::handlers::encode_response(
                &crate::coordinator::unified::streams::actor::response::error_resp(
                    error_code,
                    Some(message),
                ),
                version,
            );
        }

        if let Some(error_code) = crate::handlers::group_coordinator_error(broker, &req.group_id) {
            return crate::handlers::encode_response(&error(error_code), version);
        }

        // Kafka creates a streams group only on a join, in place of nothing or of
        // an empty classic group (a KIP-1071 cold upgrade converts it here), and
        // answers GROUP_ID_NOT_FOUND to anything else.
        if let Some(message) = ng
            .streams_group_lookup_error(&req.group_id, req.member_epoch, now_ms())
            .await?
        {
            return crate::handlers::encode_response(
                &crate::coordinator::unified::streams::actor::response::error_resp(
                    codes::GROUP_ID_NOT_FOUND,
                    Some(message),
                ),
                version,
            );
        }

        ng.mark_streams(&req.group_id);
        let handle = ng.get_or_create_streams(&req.group_id);
        let (tx, rx) = oneshot::channel();
        if handle
            .tx
            .send(StreamsGroupActorMessage::Heartbeat {
                request: Box::new(req),
                client_id: ctx.client_id.to_owned(),
                client_host: ctx.client_host(),
                reply: tx,
            })
            .await
            .is_err()
        {
            return crate::handlers::encode_response(
                &error(codes::COORDINATOR_LOAD_IN_PROGRESS),
                version,
            );
        }
        let resp = rx
            .await
            .unwrap_or_else(|_| error(codes::UNKNOWN_SERVER_ERROR));
        crate::handlers::encode_response(&resp, version)
    }
}

/// Kafka's `StreamsGroupHeartbeatRequest.getErrorResponse`: the error code
/// and the defaults of the generated response data, whose status list is
/// empty.
fn error(code: i16) -> StreamsGroupHeartbeatResponse {
    crate::coordinator::unified::streams::actor::response::error_resp(code, None)
}

#[cfg(test)]
mod tests {
    use std::{net::SocketAddr, sync::Arc};

    use assert2::assert;
    use krabka_metadata::{FeatureLevelRecord, MetadataRecord};
    use krabka_protocol::owned::streams_group_heartbeat_response;
    use krabka_security::Principal;

    /// A valid join of member `m1` with a one-subtopology topology.
    fn request(group_id: &str) -> StreamsGroupHeartbeatRequest {
        use krabka_protocol::owned::streams_group_heartbeat_request::{Subtopology, Topology};

        StreamsGroupHeartbeatRequest {
            group_id: group_id.into(),
            member_id: "m1".into(),
            member_epoch: 0,
            rebalance_timeout_ms: 1_000,
            active_tasks: Some(vec![]),
            standby_tasks: Some(vec![]),
            warmup_tasks: Some(vec![]),
            topology: Some(Topology {
                epoch: 1,
                subtopologies: vec![Subtopology {
                    subtopology_id: "0".into(),
                    source_topics: vec!["in".into()],
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    /// Kafka creates a streams group only on a join, and answers
    /// `GROUP_ID_NOT_FOUND` to a heartbeat or a leave for a group that does not
    /// exist and to any heartbeat for a group of another type
    /// (`getOrCreateStreamsGroup`, `getStreamsGroupOrThrow`, `streamsGroup`).
    /// Each row sends one request for its own group id, compares the whole
    /// response, and checks whether a streams group exists afterwards.
    #[tokio::test]
    async fn handle_finds_or_creates_the_streams_group_as_kafka_does() {
        use crate::coordinator::unified::{
            actor::GroupKindTag, streams::actor::response::error_resp,
        };

        let version = streams_group_heartbeat_response::MAX_VERSION;
        let (broker_handle, _dir) = start_broker(true).await;
        let broker = broker_handle.broker_arc_for_test();
        finalize_streams_version(&broker).await;
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = context(&principal, &peer);
        broker.group_coordinator.mark_share("share");
        let _consumer = broker
            .group_coordinator
            .get_or_create_group("consumer", GroupKindTag::Consumer);
        let heartbeat = |group_id: &str, member_epoch| StreamsGroupHeartbeatRequest {
            group_id: group_id.into(),
            member_id: "m1".into(),
            member_epoch,
            ..Default::default()
        };
        let not_found =
            |message: String| Some(error_resp(codes::GROUP_ID_NOT_FOUND, Some(message)));
        // (group id, request, expected error response or None for success,
        // a streams group exists afterwards)
        let rows = [
            (
                "absent-heartbeat",
                heartbeat("absent-heartbeat", 3),
                not_found("Streams group absent-heartbeat not found.".into()),
                false,
            ),
            (
                "absent-leave",
                heartbeat("absent-leave", -1),
                not_found("Group absent-leave not found.".into()),
                false,
            ),
            (
                "share",
                request("share"),
                not_found("Group share is not a streams group.".into()),
                false,
            ),
            (
                "consumer",
                request("consumer"),
                not_found("Group consumer is not a streams group.".into()),
                false,
            ),
            ("absent-join", request("absent-join"), None, true),
        ];

        for (group_id, req, expected, exists) in rows {
            let bytes = handle(&broker, version, 1, &encode_request(&req), &ctx)
                .await
                .expect("handle");
            let resp = decode_response(&bytes);
            match expected {
                Some(expected) => assert!(resp == expected, "{group_id}"),
                None => assert!(resp.error_code == codes::NONE, "{group_id}: {resp:?}"),
            }
            assert!(
                broker.group_coordinator.find_streams(group_id).is_some() == exists,
                "{group_id}"
            );
        }
        broker_handle.shutdown().await;
    }

    /// Kafka's `GroupCoordinatorService` refuses an invalid request before the
    /// coordinator runs it, so the response carries only the error and the
    /// group is not created.
    #[tokio::test]
    async fn handle_refuses_an_invalid_request_and_creates_no_group() {
        let version = streams_group_heartbeat_response::MAX_VERSION;
        let (broker_handle, _dir) = start_broker(true).await;
        let broker = broker_handle.broker_arc_for_test();
        finalize_streams_version(&broker).await;
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = context(&principal, &peer);
        let req = StreamsGroupHeartbeatRequest {
            member_id: String::new(),
            ..request("invalid-join")
        };

        let bytes = handle(&broker, version, 1, &encode_request(&req), &ctx)
            .await
            .expect("handle");

        assert!(
            decode_response(&bytes)
                == crate::coordinator::unified::streams::actor::response::error_resp(
                    codes::INVALID_REQUEST,
                    Some("MemberId can't be empty.".into()),
                )
        );
        assert!(
            broker
                .group_coordinator
                .find_streams("invalid-join")
                .is_none()
        );
        broker_handle.shutdown().await;
    }

    fn encode_request(req: &StreamsGroupHeartbeatRequest) -> Bytes {
        crate::test_support::encode_request(req, streams_group_heartbeat_response::MAX_VERSION)
    }

    fn decode_response(bytes: &Bytes) -> StreamsGroupHeartbeatResponse {
        crate::test_support::decode_response(bytes, streams_group_heartbeat_response::MAX_VERSION)
    }

    fn principal() -> Principal {
        crate::test_support::principal("alice")
    }

    fn context<'a>(
        principal: &'a Principal,
        peer: &'a SocketAddr,
    ) -> crate::handlers::RequestContext<'a> {
        crate::test_support::request_context(principal, peer, "streams-client")
    }

    async fn start_broker(
        streams_enabled: bool,
    ) -> (crate::broker::BrokerHandle, tempfile::TempDir) {
        crate::test_support::start_broker_with(|cfg| {
            cfg.authorizer = Arc::new(crate::authorizer::AllowAllAuthorizer);
            cfg.streams_group.enable = streams_enabled;
        })
        .await
    }

    async fn finalize_streams_version(broker: &Broker) {
        broker
            .controller
            .submit_change(vec![MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: crate::features::STREAMS_VERSION.into(),
                level: 1,
            })])
            .await
            .expect("submit streams.version");

        tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                if broker
                    .controller
                    .current_image()
                    .finalized_feature(crate::features::STREAMS_VERSION)
                    == Some(1)
                {
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("streams.version visible");
    }

    #[tokio::test]
    async fn handle_unfinalized_feature_returns_unsupported_version_with_read_allowed() {
        let version = streams_group_heartbeat_response::MAX_VERSION;
        let (broker_handle, _dir) = start_broker(true).await;
        let broker = broker_handle.broker_arc_for_test();
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = context(&principal, &peer);
        let req_bytes = encode_request(&request("streams-app-disabled-feature"));

        let resp = handle(&broker, version, 1, &req_bytes, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp);

        assert!(resp.error_code == codes::UNSUPPORTED_VERSION, "{resp:?}");
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_disabled_config_returns_unsupported_version_when_feature_finalized() {
        let version = streams_group_heartbeat_response::MAX_VERSION;
        let (broker_handle, _dir) = start_broker(false).await;
        let broker = broker_handle.broker_arc_for_test();
        finalize_streams_version(&broker).await;
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = context(&principal, &peer);
        let req_bytes = encode_request(&request("streams-app-disabled-config"));

        let resp = handle(&broker, version, 1, &req_bytes, &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp);

        assert!(resp.error_code == codes::UNSUPPORTED_VERSION, "{resp:?}");
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_persists_request_client_identity() {
        let version = streams_group_heartbeat_response::MAX_VERSION;
        let (broker_handle, _dir) = start_broker(true).await;
        let broker = broker_handle.broker_arc_for_test();
        finalize_streams_version(&broker).await;
        let principal = principal();
        let peer: SocketAddr = "127.0.0.1:9092".parse().unwrap();
        let ctx = context(&principal, &peer);

        let bytes = handle(
            &broker,
            version,
            1,
            &encode_request(&request("identity-group")),
            &ctx,
        )
        .await
        .expect("StreamsGroupHeartbeat handler");
        assert!(decode_response(&bytes).error_code == 0);

        let actor = broker
            .group_coordinator
            .get_or_create_streams("identity-group");
        let (tx, rx) = tokio::sync::oneshot::channel();
        actor
            .tx
            .send(StreamsGroupActorMessage::Describe { reply: tx })
            .await
            .expect("describe streams group");
        let view = rx.await.expect("streams group view");

        assert!(view.members.len() == 1);
        assert!(view.members[0].client_id == "streams-client");
        assert!(view.members[0].client_host == "/127.0.0.1");

        let peer: SocketAddr = "127.0.0.2:9093".parse().unwrap();
        let ctx = crate::test_support::request_context(&principal, &peer, "streams-client-b");
        let req = StreamsGroupHeartbeatRequest {
            group_id: "identity-group".into(),
            member_id: view.members[0].member_id.clone(),
            member_epoch: view.members[0].member_epoch,
            ..Default::default()
        };
        let bytes = handle(&broker, version, 2, &encode_request(&req), &ctx)
            .await
            .expect("StreamsGroupHeartbeat identity refresh");
        assert!(decode_response(&bytes).error_code == 0);

        let (tx, rx) = tokio::sync::oneshot::channel();
        actor
            .tx
            .send(StreamsGroupActorMessage::Describe { reply: tx })
            .await
            .expect("describe refreshed streams group");
        let view = rx.await.expect("refreshed streams group view");
        assert!(view.members[0].client_id == "streams-client-b");
        assert!(view.members[0].client_host == "/127.0.0.2");

        broker_handle.shutdown().await;
    }

    use super::*;

    #[test]
    fn group_read_denied_yields_group_authorization_failed() {
        use krabka_protocol::owned::streams_group_heartbeat_response::{
            self, StreamsGroupHeartbeatResponse,
        };

        let authorizer =
            crate::authorizer::SimpleAclAuthorizer::new(std::collections::HashSet::new());
        let image = krabka_metadata::MetadataImage::new(uuid::Uuid::nil());
        let principal = krabka_security::Principal {
            name: "ANONYMOUS".into(),
            auth_method: krabka_security::AuthMethod::Anonymous,
            groups: vec![],
        };
        let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));
        let ctx = crate::test_support::request_context(&principal, &peer, "streams-client");

        assert!(group_read_denied(&authorizer, &image, &ctx, "g"));

        let bytes = crate::handlers::encode_response(
            &error(codes::GROUP_AUTHORIZATION_FAILED),
            streams_group_heartbeat_response::MAX_VERSION,
        )
        .expect("encode");
        let mut cur: &[u8] = &bytes;
        let resp = StreamsGroupHeartbeatResponse::decode(
            &mut cur,
            streams_group_heartbeat_response::MAX_VERSION,
        )
        .unwrap();
        assert!(resp.error_code == codes::GROUP_AUTHORIZATION_FAILED);
    }
}
