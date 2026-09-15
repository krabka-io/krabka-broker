//! `AllocateProducerIds` (`api_key=67`). Reserves one durable, cluster-wide
//! producer-ID block for a registered broker.

use bytes::Bytes;
use krabka_metadata::NodeId;
use krabka_protocol::{
    Decode,
    owned::{
        allocate_producer_ids_request::AllocateProducerIdsRequest,
        allocate_producer_ids_response::AllocateProducerIdsResponse,
    },
};

use crate::{
    broker::Broker,
    codes,
    error::BrokerError,
    producer_id_manager::{ProducerIdAllocationError, allocate_block},
};

/// Checks `ClusterAction` on the cluster, then allocates a block.
///
/// Kafka's `ControllerApis.handleAllocateProducerIdsRequest` calls
/// `authorizeClusterOperation(request, CLUSTER_ACTION)` before the controller
/// runs. A denial becomes `AllocateProducerIdsRequest.getErrorResponse`:
/// `CLUSTER_AUTHORIZATION_FAILED` and the default block fields, and no block
/// is allocated. A request that arrives in an `Envelope` runs this check
/// against the principal that the envelope names.
pub(crate) async fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
    ctx: &crate::handlers::RequestContext<'_>,
) -> Result<Bytes, BrokerError> {
    let mut input: &[u8] = req_bytes;
    let request = AllocateProducerIdsRequest::decode(&mut input, version)?;
    if crate::handlers::cluster_action_denied(
        broker.config.authorizer.as_ref(),
        &broker.controller.current_image(),
        ctx,
    ) {
        return crate::handlers::encode_response(
            &AllocateProducerIdsResponse {
                error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
                ..Default::default()
            },
            version,
        );
    }
    serve(broker, version, &request).await
}

async fn serve(
    broker: &Broker,
    version: i16,
    request: &AllocateProducerIdsRequest,
) -> Result<Bytes, BrokerError> {
    let controller = broker.controller.clone();
    let result = match u64::try_from(request.broker_id) {
        Ok(broker_id) => allocate_block(&controller, NodeId(broker_id), request.broker_epoch).await,
        Err(_) => Err(ProducerIdAllocationError::BrokerNotRegistered(NodeId(
            u64::MAX,
        ))),
    };

    let response = match result {
        Ok(block) => AllocateProducerIdsResponse {
            error_code: codes::NONE,
            producer_id_start: block.first,
            producer_id_len: block.len,
            ..Default::default()
        },
        Err(error) => {
            let error_code = match error {
                ProducerIdAllocationError::BrokerNotRegistered(_) => {
                    codes::BROKER_ID_NOT_REGISTERED
                }
                ProducerIdAllocationError::StaleBrokerEpoch { .. } => codes::STALE_BROKER_EPOCH,
                ProducerIdAllocationError::InvalidFrontier { .. }
                | ProducerIdAllocationError::Exhausted
                | ProducerIdAllocationError::Controller(_) => codes::UNKNOWN_SERVER_ERROR,
            };
            tracing::warn!(%error, "AllocateProducerIds failed");
            AllocateProducerIdsResponse {
                error_code,
                producer_id_start: -1,
                producer_id_len: 0,
                ..Default::default()
            }
        }
    };
    crate::handlers::encode_response(&response, version)
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    crate::test_support::codec_helpers!(
        AllocateProducerIdsRequest,
        AllocateProducerIdsResponse,
        version = 0
    );

    /// Serves a request as a principal that the default `AllowAllAuthorizer`
    /// allows.
    async fn handle_allowed(
        broker: &Broker,
        version: i16,
        correlation_id: i32,
        body: &[u8],
    ) -> Result<Bytes, BrokerError> {
        let user = crate::test_support::principal("ANONYMOUS");
        let address = crate::test_support::peer();
        let ctx = crate::test_support::request_context(&user, &address, "allocate-test");
        handle(broker, version, correlation_id, body, &ctx).await
    }

    #[tokio::test]
    async fn allocates_consecutive_durable_blocks_and_fences_stale_epochs() {
        let (broker_handle, _dir) = crate::test_support::start_broker_with(|config| {
            config.audit_enabled = false;
        })
        .await;
        let broker = broker_handle.broker_arc_for_test();
        let broker_id = i32::try_from(broker.config.node_id.0).unwrap();
        let broker_epoch = broker
            .controller
            .current_image()
            .broker_epoch(broker.config.node_id)
            .expect("registered broker epoch");
        let request = |epoch| AllocateProducerIdsRequest {
            broker_id,
            broker_epoch: epoch,
            ..Default::default()
        };

        let first = decode_response(
            &handle_allowed(&broker, 0, 1, &encode_request(&request(broker_epoch)))
                .await
                .unwrap(),
        );
        let second = decode_response(
            &handle_allowed(&broker, 0, 2, &encode_request(&request(broker_epoch)))
                .await
                .unwrap(),
        );
        assert!(first.error_code == codes::NONE);
        assert!(first.producer_id_start == 0);
        assert!(first.producer_id_len == 1_000);
        assert!(second.producer_id_start == 1_000);
        assert!(broker.controller.current_image().next_producer_id() == 2_000);

        // Both calls may observe the same candidate frontier. The controller
        // accepts one exact record, and the loser retries from the committed
        // boundary instead of returning an overlapping block.
        let concurrent = encode_request(&request(broker_epoch));
        let (third_bytes, fourth_bytes) = tokio::join!(
            handle_allowed(&broker, 0, 3, &concurrent),
            handle_allowed(&broker, 0, 4, &concurrent),
        );
        let third = decode_response(&third_bytes.unwrap());
        let fourth = decode_response(&fourth_bytes.unwrap());
        let mut concurrent_starts = [third.producer_id_start, fourth.producer_id_start];
        concurrent_starts.sort_unstable();
        assert!(concurrent_starts == [2_000, 3_000]);
        assert!(third.producer_id_len == 1_000);
        assert!(fourth.producer_id_len == 1_000);
        assert!(broker.controller.current_image().next_producer_id() == 4_000);

        let stale = decode_response(
            &handle_allowed(&broker, 0, 5, &encode_request(&request(broker_epoch - 1)))
                .await
                .unwrap(),
        );
        assert!(stale.error_code == codes::STALE_BROKER_EPOCH);
        assert!(stale.producer_id_start == -1);

        let malformed = AllocateProducerIdsRequest {
            broker_id: -1,
            broker_epoch,
            ..Default::default()
        };
        let malformed = decode_response(
            &handle_allowed(&broker, 0, 6, &encode_request(&malformed))
                .await
                .unwrap(),
        );
        assert!(malformed.error_code == codes::BROKER_ID_NOT_REGISTERED);
        assert!(malformed.producer_id_start == -1);
        assert!(malformed.producer_id_len == 0);

        // Seed the last frontier that cannot fit another positive block. The
        // adapter must fail without submitting a wrapped or partial range.
        broker
            .controller
            .submit_change(vec![krabka_metadata::MetadataRecord::V1ProducerIds(
                krabka_metadata::ProducerIdsRecord {
                    broker_id: broker.config.node_id,
                    broker_epoch,
                    next_producer_id: i64::MAX - 999,
                },
            )])
            .await
            .expect("seed producer ID limit");
        let exhausted = decode_response(
            &handle_allowed(&broker, 0, 7, &encode_request(&request(broker_epoch)))
                .await
                .unwrap(),
        );
        assert!(exhausted.error_code == codes::UNKNOWN_SERVER_ERROR);
        assert!(exhausted.producer_id_start == -1);
        assert!(exhausted.producer_id_len == 0);
        assert!(broker.controller.current_image().next_producer_id() == i64::MAX - 999);
        broker_handle.shutdown().await;
    }

    /// Kafka's `ControllerApis.handleAllocateProducerIdsRequest` checks
    /// `ClusterAction` on the cluster before the controller allocates
    /// (#681). A denial is `AllocateProducerIdsRequest.getErrorResponse`, and
    /// the next block start does not move.
    #[tokio::test]
    async fn allocation_needs_cluster_action() {
        let (broker_handle, _dir) = crate::test_support::start_broker_with(|config| {
            config.audit_enabled = false;
            config.authorizer = std::sync::Arc::new(crate::test_support::GrantsInPrincipalName);
        })
        .await;
        let broker = broker_handle.broker_arc_for_test();
        let request = AllocateProducerIdsRequest {
            broker_id: i32::try_from(broker.config.node_id.0).unwrap(),
            broker_epoch: broker
                .controller
                .current_image()
                .broker_epoch(broker.config.node_id)
                .expect("registered broker epoch"),
            ..Default::default()
        };
        let refused = AllocateProducerIdsResponse {
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            ..Default::default()
        };
        let cases = [
            ("none", refused.clone(), 0),
            ("Cluster:Alter+Cluster:Describe", refused, 0),
            (
                "Cluster:ClusterAction",
                AllocateProducerIdsResponse {
                    error_code: codes::NONE,
                    producer_id_start: 0,
                    producer_id_len: 1_000,
                    ..Default::default()
                },
                1_000,
            ),
        ];

        let address = crate::test_support::peer();
        for (grants, expected, next_producer_id) in cases {
            let user = crate::test_support::principal(grants);
            let ctx = crate::test_support::request_context(&user, &address, "allocate-test");
            let bytes = crate::test_support::dispatch_context(
                &broker,
                krabka_protocol::owned::allocate_producer_ids_request::API_KEY,
                0,
                &encode_request(&request),
                &ctx,
            )
            .await;
            check!(decode_response(&bytes) == expected, "{grants}");
            check!(
                broker.controller.current_image().next_producer_id() == next_producer_id,
                "{grants}"
            );
        }
        broker_handle.shutdown().await;
    }
}
