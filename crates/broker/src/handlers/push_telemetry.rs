//! `PushTelemetry` (`api_key=72`, KIP-714).
//!
//! The handler runs Kafka's push checks against the client's instance, which
//! it builds from the current subscriptions when this broker holds none. It
//! delegates accepted nonempty payloads to client-metrics ingestion.

use {
    crate::{
        authorizer::{AuthorizationRequest, AuthorizationResult},
        broker::Broker,
        handlers::{ApiVersion, CorrelationId, DispatchResult, encode_response},
    },
    krabka_compression::CompressionType,
    krabka_metadata::{AclOperation, ResourceType},
    krabka_protocol::owned::{
        push_telemetry_request::PushTelemetryRequest,
        push_telemetry_response::PushTelemetryResponse,
    },
    uuid::Uuid,
};

#[cfg(test)]
mod tests;

use crate::{
    api_catalog::{ClientMetricsReceiver, UnstableApiVersions},
    client_metrics::{
        ingestion::IngestionError,
        manager::{PushCheck, PushDecision},
    },
    codes,
    handlers::{acl_wire::CLUSTER_RESOURCE_NAME, context::TelemetryContext},
};

/// Kafka's `CompressionType.forId`: the ids 0 to 4 and nothing else. The
/// record-batch attribute decode masks the id to three bits, which would take
/// 9 for gzip.
fn compression_for_id(id: i8) -> Option<CompressionType> {
    match id {
        0 => Some(CompressionType::None),
        1 => Some(CompressionType::Gzip),
        2 => Some(CompressionType::Snappy),
        3 => Some(CompressionType::Lz4),
        4 => Some(CompressionType::Zstd),
        _ => None,
    }
}

crate::handlers::raw_request_handler!(
    PushTelemetryRequest,
    req,
    broker,
    version,
    _correlation_id,
    req_bytes,
    ctx,
    TelemetryContext,
    {
        if broker.config.client_metrics_receiver() == ClientMetricsReceiver::Absent {
            return encode_response(
                &PushTelemetryResponse {
                    error_code: codes::UNSUPPORTED_VERSION,
                    ..Default::default()
                },
                version,
            );
        }
        let instance = Uuid::from_bytes(req.client_instance_id.0);
        let codec = compression_for_id(req.compression_type);
        let manager = &broker.client_metrics.manager;

        let attrs = ctx.client_attributes(instance);
        let unstable = broker.config.features.unstable_api_versions;
        let image = broker.controller.current_image();
        let authorized = broker
            .config
            .authorizer
            .authorize(&*image, &AuthorizationRequest {
                principal: ctx.principal,
                host: ctx.peer,
                resource_type: ResourceType::Cluster,
                resource_name: CLUSTER_RESOURCE_NAME,
                operation: AclOperation::ClusterAction,
            })
            == AuthorizationResult::Allow;
        if !authorized {
            return encode_response(
                &PushTelemetryResponse {
                    error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
                    ..Default::default()
                },
                version,
            );
        }
        let decision = manager.authorize_push(&image, &attrs, PushCheck {
            subscription_id: req.subscription_id,
            terminating: req.terminating,
            compression_supported: codec.is_some(),
            payload_len: req.metrics.len(),
            unstable,
        });

        // Kafka answers every error with `throttle_time_ms` 0; only the request
        // quota raises it.
        let error_code = match decision {
            PushDecision::Reject { error_code } => error_code,
            PushDecision::Accept { .. } if req.metrics.is_empty() => codes::NONE,
            PushDecision::Accept {
                metrics: subscribed,
            } => {
                let ct = codec.expect("authorize_push accepts only a supported codec");
                // Kafka 4.3.1 maps export failures, including oversized
                // decompression, to INVALID_RECORD. Unstable versions use
                // the retriable TELEMETRY_TOO_LARGE (KAFKA-21076).
                match broker.client_metrics.ingest_payload(
                    ct,
                    &req.metrics,
                    &subscribed,
                    instance,
                    ctx.client_id,
                ) {
                    Ok(()) => codes::NONE,
                    Err(IngestionError::TooLarge) if unstable == UnstableApiVersions::Enabled => {
                        codes::TELEMETRY_TOO_LARGE
                    }
                    Err(IngestionError::InvalidRecord | IngestionError::TooLarge) => {
                        codes::INVALID_RECORD
                    }
                }
            }
        };

        let resp = PushTelemetryResponse {
            error_code,
            ..Default::default()
        };
        encode_response(&resp, version)
    }
);
