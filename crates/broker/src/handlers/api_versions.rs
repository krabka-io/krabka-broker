//! `ApiVersions` (`api_key=18`). It returns the (min, max) supported version
//! range for every API key this broker handles.
//!
//! From v3, KIP-511 makes the request carry `client_software_name` and
//! `client_software_version`. The broker validates both against
//! `[a-zA-Z0-9](?:[a-zA-Z0-9\-.]*[a-zA-Z0-9])?`, and rejects the call with
//! `INVALID_REQUEST` if either one is empty or malformed. This mirrors
//! `ApiVersionsRequest.isValid` on the JVM. Each accepted v3+ handshake
//! increments a Prometheus counter for that (name, version) pair,
//! `krabka_broker_client_software_versions_total`, so operators can see which
//! client libraries connect.
//!
//! From v5, KIP-1242 lets a client include the cluster and node it intended to
//! reach. Both fields must be absent or present together. A complete mismatch
//! returns `REBOOTSTRAP_REQUIRED` so the client discards stale metadata.
//!
//! This file holds the wire entry point. The KIP-511 name check lives in
//! `client_info`, and the KIP-584 feature rows the response carries live in
//! `feature_keys`.

use bytes::{Bytes, BytesMut};
use futures_util::future::BoxFuture;
use krabka_protocol::{
    Decode, Encode,
    owned::{api_versions_request::ApiVersionsRequest, api_versions_response::ApiVersionsResponse},
};

mod client_info;
mod feature_keys;

#[cfg(test)]
mod tests;

pub(crate) use self::client_info::is_valid_client_info;
use self::feature_keys::{finalized_feature_keys, supported_feature_keys};
use crate::{broker::Broker, codes, error::BrokerError};

/// First `ApiVersions` request version that carries the KIP-511
/// `client_software_name` and `client_software_version` fields.
const CLIENT_INFO_MIN_VERSION: i16 = 3;

/// First `ApiVersions` version that carries the KIP-1242 routing identity.
const ROUTING_IDENTITY_MIN_VERSION: i16 = 5;

pub(crate) fn unsupported_version_response() -> Result<Bytes, BrokerError> {
    let response = ApiVersionsResponse {
        error_code: codes::UNSUPPORTED_VERSION,
        api_keys: crate::api_catalog::supported_apis(),
        ..Default::default()
    };
    let mut body = BytesMut::with_capacity(response.encoded_len(0));
    response.encode(&mut body, 0)?;
    Ok(body.freeze())
}

pub(crate) fn handle(
    broker: &Broker,
    version: i16,
    _correlation_id: i32,
    req_bytes: &[u8],
) -> BoxFuture<'static, Result<Bytes, BrokerError>> {
    let req_bytes = req_bytes.to_vec();
    let metrics = broker.metrics.clone();
    let image = broker.controller.current_image();
    let expected_cluster_id = image.cluster_id().to_string();
    let expected_node_id = i32::try_from(broker.config.node_id.0).ok();
    Box::pin(async move {
        let mut cur: &[u8] = &req_bytes;
        let req = ApiVersionsRequest::decode(&mut cur, version)?;

        let error_code = if version >= CLIENT_INFO_MIN_VERSION
            && (!is_valid_client_info(&req.client_software_name)
                || !is_valid_client_info(&req.client_software_version))
        {
            Some(codes::INVALID_REQUEST)
        } else if version >= ROUTING_IDENTITY_MIN_VERSION {
            match (&req.cluster_id, req.node_id) {
                (None, -1) => None,
                (Some(_), -1) | (None, _) => Some(codes::INVALID_REQUEST),
                (Some(cluster_id), node_id)
                    if cluster_id != &expected_cluster_id || Some(node_id) != expected_node_id =>
                {
                    Some(codes::REBOOTSTRAP_REQUIRED)
                }
                (Some(_), _) => None,
            }
        } else {
            None
        };

        // Invalid client information or an incomplete KIP-1242 identity is
        // INVALID_REQUEST. A complete but stale identity asks the client to
        // rebootstrap. Both use the normal v5 response shape with no API list.
        if let Some(error_code) = error_code {
            let resp = ApiVersionsResponse {
                error_code,
                ..Default::default()
            };
            let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
            resp.encode(&mut buf, version)?;
            return Ok(buf.freeze());
        }

        // Accepted handshake. Bump the per-(name, version) counter on
        // v3+ only; older requests don't carry the fields.
        if version >= CLIENT_INFO_MIN_VERSION {
            metrics.record_client_software(&req.client_software_name, &req.client_software_version);
        }

        let resp = ApiVersionsResponse {
            api_keys: crate::api_catalog::supported_apis(),
            // KIP-584 write-side. `supported_features` advertises the
            // broker's `crate::features` table; `finalized_features` + the
            // epoch are read from the live metadata image. A fresh broker
            // surfaces no finalized features and epoch `-1`
            // (`MetadataVersion.UNKNOWN` to JVM clients) until
            // `UpdateFeatures` (api_key 57) lands a `V1FeatureLevel` record.
            supported_features: supported_feature_keys(version),
            finalized_features_epoch: image.finalized_features_epoch(),
            finalized_features: finalized_feature_keys(&image),
            ..Default::default()
        };
        let mut buf = BytesMut::with_capacity(resp.encoded_len(version));
        resp.encode(&mut buf, version)?;
        Ok(buf.freeze())
    })
}
