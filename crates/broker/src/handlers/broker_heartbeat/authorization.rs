//! The whole-response body a `BrokerHeartbeat` ACL Deny produces.
//!
//! `BrokerHeartbeat` is an inter-broker control-plane RPC, so the handler gates
//! it on the shared `ClusterAction` gate on `Cluster("kafka-cluster")` rather
//! than one of the client operations.

use bytes::Bytes;

use super::response::denied_response_body;
use crate::{error::BrokerError, handlers::encode_response};

/// Whole-response `CLUSTER_AUTHORIZATION_FAILED (31)` response, built on Deny.
pub(super) fn denied_response(version: i16) -> Result<Bytes, BrokerError> {
    encode_response(&denied_response_body(), version)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::{
        Decode,
        owned::broker_heartbeat_response::{self, BrokerHeartbeatResponse},
    };

    use super::*;
    use crate::codes;

    /// The denied response carries `CLUSTER_AUTHORIZATION_FAILED` and leaves
    /// the broker fenced.
    #[test]
    fn denied_response_carries_cluster_authorization_failed() {
        let bytes = denied_response(broker_heartbeat_response::MAX_VERSION).expect("encode");
        let mut cur: &[u8] = &bytes;
        let resp =
            BrokerHeartbeatResponse::decode(&mut cur, broker_heartbeat_response::MAX_VERSION)
                .unwrap();
        assert!(resp.error_code == codes::CLUSTER_AUTHORIZATION_FAILED);
        assert!(resp.is_fenced);
    }
}
