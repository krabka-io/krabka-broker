//! The reply `AlterPartition` produces when its authorization gate denies the
//! request.
//!
//! `AlterPartition` is an inter-broker control-plane RPC, so Kafka checks
//! `ClusterAction` on the `Cluster` resource once for the request. A denial
//! fails the whole response rather than any individual partition row.

use bytes::Bytes;
use krabka_protocol::{
    UnknownTaggedFields, owned::alter_partition_response::AlterPartitionResponse,
};

use crate::{codes, error::BrokerError, handlers::encode_response};

/// Builds the whole-response `CLUSTER_AUTHORIZATION_FAILED (31)` reply for a
/// Deny decision.
pub(super) fn denied_response(version: i16) -> Result<Bytes, BrokerError> {
    encode_response(
        &AlterPartitionResponse {
            throttle_time_ms: 0,
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            topics: Vec::new(),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        },
        version,
    )
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_protocol::{Decode, owned::alter_partition_response};

    use super::*;

    #[test]
    fn denied_response_is_whole_response_cluster_authorization_failed() {
        let version = alter_partition_response::MAX_VERSION;
        let bytes = denied_response(version).expect("encode");
        let mut cur: &[u8] = &bytes;
        let resp = AlterPartitionResponse::decode(&mut cur, version).unwrap();
        let expected = AlterPartitionResponse {
            throttle_time_ms: 0,
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            topics: Vec::new(),
            unknown_tagged_fields: UnknownTaggedFields::default(),
        };
        assert!(resp == expected);
    }
}
