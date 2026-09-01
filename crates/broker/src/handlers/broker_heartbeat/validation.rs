//! The gates a `BrokerHeartbeat` passes before the controller acts on it: the
//! leadership predicate, the registration and epoch check, and the
//! offline-log-dir gate.
//!
//! Each one is a pure function over the decoded request and the current
//! metadata image, so the handler stays a straight line of decisions.

use krabka_metadata::MetadataImage;
use krabka_protocol::owned::broker_heartbeat_request::BrokerHeartbeatRequest;
use krabka_raft::NodeId;

use crate::codes;

pub(super) fn is_controller_leader(leader: Option<NodeId>, node_id: NodeId) -> bool {
    leader == Some(node_id)
}

pub(super) fn has_offline_log_dirs(req: &BrokerHeartbeatRequest) -> bool {
    !req.offline_log_dirs.is_empty()
}

pub(super) fn validate_registration(
    image: &MetadataImage,
    req: &BrokerHeartbeatRequest,
) -> Result<(u64, krabka_verified::BrokerHeartbeatDecision), i16> {
    let broker_id = u64::try_from(req.broker_id).map_err(|_| codes::BROKER_ID_NOT_REGISTERED)?;
    let decision = krabka_verified::broker_heartbeat_decision(
        image
            .broker(NodeId(broker_id))
            .map(|registration| registration.broker_epoch),
        req.broker_epoch,
        req.current_metadata_offset,
        req.want_fence,
        req.want_shut_down,
    );
    if !decision.registered {
        return Err(codes::BROKER_ID_NOT_REGISTERED);
    }
    if !decision.epoch_matches {
        return Err(codes::STALE_BROKER_EPOCH);
    }
    Ok((broker_id, decision))
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::{BrokerRegistrationRecord, MetadataRecord};
    use krabka_protocol::primitives::uuid::Uuid as ProtocolUuid;
    use uuid::Uuid;

    use super::*;

    #[test]
    fn leader_predicate_matches_current_node_only() {
        let cases = [
            (Some(NodeId(1)), true),
            (Some(NodeId(2)), false),
            (None, false),
        ];
        for (leader, want) in cases {
            assert!(
                is_controller_leader(leader, NodeId(1)) == want,
                "leader {leader:?}"
            );
        }
    }

    #[test]
    fn registration_validation_rejects_unknown_and_stale_brokers() {
        let mut image = MetadataImage::new(Uuid::nil());
        image.apply(&MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                node_id: NodeId(7),
                broker_epoch: 42,
                incarnation_id: Uuid::nil(),
                host: "localhost".into(),
                port: 9092,
                rack: None,
                log_dirs: vec![],
                endpoints: vec![],
                features: std::collections::BTreeMap::new(),
            },
        ));
        let mut req = BrokerHeartbeatRequest {
            broker_id: -1,
            broker_epoch: 42,
            current_metadata_offset: 42,
            ..Default::default()
        };

        assert!(validate_registration(&image, &req) == Err(codes::BROKER_ID_NOT_REGISTERED));
        req.broker_id = 8;
        assert!(validate_registration(&image, &req) == Err(codes::BROKER_ID_NOT_REGISTERED));
        req.broker_id = 7;
        req.broker_epoch = 41;
        assert!(validate_registration(&image, &req) == Err(codes::STALE_BROKER_EPOCH));
    }

    #[test]
    fn registration_validation_reports_catch_up_at_registration_offset() {
        let mut image = MetadataImage::new(Uuid::nil());
        image.apply(&MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                node_id: NodeId(7),
                broker_epoch: 42,
                incarnation_id: Uuid::nil(),
                host: "localhost".into(),
                port: 9092,
                rack: None,
                log_dirs: vec![],
                endpoints: vec![],
                features: std::collections::BTreeMap::new(),
            },
        ));
        let mut req = BrokerHeartbeatRequest {
            broker_id: 7,
            broker_epoch: 42,
            current_metadata_offset: 41,
            ..Default::default()
        };

        let (_, decision) = validate_registration(&image, &req).expect("registered broker");
        assert!(!decision.caught_up && decision.fenced);
        req.current_metadata_offset = 42;
        let (_, decision) = validate_registration(&image, &req).expect("registered broker");
        assert!(decision.caught_up && !decision.fenced);
    }

    #[test]
    fn offline_dir_gate_tracks_reported_directories() {
        let empty = BrokerHeartbeatRequest {
            offline_log_dirs: vec![],
            ..Default::default()
        };
        assert!(!has_offline_log_dirs(&empty));

        let reported = BrokerHeartbeatRequest {
            offline_log_dirs: vec![ProtocolUuid(uuid::Uuid::from_u128(0xD1).into_bytes())],
            ..Default::default()
        };
        assert!(has_offline_log_dirs(&reported));
    }
}
