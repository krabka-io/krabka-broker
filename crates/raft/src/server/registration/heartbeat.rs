//! The `BrokerHeartbeat` API (KIP-631): the controller's periodic answer to a
//! registered broker.
//!
//! The reply decides whether the broker is caught up, whether it is fenced out
//! of the ISR, and whether it has been asked to shut down, so the decision is
//! kept in one small function that a test can drive from an image alone.

use bytes::Bytes;
use krabka_metadata::NodeId;
use krabka_protocol::{
    Decode,
    owned::{
        broker_heartbeat_request::BrokerHeartbeatRequest,
        broker_heartbeat_response::BrokerHeartbeatResponse,
    },
};

use super::{
    BROKER_ID_NOT_REGISTERED, CLUSTER_AUTHORIZATION_FAILED, NOT_CONTROLLER, STALE_BROKER_EPOCH,
    SUCCESS, is_leader, response::encode,
};
use crate::{RaftError, kraft::KraftController};

pub(super) fn broker_heartbeat(
    version: i16,
    body: &[u8],
    engine: &KraftController,
    authorized: bool,
) -> Result<Bytes, RaftError> {
    let mut body = body;
    let request = BrokerHeartbeatRequest::decode(&mut body, version)?;
    let mut response = BrokerHeartbeatResponse::default();
    response.error_code = if !authorized {
        CLUSTER_AUTHORIZATION_FAILED
    } else if !is_leader(engine) {
        NOT_CONTROLLER
    } else {
        validate_heartbeat(&request, &engine.current_image(), &mut response)
    };
    encode(&response, version)
}

/// Takes the image rather than the engine it came from: the decision is a
/// function of the registration on record, and a test should be able to hand it
/// one without standing up a quorum to hold it.
fn validate_heartbeat(
    request: &BrokerHeartbeatRequest,
    image: &krabka_metadata::MetadataImage,
    response: &mut BrokerHeartbeatResponse,
) -> i16 {
    let Ok(node) = u64::try_from(request.broker_id).map(NodeId) else {
        return BROKER_ID_NOT_REGISTERED;
    };
    let Some(registration) = image.broker(node) else {
        return BROKER_ID_NOT_REGISTERED;
    };
    if registration.broker_epoch != request.broker_epoch {
        return STALE_BROKER_EPOCH;
    }
    response.is_caught_up = request.current_metadata_offset >= registration.broker_epoch;
    response.is_fenced = request.want_fence || !response.is_caught_up;
    response.should_shut_down = request.want_shut_down;
    SUCCESS
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    /// A heartbeat is answered from the registration on record: an unknown
    /// broker and a stale epoch are refused, and a known one is told whether
    /// it is caught up, fenced, and asked to shut down.
    ///
    /// `is_fenced` is the one that matters most -- a broker that is behind
    /// must be fenced whether or not it asked to be, because it is the
    /// controller's job to keep a lagging replica out of the ISR.
    #[test]
    fn a_heartbeat_is_answered_from_the_registration_on_record() {
        use krabka_metadata::{BrokerRegistrationRecord, MetadataImage, MetadataRecord};

        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1BrokerRegistration(
            BrokerRegistrationRecord {
                node_id: NodeId(7),
                broker_epoch: 100,
                incarnation_id: uuid::Uuid::nil(),
                host: "broker-7".into(),
                port: 9092,
                rack: None,
                log_dirs: vec![],
                endpoints: vec![],
                features: std::collections::BTreeMap::new(),
            },
        ));

        let answer = |broker_id: i32,
                      broker_epoch: i64,
                      offset: i64,
                      want_fence: bool,
                      want_shut_down: bool| {
            let request = BrokerHeartbeatRequest {
                broker_id,
                broker_epoch,
                current_metadata_offset: offset,
                want_fence,
                want_shut_down,
                ..Default::default()
            };
            let mut response = BrokerHeartbeatResponse::default();
            let code = validate_heartbeat(&request, &image, &mut response);
            (code, response)
        };

        // A broker nobody registered.
        let (code, _) = answer(9, 100, 100, false, false);
        check!(code == BROKER_ID_NOT_REGISTERED, "an unregistered broker");

        // A negative id is not a node id at all.
        let (code, _) = answer(-1, 100, 100, false, false);
        check!(code == BROKER_ID_NOT_REGISTERED, "a negative broker id");

        // The right broker at the wrong epoch.
        let (code, _) = answer(7, 99, 100, false, false);
        check!(code == STALE_BROKER_EPOCH, "a stale epoch");

        // Caught up, asking for nothing: not fenced, not shutting down.
        let (code, response) = answer(7, 100, 100, false, false);
        check!(code == SUCCESS);
        check!(
            (
                response.is_caught_up,
                response.is_fenced,
                response.should_shut_down
            ) == (true, false, false),
            "caught up and unfenced"
        );

        // Behind the registration: fenced even though it did not ask to be.
        let (_, response) = answer(7, 100, 99, false, false);
        check!(
            (response.is_caught_up, response.is_fenced) == (false, true),
            "a lagging broker is fenced regardless"
        );

        // Caught up but asking to be fenced, and to shut down.
        let (_, response) = answer(7, 100, 100, true, true);
        check!(
            (
                response.is_caught_up,
                response.is_fenced,
                response.should_shut_down
            ) == (true, true, true),
            "an explicit fence and shutdown are honoured"
        );
    }
}
