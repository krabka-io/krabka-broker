//! End-to-end test for the `BrokerHeartbeat` wire handler against a live
//! broker, plus the request builder and the leader wait it needs.
//!
//! It pins the response shape a controller leader returns for a registered,
//! caught-up broker.

use std::{sync::Arc, time::Duration};

use assert2::assert;
use bytes::BytesMut;
use krabka_protocol::{
    Encode, owned::broker_heartbeat_response::BrokerHeartbeatResponse,
    primitives::uuid::Uuid as ProtocolUuid,
};

use super::*;
use crate::{codes, test_support::start_broker_with_authorizer as start_broker};

fn request(
    broker_epoch: i64,
    current_metadata_offset: i64,
    offline_log_dirs: Vec<uuid::Uuid>,
) -> Bytes {
    let req = BrokerHeartbeatRequest {
        broker_id: 1,
        broker_epoch,
        current_metadata_offset,
        want_fence: false,
        want_shut_down: false,
        offline_log_dirs: offline_log_dirs
            .into_iter()
            .map(|u| ProtocolUuid(u.into_bytes()))
            .collect(),
        cordoned_log_dirs: None,
        ..Default::default()
    };
    let mut buf = BytesMut::with_capacity(
        req.encoded_len(krabka_protocol::owned::broker_heartbeat_request::MAX_VERSION),
    );
    req.encode(
        &mut buf,
        krabka_protocol::owned::broker_heartbeat_request::MAX_VERSION,
    )
    .expect("encode BrokerHeartbeatRequest");
    buf.freeze()
}

crate::test_support::response_helpers!(
    BrokerHeartbeatResponse,
    client_id = "broker-heartbeat-test"
);

async fn wait_for_leader(broker: &Broker) {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if broker
            .controller
            .watch_leader()
            .borrow()
            .is_some_and(|n| n == broker.config.node_id)
        {
            return;
        }
        assert!(
            std::time::Instant::now() <= deadline,
            "broker did not become controller leader"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn handle_leader_success_preserves_response_shape() {
    let (broker_handle, _dir) = start_broker(Arc::new(crate::authorizer::AllowAllAuthorizer)).await;
    let broker = broker_handle.broker_arc_for_test();
    wait_for_leader(&broker).await;
    let principal = krabka_security::Principal {
        name: "ANONYMOUS".into(),
        auth_method: krabka_security::AuthMethod::Anonymous,
        groups: vec![],
    };
    let peer = std::net::SocketAddr::from(([127, 0, 0, 1], 9092));
    let ctx = test_context(&principal, &peer);
    let version = krabka_protocol::owned::broker_heartbeat_request::MAX_VERSION;
    let image = broker.controller.current_image();
    let broker_epoch = image
        .broker_epoch(NodeId(1))
        .expect("broker registration should be applied");
    let req = request(broker_epoch, broker_epoch, vec![]);

    let bytes = handle(&broker, version, 11, &req, &ctx)
        .await
        .expect("BrokerHeartbeat handler");
    let resp = decode_response(&bytes, version);

    let expected = BrokerHeartbeatResponse {
        throttle_time_ms: 0,
        error_code: codes::NONE,
        is_caught_up: true,
        is_fenced: false,
        should_shut_down: false,
        unknown_tagged_fields: krabka_protocol::UnknownTaggedFields(Vec::new()),
    };
    assert!(resp == expected, "{resp:?}");

    broker_handle.shutdown().await;
}
