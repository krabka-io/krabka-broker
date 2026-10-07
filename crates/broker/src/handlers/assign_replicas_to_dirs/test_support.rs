//! The fixtures that the `AssignReplicasToDirs` unit tests share.
//!
//! The protocol version, the request builder, the response decoder, and the
//! single-broker harness are each used by more than one of the test modules
//! under this module, so they live in one file instead of once per module.

use krabka_protocol::owned::{
    assign_replicas_to_dirs_request::AssignReplicasToDirsRequest,
    assign_replicas_to_dirs_response::AssignReplicasToDirsResponse,
};

use crate::{broker::Broker, error::BrokerError, handlers::assign_replicas_to_dirs::handle};

pub(super) const VERSION: i16 = 0;

krabka_macros::assignment_dirs_fixture!(assignment_request);

/// Builds a request reported by broker 1 at `broker_epoch`. Most tests pass
/// the broker's actual registered epoch (see
/// [`crate::test_support::broker_epoch`]-style lookups on the started
/// broker's image); the authorization and epoch tests pass a wrong one on
/// purpose.
pub(super) fn request(
    broker_epoch: i64,
    dir_uuid: uuid::Uuid,
    topic_uuid: uuid::Uuid,
    partition_index: i32,
) -> AssignReplicasToDirsRequest {
    assignment_request(1, broker_epoch, dir_uuid, topic_uuid, &[partition_index])
}

crate::test_support::decode_helper!(pub(super) AssignReplicasToDirsResponse, version = VERSION);

/// The epoch broker 1 (the started broker itself) registered with. A request
/// naming this epoch is the current, non-stale one.
pub(super) fn own_broker_epoch(broker: &Broker) -> i64 {
    broker
        .controller
        .current_image()
        .broker_epoch(broker.config.node_id)
        .expect("broker 1 is self-registered")
}

/// Dispatches `body` as an `ANONYMOUS` principal that the default
/// `AllowAllAuthorizer` admits.
pub(super) async fn handle_allowed(
    broker: &Broker,
    req: AssignReplicasToDirsRequest,
) -> Result<AssignReplicasToDirsResponse, BrokerError> {
    request_identity!(
        (user, address, ctx),
        crate::test_support::principal("ANONYMOUS"),
        client_id = "assign-replicas-test",
        address = crate::test_support::peer()
    );
    handle(broker, req, VERSION, &ctx).await
}
