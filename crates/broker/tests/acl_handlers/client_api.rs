//! Drivers for the client-facing requests the enforcement tests issue as an
//! ordinary (non-super) principal — `Produce`, `Fetch`, `Metadata`,
//! `JoinGroup`, and `InitProducerId` — with the request builders they need.
//! All of them share `sasl_plain_authenticate` and the `round_trip` framing
//! primitive, and each drives one request on a freshly authenticated
//! connection.

use std::{io, net::SocketAddr};

use krabka_protocol::owned::{
    fetch_request::FetchRequest, fetch_response::FetchResponse,
    init_producer_id_request::InitProducerIdRequest,
    init_producer_id_response::InitProducerIdResponse, join_group_request::JoinGroupRequest,
    join_group_response::JoinGroupResponse, metadata_request::MetadataRequest,
    metadata_response::MetadataResponse, produce_request::ProduceRequest,
    produce_response::ProduceResponse,
};

pub use crate::kafka_wire::single_record_produce_request;
use crate::{
    FETCH_VERSION, INIT_PRODUCER_ID_VERSION, JOIN_GROUP_VERSION, METADATA_VERSION, PRODUCE_VERSION,
    support::classic::{classic_join_request, join_protocol},
};

pub async fn drive_produce_as_plain(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    req: ProduceRequest,
) -> Result<ProduceResponse, io::Error> {
    crate::framing::request_as_plain(addr, user, password, &req, 0, PRODUCE_VERSION, "Produce")
        .await
}

pub async fn drive_fetch_as_plain(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    req: FetchRequest,
) -> Result<FetchResponse, io::Error> {
    crate::framing::request_as_plain(addr, user, password, &req, 1, FETCH_VERSION, "Fetch").await
}

pub async fn drive_metadata_as_plain(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    req: MetadataRequest,
) -> Result<MetadataResponse, io::Error> {
    crate::framing::request_as_plain(addr, user, password, &req, 3, METADATA_VERSION, "Metadata")
        .await
}

pub async fn drive_join_group_as_plain(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    req: JoinGroupRequest,
) -> Result<JoinGroupResponse, io::Error> {
    crate::framing::request_as_plain(
        addr,
        user,
        password,
        &req,
        11,
        JOIN_GROUP_VERSION,
        "JoinGroup",
    )
    .await
}

pub async fn drive_init_producer_id_as_plain(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    req: InitProducerIdRequest,
) -> Result<InitProducerIdResponse, io::Error> {
    crate::framing::request_as_plain(
        addr,
        user,
        password,
        &req,
        22,
        INIT_PRODUCER_ID_VERSION,
        "InitProducerId",
    )
    .await
}

/// Build a single-protocol `JoinGroup` request with an empty `member_id`, so
/// the broker first responds with `MEMBER_ID_REQUIRED` and a generated id.
/// The request proposes the `range` assignor, the only one the
/// broker negotiates in MVP.
pub fn join_group_request(group_id: &str) -> JoinGroupRequest {
    JoinGroupRequest {
        group_instance_id: None,
        ..classic_join_request(crate::support::classic::ClassicJoinSetup {
            group_id: group_id.to_string(),
            timeouts: crate::support::classic::ClassicTimeouts {
                rebalance: krabka_units::millis(60_000),
                ..Default::default()
            },
            protocol_type: "consumer".to_string(),
            protocols: vec![join_protocol("range".to_string(), bytes::Bytes::new())],
            ..Default::default()
        })
    }
}
