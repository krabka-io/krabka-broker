//! Drivers for the client-facing requests the enforcement tests issue as an
//! ordinary (non-super) principal — `Produce`, `Fetch`, `Metadata`,
//! `JoinGroup`, and `InitProducerId` — with the request builders they need.
//! All of them share `sasl_plain_authenticate` and the `round_trip` framing
//! primitive, and each drives one request on a freshly authenticated
//! connection.

use std::{io, net::SocketAddr};

use bytes::BytesMut;
use krabka_protocol::{
    Decode, Encode,
    owned::{
        fetch_request::FetchRequest,
        fetch_response::FetchResponse,
        init_producer_id_request::InitProducerIdRequest,
        init_producer_id_response::InitProducerIdResponse,
        join_group_request::{JoinGroupRequest, JoinGroupRequestProtocol},
        join_group_response::JoinGroupResponse,
        metadata_request::MetadataRequest,
        metadata_response::MetadataResponse,
        produce_request::{PartitionProduceData, ProduceRequest, TopicProduceData},
        produce_response::ProduceResponse,
    },
    records::{Record, RecordBatch},
};

use crate::{
    FETCH_VERSION, INIT_PRODUCER_ID_VERSION, JOIN_GROUP_VERSION, METADATA_VERSION, PRODUCE_VERSION,
    framing::{round_trip, sasl_plain_authenticate},
};

/// Build a `ProduceRequest` carrying a single record (`value`) for
/// `(topic, partition)`. `acks=-1`, which is all-ISR, matches the JVM client's
/// default for durable producers.
pub fn single_record_produce_request(topic: &str, partition: i32, value: &[u8]) -> ProduceRequest {
    ProduceRequest {
        transactional_id: None,
        acks: -1,
        timeout_ms: 5_000,
        topic_data: vec![TopicProduceData {
            name: topic.to_string(),
            partition_data: vec![PartitionProduceData {
                index: partition,
                records: Some(
                    RecordBatch {
                        last_offset_delta: 0,
                        records: vec![Record {
                            offset_delta: 0,
                            value: Some(bytes::Bytes::copy_from_slice(value)),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }
                    .into(),
                ),
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    }
}

pub async fn drive_produce_as_plain(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    req: ProduceRequest,
) -> Result<ProduceResponse, io::Error> {
    let mut stream = sasl_plain_authenticate(addr, user, password).await?;
    let mut body = BytesMut::new();
    req.encode(&mut body, PRODUCE_VERSION)
        .map_err(|e| io::Error::other(format!("Produce encode: {e}")))?;
    let resp_bytes = round_trip(&mut stream, 0, PRODUCE_VERSION, 4, true, &body).await?;
    let mut cur: &[u8] = &resp_bytes;
    ProduceResponse::decode(&mut cur, PRODUCE_VERSION)
        .map_err(|e| io::Error::other(format!("Produce decode: {e}")))
}

pub async fn drive_fetch_as_plain(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    req: FetchRequest,
) -> Result<FetchResponse, io::Error> {
    let mut stream = sasl_plain_authenticate(addr, user, password).await?;
    let mut body = BytesMut::new();
    req.encode(&mut body, FETCH_VERSION)
        .map_err(|e| io::Error::other(format!("Fetch encode: {e}")))?;
    let resp_bytes = round_trip(&mut stream, 1, FETCH_VERSION, 4, true, &body).await?;
    let mut cur: &[u8] = &resp_bytes;
    FetchResponse::decode(&mut cur, FETCH_VERSION)
        .map_err(|e| io::Error::other(format!("Fetch decode: {e}")))
}

pub async fn drive_metadata_as_plain(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    req: MetadataRequest,
) -> Result<MetadataResponse, io::Error> {
    let mut stream = sasl_plain_authenticate(addr, user, password).await?;
    let mut body = BytesMut::new();
    req.encode(&mut body, METADATA_VERSION)
        .map_err(|e| io::Error::other(format!("Metadata encode: {e}")))?;
    let resp_bytes = round_trip(&mut stream, 3, METADATA_VERSION, 4, true, &body).await?;
    let mut cur: &[u8] = &resp_bytes;
    MetadataResponse::decode(&mut cur, METADATA_VERSION)
        .map_err(|e| io::Error::other(format!("Metadata decode: {e}")))
}

pub async fn drive_join_group_as_plain(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    req: JoinGroupRequest,
) -> Result<JoinGroupResponse, io::Error> {
    let mut stream = sasl_plain_authenticate(addr, user, password).await?;
    let mut body = BytesMut::new();
    req.encode(&mut body, JOIN_GROUP_VERSION)
        .map_err(|e| io::Error::other(format!("JoinGroup encode: {e}")))?;
    let resp_bytes = round_trip(&mut stream, 11, JOIN_GROUP_VERSION, 4, true, &body).await?;
    let mut cur: &[u8] = &resp_bytes;
    JoinGroupResponse::decode(&mut cur, JOIN_GROUP_VERSION)
        .map_err(|e| io::Error::other(format!("JoinGroup decode: {e}")))
}

pub async fn drive_init_producer_id_as_plain(
    addr: SocketAddr,
    user: &str,
    password: &[u8],
    req: InitProducerIdRequest,
) -> Result<InitProducerIdResponse, io::Error> {
    let mut stream = sasl_plain_authenticate(addr, user, password).await?;
    let mut body = BytesMut::new();
    req.encode(&mut body, INIT_PRODUCER_ID_VERSION)
        .map_err(|e| io::Error::other(format!("InitProducerId encode: {e}")))?;
    let resp_bytes = round_trip(&mut stream, 22, INIT_PRODUCER_ID_VERSION, 4, true, &body).await?;
    let mut cur: &[u8] = &resp_bytes;
    InitProducerIdResponse::decode(&mut cur, INIT_PRODUCER_ID_VERSION)
        .map_err(|e| io::Error::other(format!("InitProducerId decode: {e}")))
}

/// Build a single-protocol `JoinGroup` request with an empty `member_id`, so
/// the broker first responds with `MEMBER_ID_REQUIRED` and a generated id.
/// The request proposes the `range` assignor, the only one the
/// broker negotiates in MVP.
pub fn join_group_request(group_id: &str) -> JoinGroupRequest {
    JoinGroupRequest {
        group_id: group_id.to_string(),
        session_timeout_ms: 30_000,
        rebalance_timeout_ms: 60_000,
        member_id: String::new(),
        group_instance_id: None,
        protocol_type: "consumer".to_string(),
        protocols: vec![JoinGroupRequestProtocol {
            name: "range".to_string(),
            metadata: bytes::Bytes::new(),
            ..Default::default()
        }],
        ..Default::default()
    }
}
