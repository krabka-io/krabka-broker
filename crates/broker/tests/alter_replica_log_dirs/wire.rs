//! The raw-wire drivers this suite speaks to the broker with: one
//! length-prefixed request and response exchange, and the four typed calls
//! built on it (`CreateTopics`, `AlterReplicaLogDirs`, `DescribeLogDirs`).
//!
//! Every API used here is flexible, so the request header always carries its
//! tagged-fields byte and the response header always has one to strip. The
//! protocol versions the suite pins live here too, next to the encoders that
//! read them.

use std::{io, net::SocketAddr};

use assert2::assert;
use bytes::{Buf, BufMut, BytesMut};
use krabka_protocol::{
    Decode, Encode,
    owned::{
        alter_replica_log_dirs_request::{
            AlterReplicaLogDir, AlterReplicaLogDirTopic, AlterReplicaLogDirsRequest,
        },
        alter_replica_log_dirs_response::AlterReplicaLogDirsResponse,
        create_topics_request::{CreatableTopic, CreateTopicsRequest},
        create_topics_response::CreateTopicsResponse,
        describe_log_dirs_request::DescribeLogDirsRequest,
        describe_log_dirs_response::DescribeLogDirsResponse,
    },
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

const CLIENT_ID: &str = "krabka-arld-test";
const ALTER_VERSION: i16 = 2;
const DESCRIBE_VERSION: i16 = 4;

async fn round_trip(
    stream: &mut TcpStream,
    api_key: i16,
    api_version: i16,
    body: &[u8],
) -> Result<Vec<u8>, io::Error> {
    let mut frame = BytesMut::with_capacity(16 + body.len());
    frame.put_i16(api_key);
    frame.put_i16(api_version);
    frame.put_i32(1);
    frame.put_i16(i16::try_from(CLIENT_ID.len()).unwrap());
    frame.put_slice(CLIENT_ID.as_bytes());
    frame.put_u8(0); // header tagged-fields (every API here is flexible)
    frame.put_slice(body);

    stream
        .write_u32(u32::try_from(frame.len()).unwrap())
        .await?;
    stream.write_all(&frame).await?;
    stream.flush().await?;

    let resp_len = stream.read_u32().await?;
    let mut resp = vec![0u8; resp_len as usize];
    stream.read_exact(&mut resp).await?;

    let mut cur = &resp[..];
    let _corr = cur.get_i32();
    let _tagged = cur.get_u8(); // v1 response header tagged-fields
    Ok(cur.to_vec())
}

pub(crate) async fn create_topic(addr: SocketAddr, topic: &str, partitions: i32) {
    let req = CreateTopicsRequest {
        topics: vec![CreatableTopic {
            name: topic.to_string(),
            num_partitions: partitions,
            replication_factor: 1,
            ..Default::default()
        }],
        timeout_ms: 5_000,
        ..Default::default()
    };
    let version: i16 = 7;
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut body = BytesMut::new();
    req.encode(&mut body, version).unwrap();
    let resp_bytes = round_trip(&mut stream, 19, version, &body).await.unwrap();
    let mut cur: &[u8] = &resp_bytes;
    let resp = CreateTopicsResponse::decode(&mut cur, version).unwrap();
    assert!(resp.topics[0].error_code == 0, "CreateTopics must succeed");
}

pub(crate) async fn alter_replica_log_dirs(
    addr: SocketAddr,
    target_dir: &std::path::Path,
    topic: &str,
    partitions: Vec<i32>,
) -> AlterReplicaLogDirsResponse {
    let req = AlterReplicaLogDirsRequest {
        dirs: vec![AlterReplicaLogDir {
            path: target_dir.to_string_lossy().to_string(),
            topics: vec![AlterReplicaLogDirTopic {
                name: topic.to_string(),
                partitions,
                ..Default::default()
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut body = BytesMut::new();
    req.encode(&mut body, ALTER_VERSION).unwrap();
    let resp_bytes = round_trip(&mut stream, 34, ALTER_VERSION, &body)
        .await
        .unwrap();
    let mut cur: &[u8] = &resp_bytes;
    AlterReplicaLogDirsResponse::decode(&mut cur, ALTER_VERSION).unwrap()
}

pub(crate) async fn describe_log_dirs(addr: SocketAddr) -> DescribeLogDirsResponse {
    let req = DescribeLogDirsRequest {
        topics: None,
        ..Default::default()
    };
    let mut stream = TcpStream::connect(addr).await.unwrap();
    let mut body = BytesMut::new();
    req.encode(&mut body, DESCRIBE_VERSION).unwrap();
    let resp_bytes = round_trip(&mut stream, 35, DESCRIBE_VERSION, &body)
        .await
        .unwrap();
    let mut cur: &[u8] = &resp_bytes;
    DescribeLogDirsResponse::decode(&mut cur, DESCRIBE_VERSION).unwrap()
}
