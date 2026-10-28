//! Reading the partition back after the cleaner has run, and the assertion
//! that says what compaction must have left behind.
//!
//! `fetch_all` exists because the generated `FetchResponse` codec decodes only
//! the first batch of a partition, so collecting the whole log takes a loop of
//! `Fetch` calls. Its result feeds the scenario’s independent compaction assertions.

use {
    crate::{
        CLIENT_ID, kafka_wire,
        support::{
            fetch::{
                FetchByteLimit, FetchLimits, RequestWaitMillis, fetch_partition,
                single_partition_fetch,
            },
            wire::HeaderEncoding as RequestHeaderEncoding,
        },
    },
    assert2::assert,
    krabka_protocol::{
        owned::{fetch_request::FetchRequest, fetch_response::FetchResponse},
        primitives::uuid::Uuid,
    },
    std::net::SocketAddr,
    tokio::net::TcpStream,
};

/// A flattened record: key and value as plain byte vecs.
#[derive(Debug)]
pub struct FlatRecord {
    pub(crate) key: Vec<u8>,
    pub(crate) value: Vec<u8>,
}

/// Fetch all records from (topic, partition 0) and start at offset 0.
///
/// The function returns a flat list of (key, value) pairs from all batches.
///
/// The generated `FetchResponse` codec decodes only the FIRST batch from each
/// partition's `records` field and silently discards the rest of the byte
/// stream. To collect every batch, the function fetches again and again. Each
/// time it moves `fetch_offset` past the last batch it saw. It stops when the
/// broker returns no batch.
pub async fn fetch_all(addr: SocketAddr, topic: &str, topic_id: Uuid) -> Vec<FlatRecord> {
    let version: i16 = 12; // flexible
    let mut out: Vec<FlatRecord> = Vec::new();
    let mut next_offset: i64 = 0;
    loop {
        let req = FetchRequest {
            replica_id: -1,
            ..single_partition_fetch()
                .topic(topic.to_owned())
                .topic_id(topic_id)
                .partition(
                    fetch_partition()
                        .offset(next_offset)
                        .maximum(1 << 22)
                        .call(),
                )
                .limits(FetchLimits::wait_for_data_with_maximum(
                    RequestWaitMillis(100),
                    FetchByteLimit(1 << 22),
                ))
                .call()
        };
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        let resp: FetchResponse = kafka_wire::exchange()
            .stream(&mut stream)
            .request(&req)
            .api_key(1)
            .version(version)
            .client_id(CLIENT_ID)
            .header(RequestHeaderEncoding::Flexible)
            .call()
            .await
            .expect("Fetch round-trip");

        let mut got_any = false;
        for topic_resp in &resp.responses {
            for part_resp in &topic_resp.partitions {
                assert!(
                    part_resp.error_code == 0,
                    "Fetch partition error: {}",
                    part_resp.error_code
                );
                if let Some(batches) = part_resp.records.as_ref().and_then(|p| p.as_v2()) {
                    for batch in batches {
                        got_any = true;
                        let batch_last_abs = batch.base_offset + i64::from(batch.last_offset_delta);
                        for record in &batch.records {
                            let key = match &record.key {
                                Some(k) => k.to_vec(),
                                None => continue,
                            };
                            let value = record.value.as_ref().map_or_else(Vec::new, |v| v.to_vec());
                            out.push(FlatRecord { key, value });
                        }
                        next_offset = batch_last_abs + 1;
                    }
                }
            }
        }
        if !got_any {
            break;
        }
    }
    out
}
