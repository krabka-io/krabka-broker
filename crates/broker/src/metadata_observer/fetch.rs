//! One observer fetch round trip: the `API_KEY_METADATA_FETCH` request to a
//! controller voter, and the decode-and-apply step that folds the returned
//! record batches into the observer's `MetadataImage`.

use std::sync::Arc;

use krabka_metadata::{MetadataImage, from_kraft_value};
use krabka_protocol::records::RecordBatch;
use krabka_raft::NodeId;
use krabka_units::convert::ByteSizeExt as _;
use tokio::sync::watch;
use tracing::{debug, warn};

use super::ObserverConfig;

/// Runs one iteration: it fetches from `addr` at `fetch_offset`, decodes and
/// applies the records, and returns the new fetch offset. It returns `None` on
/// a transport error, so that the caller fails over.
pub(super) async fn fetch_once(
    config: &ObserverConfig,
    addr: &str,
    target: NodeId,
    fetch_offset: u64,
    image_tx: &watch::Sender<Arc<MetadataImage>>,
) -> Option<u64> {
    let req = krabka_raft::KrabkaMetadataFetchRequest {
        fetch_offset: i64::try_from(fetch_offset).unwrap_or(i64::MAX),
        max_bytes: config.max_bytes.bytes_i32(),
    };
    let mut body = Vec::with_capacity(12);
    req.encode_v0(&mut body);

    let opts = krabka_client_core::ConnectionOptions {
        client_id: config.client_id.clone(),
        dispatch_queue_capacity: config.client_dispatch_queue_capacity,
        frame_max: config.client_frame_max,
        ..krabka_client_core::ConnectionOptions::default()
    };
    let conn = match config.dialer.dial(target, addr, opts).await {
        Ok(c) => c,
        Err(e) => {
            debug!(%addr, error = %e, "observer dial failed");
            return None;
        }
    };
    let resp_body = match conn
        .raw_request(
            krabka_raft::API_KEY_METADATA_FETCH,
            0,
            bytes::Bytes::from(body),
        )
        .await
    {
        Ok(b) => b,
        Err(e) => {
            debug!(%addr, error = %e, "observer fetch request failed");
            conn.close();
            return None;
        }
    };
    conn.close();

    let mut cur: &[u8] = &resp_body;
    let resp = match krabka_raft::KrabkaMetadataFetchResponse::decode_v0(&mut cur) {
        Ok(r) => r,
        Err(e) => {
            warn!(%addr, error = %e, "observer response decode failed");
            return None;
        }
    };
    if resp.error_code != 0 {
        return None;
    }

    Some(apply_fetch_records(fetch_offset, &resp.records, image_tx))
}

fn apply_fetch_records(
    fetch_offset: u64,
    records: &[u8],
    image_tx: &watch::Sender<Arc<MetadataImage>>,
) -> u64 {
    // No new records: the controller had nothing past `fetch_offset`. Skip the
    // expensive full-image clone entirely.
    if records.is_empty() {
        return fetch_offset;
    }

    let mut next: MetadataImage = (**image_tx.borrow()).clone();
    let mut new_offset = fetch_offset;
    let mut buf: &[u8] = records;
    while !buf.is_empty() {
        let batch = match RecordBatch::decode(&mut buf) {
            Ok(b) => b,
            Err(e) => {
                warn!(error = %e, "observer batch decode failed");
                break;
            }
        };
        let index = u64::try_from(batch.base_offset.max(0)).unwrap_or(0);
        // The LeaderChange control batch carries no metadata records.
        if batch.attributes.is_control_batch() {
            new_offset = index + 1;
            continue;
        }
        for r in &batch.records {
            let Some(value) = r.value.as_ref() else {
                continue;
            };
            match from_kraft_value(value, &next) {
                Ok(rec) => {
                    if let Err(e) = next.validate(&rec) {
                        warn!(error = %e, "observer skipped record failing validation");
                        continue;
                    }
                    next.apply(&rec);
                }
                Err(e) => warn!(error = %e, "observer failed to decode record"),
            }
        }
        new_offset = index + 1;
    }
    if new_offset != fetch_offset {
        let _ = image_tx.send_replace(Arc::new(next));
    }
    new_offset.max(fetch_offset)
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use bytes::Bytes;
    use krabka_metadata::{MetadataRecord, TopicRecord, to_kraft_values};
    use krabka_protocol::records::{Record, header::Attributes};
    use uuid::Uuid;

    use super::*;

    fn topic_record(name: &str) -> MetadataRecord {
        MetadataRecord::V1Topic(TopicRecord {
            name: name.into(),
            topic_id: Uuid::new_v4(),
            partitions: 1,
            replication_factor: 1,
        })
    }

    fn metadata_batch(base_offset: i64, rec: &MetadataRecord) -> RecordBatch {
        let values = to_kraft_values(rec, &MetadataImage::new(Uuid::nil())).expect("to kraft");
        let records: Vec<Record> = values
            .into_iter()
            .enumerate()
            .map(|(idx, value)| Record {
                offset_delta: i32::try_from(idx).unwrap(),
                value: Some(value),
                ..Default::default()
            })
            .collect();
        RecordBatch {
            base_offset,
            last_offset_delta: i32::try_from(records.len().saturating_sub(1)).unwrap(),
            records,
            ..Default::default()
        }
    }

    fn control_batch(base_offset: i64) -> RecordBatch {
        RecordBatch {
            base_offset,
            attributes: Attributes::default().with_control(true),
            last_offset_delta: 0,
            records: vec![Record {
                offset_delta: 0,
                value: Some(Bytes::from_static(b"leader-change")),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn encode_batches(batches: &[RecordBatch]) -> Bytes {
        let mut out = Vec::new();
        for batch in batches {
            batch.encode(&mut out).expect("encode batch");
        }
        Bytes::from(out)
    }

    fn image_channel(cluster_id: Uuid) -> watch::Sender<Arc<MetadataImage>> {
        let (tx, _) = watch::channel(Arc::new(MetadataImage::new(cluster_id)));
        tx
    }

    #[test]
    fn apply_fetch_records_advances_past_control_batch() {
        let image_tx = image_channel(Uuid::new_v4());
        let records = encode_batches(&[control_batch(6)]);

        let new_offset = apply_fetch_records(6, &records, &image_tx);

        assert!(new_offset == 7);
    }

    #[test]
    fn apply_fetch_records_advances_data_batch_offset_and_publishes() {
        let image_tx = image_channel(Uuid::new_v4());
        let records = encode_batches(&[metadata_batch(4, &topic_record("offset-topic"))]);

        let new_offset = apply_fetch_records(4, &records, &image_tx);

        assert!(new_offset == 5);
        assert!(image_tx.borrow().topic("offset-topic").is_some());
    }
}
