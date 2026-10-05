//! One observer fetch round trip: the `API_KEY_METADATA_FETCH` request to a
//! controller voter, and the decode-and-apply step that folds the returned
//! record batches into the observer's `MetadataImage`.
//!
//! A response that names a snapshot instead of carrying records is handed to
//! [`snapshot::install_snapshot`], on the same connection, before the round
//! trip returns.

use std::sync::Arc;

use krabka_metadata::{MetadataImage, from_kraft_value};
use krabka_protocol::records::RecordBatch;
use krabka_raft::NodeId;
use krabka_units::convert::ByteSizeExt as _;
use tokio::sync::watch;
use tracing::{debug, warn};

use super::{ObserverConfig, snapshot::install_snapshot, store::ObserverStore};

/// What one successful observer fetch round trip learned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct FetchOutcome {
    /// Offset to fetch from next: one past the last batch applied.
    pub(super) next_fetch_offset: u64,
    /// The quorum's committed offset, as the controller that answered last
    /// heard it. The observer trails it by whatever the response did not
    /// carry, so it is the only value that says how far behind this node is.
    ///
    /// This is the response's `quorum_high_watermark` and not its
    /// `high_watermark`: every controller serves this fetch, so the responder
    /// may be a follower whose own watermark is clamped to a log end far below
    /// what the quorum has committed. Reading that one would let an observer
    /// that had drawn level with a lagging follower call itself caught up.
    pub(super) quorum_high_watermark: i64,
    /// Lowest offset the responder still retains. Everything below it has been
    /// pruned behind a snapshot, so a fetch there reads no records at all.
    pub(super) log_start_offset: i64,
    /// The leader the responder believes is current, when it names one.
    pub(super) leader_hint: Option<NodeId>,
}

/// Runs one iteration: it fetches from `addr` at `fetch_offset`, decodes and
/// applies the records, and returns the new fetch offset together with the
/// quorum's committed offset. It returns `None` on a transport error, so
/// that the caller fails over.
///
/// A response that carries a `snapshot_id` means `fetch_offset` has been pruned
/// away on the controller: the round trip then installs that snapshot over the
/// same connection and resumes at its end offset, instead of asking for the
/// same missing records again on every poll.
pub(super) async fn fetch_once(
    config: &ObserverConfig,
    addr: &str,
    target: NodeId,
    fetch_offset: u64,
    image_tx: &watch::Sender<Arc<MetadataImage>>,
    store: &mut ObserverStore,
) -> Option<FetchOutcome> {
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
    // One exit point past the dial, so the connection is closed on every path
    // through the fetch and the snapshot transfer that may follow it.
    let outcome = fetch_over(config, &conn, addr, target, fetch_offset, image_tx, store).await;
    conn.close();
    outcome
}

/// The round trip itself, over an open connection.
async fn fetch_over(
    config: &ObserverConfig,
    conn: &krabka_client_core::Connection,
    addr: &str,
    target: NodeId,
    fetch_offset: u64,
    image_tx: &watch::Sender<Arc<MetadataImage>>,
    store: &mut ObserverStore,
) -> Option<FetchOutcome> {
    let req = krabka_raft::KrabkaMetadataFetchRequest {
        fetch_offset: i64::try_from(fetch_offset).unwrap_or(i64::MAX),
        max_bytes: config.max_bytes.bytes_i32(),
        // The leader lists this node in `DescribeQuorum` under these, as it
        // would a Kafka broker that fetches `__cluster_metadata`.
        replica_id: i32::try_from(config.node_id.0).unwrap_or(-1),
        replica_directory_id: config.directory_id,
    };
    let mut body = Vec::with_capacity(32);
    req.encode_v0(&mut body);

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
            return None;
        }
    };

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

    let next_fetch_offset = match resp.snapshot_id {
        Some(snapshot_id) => {
            install_snapshot(
                config,
                conn,
                (target, resp.leader_epoch),
                snapshot_id,
                image_tx,
                store,
            )
            .await?
        }
        None => apply_fetch_records(fetch_offset, &resp.records, image_tx),
    };
    Some(FetchOutcome {
        next_fetch_offset,
        quorum_high_watermark: resp.quorum_high_watermark,
        log_start_offset: resp.log_start_offset,
        leader_hint: u64::try_from(resp.leader_hint).ok().map(NodeId),
    })
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
        let next_offset = index
            .saturating_add(u64::try_from(batch.last_offset_delta.max(0)).unwrap_or(0))
            .saturating_add(1);
        // A control batch carries no metadata records. Its KIP-853
        // `KRaftVersionRecord` and `VotersRecord` set the quorum the image
        // names, as they do on a controller once committed, and every record
        // this fetch returns is committed. That voter set is how a node in a
        // dynamic quorum, which knows only its bootstrap servers, learns the
        // endpoint of each voter.
        if batch.attributes.is_control_batch() {
            match krabka_raft::control_batch_image_records(&batch) {
                Ok(controls) => controls.iter().for_each(|control| next.apply(control)),
                Err(e) => warn!(error = %e, "observer failed to decode a control record"),
            }
            new_offset = next_offset;
            continue;
        }
        for r in &batch.records {
            let Some(value) = r.value.as_ref() else {
                continue;
            };
            // A KIP-835 no-op, which the controller leader appends while the
            // cluster is idle, changes nothing.
            if krabka_raft::is_kip835_noop(value) {
                continue;
            }
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
        new_offset = next_offset;
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

    /// The batch a leader of a freshly formatted dynamic quorum starts with:
    /// its `LeaderChange` marker, then the `kraft.version` and voter set of the
    /// bootstrap checkpoint. The observer takes the voters, endpoints included,
    /// into its image, which is how it reaches a leader it knows only by id.
    #[test]
    fn apply_fetch_records_takes_the_voter_set_of_a_control_batch_into_the_image() {
        use krabka_protocol::{
            owned::{
                k_raft_version_record::KRaftVersionRecord as WireKRaftVersion,
                voters_record::{
                    Endpoint, KRaftVersionFeature, Voter as WireVoter, VotersRecord as WireVoters,
                },
            },
            records::metadata::control::ControlRecord,
        };

        let cluster_id = Uuid::new_v4();
        let directory_id = Uuid::from_u128(3001);
        let controls = [
            ControlRecord::KRaftVersion(WireKRaftVersion {
                k_raft_version: 1,
                ..Default::default()
            }),
            ControlRecord::Voters(WireVoters {
                voters: vec![WireVoter {
                    voter_id: 3001,
                    voter_directory_id: krabka_protocol::primitives::uuid::Uuid(
                        *directory_id.as_bytes(),
                    ),
                    endpoints: vec![Endpoint {
                        name: "CONTROLLER_PLAINTEXT".into(),
                        host: "ducker12".into(),
                        port: 9592,
                        ..Default::default()
                    }],
                    k_raft_version_feature: KRaftVersionFeature {
                        min_supported_version: 0,
                        max_supported_version: 1,
                        ..Default::default()
                    },
                    ..Default::default()
                }],
                ..Default::default()
            }),
        ];
        let mut batch = control_batch(0);
        for (offset_delta, control) in (1_i32..).zip(&controls) {
            let (key, value) = control.encode_key_value().expect("encode control record");
            batch.records.push(Record {
                offset_delta,
                key: Some(key),
                value: Some(value),
                ..Default::default()
            });
            batch.last_offset_delta = offset_delta;
        }
        let image_tx = image_channel(cluster_id);

        let new_offset = apply_fetch_records(0, &encode_batches(&[batch]), &image_tx);

        let mut expected = MetadataImage::new(cluster_id);
        expected.apply(&MetadataRecord::V1KRaftVersion(
            krabka_metadata::KRaftVersionRecord { kraft_version: 1 },
        ));
        expected.apply(&MetadataRecord::V1Voters(krabka_metadata::VotersRecord {
            voters: krabka_metadata::VoterSet::from_voters([krabka_metadata::Voter {
                id: NodeId(3001),
                directory_id,
                endpoints: vec![krabka_metadata::VoterEndpoint {
                    name: "CONTROLLER_PLAINTEXT".into(),
                    host: "ducker12".into(),
                    port: 9592,
                }],
                kraft_version: krabka_metadata::KRaftVersionRange { min: 0, max: 1 },
            }]),
        }));
        assert!(new_offset == 3);
        assert!(**image_tx.borrow() == expected);
    }

    #[test]
    fn apply_fetch_records_advances_data_batch_offset_and_publishes() {
        let image_tx = image_channel(Uuid::new_v4());
        let records = encode_batches(&[metadata_batch(4, &topic_record("offset-topic"))]);

        let new_offset = apply_fetch_records(4, &records, &image_tx);

        assert!(new_offset == 5);
        assert!(image_tx.borrow().topic("offset-topic").is_some());
    }

    #[test]
    fn apply_fetch_records_advances_past_every_offset_in_a_batch() {
        let image_tx = image_channel(Uuid::new_v4());
        let mut batch = metadata_batch(4, &topic_record("multi-record-offset-topic"));
        batch.last_offset_delta = 999;

        let new_offset = apply_fetch_records(4, &encode_batches(&[batch]), &image_tx);

        assert!(new_offset == 1_004);
    }
}
