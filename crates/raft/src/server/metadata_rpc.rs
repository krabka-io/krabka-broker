//! The two Krabka-private metadata RPCs the controller listener serves: the
//! follower-forwarded `SubmitChange` (1003) that applies records on the leader,
//! and the observer `MetadataFetch` (1004) that streams a committed
//! `__cluster_metadata` slice to a broker.

use bytes::Bytes;
use krabka_units::prelude::{ByteSize, ByteSizeExt as _};

use crate::{
    error::RaftError,
    kraft::KraftController,
    wire::{
        KrabkaMetadataFetchRequest, KrabkaMetadataFetchResponse, KrabkaSubmitChangeRequest,
        KrabkaSubmitChangeResponse,
    },
};

/// `KrabkaSubmitChangeResponse::error_code`: the change was applied.
const SUBMIT_CHANGE_APPLIED: i16 = 0;
/// `KrabkaSubmitChangeResponse::error_code`: this node is not the leader;
/// consult `leader_hint`.
const SUBMIT_CHANGE_NOT_LEADER: i16 = 1;
/// `KrabkaSubmitChangeResponse::error_code`: metadata validation rejected the
/// records (also returned when the wincode body fails to decode).
const SUBMIT_CHANGE_REJECTED: i16 = 2;
/// `KrabkaSubmitChangeResponse::error_code`: any other engine failure.
const SUBMIT_CHANGE_FAILED: i16 = 3;

/// `leader_hint` sentinel meaning "the current leader is unknown".
const LEADER_HINT_UNKNOWN: i64 = -1;

/// Handle a follower-forwarded `submit_change` (1003). The forwarder wrapped a
/// wincode-encoded `Vec<MetadataRecord>`; we submit it to the local engine
/// (presumably the leader) and translate the result into the `error_code` enum:
/// `0` applied, `1` not leader (with `leader_hint`), `2` metadata-rejected.
pub(super) async fn dispatch_submit_change(
    body: &[u8],
    engine: &KraftController,
) -> Result<Bytes, RaftError> {
    let mut cur = body;
    let req = KrabkaSubmitChangeRequest::decode_v0(&mut cur)?;
    let records: Vec<krabka_metadata::MetadataRecord> = match <serde_wincode::SerdeCompat<
        Vec<krabka_metadata::MetadataRecord>,
    > as wincode::Deserialize>::deserialize(
        &req.records
    ) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, "submit-change body decode failed");
            let resp = KrabkaSubmitChangeResponse {
                error_code: SUBMIT_CHANGE_REJECTED,
                leader_hint: LEADER_HINT_UNKNOWN,
                result: Bytes::new(),
            };
            let mut out = Vec::with_capacity(16);
            resp.encode_v0(&mut out)?;
            return Ok(Bytes::from(out));
        }
    };
    let resp = match engine.submit_change(records).await {
        Ok(result) => KrabkaSubmitChangeResponse {
            error_code: SUBMIT_CHANGE_APPLIED,
            leader_hint: LEADER_HINT_UNKNOWN,
            result: Bytes::from(
                <serde_wincode::SerdeCompat<crate::SubmitChangeResult> as wincode::Serialize>::serialize(&result)?,
            ),
        },
        Err(RaftError::Metadata(_)) => KrabkaSubmitChangeResponse {
            error_code: SUBMIT_CHANGE_REJECTED,
            leader_hint: LEADER_HINT_UNKNOWN,
            result: Bytes::new(),
        },
        Err(RaftError::NotLeader { current_leader }) => KrabkaSubmitChangeResponse {
            error_code: SUBMIT_CHANGE_NOT_LEADER,
            leader_hint: current_leader
                .and_then(|l| i64::try_from(l.0).ok())
                .unwrap_or(LEADER_HINT_UNKNOWN),
            result: Bytes::new(),
        },
        Err(e) => {
            tracing::warn!(error = ?e, "submit-change failed");
            KrabkaSubmitChangeResponse {
                error_code: SUBMIT_CHANGE_FAILED,
                leader_hint: LEADER_HINT_UNKNOWN,
                result: Bytes::new(),
            }
        }
    };
    let mut out = Vec::with_capacity(16);
    resp.encode_v0(&mut out)?;
    Ok(Bytes::from(out))
}

/// Serve a committed `__cluster_metadata` slice to a broker-only observer (1004)
/// from the engine's `KraftLog`.
pub(super) async fn dispatch_metadata_fetch(
    body: &[u8],
    engine: &KraftController,
) -> Result<Bytes, RaftError> {
    let mut cur = body;
    let req = KrabkaMetadataFetchRequest::decode_v0(&mut cur)?;
    let fetch_offset = req.fetch_offset.max(0);
    // The decoded `int32` enters the domain here; the codec itself stays raw so
    // the request stays byte-exact. A negative budget clamps to zero, as before.
    let max_size = ByteSize::from_bytes_i64(i64::from(req.max_bytes.max(0)));
    let slice = engine.metadata_fetch(fetch_offset, max_size).await?;
    let leader_hint: i64 = engine
        .quorum_state()
        .await
        .ok()
        .and_then(|qs| qs.leader_id)
        .and_then(|l| i64::try_from(l.0).ok())
        .unwrap_or(LEADER_HINT_UNKNOWN);

    let resp = KrabkaMetadataFetchResponse {
        error_code: 0,
        leader_hint,
        log_start_offset: slice.log_start_offset,
        high_watermark: slice.high_watermark,
        quorum_high_watermark: slice.quorum_high_watermark,
        records: slice.records,
    };
    let mut out = Vec::new();
    resp.encode_v0(&mut out)?;
    Ok(Bytes::from(out))
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_ids::ApiKey;
    use krabka_metadata::{MetadataRecord, TopicRecord};
    use uuid::Uuid;

    use super::*;
    use crate::{
        server::{
            dispatch::dispatch,
            test_support::{single_voter_engine, test_engine_with_voters, wait_for_leader},
        },
        wire::{API_KEY_METADATA_FETCH, API_KEY_SUBMIT_CHANGE},
    };

    fn topic_record(name: &str) -> MetadataRecord {
        MetadataRecord::V1Topic(TopicRecord {
            name: name.into(),
            topic_id: Uuid::new_v4(),
            partitions: 1,
            replication_factor: 1,
        })
    }

    fn submit_change_body(records: &[MetadataRecord]) -> Bytes {
        let records = records.to_vec();
        let records =
            <serde_wincode::SerdeCompat<Vec<MetadataRecord>> as wincode::Serialize>::serialize(
                &records,
            )
            .expect("wincode");
        let req = KrabkaSubmitChangeRequest {
            records: Bytes::from(records),
        };
        let mut out = Vec::new();
        req.encode_v0(&mut out).expect("submit request");
        Bytes::from(out)
    }

    fn metadata_fetch_body(fetch_offset: i64, max_bytes: i32) -> Bytes {
        let req = KrabkaMetadataFetchRequest {
            fetch_offset,
            max_bytes,
        };
        let mut out = Vec::new();
        req.encode_v0(&mut out);
        Bytes::from(out)
    }

    fn decode_submit_change_response(body: &[u8]) -> KrabkaSubmitChangeResponse {
        let mut cur = body;
        KrabkaSubmitChangeResponse::decode_v0(&mut cur).expect("submit response")
    }

    fn decode_metadata_fetch_response(body: &[u8]) -> KrabkaMetadataFetchResponse {
        let mut cur = body;
        KrabkaMetadataFetchResponse::decode_v0(&mut cur).expect("metadata fetch response")
    }

    #[tokio::test]
    async fn dispatch_submit_change_encodes_success_and_decode_errors() {
        let (engine, _dir) = single_voter_engine();
        wait_for_leader(&engine).await;

        let ok_body = dispatch(
            ApiKey(API_KEY_SUBMIT_CHANGE),
            submit_change_body(&[topic_record("submit-ok")]),
            &engine,
        )
        .await
        .expect("submit dispatch");
        let ok = decode_submit_change_response(&ok_body);
        assert2::assert!(ok.error_code == 0);
        assert2::assert!(ok.leader_hint == -1);

        let bad_req = KrabkaSubmitChangeRequest {
            records: Bytes::from_static(b"not-wincode"),
        };
        let mut bad_body = Vec::new();
        bad_req.encode_v0(&mut bad_body).unwrap();
        let err_body = dispatch(
            ApiKey(API_KEY_SUBMIT_CHANGE),
            Bytes::from(bad_body),
            &engine,
        )
        .await
        .expect("decode failure dispatch");
        let err = decode_submit_change_response(&err_body);
        assert2::assert!(err.error_code == 2);
        assert2::assert!(err.leader_hint == -1);
    }

    #[tokio::test]
    async fn dispatch_submit_change_encodes_metadata_rejection() {
        let (engine, _dir) = single_voter_engine();
        wait_for_leader(&engine).await;

        let topic = topic_record("duplicate");
        let first = dispatch(
            ApiKey(API_KEY_SUBMIT_CHANGE),
            submit_change_body(std::slice::from_ref(&topic)),
            &engine,
        )
        .await
        .expect("first submit");
        assert2::assert!(decode_submit_change_response(&first).error_code == 0);

        let duplicate = dispatch(
            ApiKey(API_KEY_SUBMIT_CHANGE),
            submit_change_body(&[topic]),
            &engine,
        )
        .await
        .expect("duplicate submit");
        let duplicate = decode_submit_change_response(&duplicate);
        assert2::assert!(duplicate.error_code == 2);
        assert2::assert!(duplicate.leader_hint == -1);
    }

    #[tokio::test]
    async fn dispatch_metadata_fetch_clamps_negative_request_and_reports_unknown_leader() {
        let (engine, _dir) = test_engine_with_voters(1, std::iter::empty());

        let body = dispatch(
            ApiKey(API_KEY_METADATA_FETCH),
            metadata_fetch_body(-5, -1),
            &engine,
        )
        .await
        .expect("metadata fetch dispatch");

        let resp = decode_metadata_fetch_response(&body);
        check!(
            (
                resp.error_code,
                resp.leader_hint,
                resp.high_watermark,
                resp.records.is_empty(),
            ) == (0, -1, 0, true)
        );
    }

    #[tokio::test]
    async fn dispatch_metadata_fetch_returns_committed_records_and_leader_hint() {
        let (engine, _dir) = single_voter_engine();
        wait_for_leader(&engine).await;
        engine
            .submit_change(vec![topic_record("metadata-fetch")])
            .await
            .expect("submit");

        let body = dispatch(
            ApiKey(API_KEY_METADATA_FETCH),
            metadata_fetch_body(0, 1_048_576),
            &engine,
        )
        .await
        .expect("metadata fetch dispatch");

        let resp = decode_metadata_fetch_response(&body);
        check!(
            (
                resp.error_code,
                resp.leader_hint,
                resp.high_watermark >= 1,
                resp.records.is_empty(),
            ) == (0, 1, true, false)
        );
    }
}
