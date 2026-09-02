//! Restart recovery over the durable log: replay of the committed metadata
//! prefix into a [`MetadataImage`], replay of the KIP-853 control records into
//! the seed [`QuorumState`], and the control state as of a chosen boundary.

use krabka_ids::Offset;
use krabka_metadata::{MetadataImage, MetadataRecord, from_kraft_value};
use krabka_protocol::records::metadata::control::ControlRecord;
use krabka_verified::recovery::{
    ReplayCursorDecision, ReplayRecordDecision, replay_cursor_decision, replay_record_decision,
    should_capture_first_downgrade,
};

use super::{
    PendingDowngradeSnapshot,
    control_state::voter_set_from_wire,
    records::{decode_control_record, next_batch_offset},
};
use crate::{
    config::MetadataRaftFetchMax,
    error::RaftError,
    kraft::{log::KraftLog, types::QuorumState},
};

/// Replay committed log batches starting at `from` into `image` (idempotent:
/// records that fail `validate` are skipped). Used by restart recovery.
pub fn replay_committed(
    log: &KraftLog,
    image: &mut MetadataImage,
    from: Offset,
    max: MetadataRaftFetchMax,
) -> Result<Option<PendingDowngradeSnapshot>, RaftError> {
    let mut cursor = from;
    let target = log.hwm();
    let mut pending = None;
    while cursor < target {
        let batches = log.read_decoded(cursor, max.size())?;
        let next = next_batch_offset(&batches);
        if batches.is_empty() {
            break;
        }
        for batch in &batches {
            for rec in &batch.records {
                let ReplayRecordDecision::Apply(record_offset) = replay_record_decision(
                    batch.base_offset,
                    rec.offset_delta,
                    from.0,
                    target.0,
                    batch.attributes.is_control_batch(),
                    false,
                ) else {
                    continue;
                };
                let record_offset = Offset(record_offset);
                let Some(value) = rec.value.as_ref() else {
                    continue;
                };
                if let Ok(meta) = from_kraft_value(value, image)
                    && image.validate(&meta).is_ok()
                {
                    let is_metadata_version_downgrade = matches!(
                        &meta,
                        MetadataRecord::V1FeatureLevel(feature)
                            if feature.name
                                == krabka_metadata::metadata_version::METADATA_VERSION_FEATURE
                                && image
                                    .finalized_metadata_version()
                                    .is_some_and(|current| feature.level < current)
                    );
                    image.apply(&meta);
                    if should_capture_first_downgrade(
                        pending.is_some(),
                        is_metadata_version_downgrade,
                    ) {
                        pending = Some(PendingDowngradeSnapshot {
                            image: image.clone(),
                            end_offset: record_offset + 1,
                            epoch: batch.partition_leader_epoch,
                        });
                    }
                }
            }
        }
        match replay_cursor_decision(cursor.0, next.map(|offset| offset.0)) {
            ReplayCursorDecision::Advance(next) => cursor = Offset(next),
            ReplayCursorDecision::Stop => break,
        }
    }
    Ok(pending)
}

pub fn replay_control_records(log: &KraftLog, state: &mut QuorumState, max: MetadataRaftFetchMax) {
    let from = log.log_start_offset();
    let mut cursor = from;
    let target = log.hwm();
    while cursor < target {
        match log.read_decoded(cursor, max.size()) {
            Ok(batches) => {
                let next = next_batch_offset(&batches);
                if batches.is_empty() {
                    break;
                }
                for batch in &batches {
                    for record in &batch.records {
                        if !matches!(
                            replay_record_decision(
                                batch.base_offset,
                                record.offset_delta,
                                from.0,
                                target.0,
                                batch.attributes.is_control_batch(),
                                true,
                            ),
                            ReplayRecordDecision::Apply(_)
                        ) {
                            continue;
                        }
                        match decode_control_record(record) {
                            Ok(Some(ControlRecord::KRaftVersion(record))) => {
                                if let Ok(version) = u16::try_from(record.k_raft_version) {
                                    state.kraft_version = version;
                                }
                            }
                            Ok(Some(ControlRecord::Voters(record))) => {
                                if let Ok(voters) = voter_set_from_wire(&record) {
                                    state.voters = voters;
                                }
                            }
                            Ok(_) => {}
                            Err(error) => tracing::error!(
                                ?error,
                                offset = batch.base_offset,
                                "kraft: invalid control record during recovery"
                            ),
                        }
                    }
                }
                match replay_cursor_decision(cursor.0, next.map(|offset| offset.0)) {
                    ReplayCursorDecision::Advance(next) => cursor = Offset(next),
                    ReplayCursorDecision::Stop => break,
                }
            }
            Err(error) => {
                tracing::error!(?error, "kraft: control replay for recovery failed");
                break;
            }
        }
    }
}

pub fn control_state_at(
    log: &KraftLog,
    bootstrap: &QuorumState,
    end_offset: Offset,
    max: MetadataRaftFetchMax,
) -> Result<QuorumState, RaftError> {
    let mut state = bootstrap.clone();
    let from = log.log_start_offset();
    let mut cursor = from;
    while cursor < end_offset {
        let batches = log.read_decoded(cursor, max.size())?;
        let next = next_batch_offset(&batches);
        if batches.is_empty() {
            break;
        }
        for batch in &batches {
            for record in &batch.records {
                if !matches!(
                    replay_record_decision(
                        batch.base_offset,
                        record.offset_delta,
                        from.0,
                        end_offset.0,
                        batch.attributes.is_control_batch(),
                        true,
                    ),
                    ReplayRecordDecision::Apply(_)
                ) {
                    continue;
                }
                let Some(control) = decode_control_record(record)? else {
                    continue;
                };
                match control {
                    ControlRecord::KRaftVersion(record) => {
                        state.kraft_version =
                            u16::try_from(record.k_raft_version).map_err(|_| {
                                RaftError::ChangeRejected(
                                    "negative kraft.version control record".into(),
                                )
                            })?;
                    }
                    ControlRecord::Voters(record) => {
                        state.voters = voter_set_from_wire(&record)?;
                    }
                    _ => {}
                }
            }
        }
        match replay_cursor_decision(cursor.0, next.map(|offset| offset.0)) {
            ReplayCursorDecision::Advance(next) => cursor = Offset(next),
            ReplayCursorDecision::Stop => break,
        }
    }
    Ok(state)
}
