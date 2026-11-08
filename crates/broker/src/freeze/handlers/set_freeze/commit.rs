//! The one raft append that an accepted `SetTopicFreeze` request commits.
//!
//! The consumed break-glass proposal and the registry record travel in a single
//! `submit_change` call, so an approval and the thaw it authorized reach the
//! metadata log together or not at all.

use {
    super::outcome::{Accepted, Refusal},
    crate::{broker::Broker, codes, freeze::freeze_target, handlers::submit_failure_code},
    krabka_metadata::{MetadataRecord, TopicFreezeRecord},
    krabka_raft::{FreezeRefusal, RaftError},
};

/// Write the accepted records in one raft append.
///
/// The consumed proposal goes first, so the approval and the thaw it authorized
/// commit together.
pub(super) async fn submit(broker: &Broker, accepted: Accepted) -> Result<Accepted, Refusal> {
    let mut records = Vec::with_capacity(2);
    if let Some(proposal) = accepted.consumed_proposal.clone() {
        records.push(proposal);
    }
    records.push(MetadataRecord::V1TopicFreeze(accepted.record.clone()));

    match broker.controller.submit_change(records).await {
        Ok(_) => Ok(accepted),
        Err(error) => {
            tracing::warn!(%error, "set-topic-freeze submit failed");
            Err(submit_refusal(
                &error,
                &accepted.record,
                accepted.signature_verified,
            ))
        }
    }
}

/// The answer to an accepted request whose raft append failed with `error`.
///
/// The checks refuse a thaw with no live entry before the append. A thaw that
/// loses a race to another writer still reaches the controller after the entry
/// went, or behind a newer record, and the controller refuses it as
/// [`FreezeRefusal::Missing`] or [`FreezeRefusal::Stale`]. Either way the
/// operator named an entry that is not there to lift, so the answer is the
/// same `INVALID_REQUEST` the checks give, not a coordinator failure.
pub(super) fn submit_refusal(
    error: &RaftError,
    record: &TopicFreezeRecord,
    signature_verified: bool,
) -> Refusal {
    let lost_race = matches!(error, RaftError::FreezeRefused {
        refusal: FreezeRefusal::Missing | FreezeRefusal::Stale,
        ..
    });
    if lost_race && !record.frozen {
        return Refusal {
            signature_verified,
            ..Refusal::new(
                codes::INVALID_REQUEST,
                format!(
                    "{} has no live freeze entry this thaw can replace; describe the freezes \
                     and retry",
                    freeze_target(record.pattern_type, &record.scope)
                ),
            )
        };
    }
    Refusal {
        code: submit_failure_code(error, codes::COORDINATOR_NOT_AVAILABLE),
        message: format!("submit failed: {error}"),
        signature_verified,
    }
}
