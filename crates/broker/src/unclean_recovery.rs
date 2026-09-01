//! KIP-966 offset-aware unclean recovery.
//!
//! This module holds the pure selection helpers and the controller-side
//! Unclean Recovery Manager (URM) task. The URM polls surviving replicas for
//! their log-end-offset and last-written leader epoch with `GetReplicaLogInfo`
//! (`api_key` 93), and elects a leader out of what answers.
//!
//! Which leader is [`select_leader`]'s decision, and the most complete log is
//! its second choice: a surviving member of the partition's KIP-966
//! eligible-leader-replica set is elected ahead of a longer log that is not in
//! that set, because only the ELR member is known to hold every committed
//! record. [`ElectionBasis`] carries which of the two happened all the way to
//! the audit record and the unclean-leader-election counter, neither of which
//! should call an ELR election a loss.
//!
//! # KFC-9: the path with no caller to refuse
//!
//! An unclean recovery loses committed data, and the break-glass two-person
//! rule gates every other transition that can. It cannot gate this one. Leader
//! election and the broker-heartbeat path start a recovery with no request and
//! no principal, so there is nobody to ask for a second signature and nobody to
//! send a refusal to. [`BackgroundRecovery`] holds the three-valued rule that
//! answers that, and [`RecoveryJob::proposal`] is what separates a recovery an
//! operator asked for from one the controller started on its own.

mod background;
mod job;
mod manager;
mod policy;
mod query;
mod selection;

/// The most-complete-log ranking on its own. [`select_leader`] is what the
/// controller calls; the stateright models in `leader_failover_model` and
/// `data_path_model` drive this half directly, so it is exported for them.
///
/// Neither model carries ELR state, so neither checks the rule that runs ahead
/// of this one -- and that is the rule with something to check. The fallback
/// is audited and metered as the guess it is, while an ELR election reports
/// itself lossless on the strength of a claim about what the published set
/// holds. Teaching the models that claim is krabka-io/krabka-broker#317.
#[cfg(test)]
pub(crate) use self::selection::select_best_replica;
pub(crate) use self::{
    background::BackgroundRecovery,
    job::{RecoveryJob, RecoveryOutcome, UncleanRecoveryHandle},
    manager::UncleanRecoveryManager,
    policy::RecoveryPolicy,
    selection::{Election, ReplicaLogInfo, has_newer_leader, select_leader},
};
