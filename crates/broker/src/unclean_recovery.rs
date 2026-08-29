//! KIP-966 offset-aware unclean recovery.
//!
//! This module holds the pure selection helpers and the controller-side
//! Unclean Recovery Manager (URM) task. The URM polls surviving replicas for
//! their log-end-offset and last-written leader epoch with `GetReplicaLogInfo`
//! (`api_key` 93), and elects the most complete log.
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

pub(crate) use self::{
    background::BackgroundRecovery,
    job::{RecoveryJob, RecoveryOutcome, UncleanRecoveryHandle},
    manager::UncleanRecoveryManager,
    policy::RecoveryPolicy,
    selection::{ReplicaLogInfo, has_newer_leader, select_best_replica},
};
