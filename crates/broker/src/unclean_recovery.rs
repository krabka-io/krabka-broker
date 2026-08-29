//! KIP-966 offset-aware unclean recovery.
//!
//! This module holds the pure selection helpers and the controller-side
//! Unclean Recovery Manager (URM) task. The URM polls surviving replicas for
//! their log-end-offset and last-written leader epoch with `GetReplicaLogInfo`
//! (`api_key` 93), and elects the most complete log.

mod job;
mod manager;
mod policy;
mod query;
mod selection;

pub(crate) use self::{
    job::{RecoveryJob, RecoveryOutcome, UncleanRecoveryHandle},
    manager::UncleanRecoveryManager,
    policy::RecoveryPolicy,
    selection::{ReplicaLogInfo, has_newer_leader, select_best_replica},
};
