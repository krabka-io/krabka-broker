//! Transaction subsystem for the Krabka broker.

pub mod bootstrap;
/// The background task that completes transactions whose `Prepare*` record is
/// durable.
pub mod completion;
/// Exhaustive model of transaction completion across indeterminate
/// `__transaction_state` writes, coordinator failover, marker refusals and
/// data-leader changes: every transaction must end once the faults stop.
#[cfg(test)]
#[path = "completion_model.rs"]
mod completion_model;
pub mod coordinator;
pub mod decision;
#[cfg(test)]
mod decision_model_support;
/// Compositional end-to-end model of the exactly-once read guarantee. It
/// composes the txn-coordinator decision cores with the LSO mechanics and the
/// `read_committed` fetch-visibility core.
#[cfg(test)]
#[path = "eos_composition_model.rs"]
mod eos_composition_model;
/// KIP-939 idle-transaction reaper. It aborts a timed-out transaction, and
/// skips a 2PC transaction, which has no timeout.
pub mod expiration;
pub mod handlers;
/// KIP-98 transactional-id expiry sweep. It tombstones a terminal or idle
/// transactional id out of `__transaction_state`, and never touches an
/// `Ongoing` or `Prepare*` one.
pub mod id_expiration;
pub mod log_record;
pub mod marker;
pub mod partitioner;
pub mod state;
/// KIP-939 two-phase-commit decision cores: timeout resolution and the
/// safety-critical idle-abort predicate. They are pure, and [`two_pc_model`]
/// model-checks them.
pub mod two_pc;
/// Exhaustive stateright model of the KIP-939 2PC timeout-safety property.
#[cfg(test)]
#[path = "two_pc_model.rs"]
mod two_pc_model;
pub mod util;
pub mod version;
