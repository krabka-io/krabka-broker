//! The sustained-load and failure contract, and the judge that grades a run
//! against it.
//!
//! A soak in this repository is three things kept apart on purpose:
//!
//! - a [`contract`], a YAML file under `bench/soak/` that declares every limit
//!   before the run: host, disk, seed, client settings, throughput targets,
//!   latency budgets, recovery deadlines and the fault plan;
//! - an executor, the `soak_contract` suite of `krabka-broker`, which runs the
//!   contract's load against an isolated three-broker cluster, injects its
//!   faults and records [`evidence`];
//! - this judge, which reads the two back and returns a [`verdict`].
//!
//! A disaster-recovery contract is executed by `krabka-soak-drill`, through
//! [`drill`], inside the qualification drill instead: it runs the same load
//! through a capture and the loss of the source cluster, and reads back the
//! cluster restored from the capture. [`recovery`] places every record on its
//! side of the capture boundary and measures the recovery point and recovery
//! time.
//!
//! [`client`] is the half both executors share: the producers, the live
//! consumers and the read-back. It does IO, and the judge never calls it.
//!
//! The judge is pure: it does no IO and owns no clock, so every rule it applies
//! is unit-tested here against evidence whose answer is known. The executor
//! only measures. `krabka-soak-check` is the command-line front end, and its
//! `self-check` proves the judge catches an injected missing record and an
//! injected missed deadline.
//!
//! The design document is `crates/bench-driver/docs/soak-contract-design.md`.

pub mod client;
pub mod contract;
pub mod deadline;
pub mod drill;
pub mod evidence;
pub mod ledger;
pub mod lifecycle;
pub mod reconcile;
pub mod recovery;
pub mod schedule;
pub mod synthetic;
pub mod verdict;
