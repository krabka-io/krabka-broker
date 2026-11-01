//! Bounded stateright model of how each broker applies committed partition
//! metadata across crash-restarts, checked for convergence once the system is
//! quiescent.
//!
//! Jepsen's "Known failures" (krabka-io/krabka-broker#1932) report a partition
//! stuck after an unclean restart: the controller re-elects its leader at a
//! new epoch, and then a replica goes on at another epoch, its followers
//! logging "holding the partition back until the metadata image settles"
//! until the test ends. This model checks that every replica installs the
//! committed leader and epoch, so a path that leaves one behind or ahead fails
//! on every PR.
//!
//! # The mechanism it guards
//!
//! A `PartitionChangeRecord` carries no leader epoch: its decode
//! (`partition_change_from_kraft` in krabka-protocol's `kraft_translate`)
//! bumps the epoch of the image it meets whenever the record sets a leader.
//! Krabka writes a partition change that way whenever its batch also carries
//! the partition's recovery state or ELR, because `coalesce_partition_state`
//! (`elr/maintain.rs`) folds those into a `V1PartitionUpdate`. An unclean
//! election is always such a batch: it puts the partition in `RECOVERING`.
//!
//! Before f080173 (#1940), a restart loaded the newest checkpoint and then
//! replayed the committed log from the durable log start, which an interval
//! checkpoint does not prune. The `PartitionChangeRecord` leader-epoch bump was
//! applied twice when a restart replayed the log prefix below its checkpoint:
//! the restarted node's image held the partition one epoch above the
//! committed one, every later delta bumped from there, and its broker
//! installed that epoch. As a follower it then fetched at an epoch the leader
//! did not know (`UNKNOWN_LEADER_EPOCH`) and backed off forever; as a leader
//! it would have answered its followers `FENCED_LEADER_EPOCH` with itself at
//! the higher epoch as the hint, which their `replica_fetch_mutation` rejects.
//! `Engine::open` now replays from the checkpoint end
//! (`crates/raft/src/kraft/controller/startup.rs`), and the current
//! configuration below converges.
//!
//! # What the model runs
//!
//! Three nodes; one partition replicated on nodes 1 and 2 with
//! `unclean.leader.election.enable=true`, led by node 1 alone in its ISR.
//! Node 3 is the active controller, hosts no replica and never crashes, so
//! its image is the committed one. Every transition that touches metadata
//! runs production code on a real `MetadataImage` (see [`cluster`]): the
//! controller's encode (`encode_record`, as `prepare_records` drives it),
//! every live apply and every restart replay (`decode_committed_value`,
//! `validate`, `apply`), the restart plan (`compute_reregistration_changes`),
//! the controller's `AlterPartition` row (`handle_partition_with_recovery`
//! and `ElrPublisher::extend`), and the follower's `replica_fetch_mutation`.
//! The replay start, the supervisor's reconcile pass, the leader's fetch
//! fence and the leader's ISR proposal are restated, each citing the function
//! it mirrors.
//!
//! The faults are crash-restarts of the replicas, two in total. A restart is
//! atomic: the node replays, then re-registers, and the controller commits its
//! restart plan against the committed image with nothing uncommitted, which
//! is what the guarded admission of #1936 guarantees. Applying a batch,
//! writing a checkpoint, a reconcile pass, a fetch and an `AlterPartition` are
//! steps. A pause or a network partition only delays steps, which the
//! interleaving covers, so neither is a separate action.
//!
//! # Properties
//!
//! Both are liveness checked as safety through [`Quiescence`]: in a state
//! where no step can change anything,
//!
//! - `every_broker_installs_the_committed_target`: every replica has
//!   installed the committed leader and leader epoch;
//! - `the_committed_leader_installs_the_committed_target`: the committed
//!   leader has, which the partition needs to serve at all.
//!
//! `sometimes` witnesses keep them from passing vacuously: an unclean
//! election commits, a node restarts from a checkpoint with committed batches
//! below it, and the cluster converges after an unclean election.
//!
//! # The regression configuration
//!
//! `the_replay_before_1940_leaves_a_broker_ahead_of_the_committed_epoch` is a
//! deliberate regression witness, like the RED configurations of other
//! models: it restates the replay from the log start
//! ([`ReplayFrom::LogStart`]) and must reach a counterexample to
//! `every_broker_installs_the_committed_target` in exactly 12 actions:
//!
//! ```text
//! CrashRestart(1)    node 1 restarts; its plan elects node 2 uncleanly: a
//!                    PartitionChangeRecord with leader 2, epoch 0 -> 1
//! Apply(1), Apply(2), Reconcile(2)   node 2 leads at epoch 1
//! AlterPartition(2)  node 2 reports RECOVERED: a delta with no leader
//! Apply(1), Checkpoint(1)            node 1's checkpoint holds both batches
//! CrashRestart(1)    the replay applies both again: epoch 2 on node 1
//! Reconcile(1)       node 1 follows node 2 at epoch 2, which node 2 answers
//!                    UNKNOWN_LEADER_EPOCH on every fetch
//! Apply(2), Checkpoint(2), Reconcile(2)   quiescent, node 1 at epoch 2,
//!                    committed epoch 1
//! ```
//!
//! The leader property holds there within two restarts: the stuck-leader
//! form needs the diverged node elected through a further delta, a third
//! restart.
//!
//! # What the model leaves out
//!
//! The controller's own restart and a quorum failover; the liveness scan's
//! election on a session timeout (`compute_failover_changes`, async over the
//! liveness registry); broker registration records and broker epochs; the ELR
//! feature (off, so only recovery state makes a batch a delta); retention
//! pruning the log prefix; the lag-driven ISR shrink; more than one
//! partition, so the supervisor's one-partition-at-a-time pass shows only as
//! "install, then retarget"; the partition transition guard (a Produce holds
//! the read guard only until its leadership watch, its high-watermark wait or
//! its deadline ends it, and `install_replication_target` takes the write
//! guard only for a changed target, so the guard delays a reconcile and never
//! stops one); high-watermark arithmetic; and the data in the logs.
//!
//! [`Quiescence`]: crate::model_check::Quiescence

// Each child is declared with an explicit `#[path]`, so the declaration names
// the same file however this root is reached.
#[path = "metadata_application_model/cluster.rs"]
mod cluster;
#[path = "metadata_application_model/model.rs"]
mod model;
#[path = "metadata_application_model/state.rs"]
mod state;

use {
    self::{
        cluster::{Cluster, ReplayFrom},
        model::{EVERY_BROKER_CONVERGES, MetadataApplication},
    },
    crate::model_check::{KnownFailure, check_model_with_known_failures},
};

/// Depth cap, generated-state cap and unique-state bound of both runs.
const LIMITS: (usize, usize, usize) = (60, 1_000_000, 100_000);
/// Unique states of the current replay.
const PINNED_UNIQUE_STATES: usize = 62_322;
/// Unique states of the replay before #1940.
const PINNED_UNIQUE_STATES_LOG_START_REPLAY: usize = 61_297;

fn model(replay_from: ReplayFrom) -> MetadataApplication {
    MetadataApplication {
        cluster: Cluster::new(),
        replay_from,
        restarts: 2,
        max_log: 4,
    }
}

#[test]
fn metadata_application_converges_once_quiescent() {
    check_model_with_known_failures(
        model(ReplayFrom::CheckpointEnd),
        "metadata_application",
        LIMITS,
        PINNED_UNIQUE_STATES,
        &[],
    );
}

/// The regression witness: the replay before #1940 must still reach the
/// double-applied epoch bump.
#[test]
fn the_replay_before_1940_leaves_a_broker_ahead_of_the_committed_epoch() {
    check_model_with_known_failures(
        model(ReplayFrom::LogStart),
        "metadata_application_log_start_replay",
        LIMITS,
        PINNED_UNIQUE_STATES_LOG_START_REPLAY,
        &[KnownFailure {
            property: EVERY_BROKER_CONVERGES,
            depth: 12,
        }],
    );
}
