//! Bounded crash/failover model for the three-voter WAL shard engine.
//!
//! Bounds: three voters, two one-record batches, three leader epochs, eight
//! ordered operations, and fewer than 20,000 generated states. Every append, quorum
//! acknowledgement, retry, and crash recovery reconstructs real `Log`
//! instances and drives `Log::append`, `WalShardEngine::replicate_and_sync`,
//! or `WalShardEngine::new(OpenMode::Recover)`. Recovery performs the real
//! tail truncation and repair. Stateright enumerates node failures, retries,
//! stale-epoch attempts, and clean leader changes around those operations.
//!
//! DRIVEN: record-batch append, exact-range replica sync, quorum
//! acknowledgement/HWM advancement, and byte-agreement recovery/truncation.
//! MODELED: `KRaft` supplies a monotonically increasing leader epoch and elects
//! only a live replica that contains the already acknowledged prefix. Process
//! crashes preserve each modelled disk exactly. Filesystem calls are assumed
//! atomic at the successful operation boundaries exposed by `krabka-log`.

use std::sync::{Arc, Mutex};

use krabka_ids::Offset;
use krabka_kraft_core::NodeId;
use krabka_log::{Log, LogConfig};
use krabka_protocol::records::{Record, RecordBatch};
use krabka_units::convert::ByteSizeExt as _;
use stateright::{Checker, Model, Property};

use super::{OpenMode, WalReplica, WalShardEngine, split_batches};

const VOTERS: usize = 3;
const MAX_RECORDS: usize = 2;
const MAX_EPOCH: u8 = 2;
const MAX_STEPS: u8 = 8;
const MAX_DEPTH: usize = 10;
const MAX_STATES: usize = 20_000;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct WalState {
    steps: u8,
    logs: [Vec<u8>; VOTERS],
    leader: usize,
    leader_epoch: u8,
    live: u8,
    hwm: usize,
    committed: Vec<u8>,
    last_ack_failed: bool,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum Action {
    Append(u8),
    Acknowledge,
    Fail(usize),
    Revive(usize),
    Elect(usize),
    CrashRecover,
}

#[derive(Clone, Debug)]
struct WalModel;

impl Model for WalModel {
    type Action = Action;
    type State = WalState;

    fn init_states(&self) -> Vec<Self::State> {
        vec![WalState {
            steps: 0,
            logs: std::array::from_fn(|_| Vec::new()),
            leader: 0,
            leader_epoch: 0,
            live: 0b111,
            hwm: 0,
            committed: Vec::new(),
            last_ack_failed: false,
        }]
    }

    fn actions(&self, state: &Self::State, actions: &mut Vec<Self::Action>) {
        if state.steps == MAX_STEPS {
            return;
        }
        if is_live(state, state.leader) {
            if state.logs[state.leader].len() < MAX_RECORDS {
                actions.push(Action::Append(state.leader_epoch));
                if state.leader_epoch > 0 {
                    actions.push(Action::Append(state.leader_epoch - 1));
                }
            }
            if state.logs[state.leader].len() > state.hwm {
                actions.push(Action::Acknowledge);
            }
        }
        for voter in 0..VOTERS {
            if is_live(state, voter) {
                actions.push(Action::Fail(voter));
                if !is_live(state, state.leader)
                    && voter != state.leader
                    && state.leader_epoch < MAX_EPOCH
                    && has_committed_prefix(state, voter)
                {
                    actions.push(Action::Elect(voter));
                }
            } else {
                actions.push(Action::Revive(voter));
            }
        }
        if state.live == 0b111 && state.logs.iter().any(|log| log != &state.logs[0]) {
            actions.push(Action::CrashRecover);
        }
    }

    fn next_state(&self, last: &Self::State, action: Self::Action) -> Option<Self::State> {
        let mut state = last.clone();
        state.steps += 1;
        match action {
            Action::Append(epoch) if epoch != state.leader_epoch => {
                state.last_ack_failed = false;
            }
            Action::Append(_) => {
                state.logs = drive_append(&state);
                state.last_ack_failed = false;
            }
            Action::Acknowledge => {
                let (logs, result) = drive_ack(&state);
                state.logs = logs;
                match result {
                    Ok(hwm) => {
                        assert2::assert!(hwm >= state.committed.len());
                        assert2::assert!(
                            state.logs[state.leader][..state.committed.len()]
                                == state.committed[..]
                        );
                        state.hwm = hwm;
                        state.committed = state.logs[state.leader][..hwm].to_vec();
                        state.last_ack_failed = false;
                    }
                    Err(()) => {
                        state.last_ack_failed = true;
                    }
                }
            }
            Action::Fail(voter) => {
                state.live &= !(1 << voter);
            }
            Action::Revive(voter) => {
                state.live |= 1 << voter;
            }
            Action::Elect(voter) => {
                state.leader = voter;
                state.leader_epoch += 1;
                state.last_ack_failed = false;
            }
            Action::CrashRecover => {
                let (logs, hwm) = drive_recovery(&state)?;
                state.logs = logs;
                assert2::assert!(hwm >= state.committed.len());
                assert2::assert!(
                    state.logs[state.leader][..state.committed.len()] == state.committed[..]
                );
                state.hwm = hwm;
                state.committed = state.logs[state.leader][..hwm].to_vec();
                state.last_ack_failed = false;
            }
        }
        Some(state)
    }

    fn properties(&self) -> Vec<Property<Self>> {
        vec![
            Property::always("hwm_is_inside_leader_log", |_, state: &WalState| {
                state.hwm <= state.logs[state.leader].len()
            }),
            Property::always("committed_frontier_matches_hwm", |_, state: &WalState| {
                state.committed.len() == state.hwm
            }),
            Property::always("committed_prefix_has_a_quorum", |_, state: &WalState| {
                state
                    .logs
                    .iter()
                    .filter(|log| log.len() >= state.hwm && log[..state.hwm] == state.committed[..])
                    .count()
                    >= 2
            }),
            Property::sometimes("quorum_acknowledges", |_, state: &WalState| state.hwm > 0),
            Property::sometimes("minority_ack_fails", |_, state: &WalState| {
                state.last_ack_failed
            }),
            Property::sometimes("leader_changes", |_, state: &WalState| state.leader != 0),
            Property::sometimes("replicas_diverge", |_, state: &WalState| {
                state.logs.iter().any(|log| log != &state.logs[0])
            }),
        ]
    }
}

fn is_live(state: &WalState, voter: usize) -> bool {
    state.live & (1 << voter) != 0
}

fn has_committed_prefix(state: &WalState, voter: usize) -> bool {
    state.logs[voter].len() >= state.hwm && state.logs[voter][..state.hwm] == state.committed[..]
}

fn drive_append(state: &WalState) -> [Vec<u8>; VOTERS] {
    let (_directory, logs) = materialize(state);
    logs[state.leader]
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .append(&mut RecordBatch {
            partition_leader_epoch: i32::from(state.leader_epoch),
            records: vec![Record::default()],
            ..RecordBatch::default()
        })
        .expect("bounded WAL append");
    observe(&logs)
}

fn drive_ack(state: &WalState) -> ([Vec<u8>; VOTERS], Result<usize, ()>) {
    let (_directory, logs) = materialize(state);
    let replicas = ordered_replicas(state.leader, &logs);
    let engine = WalShardEngine::for_model(replicas, Offset(model_offset(state.hwm)));
    for voter in 0..VOTERS {
        engine.set_replica_alive(node(voter), is_live(state, voter));
    }
    let target = Offset(model_offset(state.logs[state.leader].len()));
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("bounded WAL runtime");
    let result = runtime
        .block_on(engine.replicate_and_sync(&logs[state.leader], target))
        .map(|hwm| model_index(hwm.0))
        .map_err(|_| ());
    (observe(&logs), result)
}

fn drive_recovery(state: &WalState) -> Option<([Vec<u8>; VOTERS], usize)> {
    let (_directory, logs) = materialize(state);
    let replicas = ordered_replicas(state.leader, &logs);
    let engine = WalShardEngine::new(replicas, OpenMode::Recover).ok()?;
    let hwm = model_index(engine.durable_watermark().0);
    Some((observe(&logs), hwm))
}

fn materialize(state: &WalState) -> (tempfile::TempDir, [Arc<Mutex<Log>>; VOTERS]) {
    let directory = tempfile::tempdir().expect("bounded WAL directory");
    let logs = std::array::from_fn(|voter| {
        let mut log = Log::open(
            directory.path().join(format!("voter-{voter}")),
            LogConfig::default(),
        )
        .expect("open bounded WAL");
        for epoch in &state.logs[voter] {
            log.append(&mut RecordBatch {
                partition_leader_epoch: i32::from(*epoch),
                records: vec![Record::default()],
                ..RecordBatch::default()
            })
            .expect("materialize bounded WAL");
        }
        log.sync().expect("sync bounded WAL");
        Arc::new(Mutex::new(log))
    });
    (directory, logs)
}

fn ordered_replicas(leader: usize, logs: &[Arc<Mutex<Log>>; VOTERS]) -> Vec<WalReplica> {
    std::iter::once(leader)
        .chain((0..VOTERS).filter(|voter| *voter != leader))
        .map(|voter| WalReplica::for_test(node(voter), Arc::clone(&logs[voter])))
        .collect()
}

fn observe(logs: &[Arc<Mutex<Log>>; VOTERS]) -> [Vec<u8>; VOTERS] {
    std::array::from_fn(|voter| {
        let log = logs[voter]
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let end = log.log_end_offset();
        let raw = log
            .read_raw(Offset(0), end, krabka_units::ByteSize::from_bytes(u64::MAX))
            .expect("read bounded WAL");
        split_batches(&raw.bytes)
            .expect("decode bounded WAL")
            .into_iter()
            .map(|batch| {
                u8::try_from(batch.verbatim.leader_epoch.0).expect("bounded epoch fits in u8")
            })
            .collect()
    })
}

fn node(voter: usize) -> NodeId {
    NodeId(u64::try_from(voter).expect("bounded voter fits in u64"))
}

fn model_offset(offset: usize) -> i64 {
    i64::try_from(offset).expect("bounded offset fits in i64")
}

fn model_index(offset: i64) -> usize {
    usize::try_from(offset).expect("nonnegative bounded offset fits in usize")
}

#[test]
fn quorum_wal_append_ack_and_recovery_model() {
    let checker = WalModel
        .checker()
        .target_max_depth(MAX_DEPTH)
        .target_state_count(MAX_STATES)
        .spawn_bfs()
        .join();
    eprintln!(
        "[quorum_wal_model] unique={} generated={} depth={}",
        checker.unique_state_count(),
        checker.state_count(),
        checker.max_depth()
    );
    assert2::assert!(checker.max_depth() < MAX_DEPTH, "depth cap hit");
    assert2::assert!(checker.state_count() < MAX_STATES, "state cap hit");
    checker.assert_properties();
}
