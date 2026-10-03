use super::*;

pub(super) fn is_live(state: &WalState, voter: usize) -> bool {
    state.live & (1 << voter) != 0
}

pub(super) fn has_committed_prefix(state: &WalState, voter: usize) -> bool {
    state.logs[voter].len() >= state.hwm && state.logs[voter][..state.hwm] == state.committed[..]
}

/// The rule an acknowledgement follows: a live voter can hold the leader's
/// records up to the target exactly when its log agrees with the leader's
/// wherever both have a record, and the acknowledgement succeeds when at
/// least two voters can.
pub(super) fn ack_has_a_majority(state: &WalState) -> bool {
    let leader_log = &state.logs[state.leader];
    (0..VOTERS)
        .filter(|voter| is_live(state, *voter))
        .filter(|voter| {
            let log = &state.logs[*voter];
            let shared = log.len().min(leader_log.len());
            log[..shared] == leader_log[..shared]
        })
        .count()
        >= 2
}

pub(super) fn drive_append(state: &WalState) -> [Vec<u8>; VOTERS] {
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

pub(super) fn drive_ack(state: &WalState) -> ([Vec<u8>; VOTERS], Result<usize, ()>) {
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

pub(super) fn drive_recovery(state: &WalState) -> ([Vec<u8>; VOTERS], usize) {
    let (_directory, logs) = materialize(state);
    let replicas = ordered_replicas(state.leader, &logs);
    let engine = WalShardEngine::new(replicas, OpenMode::Recover)
        .expect("WAL recovery opens every reachable replica set");
    let hwm = model_index(engine.durable_watermark().0);
    (observe(&logs), hwm)
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
