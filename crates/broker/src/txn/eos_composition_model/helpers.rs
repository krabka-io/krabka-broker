use super::*;

// base producer id; per-producer pid = PID0 + producer index

pub(super) fn model_offset(value: usize) -> i64 {
    i64::try_from(value).expect("bounded model offset fits in i64")
}

pub(super) fn model_index(value: i64) -> usize {
    usize::try_from(value).expect("model offsets are non-negative and bounded")
}

pub(super) fn tstate(id: i8) -> TxnState {
    TxnState::from_kafka_status(id).expect("valid TxnState id")
}

/// Rebuild a real `TxnEntry` for producer `p` so the real decision cores behave
/// exactly as in a live run. Partitions and timestamps do not change the
/// decision.
pub(super) fn rebuild(p: usize, pr: Prod) -> TxnEntry {
    // Per-producer pid = PID0 + index; wrap into `ProducerId` at the seam.
    let mut e = TxnEntry::new_empty(
        "tid".to_string(),
        ProducerId(PID0 + model_offset(p)),
        pr.epoch,
        60_000,
        1,
    );
    e.state = tstate(pr.state);
    e
}

// ----- derived txn structure (faithful LSO + aborted-list mechanics) -----

/// Outcome of txn (producer, generation) from the log: a marker resolves it.
pub(super) fn txn_outcome(log: &[Batch], producer: u8, generation: u8) -> Option<Kind> {
    log.iter()
        .find(|b| {
            b.producer == producer
                && b.generation == generation
                && matches!(b.kind, Kind::Commit | Kind::Abort)
        })
        .map(|b| b.kind)
}

/// LSO = base offset of the oldest still-OPEN txn (Data present, no marker yet),
/// else the log end. This is Kafka's first-unstable-offset rule. This function
/// derives the LSO from the log alone. The txn universe is whatever
/// (producer, generation) pairs appear, so the property closures stay
/// non-capturing.
pub(super) fn lso(log: &[Batch]) -> Offset {
    let mut min_open: Option<i64> = None;
    let mut seen: Vec<(u8, u8)> = Vec::new();
    for (off, b) in log.iter().enumerate() {
        if b.kind == Kind::Data && !seen.contains(&(b.producer, b.generation)) {
            seen.push((b.producer, b.generation));
            // First occurrence of this (producer, generation) — its base offset.
            if txn_outcome(log, b.producer, b.generation).is_none() {
                min_open = Some(min_open.map_or(model_offset(off), |m| m.min(model_offset(off))));
            }
        }
    }
    Offset(min_open.unwrap_or(model_offset(log.len())))
}

/// The exclusive offset a `read_committed` consumer may see.
///
/// This function drives the REAL `compute_visibility_window` on its
/// read-committed branch, where `effective_lso = lso.min(hw)`. When an open
/// txn's records sit above the HWM, `lso > hw` and the clamp returns `hw`. The
/// consumer never reads above the watermark.
pub(super) fn effective_lso(log: &[Batch], hw: Offset) -> Offset {
    let log_end = Offset(model_offset(log.len()));
    let l = lso(log);
    let vw = compute_visibility_window(
        false, // consumer, not follower
        true,  // read_committed
        FetchWatermarks {
            log_start: Offset(0),
            // The HW may sit below the log end: replication lag.
            hw,
            lso: l,
            log_end,
            // This model's topic delivers immediately.
            deliverable: hw,
        },
        Offset(0), // fetch_offset
    );
    vw.effective_lso // = lso.min(hw)
}

/// The `read_committed` visible set: `Data` batch offsets below `effective_lso`
/// whose txn did NOT abort.
pub(super) fn visible(log: &[Batch], hw: Offset) -> Vec<i64> {
    let eff = effective_lso(log, hw);
    (0..model_offset(log.len()))
        .filter(|&off| off < eff)
        .filter(|&off| {
            let b = log[model_index(off)];
            b.kind == Kind::Data && txn_outcome(log, b.producer, b.generation) != Some(Kind::Abort)
        })
        .collect()
}

// ----- independent oracles (by producer and marker, never by generation) -----

/// Kafka's first unstable offset, derived without the generation tags `lso()`
/// reads: the smallest offset of a Data batch whose producer has written no
/// control marker after it, else the log end.
pub(super) fn first_unstable_offset(log: &[Batch]) -> Offset {
    let open = log.iter().enumerate().position(|(off, b)| {
        b.kind == Kind::Data
            && !log[off + 1..]
                .iter()
                .any(|later| later.producer == b.producer && later.kind != Kind::Data)
    });
    Offset(model_offset(open.unwrap_or(log.len())))
}

/// The Data offsets a `read_committed` consumer drops as aborted, derived the
/// way Kafka's aborted-transaction index and consumer do: each Abort marker of
/// producer `p` at offset `m` aborts every Data batch of `p` after `p`'s
/// previous control marker and before `m`.
pub(super) fn aborted_by_markers(log: &[Batch]) -> Vec<i64> {
    let mut aborted = Vec::new();
    for (m, marker) in log.iter().enumerate() {
        if marker.kind != Kind::Abort {
            continue;
        }
        for (off, b) in log[..m].iter().enumerate().rev() {
            if b.producer != marker.producer {
                continue;
            }
            if b.kind != Kind::Data {
                break;
            }
            aborted.push(model_offset(off));
        }
    }
    aborted
}
