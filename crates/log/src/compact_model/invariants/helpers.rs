use super::*;

/// Keys whose newest data entry carries a value.
pub(in super::super) fn live_keys(log: &[Entry]) -> HashSet<u8> {
    let mut newest: HashMap<u8, bool> = HashMap::new();
    for entry in log {
        if let (EntryKind::Data { value }, Some(k)) = (&entry.kind, entry.key) {
            newest.insert(k, value.is_some());
        }
    }
    newest
        .into_iter()
        .filter_map(|(k, live)| live.then_some(k))
        .collect()
}

/// Indices of the commit markers that have a data entry of their producer in
/// front of them, after that producer's previous marker. The model associates a
/// producer with the data under the key equal to its id.
pub(super) fn markers_behind_data(log: &[Entry]) -> HashSet<usize> {
    let mut with_data: HashSet<u8> = HashSet::new();
    let mut held = HashSet::new();
    for (idx, entry) in log.iter().enumerate() {
        match entry.kind {
            EntryKind::Data { .. } => {
                with_data.extend(entry.key);
            }
            EntryKind::Marker {
                producer_id,
                commit: true,
            } => {
                if with_data.remove(&producer_id) {
                    held.insert(idx);
                }
            }
            EntryKind::Marker { commit: false, .. } => {}
        }
    }
    held
}

/// Indices of the entries that are the newest data entry for their key.
pub(super) fn newest_data_indices(log: &[Entry]) -> HashSet<usize> {
    let mut newest: HashMap<u8, usize> = HashMap::new();
    for (idx, entry) in log.iter().enumerate() {
        if let (EntryKind::Data { .. }, Some(k)) = (&entry.kind, entry.key) {
            newest.insert(k, idx);
        }
    }
    newest.into_values().collect()
}

/// Whether a pass at `clock` may rewrite input entry `from` into output entry
/// `to`: same key and payload, and the horizon either carried unchanged or
/// stamped from `None` to `clock + delete.retention.ms`.
fn rewrites_to(from: &Entry, to: &Entry, clock: i64) -> bool {
    from.key == to.key
        && from.kind == to.kind
        && (from.horizon == to.horizon
            || (from.horizon.is_none()
                && to.horizon == Some(clock.saturating_add(DELETE_RETENTION_MS))))
}

/// Whether the input entries `required` selects appear, in order, in
/// `output`: each matched to a later output entry with the same key and
/// payload. The horizon is not compared, because a stamp is a lawful rewrite
/// and [`Invariant::IdempotentStamp`] owns its value.
pub(super) fn retains(
    input: &[Entry],
    output: &[Entry],
    required: impl Fn(usize, &Entry) -> bool,
) -> bool {
    let mut rest = output.iter();
    input
        .iter()
        .enumerate()
        .filter(|&(idx, e)| required(idx, e))
        .all(|(_, e)| rest.any(|o| o.key == e.key && o.kind == e.kind))
}

/// Whether `output` is an order-preserving subsequence of `input` under
/// [`rewrites_to`].
///
/// `fits[i][j]` says whether `output[j..]` aligns into `input[i..]`. It is
/// filled from the back: an input entry is either skipped, deleted by the
/// pass, or matched to the next output entry, which [`rewrites_to`] must
/// allow. Greedy matching would not do: an input may hold two entries with the
/// same key and payload, only one of which the output entry's horizon fits.
pub(super) fn is_stamp_only_subsequence(input: &[Entry], output: &[Entry], clock: i64) -> bool {
    let (n, m) = (input.len(), output.len());
    let mut fits = vec![vec![false; m + 1]; n + 1];
    fits[n][m] = true;
    for i in (0..n).rev() {
        for j in (0..=m).rev() {
            let matched = j < m && rewrites_to(&input[i], &output[j], clock) && fits[i + 1][j + 1];
            fits[i][j] = fits[i + 1][j] || matched;
        }
    }
    fits[0][0]
}
