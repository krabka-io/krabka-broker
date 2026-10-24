pub(crate) mod flusher;
pub(crate) mod hot_tail;
pub(crate) mod index_log;
pub(crate) mod read;
mod reclaimer;
pub(crate) mod recovery;
mod retention;
pub(crate) mod wal_index;
pub(crate) mod wal_object;

/// Existing object namespace. Legacy reclaimers list only this path.
pub(crate) const DISKLESS_WAL_V1_PREFIX: &str = "diskless-wal";
/// Disjoint namespace for reservation-protected uploads; old reclaimers cannot discover it.
pub(crate) const DISKLESS_WAL_V2_PREFIX: &str = "krabka-diskless-wal-v2";

pub(crate) fn prefixed_object_key(prefix: Option<&str>, relative_key: &str) -> String {
    match prefix.filter(|prefix| !prefix.is_empty()) {
        Some(prefix) => format!("{prefix}/{relative_key}"),
        None => relative_key.to_owned(),
    }
}
