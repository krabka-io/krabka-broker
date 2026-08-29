//! The `Bootstrap` or `Rejoin` decision the broker makes from the state it
//! finds in its log directory.

use std::path::Path;

use krabka_broker::BootstrapMode;

/// Pick `Bootstrap` for a fresh cluster or `Rejoin` for a restart on existing
/// state. The choice depends on whether the raft log directory holds data.
///
/// The broker hands `BrokerConfig.log_dir.join("__cluster_metadata")`
/// to `ControllerConfig.log_dir` (see `broker.rs:833`), and
/// `RaftLogStore::open` then puts its segment files under
/// `<that>/@metadata-0/`. So the absolute path of the raft segments is
/// `<log_dir>/__cluster_metadata/@metadata-0/`. On the first broker
/// boot the directory does not exist yet. On every later boot it
/// has segment files from the previous run. A present and non-empty
/// directory is the signal, which matches the `log_is_empty` check in
/// `Controller::start` and needs no log store open from here.
pub fn detect_bootstrap_mode(log_dir: &Path) -> BootstrapMode {
    // Use the controller's own emptiness check (durable raft state =
    // `__cluster_metadata/quorum-state`, written only after the node has
    // participated in an election/commit) so this Bootstrap/Rejoin choice can
    // never disagree with `Controller::start_with_listener`'s mode validation.
    //
    // The bare `__cluster_metadata/@metadata-0` segment dir is created by
    // `KraftController::open` *before* the first commit. Keying Rejoin on its
    // existence (as we used to) bricked any node killed mid-election on a
    // multi-node cold start: the next boot saw the segment dir, picked Rejoin,
    // and died with "Rejoin requires non-empty raft log" — a crashloop. A node
    // with no persisted quorum-state now correctly re-Bootstraps.
    let metadata_dir = log_dir.join("__cluster_metadata");
    if krabka_raft::metadata_log_nonempty(&metadata_dir) {
        BootstrapMode::Rejoin
    } else {
        BootstrapMode::Bootstrap
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use tempfile::tempdir;

    use super::*;

    #[test]
    fn detect_bootstrap_when_log_dir_is_empty() {
        let dir = tempdir().unwrap();
        assert!(detect_bootstrap_mode(dir.path()) == BootstrapMode::Bootstrap);
    }

    #[test]
    fn detect_bootstrap_when_metadata_dir_missing() {
        let dir = tempdir().unwrap();
        // log_dir exists with unrelated content (bootstrap.json from
        // `krabka format`) but no __cluster_metadata/@metadata-0 subdir.
        std::fs::write(dir.path().join("bootstrap.json"), "{}").unwrap();
        assert!(detect_bootstrap_mode(dir.path()) == BootstrapMode::Bootstrap);
    }

    #[test]
    fn detect_rejoin_when_quorum_state_persisted() {
        let dir = tempdir().unwrap();
        let meta = dir.path().join("__cluster_metadata");
        std::fs::create_dir_all(&meta).unwrap();
        // Durable raft state — `quorum-state` is written only after the node
        // has participated in an election/commit. This marks a true Rejoin.
        std::fs::write(meta.join("quorum-state"), b"{}").unwrap();
        assert!(detect_bootstrap_mode(dir.path()) == BootstrapMode::Rejoin);
    }

    #[test]
    fn detect_bootstrap_when_segment_dir_but_no_quorum_state() {
        // Regression: a node killed mid-election on a multi-node cold start has
        // an `@metadata-0` segment dir (created by `KraftController::open`)
        // but no `quorum-state`. It must re-Bootstrap, not die in a Rejoin
        // crashloop. Previously this returned Rejoin and bricked the node.
        let dir = tempdir().unwrap();
        let meta = dir.path().join("__cluster_metadata").join("@metadata-0");
        std::fs::create_dir_all(&meta).unwrap();
        std::fs::write(meta.join("00000000000000000000.log"), b"segment").unwrap();
        assert!(detect_bootstrap_mode(dir.path()) == BootstrapMode::Bootstrap);
    }

    #[test]
    fn detect_bootstrap_when_metadata_dir_empty() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("__cluster_metadata").join("@metadata-0")).unwrap();
        // empty @metadata-0 dir is treated as no state (corner case:
        // crashed first start before any segment was written).
        assert!(detect_bootstrap_mode(dir.path()) == BootstrapMode::Bootstrap);
    }

    #[test]
    fn detect_bootstrap_when_only_outer_cluster_metadata_dir_exists() {
        let dir = tempdir().unwrap();
        // The outer __cluster_metadata dir exists but the inner
        // @metadata-0 subdir doesn't — should still be Bootstrap.
        std::fs::create_dir_all(dir.path().join("__cluster_metadata")).unwrap();
        assert!(detect_bootstrap_mode(dir.path()) == BootstrapMode::Bootstrap);
    }
}
