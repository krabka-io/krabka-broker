//! The one fixture the checkpoint tests share: a temporary directory holding an
//! as-yet unwritten `leader-epoch-checkpoint` path.

use std::path::PathBuf;

use tempfile::TempDir;

pub(super) fn fresh() -> (TempDir, PathBuf) {
    let dir = TempDir::new().unwrap();
    let path = dir.path().join("leader-epoch-checkpoint");
    (dir, path)
}
