//! Crash-safe replacement of a small sidecar file that the WAL quorum keeps on
//! disk, such as the follower's durable-offset checkpoint and the quorum
//! membership descriptor.
//!
//! The new bytes go to a temporary file and are fsynced. The previous file
//! moves aside as a backup, the temporary file is renamed over the primary,
//! and the backup is removed. At every point a crash can stop the replace, the
//! primary or the backup holds a complete copy, so a reader that falls back to
//! the backup always finds one.

use std::{
    fs::{self, File},
    io::{self, Write as _},
    path::Path,
};

/// Replaces `path` with `bytes`, writing through `temporary` and keeping the
/// previous file at `backup` until the new one is in place.
pub(super) fn replace_file_durably(
    path: &Path,
    temporary: &Path,
    backup: &Path,
    bytes: &[u8],
) -> io::Result<()> {
    replace_file_durably_with(path, temporary, backup, bytes, |from, to| {
        fs::rename(from, to)
    })
}

/// Replaces the file with `rename` as the filesystem rename, the seam a test
/// uses to stop the replace at a chosen rename as a crash would.
///
/// When only the backup is there, as after a crash between the two renames of
/// an earlier replace, it is moved back to the primary before anything is
/// deleted.
fn replace_file_durably_with(
    path: &Path,
    temporary: &Path,
    backup: &Path,
    bytes: &[u8],
    mut rename: impl FnMut(&Path, &Path) -> io::Result<()>,
) -> io::Result<()> {
    let mut file = File::create(temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    drop(file);
    if backup.exists() {
        if path.exists() {
            fs::remove_file(backup)?;
        } else {
            rename(backup, path)?;
        }
    }
    if path.exists() {
        rename(path, backup)?;
    }
    if let Err(error) = rename(temporary, path) {
        // Put the previous file back, but never over a primary that landed.
        if matches!(
            (path.try_exists(), backup.try_exists()),
            (Ok(false), Ok(true))
        ) {
            let _ = fs::rename(backup, path);
        }
        return Err(error);
    }
    if backup.exists() {
        fs::remove_file(backup)?;
    }
    // A durable file is not enough on filesystems where the rename itself is
    // only stable after the parent directory is synced. Rust does not expose
    // directory handles that can be flushed on Windows; the file sync above is
    // the strongest portable guarantee there, matching `krabka-log`.
    #[cfg(unix)]
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::assert,
        std::{fs, path::PathBuf},
    };

    /// A directory and the three paths of one sidecar file in it.
    struct Sidecar {
        _dir: tempfile::TempDir,
        path: PathBuf,
        temporary: PathBuf,
        backup: PathBuf,
    }

    fn sidecar() -> Sidecar {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sidecar");
        Sidecar {
            temporary: dir.path().join("sidecar.tmp"),
            backup: dir.path().join("sidecar.bak"),
            path,
            _dir: dir,
        }
    }

    /// What each of the three paths holds, `None` where there is no file.
    #[derive(Debug, PartialEq, Eq)]
    struct OnDisk {
        path: Option<Vec<u8>>,
        temporary: Option<Vec<u8>>,
        backup: Option<Vec<u8>>,
    }

    fn on_disk(sidecar: &Sidecar) -> OnDisk {
        let read = |path: &Path| fs::read(path).ok();
        OnDisk {
            path: read(&sidecar.path),
            temporary: read(&sidecar.temporary),
            backup: read(&sidecar.backup),
        }
    }

    #[yare::parameterized(
        first_write = { None, None },
        replaces_the_primary = { Some(b"old".as_slice()), None },
        drops_a_stale_backup = { Some(b"old".as_slice()), Some(b"stale".as_slice()) },
        promotes_a_lone_backup = { None, Some(b"backup".as_slice()) },
    )]
    fn replace_leaves_only_the_new_primary(primary: Option<&[u8]>, backup: Option<&[u8]>) {
        let sidecar = sidecar();
        if let Some(primary) = primary {
            fs::write(&sidecar.path, primary).unwrap();
        }
        if let Some(backup) = backup {
            fs::write(&sidecar.backup, backup).unwrap();
        }
        fs::write(&sidecar.temporary, b"incomplete").unwrap();

        replace_file_durably(&sidecar.path, &sidecar.temporary, &sidecar.backup, b"new").unwrap();

        assert2::assert!(
            on_disk(&sidecar)
                == OnDisk {
                    path: Some(b"new".to_vec()),
                    temporary: None,
                    backup: None,
                }
        );
    }

    #[yare::parameterized(
        rotate_primary_to_backup = { 0, Some(b"old".as_slice()), None },
        promote_lone_backup = { 0, None, Some(b"backup".as_slice()) },
        rotate_after_promoting = { 1, None, Some(b"backup".as_slice()) },
        install_new_primary = { 1, Some(b"old".as_slice()), None },
        install_after_promoting = { 2, None, Some(b"backup".as_slice()) },
    )]
    fn a_crash_at_any_rename_leaves_a_complete_copy(
        crash_at: usize,
        primary: Option<&[u8]>,
        backup: Option<&[u8]>,
    ) {
        let sidecar = sidecar();
        if let Some(primary) = primary {
            fs::write(&sidecar.path, primary).unwrap();
        }
        if let Some(backup) = backup {
            fs::write(&sidecar.backup, backup).unwrap();
        }
        let previous = primary.or(backup).unwrap().to_vec();
        let mut renames = 0;
        let mut readable_at_crash = None;

        let result = replace_file_durably_with(
            &sidecar.path,
            &sidecar.temporary,
            &sidecar.backup,
            b"new",
            |from, to| {
                if renames == crash_at {
                    let disk = on_disk(&sidecar);
                    readable_at_crash = Some(disk.path.or(disk.backup));
                    return Err(io::Error::other("crash"));
                }
                renames += 1;
                fs::rename(from, to)
            },
        );

        assert2::assert!(result.unwrap_err().to_string() == "crash");
        assert2::assert!(readable_at_crash == Some(Some(previous.clone())));
        let disk = on_disk(&sidecar);
        assert2::assert!(disk.path.or(disk.backup) == Some(previous));
    }

    #[test]
    fn a_failed_final_rename_puts_the_backup_back() {
        let sidecar = sidecar();
        fs::write(&sidecar.path, b"old").unwrap();

        let result = replace_file_durably_with(
            &sidecar.path,
            &sidecar.temporary,
            &sidecar.backup,
            b"new",
            |from, to| {
                if from == sidecar.temporary {
                    return Err(io::Error::other("final rename failed"));
                }
                fs::rename(from, to)
            },
        );

        assert!(result.unwrap_err().to_string() == "final rename failed");
        assert!(
            on_disk(&sidecar)
                == OnDisk {
                    path: Some(b"old".to_vec()),
                    temporary: Some(b"new".to_vec()),
                    backup: None,
                }
        );
    }

    #[test]
    fn a_failed_final_rename_keeps_a_primary_that_landed() {
        let sidecar = sidecar();
        fs::write(&sidecar.path, b"old").unwrap();

        let result = replace_file_durably_with(
            &sidecar.path,
            &sidecar.temporary,
            &sidecar.backup,
            b"new",
            |from, to| {
                fs::rename(from, to)?;
                if to == sidecar.path {
                    return Err(io::Error::other("rename reported failure after landing"));
                }
                Ok(())
            },
        );

        assert!(result.is_err());
        assert!(
            on_disk(&sidecar)
                == OnDisk {
                    path: Some(b"new".to_vec()),
                    temporary: None,
                    backup: Some(b"old".to_vec()),
                }
        );
    }
}
