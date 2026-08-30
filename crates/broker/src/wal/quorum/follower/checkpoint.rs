//! The durable-offset checkpoint that the WAL follower keeps beside its log.
//! It records the offset range this broker has fsynced, and it is what recovery
//! uses to discard an uncertain suffix after a restart. The write goes through a
//! temporary file and a backup copy, so a crash in the middle of the rename
//! still leaves one readable checkpoint behind.

use std::{io::Write as _, path::Path};

use krabka_ids::Offset;
use krabka_log::Log;

pub(super) const DURABLE_OFFSET_FILE: &str = "wal-durable-offset.checkpoint";
const DURABLE_OFFSET_BACKUP_FILE: &str = "wal-durable-offset.checkpoint.bak";

#[derive(Debug, Clone, Copy)]
pub(super) struct DurableRange {
    pub(super) start: Offset,
    pub(super) end: Offset,
}

pub(super) fn recover_durable_offset(log: &mut Log, path: &Path) -> Result<(), crate::BrokerError> {
    let backup = path.with_file_name(DURABLE_OFFSET_BACKUP_FILE);
    let checkpoint = if path.exists() {
        Some(path)
    } else if backup.exists() {
        Some(backup.as_path())
    } else {
        None
    };
    let durable = checkpoint.map_or_else(
        || {
            Ok(DurableRange {
                start: log.log_start_offset(),
                end: log.log_start_offset(),
            })
        },
        |checkpoint| {
            let value = std::fs::read_to_string(checkpoint)?;
            let offsets = value
                .split_ascii_whitespace()
                .map(str::parse::<i64>)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| {
                    crate::BrokerError::Replication(format!(
                        "decode WAL durable offsets {}: {error}",
                        checkpoint.display()
                    ))
                })?;
            match offsets.as_slice() {
                [start, end] => Ok(DurableRange {
                    start: Offset(*start),
                    end: Offset(*end),
                }),
                _ => Err(crate::BrokerError::Replication(format!(
                    "decode WAL durable offsets {}: expected two offsets",
                    checkpoint.display()
                ))),
            }
        },
    )?;
    let start = log.log_start_offset();
    let end = log.log_end_offset();
    let (true, true) = (
        (start..=end).contains(&durable.start),
        (durable.start..=end).contains(&durable.end),
    ) else {
        return Err(crate::BrokerError::Replication(format!(
            "WAL durable range {}..{} is outside recovered range {}..{}",
            durable.start.0, durable.end.0, start.0, end.0
        )));
    };
    log.truncate_to(durable.end)?;
    log.trim_to_offset(durable.start)?;
    log.sync()?;
    write_durable_offset(path, durable)?;
    Ok(())
}

pub(super) fn write_durable_offset(
    path: &Path,
    durable: DurableRange,
) -> Result<(), crate::BrokerError> {
    let temporary = path.with_extension("checkpoint.tmp");
    let backup = path.with_file_name(DURABLE_OFFSET_BACKUP_FILE);
    let mut file = std::fs::File::create(&temporary)?;
    writeln!(file, "{} {}", durable.start.0, durable.end.0)?;
    file.sync_all()?;
    drop(file);
    if backup.exists() {
        std::fs::remove_file(&backup)?;
    }
    if path.exists() {
        std::fs::rename(path, &backup)?;
    }
    if let Err(error) = std::fs::rename(&temporary, path) {
        restore_durable_offset_backup(path, &backup);
        return Err(error.into());
    }
    if backup.exists() {
        std::fs::remove_file(backup)?;
    }
    #[cfg(unix)]
    if let Some(parent) = path.parent() {
        std::fs::File::open(parent)?.sync_all()?;
    }
    Ok(())
}

fn restore_durable_offset_backup(path: &Path, backup: &Path) {
    let (Ok(false), Ok(true)) = (path.try_exists(), backup.try_exists()) else {
        return;
    };
    let _ = std::fs::rename(backup, path);
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_log::LogConfig;
    use krabka_protocol::records::{Record, RecordBatch};

    use super::*;

    #[test]
    fn durable_offset_backup_is_restored_only_when_primary_is_missing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(DURABLE_OFFSET_FILE);
        let backup = dir.path().join(DURABLE_OFFSET_BACKUP_FILE);
        std::fs::write(&backup, "0 4\n").unwrap();

        restore_durable_offset_backup(&path, &backup);

        assert!(std::fs::read_to_string(&path).unwrap() == "0 4\n");
        assert!(!backup.exists());
        std::fs::write(&backup, "0 3\n").unwrap();

        restore_durable_offset_backup(&path, &backup);

        assert!(std::fs::read_to_string(&path).unwrap() == "0 4\n");
        assert!(std::fs::read_to_string(&backup).unwrap() == "0 3\n");
    }

    #[test]
    fn follower_recovery_discards_a_suffix_beyond_the_durable_checkpoint() {
        let dir = tempfile::tempdir().unwrap();
        let checkpoint = dir.path().join(DURABLE_OFFSET_FILE);
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        let mut durable = RecordBatch {
            base_offset: 0,
            records: vec![Record::default()],
            ..RecordBatch::default()
        };
        log.append(&mut durable).unwrap();
        log.sync().unwrap();
        write_durable_offset(
            &checkpoint,
            DurableRange {
                start: Offset(0),
                end: Offset(1),
            },
        )
        .unwrap();
        let mut uncertain = RecordBatch {
            base_offset: 1,
            records: vec![Record::default()],
            ..RecordBatch::default()
        };
        log.append(&mut uncertain).unwrap();
        assert2::assert!((log.log_end_offset()) == (Offset(2)));
        drop(log);

        let mut reopened = Log::open(dir.path(), LogConfig::default()).unwrap();
        recover_durable_offset(&mut reopened, &checkpoint).unwrap();

        assert2::assert!((reopened.log_end_offset()) == (Offset(1)));
        assert2::assert!((std::fs::read_to_string(checkpoint).unwrap().trim()) == ("0 1"));
    }

    #[test]
    fn follower_recovery_rejects_incomplete_and_invalid_durable_ranges() {
        for (checkpoint_value, expected_error) in [
            ("1\n", "expected two offsets"),
            ("-1 0\n", "outside recovered range"),
            ("1 0\n", "outside recovered range"),
            ("0 2\n", "outside recovered range"),
        ] {
            let dir = tempfile::tempdir().unwrap();
            let checkpoint = dir.path().join(DURABLE_OFFSET_FILE);
            let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
            let mut batch = RecordBatch {
                records: vec![Record::default()],
                ..RecordBatch::default()
            };
            log.append(&mut batch).unwrap();
            log.sync().unwrap();
            std::fs::write(&checkpoint, checkpoint_value).unwrap();

            let error = recover_durable_offset(&mut log, &checkpoint).unwrap_err();

            assert!(
                error.to_string().contains(expected_error),
                "checkpoint {checkpoint_value:?}: {error}"
            );
            assert!(log.log_start_offset() == Offset(0));
            assert!(log.log_end_offset() == Offset(1));
        }
    }
}
