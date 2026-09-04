//! Reading and writing the checkpoint file itself: `open` and its strict
//! `parse`, and the atomic `flush` that every mutation ends with. The parser
//! and the writer live together because they are the two halves of one
//! byte-for-byte Kafka format, and a change to either has to move the other.

use std::{fmt::Write as _, fs, path::PathBuf};

use krabka_ids::{LeaderEpoch, Offset};
use tracing::instrument;

use super::{EpochEntry, LeaderEpochCheckpoint, is_strict_successor};
use crate::{error::LogError, io::IoTarget};

impl LeaderEpochCheckpoint {
    /// Open or recover the checkpoint at `path`. A missing file gives an
    /// empty checkpoint.
    #[instrument(
        level = "debug",
        skip_all,
        fields(path = %path.display(), entries = tracing::field::Empty),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn open(path: PathBuf) -> Result<Self, LogError> {
        let entries = match fs::read_to_string(&path) {
            Ok(s) => Self::parse(&s)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(LogError::Io(e)),
        };
        tracing::Span::current().record("entries", entries.len());
        Ok(Self {
            path,
            io: crate::io::file_io(),
            entries,
        })
    }

    fn parse(s: &str) -> Result<Vec<EpochEntry>, LogError> {
        let mut lines = s.lines();
        let _version = lines.next();
        let count: usize = lines
            .next()
            .and_then(|l| l.trim().parse().ok())
            .unwrap_or(0);
        // Do NOT pre-size from the untrusted `count`: a corrupt or hostile
        // checkpoint (local dir, or bytes restored from tiered storage) could
        // declare a huge count and trigger a multi-GB allocation before the
        // bounded `lines.take(count)` loop ever runs. `count` is used only to
        // bound the number of rows read; the Vec grows as entries are parsed.
        // Matches Kafka's CheckpointFile, which reads entries line-by-line.
        let mut out: Vec<EpochEntry> = Vec::new();
        for line in lines.take(count) {
            let mut parts = line.split_whitespace();
            let epoch: i32 = parts
                .next()
                .and_then(|t| t.parse().ok())
                .ok_or_else(|| LogError::Corrupt(format!("bad checkpoint row: {line:?}")))?;
            let start_offset: i64 = parts
                .next()
                .and_then(|t| t.parse().ok())
                .ok_or_else(|| LogError::Corrupt(format!("bad checkpoint row: {line:?}")))?;
            let entry = EpochEntry {
                epoch: LeaderEpoch(epoch),
                start_offset: Offset(start_offset),
            };
            if out
                .last()
                .is_some_and(|previous| !is_strict_successor(previous, &entry))
            {
                return Err(LogError::Corrupt(format!(
                    "leader epoch checkpoint rows are not strictly increasing: {line:?}"
                )));
            }
            out.push(entry);
        }
        Ok(out)
    }

    /// Rewrite the whole file atomically. `mutation` calls this after every
    /// change that altered the entry list.
    pub(super) fn flush(&self) -> Result<(), LogError> {
        let mut s = String::new();
        s.push_str("0\n");
        let _ = writeln!(s, "{}", self.entries.len());
        for e in &self.entries {
            let _ = writeln!(s, "{} {}", e.epoch.0, e.start_offset.0);
        }
        let tmp = self.path.with_extension("tmp");
        {
            let f = fs::File::create(&tmp).map_err(LogError::Io)?;
            crate::io::write_all(&*self.io, IoTarget::LeaderEpochCheckpoint, &f, s.as_bytes())
                .map_err(LogError::Io)?;
            self.io
                .sync_file(IoTarget::LeaderEpochCheckpoint, &f)
                .map_err(LogError::Io)?;
        }
        self.io
            .rename(IoTarget::LeaderEpochCheckpoint, &tmp, &self.path)
            .map_err(LogError::Io)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::leader_epoch_checkpoint::test_support::fresh;

    #[test]
    fn round_trip_byte_compat_format() {
        let (_d, path) = fresh();
        let mut c = LeaderEpochCheckpoint::open(path.clone()).unwrap();
        c.append(LeaderEpoch(0), Offset(0)).unwrap();
        c.append(LeaderEpoch(1), Offset(50)).unwrap();
        c.append(LeaderEpoch(2), Offset(100)).unwrap();

        let s = std::fs::read_to_string(&path).unwrap();
        assert2::assert!(s == "0\n3\n0 0\n1 50\n2 100\n");
    }

    #[test]
    fn missing_file_yields_empty() {
        let (_d, path) = fresh();
        let c = LeaderEpochCheckpoint::open(path).unwrap();
        assert2::assert!(c.entries() == &[][..]);
        assert2::assert!(c.latest_epoch() == None);
    }

    #[test]
    fn open_rejects_out_of_order_checkpoint_rows() {
        let (_d, path) = fresh();
        std::fs::write(&path, "0\n4\n0 0\n5 100\n2 50\n4 80\n").unwrap();

        let error = LeaderEpochCheckpoint::open(path).unwrap_err();
        assert2::assert!(matches!(error, LogError::Corrupt(_)));
    }

    #[test]
    fn absurd_declared_count_does_not_over_allocate() {
        // Hostile/corrupt checkpoint: header declares billions of rows but only
        // one actual entry line follows. Parsing must not pre-size a giant Vec;
        // it should grow to fit the real rows and return just those.
        let s = "0\n9999999999999\n3 42\n";
        let entries = LeaderEpochCheckpoint::parse(s).unwrap();
        assert2::assert!(
            entries
                == [EpochEntry {
                    epoch: LeaderEpoch(3),
                    start_offset: Offset(42),
                }]
        );
        // `lines.take(count)` bounds reads to the available lines, so capacity
        // stays at the grown size, not the untrusted billions.
        assert2::assert!(entries.capacity() < 9_999_999_999_999);
    }
}
