//! Per-segment `.txnindex` file. One fixed-width record per aborted
//! transaction in the segment:
//!
//!   `start_offset`: i64 (big-endian)
//!   `last_offset`:  i64 (big-endian)
//!   `producer_id`:  i64 (big-endian)
//!
//! The byte layout matches Apache Kafka's `TransactionIndex`, so
//! `kafka-dump-log --offsets-decoder` can dump it.

use std::{fs::OpenOptions, io::Write, path::PathBuf};

use krabka_ids::{Offset, ProducerId};
use tracing::instrument;
use zerocopy::{
    BigEndian, FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned, byteorder::I64,
};

use crate::error::LogError;

const ENTRY_BYTES: usize = 24;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AbortedTxn {
    pub start_offset: Offset,
    pub last_offset: Offset,
    pub producer_id: ProducerId,
}

/// On-disk byte layout of one `AbortedTxn` entry. `zerocopy` reinterprets it
/// in place from the file bytes.
#[derive(Debug, Clone, Copy, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct AbortedTxnRaw {
    start_offset: I64<BigEndian>,
    last_offset: I64<BigEndian>,
    producer_id: I64<BigEndian>,
}

const _: [(); ENTRY_BYTES] = [(); std::mem::size_of::<AbortedTxnRaw>()];

#[derive(Debug)]
pub struct TxnIndex {
    path: PathBuf,
    entries: Vec<AbortedTxn>,
}

impl TxnIndex {
    /// Open or recover a `.txnindex` file at the given path. This method
    /// reads the entire file into memory at startup. An empty file or a
    /// missing file is acceptable and means zero aborted transactions.
    #[instrument(
        level = "debug",
        skip_all,
        fields(path = %path.display(), entries = tracing::field::Empty),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    /// # Panics
    /// Panics if synchronized log state is poisoned or a segment previously validated as nonempty is unexpectedly missing its required batch or index entry.
    pub fn open(path: PathBuf) -> Result<Self, LogError> {
        let mut entries = Vec::new();
        match std::fs::read(&path) {
            Ok(bytes) => {
                if !bytes.len().is_multiple_of(ENTRY_BYTES) {
                    return Err(LogError::Corrupt(format!(
                        "txnindex {} has length {} not divisible by {}",
                        path.display(),
                        bytes.len(),
                        ENTRY_BYTES,
                    )));
                }
                let raws = <[AbortedTxnRaw]>::ref_from_bytes(&bytes)
                    .expect("length is a multiple of ENTRY_BYTES and AbortedTxnRaw is Unaligned");
                entries.reserve(raws.len());
                for raw in raws {
                    let entry = AbortedTxn {
                        start_offset: Offset(raw.start_offset.get()),
                        last_offset: Offset(raw.last_offset.get()),
                        producer_id: ProducerId(raw.producer_id.get()),
                    };
                    if !Self::entry_valid(entry) {
                        return Err(LogError::Corrupt(format!(
                            "txnindex {} contains an invalid aborted interval",
                            path.display()
                        )));
                    }
                    if !entries.contains(&entry) {
                        entries.push(entry);
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(LogError::Io(e)),
        }
        tracing::Span::current().record("entries", entries.len());
        Ok(Self { path, entries })
    }

    /// Append one aborted-txn entry.
    #[instrument(
        level = "debug",
        skip(self),
        fields(producer_id = entry.producer_id.0),
        err,
    )]
    /// # Errors
    /// Returns an error when log I/O fails, a record or index is corrupt, or the requested offset violates the segment state.
    pub fn append(&mut self, entry: AbortedTxn) -> Result<(), LogError> {
        if !Self::entry_valid(entry) {
            return Err(LogError::InvalidArgument(format!(
                "invalid aborted transaction {}..={} for producer {}",
                entry.start_offset, entry.last_offset, entry.producer_id
            )));
        }
        if self.entries.contains(&entry) {
            return Ok(());
        }
        let mut f = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(LogError::Io)?;
        let raw = AbortedTxnRaw {
            start_offset: I64::new(entry.start_offset.0),
            last_offset: I64::new(entry.last_offset.0),
            producer_id: I64::new(entry.producer_id.0),
        };
        f.write_all(raw.as_bytes()).map_err(LogError::Io)?;
        f.sync_data().map_err(LogError::Io)?;
        self.entries.push(entry);
        Ok(())
    }

    /// Remove entries whose inclusive end reaches the truncated suffix and
    /// rewrite the sidecar before it is reused as the active transaction index.
    ///
    /// # Errors
    ///
    /// Returns an I/O error if the transaction-index sidecar cannot be
    /// rewritten or synchronized.
    pub fn truncate_from(&mut self, offset: Offset) -> Result<(), LogError> {
        let entries: Vec<_> = self
            .entries
            .iter()
            .copied()
            .filter(|entry| entry.last_offset < offset)
            .collect();
        if entries.len() == self.entries.len() {
            return Ok(());
        }
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)
            .map_err(LogError::Io)?;
        for entry in &entries {
            let raw = AbortedTxnRaw {
                start_offset: I64::new(entry.start_offset.0),
                last_offset: I64::new(entry.last_offset.0),
                producer_id: I64::new(entry.producer_id.0),
            };
            file.write_all(raw.as_bytes()).map_err(LogError::Io)?;
        }
        file.sync_data().map_err(LogError::Io)?;
        self.entries = entries;
        Ok(())
    }

    #[must_use]
    pub fn entries(&self) -> &[AbortedTxn] {
        &self.entries
    }

    fn entry_valid(entry: AbortedTxn) -> bool {
        krabka_verified::aborted_transaction_interval(
            Some(entry.start_offset.0),
            entry.last_offset.0,
            entry.producer_id.get(),
        ) == Some((entry.start_offset.0, entry.last_offset.0))
    }

    /// Aborted transactions whose offset range overlaps `[start, end)`.
    pub fn aborted_in_range(
        &self,
        start: Offset,
        end: Offset,
    ) -> impl Iterator<Item = &AbortedTxn> {
        self.entries.iter().filter(move |entry| {
            krabka_verified::aborted_transaction_overlaps(
                entry.start_offset.0,
                entry.last_offset.0,
                start.0,
                end.0,
            )
        })
    }
}

#[cfg(test)]
mod tests {

    use tempfile::TempDir;

    use super::*;

    fn write_entry(path: &std::path::Path, entry: AbortedTxn) {
        let raw = AbortedTxnRaw {
            start_offset: I64::new(entry.start_offset.0),
            last_offset: I64::new(entry.last_offset.0),
            producer_id: I64::new(entry.producer_id.0),
        };
        std::fs::write(path, raw.as_bytes()).unwrap();
    }

    /// The range end is exclusive: an aborted transaction that begins exactly
    /// where the fetch ends is not in it.
    ///
    /// A consumer uses this list to skip aborted records inside the batch range
    /// it was handed. Including one that starts past the end makes it skip
    /// records it was never sent.
    #[test]
    fn an_aborted_txn_starting_at_the_range_end_is_outside_it() {
        let dir = TempDir::new().unwrap();
        let mut index = TxnIndex::open(dir.path().join("00000000000000000000.txnindex")).unwrap();
        index
            .append(AbortedTxn {
                start_offset: Offset(10),
                last_offset: Offset(20),
                producer_id: ProducerId(1),
            })
            .unwrap();

        let found =
            |start: i64, end: i64| index.aborted_in_range(Offset(start), Offset(end)).count();
        assert2::check!(found(0, 10) == 0, "a fetch ending where the txn begins");
        assert2::check!(found(0, 11) == 1, "a fetch reaching one past its start");
        assert2::check!(found(20, 30) == 1, "a fetch starting on its last offset");
        assert2::check!(found(21, 30) == 0, "a fetch starting past its last offset");
    }

    #[test]
    fn empty_file_yields_empty_entries() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("00.txnindex");
        let idx = TxnIndex::open(path).unwrap();
        assert2::assert!(idx.entries() == &[]);
    }

    #[test]
    fn malformed_intervals_fail_closed_at_append_and_open() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("00.txnindex");
        let mut index = TxnIndex::open(path).unwrap();
        for entry in [
            AbortedTxn {
                start_offset: Offset(8),
                last_offset: Offset(7),
                producer_id: ProducerId(1),
            },
            AbortedTxn {
                start_offset: Offset(7),
                last_offset: Offset(8),
                producer_id: ProducerId(-1),
            },
        ] {
            assert2::assert!(let LogError::InvalidArgument(_) = index.append(entry).unwrap_err());
        }
        assert2::assert!(index.entries().is_empty());

        let partial = dir.path().join("partial.txnindex");
        std::fs::write(&partial, [0_u8]).unwrap();
        assert2::assert!(let LogError::Corrupt(_) = TxnIndex::open(partial).unwrap_err());

        for (name, entry) in [
            (
                "inverted",
                AbortedTxn {
                    start_offset: Offset(8),
                    last_offset: Offset(7),
                    producer_id: ProducerId(1),
                },
            ),
            (
                "negative-producer",
                AbortedTxn {
                    start_offset: Offset(7),
                    last_offset: Offset(8),
                    producer_id: ProducerId(-1),
                },
            ),
        ] {
            let path = dir.path().join(format!("{name}.txnindex"));
            write_entry(&path, entry);
            assert2::assert!(let LogError::Corrupt(_) = TxnIndex::open(path).unwrap_err());
        }
    }

    #[test]
    fn exact_aborted_interval_retry_is_idempotent() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("00.txnindex");
        let entry = AbortedTxn {
            start_offset: Offset(5),
            last_offset: Offset(7),
            producer_id: ProducerId(1000),
        };
        let mut index = TxnIndex::open(path.clone()).unwrap();
        index.append(entry).unwrap();
        index.append(entry).unwrap();
        assert2::assert!(index.entries() == [entry]);

        let raw = AbortedTxnRaw {
            start_offset: I64::new(entry.start_offset.0),
            last_offset: I64::new(entry.last_offset.0),
            producer_id: I64::new(entry.producer_id.0),
        };
        let mut bytes = raw.as_bytes().to_vec();
        bytes.extend_from_slice(raw.as_bytes());
        std::fs::write(&path, bytes).unwrap();
        assert2::assert!(TxnIndex::open(path).unwrap().entries() == [entry]);
    }

    #[test]
    fn mutation_io_failures_leave_the_in_memory_index_unchanged() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("00.txnindex");
        let original = AbortedTxn {
            start_offset: Offset(5),
            last_offset: Offset(7),
            producer_id: ProducerId(1000),
        };
        let mut index = TxnIndex::open(path.clone()).unwrap();
        index.append(original).unwrap();
        std::fs::remove_file(&path).unwrap();
        std::fs::create_dir(&path).unwrap();

        assert2::assert!(
            let LogError::Io(_) = index
                .append(AbortedTxn {
                    start_offset: Offset(10),
                    last_offset: Offset(12),
                    producer_id: ProducerId(1000),
                })
                .unwrap_err()
        );
        assert2::assert!(index.entries() == [original]);
        assert2::assert!(let LogError::Io(_) = index.truncate_from(Offset(0)).unwrap_err());
        assert2::assert!(index.entries() == [original]);
    }

    #[test]
    fn empty_or_inverted_fetch_range_matches_no_aborted_interval() {
        let dir = TempDir::new().unwrap();
        let mut index = TxnIndex::open(dir.path().join("00.txnindex")).unwrap();
        index
            .append(AbortedTxn {
                start_offset: Offset(10),
                last_offset: Offset(20),
                producer_id: ProducerId(1),
            })
            .unwrap();
        assert2::assert!(index.aborted_in_range(Offset(10), Offset(10)).count() == 0);
        assert2::assert!(index.aborted_in_range(Offset(20), Offset(10)).count() == 0);
    }

    #[test]
    fn append_round_trips_through_disk() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("00.txnindex");
        let mut idx = TxnIndex::open(path.clone()).unwrap();
        idx.append(AbortedTxn {
            start_offset: Offset(5),
            last_offset: Offset(7),
            producer_id: ProducerId(1000),
        })
        .unwrap();
        idx.append(AbortedTxn {
            start_offset: Offset(10),
            last_offset: Offset(12),
            producer_id: ProducerId(1000),
        })
        .unwrap();

        let idx2 = TxnIndex::open(path).unwrap();
        assert2::assert!(
            idx2.entries()
                == &[
                    AbortedTxn {
                        start_offset: Offset(5),
                        last_offset: Offset(7),
                        producer_id: ProducerId(1000)
                    },
                    AbortedTxn {
                        start_offset: Offset(10),
                        last_offset: Offset(12),
                        producer_id: ProducerId(1000)
                    },
                ]
        );
    }

    #[test]
    fn truncate_from_removes_overlapping_tail_entries_and_is_retryable() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("00.txnindex");
        let mut index = TxnIndex::open(path.clone()).unwrap();
        for (start, last) in [(0, 4), (5, 9)] {
            index
                .append(AbortedTxn {
                    start_offset: Offset(start),
                    last_offset: Offset(last),
                    producer_id: ProducerId(1),
                })
                .unwrap();
        }

        for _ in 0..2 {
            index.truncate_from(Offset(5)).unwrap();
            assert2::assert!(index.entries().len() == 1);
            assert2::assert!(index.entries()[0].last_offset == Offset(4));
        }
        let reopened = TxnIndex::open(path).unwrap();
        assert2::assert!(reopened.entries() == index.entries());
    }

    #[test]
    fn aborted_in_range_overlaps() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("00.txnindex");
        let mut idx = TxnIndex::open(path).unwrap();
        idx.append(AbortedTxn {
            start_offset: Offset(0),
            last_offset: Offset(4),
            producer_id: ProducerId(1),
        })
        .unwrap();
        idx.append(AbortedTxn {
            start_offset: Offset(10),
            last_offset: Offset(14),
            producer_id: ProducerId(2),
        })
        .unwrap();

        let in_3_to_12 = idx
            .aborted_in_range(Offset(3), Offset(12))
            .copied()
            .collect::<Vec<_>>();
        let in_5_to_9 = idx
            .aborted_in_range(Offset(5), Offset(9))
            .copied()
            .collect::<Vec<_>>();
        assert2::assert!(
            in_3_to_12
                == vec![
                    AbortedTxn {
                        start_offset: Offset(0),
                        last_offset: Offset(4),
                        producer_id: ProducerId(1),
                    },
                    AbortedTxn {
                        start_offset: Offset(10),
                        last_offset: Offset(14),
                        producer_id: ProducerId(2),
                    },
                ]
        );
        assert2::assert!(in_5_to_9 == Vec::new());
    }
}
