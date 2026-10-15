//! How fast one partition appends under `preallocate`, where the active
//! segment writes through `O_DIRECT`.
//!
//! The bench appends one verbatim batch after another to a `preallocate` log,
//! as the partition writer does for one producer with one request in flight,
//! then syncs, and prints the rate over the whole run: the rate at which the
//! segment's write threads put the bytes on the disk, since an append returns
//! once its write is queued. It is the single-partition write path of
//! krabka-io/krabka-broker#1370 without the network and the client.
//!
//! It is not a criterion bench. One run writes far more than a disk's write
//! cache, so one number per run is the measurement, and a comparison needs
//! repeated runs interleaved with the other build on the same disk. The README
//! says how.
//!
//! Point `KRABKA_IOBENCH_DIR` at a directory on the disk under test; the
//! default is the system temporary directory. A filesystem without `O_DIRECT`,
//! such as tmpfs, leaves the log on the page cache, and the bench prints a
//! `skipping` line instead of a rate. `KRABKA_DIRECT_BATCH_KIB` sets the batch
//! size (default 1024, the client's `batch.size` in the issue's workload) and
//! `KRABKA_DIRECT_TOTAL_MIB` the bytes written (default 256).
//!
//! The bench is Linux-only. On every other target it builds to an empty
//! `main`.

#[cfg(target_os = "linux")]
mod linux {
    use {
        krabka_ids::{LeaderEpoch, ProducerId},
        krabka_log::{Log, LogConfig, SegmentAllocation, VerbatimBatch},
        krabka_units::prelude::{gibibytes, mebibytes},
        std::{path::PathBuf, time::Instant},
    };

    krabka_macros::record_batch_fixture!(make_batch);

    /// The payload of each record: the 1 KB records of the issue's workload.
    const RECORD_BYTES: usize = 1000;

    fn env_or(name: &str, default: usize) -> usize {
        std::env::var(name).map_or(default, |value| {
            value
                .parse()
                .unwrap_or_else(|_| panic!("{name} is not a number: {value}"))
        })
    }

    /// A batch of `records` records of [`RECORD_BYTES`] each, as a producer
    /// sends it.
    fn verbatim_batch(records: usize) -> VerbatimBatch {
        let batch = make_batch(i32::try_from(records).unwrap(), RECORD_BYTES);
        let mut bytes = bytes::BytesMut::with_capacity(batch.encoded_len());
        batch.encode(&mut bytes).unwrap();
        VerbatimBatch {
            bytes: bytes.freeze(),
            last_offset_delta: batch.last_offset_delta,
            max_timestamp: batch.max_timestamp,
            leader_epoch: LeaderEpoch(batch.partition_leader_epoch),
            producer_id: ProducerId(batch.producer_id),
            producer_epoch: batch.producer_epoch,
            base_sequence: batch.base_sequence,
            is_transactional: false,
        }
    }

    pub(super) fn main() {
        let dir =
            std::env::var_os("KRABKA_IOBENCH_DIR").map_or_else(std::env::temp_dir, PathBuf::from);
        let batch_kib = env_or("KRABKA_DIRECT_BATCH_KIB", 1024);
        let total_mib = env_or("KRABKA_DIRECT_TOTAL_MIB", 256);
        let batch = verbatim_batch(batch_kib * 1024 / RECORD_BYTES);
        let tmp = tempfile::tempdir_in(&dir).unwrap();
        let mut log = Log::open(tmp.path(), LogConfig {
            segment_allocation: SegmentAllocation::Preallocate,
            segment_size: gibibytes(1),
            tail_cache_size: mebibytes(32),
            ..LogConfig::default()
        })
        .unwrap();

        // A group append goes ahead only on a segment that writes directly,
        // so a group of two small batches doubles as the probe.
        let probe = verbatim_batch(1);
        log.append_verbatim(&probe).unwrap();
        let Some(probed) = log.try_append_verbatim_group(&[&probe, &probe]) else {
            eprintln!(
                "skipping direct_append: {} does not take O_DIRECT writes",
                dir.display()
            );
            return;
        };
        probed.unwrap();
        log.sync().unwrap();

        let appends = total_mib * 1024 * 1024 / batch.bytes.len();
        let start = Instant::now();
        for _ in 0..appends {
            log.append_verbatim(&batch).unwrap();
        }
        let queued = start.elapsed();
        log.sync().unwrap();
        let elapsed = start.elapsed();

        let bytes = (appends * batch.bytes.len()) as f64;
        println!(
            "direct_append batch={batch_kib}KiB total={total_mib}MiB: {:.0} MB/s (queued {:.2} s, synced {:.2} s)",
            bytes / elapsed.as_secs_f64() / 1.0e6,
            queued.as_secs_f64(),
            elapsed.as_secs_f64(),
        );
    }
}

#[cfg(target_os = "linux")]
fn main() { linux::main(); }

#[cfg(not(target_os = "linux"))]
fn main() {}
