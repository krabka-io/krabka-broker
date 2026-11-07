//! What a failed local log read does to the partition's log dir, and the
//! error code its partition row answers. Fetch and `ShareFetch` both read the
//! local log, and both settle a failed read here.

use {
    crate::{codes, error::BrokerError, log_dir_status::LogDirRegistry, partition::Partition},
    krabka_log::LogError,
};

/// Settles a failed local log read of `part`, as Kafka's
/// `UnifiedLog.maybeHandleIOException` and `Errors.forException` do for
/// what `ReplicaManager.readFromLog` catches. It returns the row's code:
///   * an I/O fault takes the partition's log dir offline, so the next
///     request reaches the offline-dir refusal and the controller moves
///     leadership away instead of the same read failing on every retry. It
///     is `KAFKA_STORAGE_ERROR`;
///   * a batch that does not check or decode is `CORRUPT_MESSAGE`, the code
///     of Kafka's `CorruptRecordException`, which
///     `FileLogInputStream.nextBatch` raises for a batch whose size is
///     smaller than a batch header;
///   * anything else is the generic row that `Errors.forException` gives it.
///
/// It does not map an offset below the log start: Fetch answers that with
/// `OFFSET_OUT_OF_RANGE`, and `ShareFetch` moves its start offset instead.
pub(super) fn local_read_failure(
    part: &Partition,
    error: &BrokerError,
    log_dir_status: &LogDirRegistry,
) -> i16 {
    match error {
        BrokerError::Log(LogError::Io(io_error)) => {
            log_dir_status.mark_offline(
                &part.log_dir.load(),
                &format!("partition read failed: {io_error}"),
            );
            codes::KAFKA_STORAGE_ERROR
        }
        BrokerError::Log(
            LogError::CrcMismatch { .. }
            | LogError::PartialBatch { .. }
            | LogError::Records(_)
            | LogError::Corrupt(_),
        ) => codes::CORRUPT_MESSAGE,
        other => codes::from_broker_error(other),
    }
}

#[cfg(test)]
mod tests {
    use {
        super::local_read_failure,
        crate::{
            codes::{CORRUPT_MESSAGE, KAFKA_STORAGE_ERROR, UNKNOWN_SERVER_ERROR},
            error::BrokerError,
            log_dir_status::LogDirRegistry,
            partition::test_support::test_partition,
        },
        assert2::assert,
        krabka_log::{LogError, Offset},
        krabka_protocol::records::RecordsError,
        std::{io, sync::Arc},
        tokio::sync::Notify,
    };

    /// The code and the log dir's offline state after a failed read.
    #[derive(Debug, PartialEq, Eq)]
    struct Settled {
        code: i16,
        offline: bool,
    }

    #[rstest::rstest]
    #[case::disk_io(
        LogError::Io(io::Error::other("EIO")),
        Settled { code: KAFKA_STORAGE_ERROR, offline: true },
    )]
    #[case::crc_mismatch(
        LogError::CrcMismatch { segment: Offset(0), file_offset: 0, expected: 1, computed: 2 },
        Settled { code: CORRUPT_MESSAGE, offline: false },
    )]
    #[case::partial_batch(
        LogError::PartialBatch { segment: Offset(0), file_offset: 0 },
        Settled { code: CORRUPT_MESSAGE, offline: false },
    )]
    #[case::record_parse(
        LogError::Records(RecordsError::RecordParse("bad batch".into())),
        Settled { code: CORRUPT_MESSAGE, offline: false },
    )]
    #[case::corrupt_index(
        LogError::Corrupt("txn index".into()),
        Settled { code: CORRUPT_MESSAGE, offline: false },
    )]
    #[case::bad_segment_name(
        LogError::BadSegmentName("x.log".into()),
        Settled { code: UNKNOWN_SERVER_ERROR, offline: false },
    )]
    #[case::offset_mismatch(
        LogError::OffsetMismatch { expected: Offset(1), actual: Offset(0) },
        Settled { code: UNKNOWN_SERVER_ERROR, offline: false },
    )]
    #[tokio::test]
    async fn a_failed_local_read_settles_as_kafka_does(
        #[case] error: LogError,
        #[case] expected: Settled,
    ) {
        let (partition, dir) = test_partition(Arc::new(Notify::new()));
        let log_dirs = LogDirRegistry::probe(&[dir.path().to_path_buf()]);

        let code = local_read_failure(&partition, &BrokerError::Log(error), &log_dirs);

        assert!(
            Settled {
                code,
                offline: log_dirs.is_offline(dir.path()),
            } == expected
        );
    }

    /// A broker-side I/O error, such as a read task that panicked, is not the
    /// disk's fault and leaves the dir online.
    #[tokio::test]
    async fn a_broker_io_error_leaves_the_dir_online() {
        let (partition, dir) = test_partition(Arc::new(Notify::new()));
        let log_dirs = LogDirRegistry::probe(&[dir.path().to_path_buf()]);

        let code = local_read_failure(
            &partition,
            &BrokerError::Io(io::Error::other("task panicked")),
            &log_dirs,
        );

        assert!(code == UNKNOWN_SERVER_ERROR);
        assert!(!log_dirs.is_offline(dir.path()));
    }
}
