//! The `run_writer!` shorthand that the writer-loop tests use to spawn the
//! writer task with its five argument tuples.
//!
//! The macro lives in its own `#[macro_use]` module, declared before the test
//! modules that expand it, because `macro_rules!` is only in scope after its
//! definition.

macro_rules! run_writer {
    ($topic:expr, $partition:expr, $log:expr, $log_dir:expr, $rx:expr,
     $append:expr, $replica:expr, $hw:expr, $delivery:expr,
     $status:expr, $producer:expr, $wal:expr $(,)?) => {
        run(
            ($topic, $partition),
            ($log, $log_dir),
            $rx,
            ($append, $replica, $hw, $delivery),
            ($status, $producer, $wal),
        )
    };
    ($topic:expr, $partition:expr, $log:expr, $log_dir:expr, $rx:expr,
     $append:expr, $replica:expr, $hw:expr, $status:expr, $producer:expr, $wal:expr $(,)?) => {
        run_writer!(
            $topic,
            $partition,
            $log,
            $log_dir,
            $rx,
            $append,
            $replica,
            $hw,
            DeliveryHandles::new(),
            $status,
            $producer,
            $wal,
        )
    };
}
