//! Fixtures shared by the `WriteTxnMarkers` unit tests: a running broker with
//! auditing switched off, and a locally-led partition opened beneath it so
//! that the handler finds it in `broker.partitions`.

use std::{path::Path, sync::Arc};

use krabka_ids::PartitionIndex;

use crate::broker::{Broker, BrokerHandle};

/// Open `topic-partition` under `log_dir` and register it with the broker.
/// The partition starts with no local leader role; the caller installs one.
pub(crate) fn open_partition(
    broker: &Broker,
    log_dir: &Path,
    topic: &str,
    partition: i32,
) -> Arc<crate::partition::Partition> {
    let part = crate::test_support::open_partition(log_dir, topic, partition);
    broker
        .partitions
        .insert(topic.into(), PartitionIndex(partition), Arc::clone(&part));
    part
}

/// Start a broker with auditing switched off, and wait until its group
/// coordinator serves `__consumer_offsets`: the offsets-partition marker tests
/// append to that topic, and no broker creates it when it starts.
pub(super) async fn start_broker() -> (BrokerHandle, tempfile::TempDir) {
    crate::test_support::start_group_broker_with(|cfg| cfg.audit_enabled = false).await
}
