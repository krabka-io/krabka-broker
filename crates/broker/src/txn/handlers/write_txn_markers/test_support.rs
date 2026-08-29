//! Fixtures shared by the `WriteTxnMarkers` unit tests: a running broker with
//! auditing switched off, and a locally-led partition opened beneath it so
//! that the handler finds it in `broker.partitions`.

use std::{path::Path, sync::Arc};

use krabka_ids::PartitionIndex;
use krabka_log::{Log, LogConfig};

use crate::broker::{Broker, BrokerHandle};

pub(super) fn open_partition(broker: &Broker, log_dir: &Path, topic: &str, partition: i32) {
    let part_dir = crate::log_dir::partition_dir(log_dir, topic, partition);
    std::fs::create_dir_all(&part_dir).expect("create partition dir");
    let log = Log::open(&part_dir, LogConfig::default()).expect("open partition log");
    let part = crate::broker::spawn_partition(
        topic.to_string(),
        PartitionIndex(partition),
        log_dir.to_path_buf(),
        log,
        crate::log_dir_status::LogDirRegistry::default(),
        Arc::new(crate::producer_state::ProducerState::new()),
        false,
    );
    broker
        .partitions
        .insert(topic.to_string(), PartitionIndex(partition), part);
}

pub(super) async fn start_broker() -> (BrokerHandle, tempfile::TempDir) {
    crate::test_support::start_broker_with(|cfg| {
        cfg.audit_enabled = false;
    })
    .await
}
