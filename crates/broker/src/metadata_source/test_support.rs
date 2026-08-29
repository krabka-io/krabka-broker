//! Fixtures that more than one of this module's unit-test modules needs: a
//! throwaway topic record to submit, and a wait for a freshly started
//! controller to elect itself.

use krabka_metadata::{MetadataRecord, TopicRecord};
use uuid::Uuid;

pub(super) fn topic_record(name: &str) -> MetadataRecord {
    MetadataRecord::V1Topic(TopicRecord {
        name: name.into(),
        topic_id: Uuid::new_v4(),
        partitions: 1,
        replication_factor: 1,
    })
}

pub(super) async fn wait_for_controller_leader(ctrl: &krabka_raft::ControllerHandle) {
    let mut leader_rx = ctrl.watch_leader();
    tokio::time::timeout(std::time::Duration::from_secs(2), async {
        while leader_rx.borrow().is_none() {
            leader_rx.changed().await.unwrap();
        }
    })
    .await
    .expect("controller should elect itself");
}
