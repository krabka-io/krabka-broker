//! The clean-shutdown proof: what a graceful stop leaves, what a crash leaves,
//! and how a controller reads either one.

use assert2::assert;
use krabka_metadata::{
    BrokerEndpoint, BrokerRegistrationRecord, MetadataImage, MetadataRecord, NodeId,
};
use krabka_security::ListenerProtocol;

use super::{FILE_NAME, UNPROVEN, restart_was_clean, take, write};

const NODE: NodeId = NodeId(2);

/// An image holding one registration for [`NODE`] at `broker_epoch`.
fn image_registering_node_at(broker_epoch: i64) -> MetadataImage {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    image.apply(&MetadataRecord::V1BrokerRegistration(
        BrokerRegistrationRecord {
            node_id: NODE,
            broker_epoch,
            incarnation_id: uuid::Uuid::from_u128(7),
            host: "broker-2".into(),
            port: 9092,
            rack: None,
            endpoints: vec![BrokerEndpoint {
                name: "PLAINTEXT".into(),
                host: "broker-2".into(),
                port: 9092,
                protocol: ListenerProtocol::Plaintext,
            }],
            log_dirs: vec![uuid::Uuid::from_u128(11)],
            features: std::collections::BTreeMap::new(),
        },
    ));
    image
}

/// The crash case, and the default the whole feature rests on: a log dir that
/// never held a proof cannot read as a clean restart against *any* broker
/// epoch, including the lowest one a broker can hold.
#[test]
fn a_log_dir_with_no_proof_never_reads_as_a_clean_restart() {
    let dir = tempfile::tempdir().expect("temp dir");
    let image = image_registering_node_at(0);

    assert!(!restart_was_clean(&image, NODE, take(dir.path())));
}

/// A graceful stop's epoch comes back to the next start.
#[test]
fn a_written_proof_comes_back_to_the_next_start() {
    let dir = tempfile::tempdir().expect("temp dir");
    write(dir.path(), 4242);

    assert!(take(dir.path()) == 4242);
}

/// The proof covers one restart. A broker that starts and then dies has
/// already spent it, so the start after that finds nothing.
#[test]
fn a_proof_is_spent_by_the_start_that_reads_it() {
    let dir = tempfile::tempdir().expect("temp dir");
    write(dir.path(), 17);

    assert!(take(dir.path()) == 17);
    assert!(take(dir.path()) == UNPROVEN);
}

/// A file holding something that is not an epoch is not a proof, and does not
/// stay behind for a later start to retry.
#[test]
fn an_unparsable_proof_is_unproven_and_still_spent() {
    let dir = tempfile::tempdir().expect("temp dir");
    std::fs::write(dir.path().join(FILE_NAME), b"not-an-epoch").expect("seed file");

    assert!(take(dir.path()) == UNPROVEN);
    assert!(!dir.path().join(FILE_NAME).exists());
}

/// The controller's rule: the offered epoch has to be the epoch the cluster
/// still holds. Anything else -- a stale epoch, the unproven sentinel, or no
/// registration at all -- is an unclean restart.
#[test]
fn only_the_held_epoch_proves_a_clean_restart() {
    let image = image_registering_node_at(90);

    assert!(restart_was_clean(&image, NODE, 90));
    assert!(!restart_was_clean(&image, NODE, 89));
    assert!(!restart_was_clean(&image, NODE, UNPROVEN));
    assert!(!restart_was_clean(
        &MetadataImage::new(uuid::Uuid::nil()),
        NODE,
        90
    ));
}
