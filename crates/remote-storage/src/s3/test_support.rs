//! Fixtures shared by the S3 backend's unit tests: an `InMemory`-backed
//! [`S3RemoteStorage`], the segment metadata and on-disk segment files a copy
//! needs, and the throwaway signing key and chain stamp a WORM archive needs.

use std::{collections::BTreeMap, io::Write, path::PathBuf, sync::Arc};

use bytes::Bytes;
use krabka_ids::LeaderEpoch;
use object_store::{ObjectStore, memory::InMemory};
use ring::{rand::SystemRandom, signature::Ed25519KeyPair};
use tempfile::TempDir;
use uuid::Uuid;

use super::S3RemoteStorage;
use crate::{
    metadata::{
        RemoteLogSegmentId, RemoteLogSegmentMetadata, RemoteLogSegmentState, TopicIdPartition,
    },
    storage_manager::LogSegmentData,
    worm::{ChainHead, ChainStamp, EpochId, ManifestSeq, WormChainRecord, WormConfig},
};

pub(super) const WORM_KEY_ID: &str = "s3-worm-key";

pub(super) fn rsm(prefix: Option<&str>) -> S3RemoteStorage {
    S3RemoteStorage::with_store(Arc::new(InMemory::new()), prefix.map(str::to_string))
}

pub(super) fn sample_metadata(id: u128) -> RemoteLogSegmentMetadata {
    RemoteLogSegmentMetadata::new(
        RemoteLogSegmentId::new(
            TopicIdPartition::new(Uuid::from_u128(1), "orders", 0),
            Uuid::from_u128(id),
        ),
        0,
        99,
        123,
        1,
        456,
        crate::metadata::RemoteLogSegmentDetails::new(
            8,
            RemoteLogSegmentState::CopySegmentStarted,
            BTreeMap::from([(LeaderEpoch(0), 0)]),
        ),
    )
    .unwrap()
}

pub(super) fn write_file(dir: &std::path::Path, name: &str, contents: &[u8]) -> PathBuf {
    let p = dir.join(name);
    std::fs::File::create(&p)
        .unwrap()
        .write_all(contents)
        .unwrap();
    p
}

pub(super) fn sample_data(src: &std::path::Path, with_txn: bool) -> LogSegmentData {
    LogSegmentData {
        log_segment: write_file(src, "00.log", b"0123456789"),
        offset_index: write_file(src, "00.index", b"OFFSET-IDX"),
        time_index: write_file(src, "00.timeindex", b"TIME-IDX"),
        transaction_index: with_txn.then(|| write_file(src, "00.txnindex", b"TXN-IDX")),
        producer_snapshot_index: Some(write_file(src, "00.snapshot", b"SNAP")),
        leader_epoch_index: Bytes::from_static(b"EPOCH-BYTES"),
    }
}

/// The chain epoch every stamped fixture belongs to.
pub(super) fn worm_epoch() -> EpochId {
    EpochId(Uuid::from_u128(0x5eed))
}

/// A [`WormConfig`] naming a throwaway PKCS#8 Ed25519 key written into
/// `dir`. `ring` mints it because `krabka-audit` exposes no key generator.
pub(super) fn worm_config(dir: &std::path::Path, write_only: bool) -> WormConfig {
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
    let path = dir.join("worm.pk8");
    std::fs::write(&path, pkcs8.as_ref()).unwrap();
    WormConfig {
        signing_key_path: Some(path),
        signing_key_id: Some(WORM_KEY_ID.to_string()),
        write_only,
    }
}

/// An archive backed by `store`, signing with a key under `keys`.
pub(super) fn worm_rsm(
    store: Arc<dyn ObjectStore>,
    keys: &TempDir,
    write_only: bool,
) -> S3RemoteStorage {
    S3RemoteStorage::with_store(store, None)
        .with_worm(&worm_config(keys.path(), write_only))
        .unwrap()
}

/// [`sample_metadata`] plus the chain stamp the broker leaves on a segment
/// before it asks for the copy.
pub(super) fn stamped_metadata(
    id: u128,
    seq: u64,
    prev_head: ChainHead,
) -> RemoteLogSegmentMetadata {
    sample_metadata(id).with_custom_metadata(
        WormChainRecord::request(ChainStamp {
            epoch_id: worm_epoch(),
            seq: ManifestSeq(seq),
            prev_head,
        })
        .to_custom_metadata(),
    )
}
