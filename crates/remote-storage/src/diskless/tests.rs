use {
    super::{
        WalIndexEntry,
        capture::{
            CapturedWalRange, DisklessPartitionCapture, DisklessWalCapture, WalCaptureProjection,
        },
        index::{
            REPLAY_FENCE_KEY, WAL_OBJECT_RESERVATION_PARTITION, WalDeleteFloorKey,
            WalDeleteFloorRecord, WalFlushRecord, WalIndexKey, WalObjectReservationKey,
        },
    },
    assert2::check,
    uuid::Uuid,
};

const TOPIC_ID: Uuid = Uuid::from_u128(0x0011_2233_4455_6677_8899_aabb_ccdd_eeff);

fn floor_record() -> WalDeleteFloorRecord {
    WalDeleteFloorRecord {
        topic_id: TOPIC_ID,
        partition: 3,
        floor: 42,
    }
}

fn flush_record() -> WalFlushRecord {
    WalFlushRecord {
        object_key: "wal/a".to_owned(),
        format_version: WalFlushRecord::FORMAT_VERSION,
        entries: vec![WalIndexEntry {
            topic_id: TOPIC_ID,
            partition: 3,
            first_offset: 10,
            last_offset: 12,
            byte_start: 6,
            byte_len: 64,
            max_timestamp_ms: 1_700_000_000_000,
        }],
    }
}

/// The exact value bytes of [`floor_record`]: the `i16` version 0, then
/// the wincode body. A change here is a change to the 1.x on-disk
/// contract.
const GOLDEN_FLOOR_RECORD: &[u8] = include_bytes!("../../tests/fixtures/golden_floor_record.bin");

/// The exact value bytes of [`flush_record`]: the `i16` version 2, then
/// the wincode body of `object_key` and `entries`. A change here is a
/// change to the 1.x on-disk contract.
const GOLDEN_FLUSH_RECORD: &[u8] = include_bytes!("../../tests/fixtures/golden_flush_record.bin");

/// The exact key bytes of the range key of [`flush_record`]: the `i16`
/// key version 0, then the topic id, partition and first offset.
const GOLDEN_RANGE_KEY: &[u8] = include_bytes!("../../tests/fixtures/golden_range_key.bin");

/// The exact key bytes of the delete-floor key of [`floor_record`]: the
/// `i16` key version 1, then the topic id and partition.
const GOLDEN_FLOOR_KEY: &[u8] = include_bytes!("../../tests/fixtures/golden_floor_key.bin");

#[test]
fn index_values_encode_to_the_golden_bytes_and_decode_back() {
    check!(floor_record().to_bytes().unwrap().as_ref() == GOLDEN_FLOOR_RECORD);
    check!(WalDeleteFloorRecord::from_bytes(GOLDEN_FLOOR_RECORD) == Ok(floor_record()));
    check!(flush_record().to_bytes().unwrap().as_ref() == GOLDEN_FLUSH_RECORD);
    check!(WalFlushRecord::from_bytes(GOLDEN_FLUSH_RECORD) == Ok(flush_record()));
}

#[test]
fn index_keys_encode_to_the_golden_bytes_and_decode_back() {
    let range = WalIndexKey::from(&flush_record().entries[0]);
    let floor = WalDeleteFloorKey {
        topic_id: TOPIC_ID,
        partition: 3,
    };
    check!(range.to_bytes().as_ref() == GOLDEN_RANGE_KEY);
    check!(WalIndexKey::from_bytes(GOLDEN_RANGE_KEY) == Some(range));
    check!(floor.to_bytes().as_ref() == GOLDEN_FLOOR_KEY);
    check!(WalDeleteFloorKey::from_bytes(GOLDEN_FLOOR_KEY) == Some(floor));
    // Neither key type decodes as the other, nor does the replay fence.
    check!(WalIndexKey::from_bytes(GOLDEN_FLOOR_KEY) == None);
    check!(WalDeleteFloorKey::from_bytes(GOLDEN_RANGE_KEY) == None);
    check!(WalIndexKey::from_bytes(REPLAY_FENCE_KEY) == None);
    check!(WalDeleteFloorKey::from_bytes(REPLAY_FENCE_KEY) == None);
}

#[test]
fn object_reservation_reuses_the_1x_delete_floor_wire_format() {
    let object_id = Uuid::new_v4();
    let reservation = WalObjectReservationKey::new(object_id);
    let key_bytes = reservation.to_bytes();
    let floor_key = WalDeleteFloorKey::from_bytes(&key_bytes).unwrap();
    let old_reader_value = WalDeleteFloorRecord {
        topic_id: floor_key.topic_id,
        partition: floor_key.partition,
        floor: 0,
    };

    check!(WalObjectReservationKey::from_bytes(&key_bytes) == Some(reservation));
    check!(floor_key.partition == WAL_OBJECT_RESERVATION_PARTITION);
    check!(floor_key.topic_id.get_version_num() == 8);
    check!(old_reader_value.to_bytes().unwrap()[..2] == 0i16.to_be_bytes());
    check!(
        WalDeleteFloorRecord::from_bytes(&old_reader_value.to_bytes().unwrap())
            == Ok(old_reader_value)
    );
}

/// `golden` with its leading two-byte version replaced by `version`.
fn with_version(golden: &[u8], version: i16) -> Vec<u8> {
    let mut bytes = version.to_be_bytes().to_vec();
    bytes.extend_from_slice(&golden[2..]);
    bytes
}

const PRE_1_0_NOTE: &str = " A record written before krabka 1.0 has no version prefix and is not readable; reformat the cluster";

#[yare::parameterized(
    flush_future_version = {
        WalFlushRecord::from_bytes(&with_version(GOLDEN_FLUSH_RECORD, 3)).map(|_| ()),
        format!(
            "unsupported diskless WAL index format version 3: this build reads version 2.{PRE_1_0_NOTE}"
        ),
    },
    flush_pre_1_0_layout = {
        WalFlushRecord::from_bytes(&GOLDEN_FLUSH_RECORD[2..]).map(|_| ()),
        format!(
            "unsupported diskless WAL index format version 1280: this build reads version 2.{PRE_1_0_NOTE}"
        ),
    },
    floor_future_version = {
        WalDeleteFloorRecord::from_bytes(&with_version(GOLDEN_FLOOR_RECORD, 1)).map(|_| ()),
        format!(
            "unsupported diskless WAL delete-floor record version 1: this build reads version 0.{PRE_1_0_NOTE}"
        ),
    },
    floor_pre_1_0_layout = {
        WalDeleteFloorRecord::from_bytes(&GOLDEN_FLOOR_RECORD[2..]).map(|_| ()),
        format!(
            "unsupported diskless WAL delete-floor record version 4096: this build reads version 0.{PRE_1_0_NOTE}"
        ),
    },
    empty_value = {
        WalDeleteFloorRecord::from_bytes(&[]).map(|_| ()),
        "truncated diskless WAL delete-floor record: 0 bytes hold no version".to_owned(),
    },
)]
fn index_values_of_another_version_are_refused(decoded: Result<(), String>, expected: String) {
    check!(decoded == Err(expected));
}

fn capture() -> DisklessWalCapture {
    DisklessWalCapture {
        format_version: DisklessWalCapture::FORMAT_VERSION,
        captured_at_ms: 1_700_000_000_000,
        source_cutoffs: vec![5],
        partitions: vec![DisklessPartitionCapture {
            topic: "orders".to_owned(),
            topic_id: TOPIC_ID,
            partition: 3,
            delete_floor: 10,
            recovery_cutoff: 13,
            ranges: vec![CapturedWalRange {
                object_key: "wal/a".to_owned(),
                entry: flush_record().entries[0].clone(),
            }],
        }],
        metadata_snapshot_sha256: None,
        rlmm_snapshot_sha256: None,
        group_offsets_sha256: None,
        authentication: None,
    }
}

/// The exact `diskless-wal-index.json` bytes of [`capture`]. A change here
/// is a change to the 1.x capture format.
const GOLDEN_CAPTURE: &str = concat!(
    r#"{"format_version":1,"captured_at_ms":1700000000000,"source_cutoffs":[5],"#,
    r#""partitions":[{"topic":"orders","topic_id":"00112233-4455-6677-8899-aabbccddeeff","#,
    r#""partition":3,"delete_floor":10,"recovery_cutoff":13,"ranges":[{"object_key":"wal/a","#,
    r#""entry":{"topic_id":"00112233-4455-6677-8899-aabbccddeeff","partition":3,"#,
    r#""first_offset":10,"last_offset":12,"byte_start":6,"byte_len":64,"#,
    r#""max_timestamp_ms":1700000000000}}]}]}"#,
);

#[test]
fn capture_encodes_to_the_golden_bytes_and_decodes_back() {
    check!(serde_json::to_string(&capture()).unwrap() == GOLDEN_CAPTURE);
    check!(DisklessWalCapture::from_slice(GOLDEN_CAPTURE.as_bytes()) == Ok(capture()));
}

fn capture_with_version(version: Option<u64>) -> serde_json::Value {
    let mut value: serde_json::Value = serde_json::from_str(GOLDEN_CAPTURE).unwrap();
    if let Some(version) = version {
        value["format_version"] = serde_json::json!(version);
    } else {
        value.as_object_mut().unwrap().remove("format_version");
    }
    value
}

#[yare::parameterized(
    future_version = {
        capture_with_version(Some(2)),
        "unsupported diskless WAL capture format version 2: this build reads version 1",
    },
    missing_pre_1_0_version = {
        capture_with_version(None),
        "diskless WAL capture has no format_version: it predates krabka 1.0, which this build does not read",
    },
)]
fn capture_of_a_missing_or_unknown_version_is_refused(json: serde_json::Value, expected: &str) {
    check!(
        DisklessWalCapture::from_slice(&serde_json::to_vec(&json).unwrap())
            == Err(expected.to_owned())
    );
}

#[yare::parameterized(
    unknown_version = {
        with_version(GOLDEN_RANGE_KEY, 7),
        "unknown diskless WAL index key version 7",
    },
    missing_pre_1_0_version = {
        GOLDEN_RANGE_KEY[2..].to_vec(),
        "unknown diskless WAL index key version 17",
    },
    one_byte_delete_floor_tag = {
        [&[0xf0], &GOLDEN_FLOOR_KEY[2..]].concat(),
        "unknown diskless WAL index key version -4096",
    },
    wrong_length = { GOLDEN_RANGE_KEY[..29].to_vec(), "invalid diskless WAL index key" },
)]
fn index_keys_of_another_version_are_refused(key: Vec<u8>, expected: &str) {
    let mut projection = WalCaptureProjection::default();
    check!(projection.apply(Some(&key), Some(GOLDEN_FLUSH_RECORD)) == Err(expected.to_owned()));
}
