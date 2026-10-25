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

/// The object a reservation fixture names: a version 4 UUID, as object keys use.
const RESERVED_OBJECT: Uuid = Uuid::from_u128(0x0011_2233_4455_4677_8899_aabb_ccdd_eeff);

/// The exact key bytes of the reservation of [`RESERVED_OBJECT`]: a 1.x
/// delete-floor key (key version 1) whose topic id is the object id with its
/// version nibble set to 8, on partition `i32::MIN`. A change here is a
/// change to the 1.x on-disk contract.
const GOLDEN_RESERVATION_KEY: &[u8] =
    include_bytes!("../../tests/fixtures/golden_reservation_key.bin");

/// The exact value bytes of that reservation: a 1.x `WalDeleteFloorRecord`
/// (version 0) for the same topic id and partition, at floor 0.
const GOLDEN_RESERVATION_RECORD: &[u8] =
    include_bytes!("../../tests/fixtures/golden_reservation_record.bin");

/// krabka.version 2 writes object reservations in the 1.x delete-floor wire
/// format. This build decodes the bytes as a reservation, and a 1.x reader,
/// which knows only delete floors, decodes the same bytes as an inert floor of
/// 0 on a partition no topic has.
#[test]
fn object_reservations_decode_from_the_golden_bytes_as_1x_delete_floors() {
    let reservation = WalObjectReservationKey::new(RESERVED_OBJECT);
    let floor_key = WalDeleteFloorKey {
        topic_id: Uuid::from_u128(0x0011_2233_4455_8677_8899_aabb_ccdd_eeff),
        partition: WAL_OBJECT_RESERVATION_PARTITION,
    };
    let floor_record = WalDeleteFloorRecord {
        topic_id: floor_key.topic_id,
        partition: floor_key.partition,
        floor: 0,
    };

    check!(reservation.to_bytes().as_ref() == GOLDEN_RESERVATION_KEY);
    check!(WalObjectReservationKey::from_bytes(GOLDEN_RESERVATION_KEY) == Some(reservation));
    check!(WalDeleteFloorKey::from_bytes(GOLDEN_RESERVATION_KEY) == Some(floor_key));
    check!(floor_record.to_bytes().unwrap().as_ref() == GOLDEN_RESERVATION_RECORD);
    check!(WalDeleteFloorRecord::from_bytes(GOLDEN_RESERVATION_RECORD) == Ok(floor_record));
    // An ordinary delete-floor key is not a reservation.
    check!(WalObjectReservationKey::from_bytes(GOLDEN_FLOOR_KEY) == None);
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
