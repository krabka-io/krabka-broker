//! Consumer checks for direct state and array derives, with small codec interfaces.
#![cfg(test)]

use assert2::assert;

#[path = "support/codec.rs"]
mod error;

use {
    bytes::{BufMut, BytesMut},
    error::{BrokerError, take},
    std::iter,
};

fn get_i32(buf: &mut &[u8]) -> Result<i32, BrokerError> { Ok(i32::from_be_bytes(take(buf)?)) }
fn get_i64(buf: &mut &[u8]) -> Result<i64, BrokerError> { Ok(i64::from_be_bytes(take(buf)?)) }

#[path = "support/framing.rs"]
mod framing;

use framing::coordinator;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Offset(i64);

#[derive(Debug, Clone, PartialEq, Eq)]
struct StateBatch {
    first_offset: Offset,
    last_offset: Offset,
    delivery_state: i8,
    delivery_count: i16,
}

fn check_version(buf: &mut &[u8]) -> Result<(), BrokerError> {
    if take::<2>(buf)? == [0, 0] {
        Ok(())
    } else {
        Err("unsupported share-state value version")
    }
}

fn put_batches(buf: &mut BytesMut, batches: &[StateBatch]) {
    // shortcut: fixture array counts and tag sizes fit one byte; use protocol helpers for wider values.
    buf.put_u8(u8::try_from(batches.len() + 1).unwrap());
    for batch in batches {
        buf.put_i64(batch.first_offset.0);
        buf.put_i64(batch.last_offset.0);
        buf.put_i8(batch.delivery_state);
        buf.put_i16(batch.delivery_count);
        buf.put_u8(0);
    }
}

fn get_batches(buf: &mut &[u8]) -> Result<Vec<StateBatch>, BrokerError> {
    let [length] = take(buf)?;
    let mut batches = Vec::new();
    for _ in 0..length.checked_sub(1).ok_or("null batches")? {
        let batch = StateBatch {
            first_offset: Offset(get_i64(buf)?),
            last_offset: Offset(get_i64(buf)?),
            delivery_state: i8::from_be_bytes(take(buf)?),
            delivery_count: i16::from_be_bytes(take(buf)?),
        };
        if take::<1>(buf)? != [0] {
            return Err("bad batch trailer");
        }
        batches.push(batch);
    }
    Ok(batches)
}

fn put_value_tags(buf: &mut BytesMut, count: i32) {
    if count == -1 {
        buf.put_u8(0);
    } else {
        buf.extend_from_slice(&[1, 0, 4]);
        buf.put_i32(count);
    }
}

fn get_value_tags(buf: &mut &[u8]) -> Result<i32, BrokerError> {
    match take::<1>(buf)? {
        [0] => Ok(-1),
        [1] if take::<2>(buf)? == [0, 4] => get_i32(buf),
        _ => Err("bad value tags"),
    }
}

#[derive(Debug, PartialEq, Eq, krabka_macros::ShareStateValue)]
#[share_state(encode_doc = "snapshot bytes", decode_doc = "invalid snapshot")]
struct SnapshotValue {
    snapshot_epoch: i32,
    state_epoch: i32,
    leader_epoch: i32,
    start_offset: Offset,
    delivery_complete_count: i32,
    create_timestamp: i64,
    write_timestamp: i64,
    state_batches: Vec<StateBatch>,
}

#[derive(Debug, PartialEq, Eq, krabka_macros::ShareStateValue)]
#[share_state(encode_doc = "update bytes", decode_doc = "invalid update")]
struct Update {
    snapshot_epoch: i32,
    leader_epoch: i32,
    start_offset: Offset,
    delivery_complete_count: i32,
    state_batches: Vec<StateBatch>,
}

fn hex(text: &str) -> Vec<u8> {
    text.split_whitespace()
        .flat_map(|part| {
            (0..part.len())
                .step_by(2)
                .map(|index| u8::from_str_radix(part.get(index..index + 2).unwrap(), 16).unwrap())
        })
        .collect()
}

#[derive(Debug, PartialEq, Eq)]
enum ShareValue {
    Snapshot(SnapshotValue),
    Update(Update),
}

struct ShareCodecScenario {
    value: Option<ShareValue>,
    golden: Vec<u8>,
    encoded: Vec<u8>,
    decoded: Option<Result<ShareValue, BrokerError>>,
    truncated: Vec<Result<ShareValue, BrokerError>>,
    unsupported: Option<Result<Update, BrokerError>>,
}

#[rstest::fixture]
fn share_codec() -> ShareCodecScenario {
    // Start this scenario with fresh inputs and observations.

    ShareCodecScenario {
        value: None,
        golden: Vec::new(),
        encoded: Vec::new(),
        decoded: None,
        truncated: Vec::new(),
        unsupported: None,
    }
}

#[rstest_bdd_macros::given("an empty share snapshot at state epoch 1 and timestamp 1000")]
fn share_snapshot(share_codec: &mut ShareCodecScenario) {
    share_codec.value = Some(ShareValue::Snapshot(SnapshotValue {
        snapshot_epoch: 0,
        state_epoch: 1,
        leader_epoch: 0,
        start_offset: Offset(-1),
        delivery_complete_count: -1,
        create_timestamp: 1000,
        write_timestamp: 1000,
        state_batches: vec![],
    }));
}

#[rstest_bdd_macros::given("a share update at snapshot epoch 3 covering offsets 12 through 15")]
fn share_update(share_codec: &mut ShareCodecScenario) {
    share_codec.value = Some(ShareValue::Update(Update {
        snapshot_epoch: 3,
        leader_epoch: 4,
        start_offset: Offset(12),
        delivery_complete_count: 7,
        state_batches: vec![StateBatch {
            first_offset: Offset(12),
            last_offset: Offset(15),
            delivery_state: 0,
            delivery_count: 1,
        }],
    }));
}

#[rstest_bdd_macros::given("Kafka share-state bytes {golden}")]
fn share_bytes(share_codec: &mut ShareCodecScenario, golden: String) {
    share_codec.golden = hex(&golden);
}

#[rstest_bdd_macros::when("the share-state value is encoded and its wire bytes are decoded")]
fn share_value_codec(share_codec: &mut ShareCodecScenario) {
    match share_codec
        .value
        .as_ref()
        .expect("a share-state value was supplied")
    {
        ShareValue::Snapshot(value) => {
            share_codec.encoded = value.encode().to_vec();
            share_codec.decoded =
                Some(SnapshotValue::decode(&share_codec.golden).map(ShareValue::Snapshot));
            share_codec.truncated = (0..share_codec.golden.len())
                .map(|end| {
                    SnapshotValue::decode(&share_codec.golden[..end]).map(ShareValue::Snapshot)
                })
                .collect();
        }
        ShareValue::Update(value) => {
            share_codec.encoded = value.encode().to_vec();
            share_codec.decoded = Some(Update::decode(&share_codec.golden).map(ShareValue::Update));
            share_codec.truncated = (0..share_codec.golden.len())
                .map(|end| Update::decode(&share_codec.golden[..end]).map(ShareValue::Update))
                .collect();
        }
    }
    share_codec.unsupported = Some(Update::decode(&hex("0001")));
}

#[rstest_bdd_macros::then("the share-state codec preserves Kafka scalar array and tag order")]
fn share_wire_order(share_codec: &ShareCodecScenario) {
    assert!(share_codec.encoded == share_codec.golden);
    let decoded = share_codec
        .decoded
        .as_ref()
        .expect("wire value decoded")
        .as_ref()
        .expect("golden bytes accepted");
    assert!(Some(decoded) == share_codec.value.as_ref());
}

#[rstest_bdd_macros::then("every incomplete share-state prefix is rejected")]
fn share_truncation(share_codec: &ShareCodecScenario) {
    for decoded in &share_codec.truncated {
        assert!(decoded.is_err());
    }
    assert!(share_codec.unsupported == Some(Err("unsupported share-state value version")));
}

rstest_bdd_macros::scenarios!("tests/features/share_state_codec", fixtures = [share_codec: ShareCodecScenario]);

fn write_items(buf: &mut BytesMut, items: &[i32]) {
    buf.put_u8(u8::try_from(items.len() + 1).unwrap());
    for &value in items {
        buf.put_i32(value);
    }
}

fn read_items(buf: &mut &[u8]) -> Result<Vec<i32>, BrokerError> {
    let [length] = take(buf)?;
    iter::repeat_with(|| get_i32(buf))
        .take(usize::from(length.checked_sub(1).ok_or("null array")?))
        .collect()
}

#[derive(Debug, PartialEq, Eq, krabka_macros::FlexibleArrayValue)]
#[array_value(record = "AssignmentRecord", encode = write_items, decode = read_items)]
struct Assignment {
    items: Vec<i32>,
}

#[yare::parameterized(
    empty_array = { vec![], hex("0000 01 00") },
    multiple_items = { vec![3, -1], hex("0000 03 00000003 ffffffff 00") },
)]
fn array_derive_keeps_version_field_framing_and_record_errors(items: Vec<i32>, golden: Vec<u8>) {
    let value = Assignment { items };
    assert2::assert!(value.encode() == golden);
    assert2::assert!(Assignment::decode(&golden) == Ok(value));
    for end in 0..golden.len() {
        assert2::assert!(Assignment::decode(&golden[..end]).is_err());
    }
    assert2::assert!(Assignment::decode(&hex("0001")) == Err("unknown AssignmentRecord version"));
    assert2::assert!(Assignment::decode(&hex("0000 00 00")) == Err("null array"));
    assert2::assert!(Assignment::decode(&hex("0000 01 01")) == Err("bad trailer"));
}
