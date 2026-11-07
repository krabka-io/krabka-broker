use {
    super::{WalObjectBuilder, WalObjectEntry, WalObjectError, parse_wal_object},
    assert2::check,
    bytes::Bytes,
    uuid::Uuid,
};

/// The exact bytes of [`golden_entries`] framed around the runs `abc` and
/// `defgh`: the `CKWL` header with version 1, the two runs, two 48-byte
/// footer entries, then the footer length 96 and the trailing magic. A change
/// here is a change to the 1.x on-disk contract.
const GOLDEN_WAL_OBJECT: &[u8] = include_bytes!("../../../tests/fixtures/golden_wal_object.bin");

/// Offset of the first footer entry in [`GOLDEN_WAL_OBJECT`].
const FIRST_ENTRY: usize = 14;
/// Offset of the second footer entry in [`GOLDEN_WAL_OBJECT`].
const SECOND_ENTRY: usize = FIRST_ENTRY + 48;
/// Offset of the trailer's footer length in [`GOLDEN_WAL_OBJECT`].
const FOOTER_LEN_AT: usize = SECOND_ENTRY + 48;
/// Offset of `byte_start` inside a footer entry.
const BYTE_START: usize = 36;
/// Offset of `byte_len` inside a footer entry.
const BYTE_LEN: usize = 44;

fn golden_entries() -> Vec<WalObjectEntry> {
    vec![
        WalObjectEntry {
            topic_id: Uuid::from_u128(0x0011_2233_4455_6677_8899_aabb_ccdd_eeff),
            partition: 3,
            first_offset: 10,
            last_offset: 12,
            byte_start: 6,
            byte_len: 3,
        },
        WalObjectEntry {
            topic_id: Uuid::from_u128(0xffee_ddcc_bbaa_9988_7766_5544_3322_1100),
            partition: -1,
            first_offset: 0x0102_0304_0506_0708,
            last_offset: 0x0102_0304_0506_0709,
            byte_start: 9,
            byte_len: 5,
        },
    ]
}

#[test]
fn wal_objects_encode_to_the_golden_bytes_and_parse_back() {
    let mut builder = WalObjectBuilder::new();
    for (entry, run) in golden_entries().iter().zip([&b"abc"[..], b"defgh"]) {
        builder
            .append_run(
                entry.topic_id,
                entry.partition,
                entry.first_offset,
                entry.last_offset,
                run,
            )
            .unwrap();
    }

    check!(builder.finish().unwrap().as_ref() == GOLDEN_WAL_OBJECT);
    check!(parse_wal_object(&Bytes::from_static(GOLDEN_WAL_OBJECT)).unwrap() == golden_entries());
}

/// [`GOLDEN_WAL_OBJECT`] with `edit` applied.
fn mutated(edit: fn(&mut Vec<u8>)) -> Bytes {
    let mut bytes = GOLDEN_WAL_OBJECT.to_vec();
    edit(&mut bytes);
    Bytes::from(bytes)
}

fn put(bytes: &mut [u8], at: usize, value: &[u8]) {
    bytes[at..at + value.len()].copy_from_slice(value);
}

#[yare::parameterized(
    too_short = {
        |b| b.truncate(13),
        WalObjectError::TooShort,
    },
    bad_header_magic = {
        |b| b[0] = b'X',
        WalObjectError::BadMagic,
    },
    bad_trailer_magic = {
        |b| *b.last_mut().unwrap() = b'X',
        WalObjectError::BadMagic,
    },
    bad_version = {
        |b| put(b, 4, &2u16.to_le_bytes()),
        WalObjectError::BadVersion(2),
    },
    footer_len_not_a_multiple_of_an_entry = {
        |b| put(b, FOOTER_LEN_AT, &95u32.to_le_bytes()),
        WalObjectError::BadManifest,
    },
    footer_longer_than_the_object = {
        |b| put(b, FOOTER_LEN_AT, &144u32.to_le_bytes()),
        WalObjectError::BadManifest,
    },
    footer_starts_inside_the_header = {
        // Drops the runs and the first six footer bytes, so the 96-byte
        // footer would start at offset 0.
        |b| drop(b.drain(6..FIRST_ENTRY + 6)),
        WalObjectError::BadManifest,
    },
    entry_runs_past_the_footer = {
        |b| put(b, SECOND_ENTRY + BYTE_LEN, &6u32.to_le_bytes()),
        WalObjectError::BadManifest,
    },
    entry_starts_inside_the_header = {
        |b| put(b, FIRST_ENTRY + BYTE_START, &5u64.to_le_bytes()),
        WalObjectError::BadManifest,
    },
    entry_end_overflows = {
        |b| put(b, FIRST_ENTRY + BYTE_START, &u64::MAX.to_le_bytes()),
        WalObjectError::BadManifest,
    },
)]
fn malformed_wal_objects_are_refused(edit: fn(&mut Vec<u8>), expected: WalObjectError) {
    check!(
        parse_wal_object(&mutated(edit)).map_err(|error| error.to_string())
            == Err(expected.to_string())
    );
}
