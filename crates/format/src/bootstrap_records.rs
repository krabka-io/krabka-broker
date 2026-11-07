//! `bootstrap.records.bin`: the records that `krabka format` seeds into the
//! metadata log directory, and that the broker submits on its first start.
//!
//! The file is a big-endian `i16` version, [`BOOTSTRAP_RECORDS_VERSION`],
//! once at the start, then each record as a `u32` little-endian length and
//! its `SerdeCompat<MetadataRecord>` wincode bytes, until the end of the file:
//!
//! ```text
//! [i16 BE version][u32 LE length][wincode MetadataRecord]...
//! ```
//!
//! It is part of the 1.x on-disk contract: a 1.x broker reads every version
//! that an earlier 1.x `krabka format` wrote. The encoder and the decoder sit
//! together here so that the layout is declared once. Each caller does its own
//! file I/O.

use {
    krabka_metadata::MetadataRecord,
    serde_wincode::SerdeCompat,
    thiserror::Error,
    wincode::{Deserialize as _, Serialize as _},
};

/// The file name, in the metadata log directory.
pub const BOOTSTRAP_RECORDS: &str = "bootstrap.records.bin";

/// The format version this build writes and reads: the big-endian `i16` at
/// the front of the file, before the first record.
pub const BOOTSTRAP_RECORDS_VERSION: i16 = 0;

/// Why the version header of `bootstrap.records.bin` is refused.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum BootstrapRecordsVersionError {
    /// The file is too short to hold a version.
    #[error(
        "{BOOTSTRAP_RECORDS} has no version header: it predates krabka 1.0, so the node must be \
         reformatted"
    )]
    Missing,
    /// The file names a version this build does not read. A file written
    /// before 1.0 starts with a record length instead, which reads as an
    /// unknown version.
    #[error(
        "unrecognized version {found} of {BOOTSTRAP_RECORDS}: a file without a version header \
         predates krabka 1.0, so the node must be reformatted"
    )]
    Unsupported {
        /// The version the first two bytes name.
        found: i16,
    },
}

/// Why the bytes of `bootstrap.records.bin` do not decode.
#[derive(Debug, Error)]
pub enum BootstrapRecordsError {
    /// The version header is missing or unknown.
    #[error(transparent)]
    Version(#[from] BootstrapRecordsVersionError),
    /// The bytes end inside a record's length.
    #[error("truncated length prefix")]
    TruncatedLength,
    /// The bytes end inside a record's body.
    #[error("truncated record body")]
    TruncatedBody,
    /// A record's body is not a wincode `MetadataRecord`.
    #[error("decode: {0}")]
    Decode(wincode::ReadError),
}

/// Encodes one record as the wincode bytes that a frame of the file holds.
pub fn encode_bootstrap_record(record: &MetadataRecord) -> Result<Vec<u8>, String> {
    <SerdeCompat<MetadataRecord>>::serialize(record).map_err(|e| format!("serialize record: {e}"))
}

/// Lays out already encoded records as the file: the version header, then
/// each blob behind its `u32` little-endian length.
pub fn frame_bootstrap_records(blobs: &[Vec<u8>]) -> Result<Vec<u8>, String> {
    let mut bytes = BOOTSTRAP_RECORDS_VERSION.to_be_bytes().to_vec();
    for blob in blobs {
        let len = u32::try_from(blob.len())
            .map_err(|_| format!("record too large: {} bytes", blob.len()))?;
        bytes.extend_from_slice(&len.to_le_bytes());
        bytes.extend_from_slice(blob);
    }
    Ok(bytes)
}

/// Encodes `records` as `krabka format` writes `bootstrap.records.bin`.
///
/// # Errors
/// Returns a message when a record does not serialize or is longer than a
/// `u32` length can say.
pub fn encode_bootstrap_records(records: &[MetadataRecord]) -> Result<Vec<u8>, String> {
    let blobs = records
        .iter()
        .map(encode_bootstrap_record)
        .collect::<Result<Vec<_>, _>>()?;
    frame_bootstrap_records(&blobs)
}

/// Decodes the bytes of `bootstrap.records.bin` into its records, in order.
///
/// # Errors
/// Returns [`BootstrapRecordsError::Version`] when the version header is
/// missing or unknown, and another [`BootstrapRecordsError`] when a record is
/// truncated or does not decode.
pub fn decode_bootstrap_records(
    bytes: &[u8],
) -> Result<Vec<MetadataRecord>, BootstrapRecordsError> {
    let Some((&header, mut cur)) = bytes.split_first_chunk::<2>() else {
        return Err(BootstrapRecordsVersionError::Missing.into());
    };
    let found = i16::from_be_bytes(header);
    if found != BOOTSTRAP_RECORDS_VERSION {
        return Err(BootstrapRecordsVersionError::Unsupported { found }.into());
    }
    let mut out = Vec::new();
    while !cur.is_empty() {
        let Some((prefix, rest)) = cur.split_first_chunk::<4>() else {
            return Err(BootstrapRecordsError::TruncatedLength);
        };
        let len = u32::from_le_bytes(*prefix) as usize;
        let Some((body, rest)) = rest.split_at_checked(len) else {
            return Err(BootstrapRecordsError::TruncatedBody);
        };
        out.push(
            <SerdeCompat<MetadataRecord>>::deserialize(body)
                .map_err(BootstrapRecordsError::Decode)?,
        );
        cur = rest;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    /// The records the golden file holds.
    fn golden_records() -> Vec<MetadataRecord> {
        vec![MetadataRecord::V1FeatureLevel(
            krabka_metadata::FeatureLevelRecord {
                name: "metadata.version".into(),
                level: 30,
            },
        )]
    }

    /// `golden_records` as a 1.0 `krabka format` writes them: the version
    /// header, then one `u32` little-endian length and the wincode record.
    const GOLDEN_BYTES: &[u8] = include_bytes!("../tests/fixtures/bootstrap.records.bin");

    #[test]
    fn the_encoder_produces_the_golden_bytes() {
        assert2::assert!(encode_bootstrap_records(&golden_records()).unwrap() == GOLDEN_BYTES);
    }

    #[test]
    fn the_golden_bytes_decode_to_their_records() {
        assert2::assert!(decode_bootstrap_records(GOLDEN_BYTES).unwrap() == golden_records());
    }

    /// A file holding only the header has no records.
    #[test]
    fn a_header_alone_holds_no_records() {
        assert2::assert!(
            decode_bootstrap_records(&BOOTSTRAP_RECORDS_VERSION.to_be_bytes())
                .unwrap()
                .is_empty()
        );
    }

    /// A missing header and an unknown version are refused. A file
    /// `krabka format` wrote before 1.0 starts with the first record's `u32`
    /// little-endian length, which reads as an unknown version.
    #[test]
    fn a_missing_or_unknown_version_is_refused() {
        struct Case {
            what: &'static str,
            bytes: Vec<u8>,
            want: BootstrapRecordsVersionError,
        }

        let mut pre_1_0 = Vec::new();
        for record in golden_records() {
            let blob = <SerdeCompat<MetadataRecord>>::serialize(&record).unwrap();
            pre_1_0.extend_from_slice(&u32::try_from(blob.len()).unwrap().to_le_bytes());
            pre_1_0.extend_from_slice(&blob);
        }
        let pre_1_0_version = i16::from_be_bytes([pre_1_0[0], pre_1_0[1]]);
        let mut version_one = GOLDEN_BYTES.to_vec();
        version_one[..2].copy_from_slice(&1_i16.to_be_bytes());
        let cases: [Case; 5] = [
            Case {
                what: "empty",
                bytes: Vec::new(),
                want: BootstrapRecordsVersionError::Missing,
            },
            Case {
                what: "one byte",
                bytes: vec![0],
                want: BootstrapRecordsVersionError::Missing,
            },
            Case {
                what: "version 1",
                bytes: version_one,
                want: BootstrapRecordsVersionError::Unsupported { found: 1 },
            },
            Case {
                what: "version -1",
                bytes: vec![0xff, 0xff],
                want: BootstrapRecordsVersionError::Unsupported { found: -1 },
            },
            Case {
                what: "the pre-1.0 layout",
                bytes: pre_1_0,
                want: BootstrapRecordsVersionError::Unsupported {
                    found: pre_1_0_version,
                },
            },
        ];
        for Case { what, bytes, want } in cases {
            let error = decode_bootstrap_records(&bytes).unwrap_err();
            check!(error.to_string().contains("predates krabka 1.0"), "{what}");
            let BootstrapRecordsError::Version(got) = error else {
                panic!("{what}: not a version error: {error}");
            };
            check!(got == want, "{what}");
        }
    }

    /// The framing after the header: a short length, a short body, and a
    /// body that does not decode are each refused with their own message.
    #[test]
    fn malformed_framing_after_the_header_is_refused() {
        struct Case {
            what: &'static str,
            bytes: Vec<u8>,
            want: &'static str,
        }

        let header = BOOTSTRAP_RECORDS_VERSION.to_be_bytes();
        let framed = |tail: &[u8]| [&header[..], tail].concat();
        let cases: [Case; 4] = [
            Case {
                what: "a truncated length",
                bytes: framed(&[0, 0, 0]),
                want: "truncated length prefix",
            },
            Case {
                what: "a truncated body",
                bytes: framed(&[&100u32.to_le_bytes()[..], &[0; 4]].concat()),
                want: "truncated record body",
            },
            Case {
                what: "an undecodable body",
                bytes: framed(&[&8u32.to_le_bytes()[..], &[0xff; 8]].concat()),
                want: "decode:",
            },
            Case {
                what: "a zero-length body",
                bytes: framed(&0u32.to_le_bytes()),
                want: "decode:",
            },
        ];
        for Case { what, bytes, want } in cases {
            let error = decode_bootstrap_records(&bytes).unwrap_err();
            check!(error.to_string().starts_with(want), "{what}: {error}");
        }
    }
}
