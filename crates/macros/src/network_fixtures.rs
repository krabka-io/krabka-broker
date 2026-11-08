//! Independent wire and file-region fixtures shared by network tests and benchmarks.

use {
    crate::{
        fixtures::{name, named_items},
        meta::argument_array,
    },
    moxy::{ast::ParseError, token::TokenStream},
};

pub fn file_region(input: TokenStream) -> Result<TokenStream, ParseError> {
    named_items(input, |name| {
        moxy::template! {
        fn {{ name }}(region: &::krabka_protocol::records::FileRegion) -> Vec<u8> {
            use ::std::os::unix::fs::FileExt as _;
            let mut buf = vec![0u8; region.len];
            let mut filled = 0;
            let mut off = region.offset;
            while filled < buf.len() {
                let n = region.file.read_at(&mut buf[filled..], off).unwrap();
                ::assert2::assert!(n > 0);
                filled += n; off += n as u64;
            }
            buf
        }
        }
    })
}

/// Export the original paths and snapshot policy before constructing the caller's epoch bytes.
pub fn export_segment_data(input: TokenStream) -> Result<TokenStream, ParseError> {
    named_items(input, |name| {
        moxy::template! {
        #[derive(Clone, Copy)]
        pub enum ProducerSnapshotExport { Include, Omit }
        pub fn {{ name }}(
            export: &::krabka_log::SegmentExport,
            snapshot: ProducerSnapshotExport,
            epoch_bytes: impl FnOnce() -> ::bytes::Bytes,
        ) -> ::krabka_remote_storage::LogSegmentData {
            ::krabka_remote_storage::LogSegmentData {
                log_segment: export.log_path.clone(),
                offset_index: export.offset_index_path.clone(),
                time_index: export.time_index_path.clone(),
                transaction_index: export.transaction_index_path.clone(),
                producer_snapshot_index: matches!(snapshot, ProducerSnapshotExport::Include).then(|| export.producer_snapshot_path.clone()),
                leader_epoch_index: epoch_bytes(),
            }
        }
        }
    })
}

/// Stage checkpoint bytes under the JVM decoder's caller-chosen file name.
pub fn jvm_checkpoint_dump(input: TokenStream) -> Result<TokenStream, ParseError> {
    let name = name(input)?;
    Ok(moxy::template! {
        fn {{ name }}(dir: &::std::path::Path, filename: &str, bytes: &[u8], image: &str) -> ::std::process::Output {
            use ::std::io::Write as _;
            ::std::fs::File::create(dir.join(filename)).unwrap().write_all(bytes).unwrap();
            ::std::process::Command::new("docker")
                .args([
                    "run", "--rm", "-v", &format!("{}:/work", dir.display()),
                    image, "/opt/kafka/bin/kafka-dump-log.sh", "--cluster-metadata-decoder",
                    "--files", &format!("/work/{filename}"),
                ])
                .output().expect("docker run kafka-dump-log")
        }
    })
}

/// Define an epoch-millisecond reader with the caller's attributes and overflow fallback.
pub fn epoch_millis(input: TokenStream) -> Result<TokenStream, ParseError> {
    let [signature, overflow]: [TokenStream; 2] = argument_array(input)?;
    Ok(moxy::template! {
        {{ signature }}() -> i64 {
            use ::std::time::{SystemTime, UNIX_EPOCH};
            SystemTime::now().duration_since(UNIX_EPOCH)
                .map_or(0, |duration| i64::try_from(duration.as_millis()).unwrap_or({{ overflow }}))
        }
    })
}

/// The deliberately distinctive proposal UUID used by both signing golden fixtures.
pub fn break_glass_golden_id(input: TokenStream) -> Result<TokenStream, ParseError> {
    named_items(input, |name| {
        moxy::template! {
            const {{ name }}: [u8; 16] = [
                0x0B, 0xAD, 0xC0, 0xFF, 0xEE, 0x00, 0x40, 0x00, 0x80, 0x00, 0x01, 0x02,
                0x03, 0x04, 0x05, 0x06,
            ];
        }
    })
}
