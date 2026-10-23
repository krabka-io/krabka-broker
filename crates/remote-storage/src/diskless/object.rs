//! CKWL object framing, independent of capture selection and trust policy.

use {
    bytes::{BufMut, Bytes, BytesMut},
    uuid::Uuid,
};

const MAGIC: [u8; 4] = *b"CKWL";
const OBJECT_VERSION: u16 = 1;
const HEADER_LEN: usize = 6;
const TRAILER_LEN: usize = 8;
const OBJECT_ENTRY_LEN: usize = 48;

/// One run recorded by a CKWL footer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalObjectEntry {
    pub topic_id: Uuid,
    pub partition: i32,
    pub first_offset: i64,
    pub last_offset: i64,
    pub byte_start: u64,
    pub byte_len: u32,
}

/// Invalid combined diskless-WAL object framing.
#[derive(Debug, thiserror::Error)]
pub enum WalObjectError {
    /// Object does not contain the minimum header and trailer.
    #[error("wal object too short")]
    TooShort,
    /// Header or trailer magic is invalid.
    #[error("bad wal object magic")]
    BadMagic,
    /// Object uses an unsupported framing version.
    #[error("unsupported wal object version {0}")]
    BadVersion(u16),
    /// Footer length, entry encoding, or byte ranges are invalid.
    #[error("corrupt wal object manifest")]
    BadManifest,
}

/// Builder for combined CKWL objects.
#[derive(Default)]
pub struct WalObjectBuilder {
    body: BytesMut,
    entries: Vec<WalObjectEntry>,
}

impl WalObjectBuilder {
    #[must_use]
    pub fn new() -> Self { Self::default() }

    #[must_use]
    pub fn body_len(&self) -> usize { self.body.len() }

    #[must_use]
    pub fn is_empty(&self) -> bool { self.entries.is_empty() }

    /// Append one contiguous partition run.
    ///
    /// # Panics
    /// Panics only if an in-memory object exceeds platform or CKWL length limits.
    pub fn append_run(
        &mut self,
        topic_id: Uuid,
        partition: i32,
        first_offset: i64,
        last_offset: i64,
        run: &[u8],
    ) {
        let byte_start = u64::try_from(HEADER_LEN + self.body.len()).expect("object size fits u64");
        let byte_len = u32::try_from(run.len()).expect("run size fits u32");
        self.body.extend_from_slice(run);
        self.entries.push(WalObjectEntry {
            topic_id,
            partition,
            first_offset,
            last_offset,
            byte_start,
            byte_len,
        });
    }

    #[must_use]
    /// # Panics
    /// Panics only if the in-memory footer exceeds 4 GiB.
    pub fn finish(self) -> Bytes {
        let mut out = BytesMut::with_capacity(
            HEADER_LEN + self.body.len() + self.entries.len() * OBJECT_ENTRY_LEN + TRAILER_LEN,
        );
        out.extend_from_slice(&MAGIC);
        out.put_u16_le(OBJECT_VERSION);
        out.extend_from_slice(&self.body);
        let footer_start = out.len();
        for entry in self.entries {
            out.extend_from_slice(entry.topic_id.as_bytes());
            out.put_i32_le(entry.partition);
            out.put_i64_le(entry.first_offset);
            out.put_i64_le(entry.last_offset);
            out.put_u64_le(entry.byte_start);
            out.put_u32_le(entry.byte_len);
        }
        out.put_u32_le(u32::try_from(out.len() - footer_start).expect("footer fits u32"));
        out.extend_from_slice(&MAGIC);
        out.freeze()
    }

    /// Append one run and finish the object.
    #[must_use]
    pub fn finish_with_run(
        mut self,
        topic_id: Uuid,
        partition: i32,
        first_offset: i64,
        last_offset: i64,
        run: &[u8],
    ) -> Bytes {
        self.append_run(topic_id, partition, first_offset, last_offset, run);
        self.finish()
    }
}

/// Parse and validate a CKWL footer.
///
/// # Errors
/// Returns an error for malformed framing, entries, versions, or byte ranges.
pub fn parse_wal_object(object: &Bytes) -> Result<Vec<WalObjectEntry>, WalObjectError> {
    if object.len() < HEADER_LEN + TRAILER_LEN {
        return Err(WalObjectError::TooShort);
    }
    if object[..4] != MAGIC || object[object.len() - 4..] != MAGIC {
        return Err(WalObjectError::BadMagic);
    }
    let version = u16::from_le_bytes([object[4], object[5]]);
    if version != OBJECT_VERSION {
        return Err(WalObjectError::BadVersion(version));
    }
    let trailer = object.len() - TRAILER_LEN;
    let footer_len = usize::try_from(u32::from_le_bytes(
        object[trailer..trailer + 4]
            .try_into()
            .map_err(|_| WalObjectError::BadManifest)?,
    ))
    .map_err(|_| WalObjectError::BadManifest)?;
    if footer_len % OBJECT_ENTRY_LEN != 0 {
        return Err(WalObjectError::BadManifest);
    }
    let footer = trailer
        .checked_sub(footer_len)
        .filter(|start| *start >= HEADER_LEN)
        .ok_or(WalObjectError::BadManifest)?;
    let mut entries = Vec::with_capacity(footer_len / OBJECT_ENTRY_LEN);
    for raw in object[footer..trailer].as_chunks::<OBJECT_ENTRY_LEN>().0 {
        let entry = WalObjectEntry {
            topic_id: Uuid::from_slice(&raw[..16]).map_err(|_| WalObjectError::BadManifest)?,
            partition: i32::from_le_bytes(
                raw[16..20]
                    .try_into()
                    .map_err(|_| WalObjectError::BadManifest)?,
            ),
            first_offset: i64::from_le_bytes(
                raw[20..28]
                    .try_into()
                    .map_err(|_| WalObjectError::BadManifest)?,
            ),
            last_offset: i64::from_le_bytes(
                raw[28..36]
                    .try_into()
                    .map_err(|_| WalObjectError::BadManifest)?,
            ),
            byte_start: u64::from_le_bytes(
                raw[36..44]
                    .try_into()
                    .map_err(|_| WalObjectError::BadManifest)?,
            ),
            byte_len: u32::from_le_bytes(
                raw[44..48]
                    .try_into()
                    .map_err(|_| WalObjectError::BadManifest)?,
            ),
        };
        let start = usize::try_from(entry.byte_start).map_err(|_| WalObjectError::BadManifest)?;
        let end = start
            .checked_add(usize::try_from(entry.byte_len).map_err(|_| WalObjectError::BadManifest)?)
            .filter(|end| *end <= footer)
            .ok_or(WalObjectError::BadManifest)?;
        let _ = end;
        if start < HEADER_LEN {
            return Err(WalObjectError::BadManifest);
        }
        entries.push(entry);
    }
    Ok(entries)
}
