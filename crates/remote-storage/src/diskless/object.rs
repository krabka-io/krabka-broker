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
    /// A run or the footer is too large for the CKWL `u32` length fields.
    #[error("wal object exceeds the CKWL 4 GiB length limit")]
    TooLarge,
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
    /// # Errors
    /// Returns [`WalObjectError::TooLarge`], and leaves the builder unchanged,
    /// if the run does not fit the CKWL `u32` length field.
    pub fn append_run(
        &mut self,
        topic_id: Uuid,
        partition: i32,
        first_offset: i64,
        last_offset: i64,
        run: &[u8],
    ) -> Result<(), WalObjectError> {
        let byte_start =
            u64::try_from(HEADER_LEN + self.body.len()).map_err(|_| WalObjectError::TooLarge)?;
        let byte_len = u32::try_from(run.len()).map_err(|_| WalObjectError::TooLarge)?;
        self.body.extend_from_slice(run);
        self.entries.push(WalObjectEntry {
            topic_id,
            partition,
            first_offset,
            last_offset,
            byte_start,
            byte_len,
        });
        Ok(())
    }

    /// Frame the appended runs as one CKWL object.
    ///
    /// # Errors
    /// Returns [`WalObjectError::TooLarge`] if the footer does not fit the
    /// CKWL `u32` length field. No partial object is returned.
    pub fn finish(self) -> Result<Bytes, WalObjectError> {
        let footer_len = self
            .entries
            .len()
            .checked_mul(OBJECT_ENTRY_LEN)
            .and_then(|len| u32::try_from(len).ok())
            .ok_or(WalObjectError::TooLarge)?;
        let mut out = BytesMut::with_capacity(
            HEADER_LEN + self.body.len() + self.entries.len() * OBJECT_ENTRY_LEN + TRAILER_LEN,
        );
        out.extend_from_slice(&MAGIC);
        out.put_u16_le(OBJECT_VERSION);
        out.extend_from_slice(&self.body);
        for entry in self.entries {
            out.extend_from_slice(entry.topic_id.as_bytes());
            out.put_i32_le(entry.partition);
            out.put_i64_le(entry.first_offset);
            out.put_i64_le(entry.last_offset);
            out.put_u64_le(entry.byte_start);
            out.put_u32_le(entry.byte_len);
        }
        out.put_u32_le(footer_len);
        out.extend_from_slice(&MAGIC);
        Ok(out.freeze())
    }

    /// Append one run and finish the object.
    ///
    /// # Errors
    /// Returns [`WalObjectError::TooLarge`] if the run or footer does not fit
    /// the CKWL `u32` length fields.
    pub fn finish_with_run(
        mut self,
        topic_id: Uuid,
        partition: i32,
        first_offset: i64,
        last_offset: i64,
        run: &[u8],
    ) -> Result<Bytes, WalObjectError> {
        self.append_run(topic_id, partition, first_offset, last_offset, run)?;
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
    let (Some(&[m0, m1, m2, m3, v0, v1]), Some(&[l0, l1, l2, l3, t0, t1, t2, t3])) = (
        object.first_chunk::<HEADER_LEN>(),
        object.last_chunk::<TRAILER_LEN>(),
    ) else {
        return Err(WalObjectError::TooShort);
    };
    if [m0, m1, m2, m3] != MAGIC || [t0, t1, t2, t3] != MAGIC {
        return Err(WalObjectError::BadMagic);
    }
    let version = u16::from_le_bytes([v0, v1]);
    if version != OBJECT_VERSION {
        return Err(WalObjectError::BadVersion(version));
    }
    let trailer = object.len() - TRAILER_LEN;
    let footer_len = usize::try_from(u32::from_le_bytes([l0, l1, l2, l3]))
        .map_err(|_| WalObjectError::BadManifest)?;
    if footer_len % OBJECT_ENTRY_LEN != 0 {
        return Err(WalObjectError::BadManifest);
    }
    let footer = trailer
        .checked_sub(footer_len)
        .filter(|start| *start >= HEADER_LEN)
        .ok_or(WalObjectError::BadManifest)?;
    let mut entries = Vec::with_capacity(footer_len / OBJECT_ENTRY_LEN);
    let footer_bytes = object
        .get(footer..trailer)
        .ok_or(WalObjectError::BadManifest)?;
    for raw in footer_bytes.as_chunks::<OBJECT_ENTRY_LEN>().0 {
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
