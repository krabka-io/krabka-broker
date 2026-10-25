//! Compacted diskless index-topic keys and values.

use {
    super::WalIndexEntry,
    bytes::Bytes,
    serde::{Deserialize, Serialize},
    uuid::Uuid,
};

/// Stable keyed replay fence ignored by every diskless index projection.
pub const REPLAY_FENCE_KEY: &[u8] = b"__krabka_diskless_replay_fence";

/// Compaction key for one logical range.
///
/// Every `__diskless_wal_index` key starts with a big-endian `i16` key
/// version. As in Kafka's coordinator records, the key version names the
/// record type the key and its value belong to, so a reader dispatches on it
/// before it looks at anything else and a key type it does not know is
/// refused instead of being decoded as another type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WalIndexKey {
    pub topic_id: Uuid,
    pub partition: i32,
    pub first_offset: i64,
}

impl WalIndexKey {
    /// Key version of a range key, whose value is a [`WalFlushRecord`].
    ///
    /// Part of the 1.x on-disk contract: a 1.x broker reads every range key
    /// that any earlier 1.x broker wrote.
    pub const KEY_VERSION: i16 = 0;
    const LEN: usize = PARTITION_KEY_PREFIX_LEN + 8;

    #[must_use]
    pub fn to_bytes(self) -> Bytes {
        let mut out = encode_partition_key_prefix::<{ Self::LEN }>(
            Self::KEY_VERSION,
            self.topic_id,
            self.partition,
        );
        out.extend_from_slice(&self.first_offset.to_be_bytes());
        out.into()
    }

    /// Decode a range key, or `None` when `bytes` are not one: another key
    /// version, or the wrong length.
    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let PartitionKeyPrefix {
            topic_id,
            partition,
            rest,
        } = decode_partition_key_prefix::<{ Self::LEN }>(bytes, Self::KEY_VERSION)?;
        Some(Self {
            topic_id,
            partition,
            first_offset: i64::from_be_bytes(rest.try_into().ok()?),
        })
    }
}

/// The leading key version of a `__diskless_wal_index` key, or `None` for a
/// key shorter than the version itself.
#[must_use]
pub fn wal_index_key_version(key: &[u8]) -> Option<i16> {
    Some(i16::from_be_bytes(key.get(..2)?.try_into().ok()?))
}

/// Length of the big-endian `i16` key version, topic id and big-endian `i32`
/// partition that every partition-scoped `__diskless_wal_index` key starts
/// with.
const PARTITION_KEY_PREFIX_LEN: usize = 22;

/// Start a `KEY_LEN`-byte partition-scoped key with its version, topic id and
/// partition.
///
/// The caller appends the rest of the key, if any.
fn encode_partition_key_prefix<const KEY_LEN: usize>(
    version: i16,
    topic_id: Uuid,
    partition: i32,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(KEY_LEN);
    out.extend_from_slice(&version.to_be_bytes());
    out.extend_from_slice(topic_id.as_bytes());
    out.extend_from_slice(&partition.to_be_bytes());
    out
}

/// A partition-scoped key split into its topic id, partition and trailing bytes.
struct PartitionKeyPrefix<'a> {
    topic_id: Uuid,
    partition: i32,
    rest: &'a [u8],
}

/// Split a `KEY_LEN`-byte partition-scoped key into its topic id, partition and
/// the bytes after them, or `None` when `bytes` are the wrong length or carry a
/// key version other than `version`.
fn decode_partition_key_prefix<const KEY_LEN: usize>(
    bytes: &[u8],
    version: i16,
) -> Option<PartitionKeyPrefix<'_>> {
    let bytes: &[u8; KEY_LEN] = bytes.try_into().ok()?;
    if wal_index_key_version(bytes) != Some(version) {
        return None;
    }
    let topic_id = Uuid::from_bytes(bytes.get(2..18)?.try_into().ok()?);
    let partition = i32::from_be_bytes(bytes.get(18..PARTITION_KEY_PREFIX_LEN)?.try_into().ok()?);
    Some(PartitionKeyPrefix {
        topic_id,
        partition,
        rest: bytes.get(PARTITION_KEY_PREFIX_LEN..)?,
    })
}

impl From<&WalIndexEntry> for WalIndexKey {
    fn from(value: &WalIndexEntry) -> Self {
        Self {
            topic_id: value.topic_id,
            partition: value.partition,
            first_offset: value.first_offset,
        }
    }
}

/// Compaction key for a partition delete floor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WalDeleteFloorKey {
    pub topic_id: Uuid,
    pub partition: i32,
}

/// Reserved delete-floor partition used to protect an uploaded object until
/// its range records are committed. Its key/value remain readable by 1.x
/// brokers: older readers project it as floor zero on a negative partition,
/// which no data partition can address.
pub const WAL_OBJECT_RESERVATION_PARTITION: i32 = i32::MIN;

/// Object UUID encoded into the existing delete-floor key space.
///
/// Diskless object names use `UUIDv4`. Changing only the version nibble to v8
/// creates a reversible namespace that cannot collide with generated diskless
/// topic IDs, while the negative partition sentinel keeps the record inert to
/// older delete-floor projections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WalObjectReservationKey {
    object_id: Uuid,
}

impl WalObjectReservationKey {
    const KEY_VERSION: i16 = WalDeleteFloorKey::KEY_VERSION;

    #[must_use]
    pub fn new(object_id: Uuid) -> Self { Self { object_id } }

    #[must_use]
    pub fn object_id(self) -> Uuid { self.object_id }

    #[must_use]
    pub fn to_bytes(self) -> Bytes {
        WalDeleteFloorKey {
            topic_id: reservation_topic_id(self.object_id),
            partition: WAL_OBJECT_RESERVATION_PARTITION,
        }
        .to_bytes()
    }

    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let key = WalDeleteFloorKey::from_bytes(bytes)?;
        if key.partition != WAL_OBJECT_RESERVATION_PARTITION || key.topic_id.get_version_num() != 8
        {
            return None;
        }
        Some(Self {
            object_id: object_id_from_reservation_topic(key.topic_id),
        })
    }

    /// This record deliberately shares the v1 delete-floor key/value
    /// versions, so persisted 1.x readers can still replay it.
    #[must_use]
    pub const fn key_version() -> i16 { Self::KEY_VERSION }
}

fn reservation_topic_id(object_id: Uuid) -> Uuid {
    let mut bytes = *object_id.as_bytes();
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    Uuid::from_bytes(bytes)
}

fn object_id_from_reservation_topic(topic_id: Uuid) -> Uuid {
    let mut bytes = *topic_id.as_bytes();
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    Uuid::from_bytes(bytes)
}

impl WalDeleteFloorKey {
    /// Key version of a delete-floor key, whose value is a
    /// [`WalDeleteFloorRecord`].
    ///
    /// Part of the 1.x on-disk contract: a 1.x broker reads every delete-floor
    /// key that any earlier 1.x broker wrote.
    pub const KEY_VERSION: i16 = 1;
    const LEN: usize = PARTITION_KEY_PREFIX_LEN;

    #[must_use]
    pub fn to_bytes(self) -> Bytes {
        encode_partition_key_prefix::<{ Self::LEN }>(
            Self::KEY_VERSION,
            self.topic_id,
            self.partition,
        )
        .into()
    }

    /// Decode a delete-floor key, or `None` when `bytes` are not one: another
    /// key version, or the wrong length.
    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let PartitionKeyPrefix {
            topic_id,
            partition,
            ..
        } = decode_partition_key_prefix::<{ Self::LEN }>(bytes, Self::KEY_VERSION)?;
        Some(Self {
            topic_id,
            partition,
        })
    }
}

/// Split the leading big-endian `i16` version off a `__diskless_wal_index`
/// value, refusing any version other than `expected`.
fn split_value_version<'a>(
    bytes: &'a [u8],
    record: &str,
    expected: i16,
) -> Result<&'a [u8], String> {
    let (version, body) = bytes
        .split_first_chunk::<2>()
        .ok_or_else(|| format!("truncated {record}: {} bytes hold no version", bytes.len()))?;
    let version = i16::from_be_bytes(*version);
    if version != expected {
        return Err(format!(
            "unsupported {record} version {version}: this build reads version {expected}. \
             A record written before krabka 1.0 has no version prefix and is not readable; \
             reformat the cluster"
        ));
    }
    Ok(body)
}

fn encode_value_version(version: i16, body: &[u8]) -> Bytes {
    let mut out = Vec::with_capacity(2 + body.len());
    out.extend_from_slice(&version.to_be_bytes());
    out.extend_from_slice(body);
    out.into()
}

/// Durable `DeleteRecords` floor.
///
/// The value is a big-endian `i16` [`Self::VERSION`] followed by the wincode
/// body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalDeleteFloorRecord {
    pub topic_id: Uuid,
    pub partition: i32,
    pub floor: i64,
}

impl WalDeleteFloorRecord {
    /// Value version of a delete-floor record.
    ///
    /// Part of the 1.x on-disk contract: a 1.x broker reads every delete-floor
    /// record that any earlier 1.x broker wrote.
    pub const VERSION: i16 = 0;

    /// Encode with the index topic codec.
    ///
    /// # Errors
    /// Returns the codec error when the record cannot be encoded.
    pub fn to_bytes(&self) -> Result<Bytes, String> {
        let body = <serde_wincode::SerdeCompat<Self> as wincode::Serialize>::serialize(self)
            .map_err(|error| error.to_string())?;
        Ok(encode_value_version(Self::VERSION, &body))
    }

    /// Decode the index topic codec.
    ///
    /// # Errors
    /// Returns an error for a missing or unsupported version, or when the body
    /// is malformed.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        let body = split_value_version(bytes, "diskless WAL delete-floor record", Self::VERSION)?;
        <serde_wincode::SerdeCompat<Self> as wincode::Deserialize>::deserialize(body)
            .map_err(|error| error.to_string())
    }
}

/// Durable index value for a flushed object.
///
/// The value is a big-endian `i16` `format_version` followed by the wincode
/// body of `object_key` and `entries`, so a reader checks the version before it
/// decodes anything else.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalFlushRecord {
    pub object_key: String,
    pub format_version: u16,
    pub entries: Vec<WalIndexEntry>,
}

/// The wincode body of a [`WalFlushRecord`], as it is encoded.
#[derive(Serialize)]
struct WalFlushBodyRef<'a> {
    object_key: &'a str,
    entries: &'a [WalIndexEntry],
}

/// The wincode body of a [`WalFlushRecord`], as it is decoded.
#[derive(Deserialize)]
struct WalFlushBody {
    object_key: String,
    entries: Vec<WalIndexEntry>,
}

impl WalFlushRecord {
    /// Value version of a flush record.
    ///
    /// Part of the 1.x on-disk contract: a 1.x broker reads every flush record
    /// that any earlier 1.x broker wrote. It is encoded as a big-endian `i16`.
    pub const FORMAT_VERSION: u16 = 2;

    /// Encode one keyed WAL range in the format a flusher publishes.
    ///
    /// # Errors
    /// Returns the index codec's serialization error.
    pub fn keyed_entry(object_key: &str, entry: WalIndexEntry) -> Result<(Bytes, Bytes), String> {
        let key = WalIndexKey::from(&entry).to_bytes();
        let value = Self {
            object_key: object_key.into(),
            format_version: Self::FORMAT_VERSION,
            entries: vec![entry],
        }
        .to_bytes()?;
        Ok((key, value))
    }

    /// Encode with the index topic codec.
    ///
    /// # Errors
    /// Returns an error for an unsupported version or codec failure.
    pub fn to_bytes(&self) -> Result<Bytes, String> {
        if self.format_version != Self::FORMAT_VERSION {
            return Err(format!(
                "unsupported diskless WAL index format version {}",
                self.format_version
            ));
        }
        let version = Self::wire_version();
        let body =
            <serde_wincode::SerdeCompat<WalFlushBodyRef<'_>> as wincode::Serialize>::serialize(
                &WalFlushBodyRef {
                    object_key: &self.object_key,
                    entries: &self.entries,
                },
            )
            .map_err(|error| error.to_string())?;
        Ok(encode_value_version(version, &body))
    }

    /// Decode with strict format-version checking.
    ///
    /// # Errors
    /// Returns an error for a missing or unsupported version, or malformed
    /// bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        let body = split_value_version(bytes, "diskless WAL index format", Self::wire_version())?;
        let body =
            <serde_wincode::SerdeCompat<WalFlushBody> as wincode::Deserialize>::deserialize(body)
                .map_err(|error| error.to_string())?;
        Ok(Self {
            object_key: body.object_key,
            format_version: Self::FORMAT_VERSION,
            entries: body.entries,
        })
    }

    /// [`Self::FORMAT_VERSION`] as the `i16` the value leads with. The
    /// version is far below `i16::MAX`, so the conversion keeps its value.
    const fn wire_version() -> i16 { Self::FORMAT_VERSION.cast_signed() }
}
