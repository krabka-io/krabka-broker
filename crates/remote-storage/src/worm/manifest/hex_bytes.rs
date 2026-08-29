//! The byte values a manifest serialises as a lowercase hex string.
//!
//! `Sha256Digest` fixes a 32-byte digest and `HexBytes` carries a key or a
//! signature, whose length the type system cannot pin. Both share the hex
//! codec in this file, and `ChainHead` uses it too.

use std::fmt;

use krabka_audit::chain::{from_hex32, to_hex};
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};
use sha2::{Digest, Sha256};

/// `SHA-256` digest of one archived object's body.
///
/// Serialises as a lowercase hex string.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Sha256Digest(pub [u8; 32]);

impl Sha256Digest {
    /// Digest of `bytes`.
    #[must_use]
    pub fn of(bytes: &[u8]) -> Self {
        Self(Sha256::digest(bytes).into())
    }
}

/// Variable-length bytes that serialise as a lowercase hex string.
///
/// Used for the public key and the signature, neither of which has a fixed
/// length the type system can pin.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct HexBytes(pub Vec<u8>);

pub(super) fn serialize_hex<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&to_hex(bytes))
}

pub(super) fn deserialize_hex32<'de, D>(deserializer: D) -> Result<[u8; 32], D::Error>
where
    D: Deserializer<'de>,
{
    let text = String::deserialize(deserializer)?;
    from_hex32(&text)
        .ok_or_else(|| de::Error::custom(format!("expected 64 hex characters, got `{text}`")))
}

impl fmt::Display for Sha256Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&to_hex(&self.0))
    }
}

impl fmt::Debug for Sha256Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Sha256Digest({})", to_hex(&self.0))
    }
}

impl Serialize for Sha256Digest {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serialize_hex(&self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for Sha256Digest {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserialize_hex32(deserializer).map(Self)
    }
}

impl fmt::Display for HexBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&to_hex(&self.0))
    }
}

impl fmt::Debug for HexBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HexBytes({})", to_hex(&self.0))
    }
}

impl Serialize for HexBytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serialize_hex(&self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for HexBytes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let text = String::deserialize(deserializer)?;
        hex::decode(&text).map(Self).map_err(de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::worm::manifest::ChainHead;

    #[test]
    fn hex_newtypes_reject_bad_input_without_panicking() {
        let head = ChainHead([0xa1; 32]);
        let encoded = serde_json::to_string(&head).unwrap();
        check!(encoded == format!("\"{}\"", "a1".repeat(32)));
        check!(serde_json::from_str::<ChainHead>(&encoded).unwrap() == head);
        check!(head.to_string() == "a1".repeat(32));
        check!(format!("{head:?}") == format!("ChainHead({})", "a1".repeat(32)));

        let digest = Sha256Digest::of(b"body");
        check!(
            serde_json::from_str::<Sha256Digest>(&serde_json::to_string(&digest).unwrap()).unwrap()
                == digest
        );
        check!(format!("{digest:?}") == format!("Sha256Digest({digest})"));

        for (name, json) in [
            ("empty string", "\"\"".to_string()),
            ("too short", format!("\"{}\"", "00".repeat(31))),
            ("too long", format!("\"{}\"", "00".repeat(33))),
            ("odd length", format!("\"{}0\"", "00".repeat(31))),
            ("non-hex characters", format!("\"{}\"", "zz".repeat(32))),
            ("not a string", "12345".to_string()),
            ("null", "null".to_string()),
            ("array", "[]".to_string()),
        ] {
            check!(
                serde_json::from_str::<ChainHead>(&json).is_err(),
                "ChainHead case {name}"
            );
            check!(
                serde_json::from_str::<Sha256Digest>(&json).is_err(),
                "Sha256Digest case {name}"
            );
        }

        for (name, json, expected) in [
            ("empty", "\"\"", Some(Vec::new())),
            (
                "even-length hex",
                "\"00ff10\"",
                Some(vec![0x00, 0xff, 0x10]),
            ),
            ("odd length", "\"abc\"", None),
            ("non-hex characters", "\"zz\"", None),
            ("not a string", "42", None),
            ("null", "null", None),
        ] {
            check!(
                serde_json::from_str::<HexBytes>(json).ok().map(|h| h.0) == expected,
                "HexBytes case {name}"
            );
        }

        check!(HexBytes(vec![0xde, 0xad]).to_string() == "dead");
        check!(format!("{:?}", HexBytes(vec![0xde, 0xad])) == "HexBytes(dead)");
    }
}
