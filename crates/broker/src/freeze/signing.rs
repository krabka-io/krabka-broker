//! The detached operator signature over a freeze record (KFC-9).
//!
//! The `set_by` name on a freeze record is the broker's word for who set it.
//! That is not good enough for the one record whose whole job is to say that a
//! privileged person did a privileged thing: anyone who can write the metadata
//! log can write any name into that field. So the record carries an Ed25519
//! signature that the operator's own machine makes before the request leaves
//! it. The broker verifies the signature and cannot make one, and the metadata
//! log keeps it, so an auditor re-verifies it later with no trust in any
//! broker.
//!
//! # The canonical bytes
//!
//! [`freeze_signing_bytes`] is the one definition of the signed payload. The
//! operator's command builds it, and the broker rebuilds it to verify. The
//! layout is:
//!
//! ```text
//! FREEZE_DOMAIN                     b"crabka-topic-freeze-v1\0"
//! cluster_id      u32 big-endian length, then the UTF-8 bytes
//! pattern_type    u8                3 literal, 4 prefixed
//! scope           u32 big-endian length, then the UTF-8 bytes
//! frozen          u8                1 freeze, 0 thaw
//! reason          u32 big-endian length, then the UTF-8 bytes
//! set_by          u32 big-endian length, then the UTF-8 bytes
//! set_at_ms       i64 big-endian
//! proposal_id     16 raw bytes
//! ```
//!
//! Every length prefix is a `u32` in big-endian order, which is the shape
//! [`crabka_audit::signing::checkpoint_signing_bytes`] gives the one variable
//! field it covers. The prefixes are what keep two different records from
//! building one byte string: without them a scope of `"a"` with a reason of
//! `"bc"` and a scope of `"ab"` with a reason of `"c"` would sign the same.
//!
//! [`crate::signing_domains::FREEZE_DOMAIN`] is the separator, and it differs
//! from every other separator in the workspace. A signature made for one
//! purpose then never verifies as another.
//!
//! # What each field is covering
//!
//! Three fields are in the payload to answer a named attack.
//!
//! `frozen` is signed, so a signature captured from a freeze cannot be
//! replayed as the thaw. Without it the two records differ by one byte and one
//! signature would authorize both.
//!
//! `cluster_id` is signed, so a signed freeze cannot be replayed into a second
//! cluster.
//!
//! `set_at_ms` is signed, and [`verify_freeze_signature`] checks it two ways.
//! It must sit inside `freeze.signature_max_skew` of this broker's clock, and
//! it must be newer than the timestamp of the entry it replaces. Those two
//! checks are what kill the replay of an old signed thaw. The skew window is a
//! clock assumption, which KFC-8 exists to measure.
//!
//! # One code for every refusal
//!
//! Every failure that [`verify_freeze_signature`] reports answers with
//! `OPERATOR_SIGNATURE_INVALID` (1009). The response message says which check
//! failed and the code does not, because a code that separates them tells an
//! attacker which check they got past.

use crabka_metadata::{PatternType, TopicFreezeRecord};
use crabka_protocol::krabka::freeze::{PATTERN_TYPE_LITERAL, PATTERN_TYPE_PREFIXED};
use crabka_units::{Time, convert::TimeExt as _};

use crate::{codes, operator_keys::OperatorKeys, signing_domains::FREEZE_DOMAIN};

/// The canonical bytes that an operator signs to author `record` in the
/// cluster named by `cluster_id`.
///
/// `cluster_id` is the string form that `Metadata` and `DescribeCluster`
/// already give a client, so the operator's command signs the identifier it
/// read from the cluster it means to act on.
pub(crate) fn freeze_signing_bytes(cluster_id: &str, record: &TopicFreezeRecord) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(signing_bytes_capacity(cluster_id, record));
    bytes.extend_from_slice(FREEZE_DOMAIN);
    put_len_prefixed(&mut bytes, cluster_id.as_bytes());
    bytes.push(pattern_type_byte(record.pattern_type));
    put_len_prefixed(&mut bytes, record.scope.as_bytes());
    bytes.push(u8::from(record.frozen));
    put_len_prefixed(&mut bytes, record.reason.as_bytes());
    put_len_prefixed(&mut bytes, record.set_by.as_bytes());
    bytes.extend_from_slice(&record.set_at_ms.to_be_bytes());
    bytes.extend_from_slice(record.proposal_id.as_bytes());
    bytes
}

/// Everything outside the record that a signature check reads.
pub(crate) struct FreezeSignatureCheck<'a> {
    /// The operator key trust set from `[[operator_keys]]`.
    pub keys: &'a OperatorKeys,
    /// The cluster this broker belongs to, in string form.
    pub cluster_id: &'a str,
    /// The principal the broker authenticated on the connection.
    pub connection_principal: &'a str,
    /// How far `set_at_ms` may sit from `now_ms`, from
    /// `freeze.signature_max_skew`.
    pub max_skew: Time,
    /// This broker's clock, in milliseconds since the Unix epoch.
    pub now_ms: i64,
    /// The live registry entry that the incoming record replaces, when there
    /// is one. Its `set_at_ms` is the floor that the incoming timestamp must
    /// pass.
    pub replaces: Option<&'a TopicFreezeRecord>,
}

/// Why the broker refused a signed freeze record.
///
/// Every variant answers with the same wire code. The variant decides the
/// message alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SignatureRefusal {
    /// No `[[operator_keys]]` entry carries the `key_id` the record names.
    UnknownKeyId,
    /// The record claims an author that is not the principal bound to the key.
    AuthorIsNotTheKeyPrincipal,
    /// The record claims an author that is not the principal on the
    /// connection.
    AuthorIsNotTheConnectionPrincipal,
    /// `set_at_ms` sits further from this broker's clock than
    /// `freeze.signature_max_skew` allows.
    TimestampOutsideSkewWindow,
    /// `set_at_ms` is not newer than the entry this record replaces.
    TimestampNotNewerThanTheEntryItReplaces,
    /// The signature does not verify over the canonical bytes.
    SignatureDidNotVerify,
}

impl SignatureRefusal {
    /// The wire error code of every refusal.
    ///
    /// It is a constant and not a method on purpose. One code covers all six
    /// checks, because a code that separated them would tell an attacker which
    /// check they got past. [`Self::message`] is the only thing that varies.
    pub(crate) const CODE: i16 = codes::OPERATOR_SIGNATURE_INVALID;

    /// The `error_message` that names the check that failed.
    pub(crate) fn message(self) -> &'static str {
        match self {
            SignatureRefusal::UnknownKeyId => "no operator key is configured under that key_id",
            SignatureRefusal::AuthorIsNotTheKeyPrincipal => {
                "the record names an author that the signing key does not speak for"
            }
            SignatureRefusal::AuthorIsNotTheConnectionPrincipal => {
                "the record names an author that is not the principal on this connection"
            }
            SignatureRefusal::TimestampOutsideSkewWindow => {
                "set_at_ms is outside the signature skew window of this broker"
            }
            SignatureRefusal::TimestampNotNewerThanTheEntryItReplaces => {
                "set_at_ms is not newer than the entry this record replaces"
            }
            SignatureRefusal::SignatureDidNotVerify => {
                "the signature does not verify over the canonical bytes of this record"
            }
        }
    }

    /// The refusal as a response carries it: the code and the text.
    ///
    /// The code is [`Self::CODE`] for every variant, and the text is the only
    /// part that separates the six checks.
    pub(crate) fn wire(self) -> (i16, &'static str) {
        (Self::CODE, self.message())
    }
}

/// Verify the detached operator signature that `record` carries.
///
/// The function is the one place that holds all six rules. It checks that the
/// trust set knows the `key_id`, that the claimed author is the principal
/// bound to that key, that the same author is the principal on the connection,
/// that `set_at_ms` sits inside the skew window, that `set_at_ms` is newer than
/// the entry the record replaces, and that the signature verifies over
/// [`freeze_signing_bytes`].
///
/// # Errors
///
/// Returns the [`SignatureRefusal`] of the first rule that fails. Every one of
/// them carries `OPERATOR_SIGNATURE_INVALID` (1009).
pub(crate) fn verify_freeze_signature(
    check: &FreezeSignatureCheck<'_>,
    record: &TopicFreezeRecord,
) -> Result<(), SignatureRefusal> {
    let key = check
        .keys
        .get(&record.key_id)
        .ok_or(SignatureRefusal::UnknownKeyId)?;
    if key.principal() != record.set_by {
        return Err(SignatureRefusal::AuthorIsNotTheKeyPrincipal);
    }
    if record.set_by != check.connection_principal {
        return Err(SignatureRefusal::AuthorIsNotTheConnectionPrincipal);
    }
    if !inside_skew_window(record.set_at_ms, check.now_ms, check.max_skew) {
        return Err(SignatureRefusal::TimestampOutsideSkewWindow);
    }
    if let Some(replaced) = check.replaces
        && record.set_at_ms <= replaced.set_at_ms
    {
        return Err(SignatureRefusal::TimestampNotNewerThanTheEntryItReplaces);
    }
    let message = freeze_signing_bytes(check.cluster_id, record);
    if !check
        .keys
        .verify(&record.key_id, &record.set_by, &message, &record.signature)
    {
        return Err(SignatureRefusal::SignatureDidNotVerify);
    }
    Ok(())
}

/// Whether `set_at_ms` sits within `max_skew` of `now_ms`, in either
/// direction.
///
/// A record from the future is as suspect as one from the past, so the window
/// is symmetric. The subtraction saturates, which keeps a clock at the far end
/// of the `i64` range from wrapping into the window.
fn inside_skew_window(set_at_ms: i64, now_ms: i64, max_skew: Time) -> bool {
    let distance = now_ms.saturating_sub(set_at_ms).unsigned_abs();
    let window = u64::try_from(max_skew.millis_i64()).unwrap_or(0);
    distance <= window
}

/// The pattern type's byte in the canonical layout.
///
/// It is Kafka's ACL discriminant, the same byte the wire request carries, so
/// the bytes the operator signs and the bytes they send name one value.
fn pattern_type_byte(pattern_type: PatternType) -> u8 {
    let wire = match pattern_type {
        PatternType::Literal => PATTERN_TYPE_LITERAL,
        PatternType::Prefixed => PATTERN_TYPE_PREFIXED,
    };
    wire.to_be_bytes()[0]
}

/// Append `field` behind its `u32` big-endian length.
///
/// A field longer than `u32::MAX` cannot arrive: the request body is capped
/// far below it. The saturation keeps the function total rather than
/// panicking.
fn put_len_prefixed(bytes: &mut Vec<u8>, field: &[u8]) {
    let len = u32::try_from(field.len()).unwrap_or(u32::MAX);
    bytes.extend_from_slice(&len.to_be_bytes());
    bytes.extend_from_slice(field);
}

/// How many bytes [`freeze_signing_bytes`] writes, for the one allocation.
fn signing_bytes_capacity(cluster_id: &str, record: &TopicFreezeRecord) -> usize {
    const LEN_PREFIX: usize = size_of::<u32>();
    const PATTERN_TYPE_AND_FROZEN: usize = 2;
    const SET_AT_MS: usize = size_of::<i64>();
    const PROPOSAL_ID: usize = 16;

    FREEZE_DOMAIN.len()
        + 4 * LEN_PREFIX
        + cluster_id.len()
        + record.scope.len()
        + record.reason.len()
        + record.set_by.len()
        + PATTERN_TYPE_AND_FROZEN
        + SET_AT_MS
        + PROPOSAL_ID
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use assert2::{assert, check};
    use crabka_units::{minutes, secs};
    use ring::signature::{Ed25519KeyPair, KeyPair as _};
    use tempfile::TempDir;
    use uuid::Uuid;

    use super::*;
    use crate::operator_keys::OperatorKeyEntry;

    const CLUSTER_ID: &str = "5150ba5e-0000-4000-8000-00000000c0de";
    const OTHER_CLUSTER_ID: &str = "deadbeef-0000-4000-8000-00000000c0de";
    const ALICE: &str = "User:alice";
    const ALICE_KEY: &str = "alice-yubi";
    const BOB: &str = "User:bob";
    const BOB_KEY: &str = "bob-yubi";
    const NOW_MS: i64 = 1_770_000_000_000;
    const PROPOSAL: Uuid = Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);

    // Two loaded operator keys and the key pairs that sign for them.
    struct Trust {
        keys: OperatorKeys,
        alice: Ed25519KeyPair,
        bob: Ed25519KeyPair,
        _dir: TempDir,
    }

    fn fresh_key(dir: &TempDir, name: &str) -> (Ed25519KeyPair, PathBuf) {
        let rng = ring::rand::SystemRandom::new();
        let pkcs8 = Ed25519KeyPair::generate_pkcs8(&rng).expect("generate pkcs8");
        let pair = Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).expect("parse pkcs8");
        let path = dir.path().join(name);
        std::fs::write(&path, pair.public_key().as_ref()).expect("write public key");
        (pair, path)
    }

    fn trust() -> Trust {
        let dir = TempDir::new().expect("tempdir");
        let (alice, alice_path) = fresh_key(&dir, "alice.pub");
        let (bob, bob_path) = fresh_key(&dir, "bob.pub");
        let keys = OperatorKeys::load(&[
            OperatorKeyEntry {
                key_id: ALICE_KEY.to_owned(),
                principal: ALICE.to_owned(),
                public_key_path: alice_path,
            },
            OperatorKeyEntry {
                key_id: BOB_KEY.to_owned(),
                principal: BOB.to_owned(),
                public_key_path: bob_path,
            },
        ])
        .expect("load trust set");
        Trust {
            keys,
            alice,
            bob,
            _dir: dir,
        }
    }

    // An unsigned freeze record with every field set.
    fn record() -> TopicFreezeRecord {
        TopicFreezeRecord {
            scope: "orders".to_owned(),
            pattern_type: PatternType::Literal,
            frozen: true,
            reason: "DR cutover".to_owned(),
            set_by: ALICE.to_owned(),
            set_at_ms: NOW_MS,
            proposal_id: Uuid::nil(),
            key_id: ALICE_KEY.to_owned(),
            signature: Vec::new(),
        }
    }

    // `record` signed by `pair` for `cluster_id`.
    fn signed(
        pair: &Ed25519KeyPair,
        cluster_id: &str,
        record: &TopicFreezeRecord,
    ) -> TopicFreezeRecord {
        let bytes = freeze_signing_bytes(cluster_id, record);
        TopicFreezeRecord {
            signature: pair.sign(&bytes).as_ref().to_vec(),
            ..record.clone()
        }
    }

    fn check_against<'a>(trust: &'a Trust, principal: &'a str) -> FreezeSignatureCheck<'a> {
        FreezeSignatureCheck {
            keys: &trust.keys,
            cluster_id: CLUSTER_ID,
            connection_principal: principal,
            max_skew: minutes(5),
            now_ms: NOW_MS,
            replaces: None,
        }
    }

    /// The documented layout, written out by hand rather than by the code that
    /// [`freeze_signing_bytes`] runs. A change to either one that the other
    /// does not follow fails here.
    #[test]
    fn the_canonical_bytes_match_the_documented_layout() {
        let record = TopicFreezeRecord {
            scope: "tenant-a.".to_owned(),
            pattern_type: PatternType::Prefixed,
            frozen: false,
            reason: "thaw".to_owned(),
            set_by: ALICE.to_owned(),
            set_at_ms: 0x0102_0304_0506_0708,
            proposal_id: PROPOSAL,
            key_id: ALICE_KEY.to_owned(),
            signature: vec![0xAB; 64],
        };

        let mut expected = Vec::new();
        expected.extend_from_slice(b"crabka-topic-freeze-v1\0");
        expected.extend_from_slice(&[0, 0, 0, 36]);
        expected.extend_from_slice(CLUSTER_ID.as_bytes());
        expected.push(4);
        expected.extend_from_slice(&[0, 0, 0, 9]);
        expected.extend_from_slice(b"tenant-a.");
        expected.push(0);
        expected.extend_from_slice(&[0, 0, 0, 4]);
        expected.extend_from_slice(b"thaw");
        expected.extend_from_slice(&[0, 0, 0, 10]);
        expected.extend_from_slice(b"User:alice");
        expected.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]);
        expected.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]);

        check!(freeze_signing_bytes(CLUSTER_ID, &record) == expected);
        // The key id and the signature are not in the payload. A signature
        // cannot cover itself, and the key that made it is named beside it.
        check!(
            freeze_signing_bytes(
                CLUSTER_ID,
                &TopicFreezeRecord {
                    key_id: BOB_KEY.to_owned(),
                    signature: Vec::new(),
                    ..record
                }
            ) == expected
        );
    }

    #[test]
    fn a_literal_freeze_takes_the_kafka_pattern_type_byte() {
        let literal = freeze_signing_bytes(CLUSTER_ID, &record());
        let prefixed = freeze_signing_bytes(
            CLUSTER_ID,
            &TopicFreezeRecord {
                pattern_type: PatternType::Prefixed,
                ..record()
            },
        );
        let pattern_byte = FREEZE_DOMAIN.len() + 4 + CLUSTER_ID.len();

        check!(literal[pattern_byte] == 3);
        check!(prefixed[pattern_byte] == 4);
    }

    /// A length prefix is what keeps two different records from building one
    /// byte string.
    #[test]
    fn moving_a_character_between_two_fields_changes_the_bytes() {
        let left = TopicFreezeRecord {
            scope: "ab".to_owned(),
            reason: "c".to_owned(),
            ..record()
        };
        let right = TopicFreezeRecord {
            scope: "a".to_owned(),
            reason: "bc".to_owned(),
            ..record()
        };

        check!(freeze_signing_bytes(CLUSTER_ID, &left) != freeze_signing_bytes(CLUSTER_ID, &right));
    }

    #[test]
    fn a_good_signature_verifies() {
        let trust = trust();
        let record = signed(&trust.alice, CLUSTER_ID, &record());

        check!(verify_freeze_signature(&check_against(&trust, ALICE), &record) == Ok(()));
    }

    #[test]
    fn a_signature_survives_a_thaw_that_names_its_proposal() {
        let trust = trust();
        let thaw = signed(
            &trust.alice,
            CLUSTER_ID,
            &TopicFreezeRecord {
                frozen: false,
                proposal_id: PROPOSAL,
                ..record()
            },
        );

        check!(verify_freeze_signature(&check_against(&trust, ALICE), &thaw) == Ok(()));
    }

    /// The attack table. Every row is refused, every row answers 1009, and no
    /// row's code says which check it failed.
    #[test]
    fn every_forged_or_replayed_record_is_refused_with_one_code() {
        let trust = trust();
        let good = record();
        let replaced = TopicFreezeRecord {
            set_at_ms: NOW_MS,
            ..record()
        };

        // Each case builds the record the broker sees and the check it runs.
        let cases: Vec<(&str, TopicFreezeRecord, FreezeSignatureCheck<'_>)> = vec![
            (
                "a signature over frozen: true presented as the thaw",
                TopicFreezeRecord {
                    frozen: false,
                    ..signed(&trust.alice, CLUSTER_ID, &good)
                },
                check_against(&trust, ALICE),
            ),
            (
                "a signature made for another cluster",
                signed(&trust.alice, OTHER_CLUSTER_ID, &good),
                check_against(&trust, ALICE),
            ),
            (
                "an unknown key_id",
                TopicFreezeRecord {
                    key_id: "carol-yubi".to_owned(),
                    ..signed(&trust.alice, CLUSTER_ID, &good)
                },
                check_against(&trust, ALICE),
            ),
            (
                "an author that the signing key does not speak for",
                signed(
                    &trust.alice,
                    CLUSTER_ID,
                    &TopicFreezeRecord {
                        set_by: BOB.to_owned(),
                        ..good.clone()
                    },
                ),
                check_against(&trust, BOB),
            ),
            (
                "an author that is not the principal on the connection",
                signed(&trust.alice, CLUSTER_ID, &good),
                check_against(&trust, BOB),
            ),
            (
                "another operator's key over the same record",
                TopicFreezeRecord {
                    key_id: BOB_KEY.to_owned(),
                    set_by: BOB.to_owned(),
                    ..signed(&trust.alice, CLUSTER_ID, &good)
                },
                check_against(&trust, BOB),
            ),
            (
                "a timestamp older than the skew window",
                signed(
                    &trust.alice,
                    CLUSTER_ID,
                    &TopicFreezeRecord {
                        set_at_ms: NOW_MS - minutes(5).millis_i64() - 1,
                        ..good.clone()
                    },
                ),
                check_against(&trust, ALICE),
            ),
            (
                "a timestamp newer than the skew window",
                signed(
                    &trust.alice,
                    CLUSTER_ID,
                    &TopicFreezeRecord {
                        set_at_ms: NOW_MS + minutes(5).millis_i64() + 1,
                        ..good.clone()
                    },
                ),
                check_against(&trust, ALICE),
            ),
            (
                "a timestamp replayed from the entry it replaces",
                signed(&trust.alice, CLUSTER_ID, &good),
                FreezeSignatureCheck {
                    replaces: Some(&replaced),
                    ..check_against(&trust, ALICE)
                },
            ),
            (
                "a timestamp older than the entry it replaces",
                signed(
                    &trust.alice,
                    CLUSTER_ID,
                    &TopicFreezeRecord {
                        set_at_ms: NOW_MS - 1,
                        ..good.clone()
                    },
                ),
                FreezeSignatureCheck {
                    replaces: Some(&replaced),
                    ..check_against(&trust, ALICE)
                },
            ),
            (
                "an empty signature",
                TopicFreezeRecord {
                    signature: Vec::new(),
                    ..good.clone()
                },
                check_against(&trust, ALICE),
            ),
            (
                "a signature truncated by one byte",
                {
                    let mut record = signed(&trust.alice, CLUSTER_ID, &good);
                    record.signature.pop();
                    record
                },
                check_against(&trust, ALICE),
            ),
        ];

        for (label, record, check) in cases {
            assert!(
                let Err(refusal) = verify_freeze_signature(&check, &record),
                "case {label}"
            );
            let (code, message) = refusal.wire();
            check!(code == codes::OPERATOR_SIGNATURE_INVALID, "{label}");
            check!(code == 1009, "{label}");
            check!(!message.is_empty(), "{label}");
        }
    }

    /// A one-bit flip anywhere in the canonical bytes breaks the signature.
    /// The signature is made over the good bytes and then verified against a
    /// record that rebuilds one flipped bit, which is what an attacker who
    /// edits a captured record produces.
    #[test]
    fn a_one_bit_flip_anywhere_in_the_canonical_bytes_is_refused() {
        let trust = trust();
        let good = signed(&trust.alice, CLUSTER_ID, &record());
        let bytes = freeze_signing_bytes(CLUSTER_ID, &good);

        for index in 0..bytes.len() {
            for bit in 0..8_u32 {
                let mut flipped = bytes.clone();
                flipped[index] ^= 1_u8 << bit;
                check!(
                    !trust
                        .keys
                        .verify(ALICE_KEY, ALICE, &flipped, &good.signature),
                    "byte {index} bit {bit}"
                );
            }
        }
    }

    /// The refusal reasons are what the response message reads, so each one
    /// has to be reachable and each one has to name its own check.
    #[test]
    fn each_refusal_names_its_own_check_under_one_code() {
        let trust = trust();
        let good = record();
        let replaced = TopicFreezeRecord {
            set_at_ms: NOW_MS,
            ..record()
        };

        let cases: Vec<(
            &str,
            TopicFreezeRecord,
            FreezeSignatureCheck<'_>,
            SignatureRefusal,
        )> = vec![
            (
                "an unknown key_id",
                TopicFreezeRecord {
                    key_id: "carol-yubi".to_owned(),
                    ..signed(&trust.alice, CLUSTER_ID, &good)
                },
                check_against(&trust, ALICE),
                SignatureRefusal::UnknownKeyId,
            ),
            (
                "an author the key does not speak for",
                signed(
                    &trust.alice,
                    CLUSTER_ID,
                    &TopicFreezeRecord {
                        set_by: BOB.to_owned(),
                        ..good.clone()
                    },
                ),
                check_against(&trust, BOB),
                SignatureRefusal::AuthorIsNotTheKeyPrincipal,
            ),
            (
                "an author that is not the connection principal",
                signed(&trust.alice, CLUSTER_ID, &good),
                check_against(&trust, BOB),
                SignatureRefusal::AuthorIsNotTheConnectionPrincipal,
            ),
            (
                "a timestamp outside the skew window",
                signed(
                    &trust.alice,
                    CLUSTER_ID,
                    &TopicFreezeRecord {
                        set_at_ms: NOW_MS + minutes(5).millis_i64() + 1,
                        ..good.clone()
                    },
                ),
                check_against(&trust, ALICE),
                SignatureRefusal::TimestampOutsideSkewWindow,
            ),
            (
                "a replayed timestamp",
                signed(&trust.alice, CLUSTER_ID, &good),
                FreezeSignatureCheck {
                    replaces: Some(&replaced),
                    ..check_against(&trust, ALICE)
                },
                SignatureRefusal::TimestampNotNewerThanTheEntryItReplaces,
            ),
            (
                "a signature over another cluster",
                signed(&trust.alice, OTHER_CLUSTER_ID, &good),
                check_against(&trust, ALICE),
                SignatureRefusal::SignatureDidNotVerify,
            ),
        ];

        let mut messages: Vec<&'static str> = Vec::new();
        for (label, record, check, expected) in cases {
            check!(
                verify_freeze_signature(&check, &record) == Err(expected),
                "{label}"
            );
            check!(
                expected.wire().0 == codes::OPERATOR_SIGNATURE_INVALID,
                "{label}"
            );
            messages.push(expected.message());
        }
        let unique: std::collections::BTreeSet<&str> = messages.iter().copied().collect();
        check!(unique.len() == messages.len());
    }

    #[test]
    fn a_timestamp_on_the_edge_of_the_skew_window_is_accepted() {
        let trust = trust();
        let window = minutes(5);

        for (label, set_at_ms, expected) in [
            ("exactly the window in the past", NOW_MS - 300_000, true),
            ("exactly the window in the future", NOW_MS + 300_000, true),
            ("one millisecond past the window", NOW_MS - 300_001, false),
            ("one millisecond after the window", NOW_MS + 300_001, false),
            ("the same moment", NOW_MS, true),
        ] {
            let record = signed(
                &trust.alice,
                CLUSTER_ID,
                &TopicFreezeRecord {
                    set_at_ms,
                    ..record()
                },
            );
            let check = FreezeSignatureCheck {
                max_skew: window,
                ..check_against(&trust, ALICE)
            };
            check!(
                verify_freeze_signature(&check, &record).is_ok() == expected,
                "{label}"
            );
        }
    }

    #[test]
    fn the_skew_window_takes_the_configured_width() {
        let trust = trust();
        let record = signed(
            &trust.alice,
            CLUSTER_ID,
            &TopicFreezeRecord {
                set_at_ms: NOW_MS - 30_000,
                ..record()
            },
        );

        for (label, max_skew, expected) in [
            ("a window wider than the offset", minutes(1), true),
            ("a window narrower than the offset", secs(10), false),
            ("a window of zero", secs(0), false),
        ] {
            let check = FreezeSignatureCheck {
                max_skew,
                ..check_against(&trust, ALICE)
            };
            check!(
                verify_freeze_signature(&check, &record).is_ok() == expected,
                "{label}"
            );
        }
    }

    #[test]
    fn a_newer_timestamp_replaces_a_live_entry() {
        let trust = trust();
        let replaced = TopicFreezeRecord {
            set_at_ms: NOW_MS - 1,
            ..record()
        };
        let record = signed(&trust.alice, CLUSTER_ID, &record());
        let check = FreezeSignatureCheck {
            replaces: Some(&replaced),
            ..check_against(&trust, ALICE)
        };

        check!(verify_freeze_signature(&check, &record) == Ok(()));
    }
}
