//! The per-partition manifest chain, and where the next manifest joins it.
//!
//! Each copy stamps a [`WormChainRecord`] onto the segment's
//! [`CustomMetadata`]. The record is the receipt: it says which chain run the
//! manifest belongs to, where in the run it sits, and what head it produced.
//! [`next_chain_stamp`] reads those receipts back and works out where the next
//! manifest goes.

use krabka_verified::{ChainStep, chain::select_chain_tip, chain_step};
use serde::{Deserialize, Serialize};

use crate::{
    metadata::{CustomMetadata, RemoteLogSegmentMetadata, RemoteLogSegmentState},
    worm::{
        error::WormError,
        manifest::{ChainHead, ChainStamp, EpochId, ManifestSeq},
    },
};

/// The chain receipt a copy leaves on a segment's custom metadata.
///
/// The record has two forms. The **request** form, from
/// [`WormChainRecord::request`], carries no head: the broker builds it before
/// the copy, when the manifest bytes do not exist yet. The **receipt** form,
/// from [`WormChainRecord::with_head`], carries the head the manifest produced
/// and is the only form [`next_chain_stamp`] continues from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WormChainRecord {
    /// Chain run this manifest belongs to.
    pub epoch_id: EpochId,
    /// Position within the run.
    pub seq: ManifestSeq,
    /// Chain head before this manifest.
    pub prev_head: ChainHead,
    /// The head *after* this manifest. `None` in the pre-copy request form.
    pub head: Option<ChainHead>,
    /// Object-store version id of the manifest object, when the bucket has
    /// versioning on.
    pub manifest_version_id: Option<String>,
}

impl WormChainRecord {
    /// The pre-copy request form of a record at `stamp`, with no head yet.
    #[must_use]
    pub fn request(stamp: ChainStamp) -> Self {
        Self {
            epoch_id: stamp.epoch_id,
            seq: stamp.seq,
            prev_head: stamp.prev_head,
            head: None,
            manifest_version_id: None,
        }
    }

    /// Turns a request form into a receipt by recording the head the manifest
    /// produced.
    #[must_use]
    pub fn with_head(mut self, head: ChainHead) -> Self {
        self.head = Some(head);
        self
    }

    /// Records the object-store version id of the manifest object.
    #[must_use]
    pub fn with_manifest_version(mut self, version_id: Option<String>) -> Self {
        self.manifest_version_id = version_id;
        self
    }

    /// Encodes the record as the JSON bytes of a [`CustomMetadata`].
    ///
    /// # Panics
    ///
    /// Panics if `serde_json` cannot serialise the record. Every field is a
    /// `UUID`, an integer, a fixed-size byte array, or a string, so no
    /// serialisation of this type can fail.
    #[must_use]
    pub fn to_custom_metadata(&self) -> CustomMetadata {
        let json = serde_json::to_vec(self)
            .expect("WormChainRecord holds only infallibly serialisable fields");
        CustomMetadata(json)
    }

    /// Decodes a record from a segment's [`CustomMetadata`].
    ///
    /// Never panics. Arbitrary bytes produce an error.
    ///
    /// # Errors
    ///
    /// Returns [`WormError::MalformedChainRecord`] when `cm` does not hold the
    /// JSON of a chain record: bytes that are not `UTF-8`, text that is not
    /// JSON, an object with a missing or unknown field, or a hex string that
    /// is not 64 characters.
    pub fn from_custom_metadata(cm: &CustomMetadata) -> Result<Self, WormError> {
        serde_json::from_slice(&cm.0).map_err(|e| WormError::MalformedChainRecord(e.to_string()))
    }

    /// The chain position the next manifest takes after this one.
    ///
    /// `None` for the request form, which has produced no head to chain onto.
    #[must_use]
    pub fn next_stamp(&self) -> Option<ChainStamp> {
        let head = self.head?;
        match chain_step(self.seq.0, self.seq.0, true) {
            ChainStep::Continue(next) => Some(ChainStamp {
                epoch_id: self.epoch_id,
                seq: ManifestSeq(next),
                prev_head: head,
            }),
            ChainStep::SequenceMismatch | ChainStep::HeadMismatch | ChainStep::Exhausted => None,
        }
    }
}

/// Next chain position for a partition, given every segment the metadata
/// manager knows about.
///
/// Picks the receipt on the segment with the greatest `start_offset`, breaking
/// a tie by the greatest `seq`, and continues from it. A segment in a delete
/// state is ignored, and so is a record in the request form, which carries no
/// head.
///
/// Returns a **fresh epoch at genesis** when no receipt survives. That is a new
/// partition, or a restart on the non-durable in-memory metadata manager, and
/// in both cases the old chain cannot be continued. A new epoch says so, rather
/// than restarting the old chain at sequence zero and looking like a rewrite.
/// Returns `None` when the selected receipt is at `u64::MAX`, because no later
/// sequence exists and restarting at genesis would hide exhaustion as a new
/// chain run.
///
/// `new_epoch_id` is a parameter and not a `Uuid::new_v4()` call inside, so the
/// function stays pure and testable.
#[must_use]
pub fn next_chain_stamp(
    segments: &[RemoteLogSegmentMetadata],
    new_epoch_id: EpochId,
) -> Option<ChainStamp> {
    let mut candidates = Vec::with_capacity(segments.len());
    let mut receipts = Vec::with_capacity(segments.len());
    for md in segments {
        let receipt = if matches!(
            md.state(),
            RemoteLogSegmentState::DeleteSegmentStarted
                | RemoteLogSegmentState::DeleteSegmentFinished
        ) {
            None
        } else {
            md.custom_metadata()
                .and_then(|custom| WormChainRecord::from_custom_metadata(custom).ok())
                .filter(|record| record.head.is_some())
        };
        let sequence = receipt.as_ref().map_or(0, |record| record.seq.0);
        candidates.push((md.start_offset(), sequence, receipt.is_some()));
        receipts.push(receipt);
    }
    let Some(index) = select_chain_tip(&candidates) else {
        return Some(ChainStamp {
            epoch_id: new_epoch_id,
            seq: ManifestSeq(0),
            prev_head: ChainHead::GENESIS,
        });
    };
    receipts.get(index)?.as_ref()?.next_stamp()
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_ids::LeaderEpoch;
    use proptest::{collection::vec as prop_vec, num::u8::ANY as ANY_U8, proptest};
    use uuid::Uuid;

    use super::*;
    use crate::metadata::{RemoteLogSegmentDetails, RemoteLogSegmentId, TopicIdPartition};

    fn epoch() -> EpochId {
        EpochId(Uuid::from_u128(0x1234))
    }

    fn head(byte: u8) -> ChainHead {
        ChainHead([byte; 32])
    }

    fn sample_metadata(
        start_offset: i64,
        state: RemoteLogSegmentState,
        custom: Option<CustomMetadata>,
    ) -> RemoteLogSegmentMetadata {
        let md = RemoteLogSegmentMetadata::new(
            RemoteLogSegmentId::new(
                TopicIdPartition::new(Uuid::from_u128(1), "orders", 0),
                Uuid::from_u128(u128::try_from(start_offset).unwrap() + 100),
            ),
            start_offset,
            start_offset + 99,
            123,
            1,
            456,
            RemoteLogSegmentDetails::new(
                8,
                state,
                maplit::btreemap! {LeaderEpoch(0) => start_offset},
            ),
        )
        .unwrap();
        match custom {
            Some(cm) => md.with_custom_metadata(cm),
            None => md,
        }
    }

    fn receipt(seq: u64, prev: u8, produced: u8) -> CustomMetadata {
        WormChainRecord::request(ChainStamp {
            epoch_id: epoch(),
            seq: ManifestSeq(seq),
            prev_head: head(prev),
        })
        .with_head(head(produced))
        .to_custom_metadata()
    }

    #[test]
    fn next_chain_stamp_starts_new_epoch_on_empty_partition() {
        let fresh = EpochId(Uuid::from_u128(0xfeed));
        check!(
            next_chain_stamp(&[], fresh)
                == Some(ChainStamp {
                    epoch_id: fresh,
                    seq: ManifestSeq(0),
                    prev_head: ChainHead::GENESIS,
                })
        );
    }

    #[test]
    fn next_chain_stamp_continues_from_highest_offset_receipt() {
        let segments = [
            sample_metadata(
                0,
                RemoteLogSegmentState::CopySegmentFinished,
                Some(receipt(0, 0x00, 0xaa)),
            ),
            sample_metadata(
                200,
                RemoteLogSegmentState::CopySegmentFinished,
                Some(receipt(2, 0xbb, 0xcc)),
            ),
            sample_metadata(
                100,
                RemoteLogSegmentState::CopySegmentFinished,
                Some(receipt(1, 0xaa, 0xbb)),
            ),
        ];
        check!(
            next_chain_stamp(&segments, EpochId(Uuid::from_u128(0xfeed)))
                == Some(ChainStamp {
                    epoch_id: epoch(),
                    seq: ManifestSeq(3),
                    prev_head: head(0xcc),
                })
        );
    }

    #[test]
    fn next_chain_stamp_breaks_offset_ties_by_greatest_seq() {
        let request_at_same_offset = WormChainRecord::request(ChainStamp {
            epoch_id: epoch(),
            seq: ManifestSeq(9),
            prev_head: head(0xcc),
        })
        .with_head(head(0xdd))
        .to_custom_metadata();
        let segments = [
            sample_metadata(
                100,
                RemoteLogSegmentState::CopySegmentFinished,
                Some(receipt(1, 0xaa, 0xbb)),
            ),
            sample_metadata(
                100,
                RemoteLogSegmentState::CopySegmentFinished,
                Some(request_at_same_offset),
            ),
        ];
        check!(
            next_chain_stamp(&segments, EpochId(Uuid::from_u128(0xfeed)))
                == Some(ChainStamp {
                    epoch_id: epoch(),
                    seq: ManifestSeq(10),
                    prev_head: head(0xdd),
                })
        );
    }

    #[test]
    fn next_chain_stamp_ignores_request_form_records() {
        let request_only = WormChainRecord::request(ChainStamp {
            epoch_id: epoch(),
            seq: ManifestSeq(7),
            prev_head: head(0xbb),
        })
        .to_custom_metadata();
        let segments = [
            sample_metadata(
                100,
                RemoteLogSegmentState::CopySegmentFinished,
                Some(receipt(1, 0xaa, 0xbb)),
            ),
            // Highest offset, but the copy never finished, so no head.
            sample_metadata(
                200,
                RemoteLogSegmentState::CopySegmentStarted,
                Some(request_only),
            ),
        ];
        check!(
            next_chain_stamp(&segments, EpochId(Uuid::from_u128(0xfeed)))
                == Some(ChainStamp {
                    epoch_id: epoch(),
                    seq: ManifestSeq(2),
                    prev_head: head(0xbb),
                })
        );
    }

    #[test]
    fn next_chain_stamp_ignores_deleted_segments() {
        for (name, deleted_state) in [
            (
                "delete started",
                RemoteLogSegmentState::DeleteSegmentStarted,
            ),
            (
                "delete finished",
                RemoteLogSegmentState::DeleteSegmentFinished,
            ),
        ] {
            let segments = [
                sample_metadata(
                    100,
                    RemoteLogSegmentState::CopySegmentFinished,
                    Some(receipt(1, 0xaa, 0xbb)),
                ),
                sample_metadata(300, deleted_state, Some(receipt(5, 0xee, 0xff))),
            ];
            check!(
                next_chain_stamp(&segments, EpochId(Uuid::from_u128(0xfeed)))
                    == Some(ChainStamp {
                        epoch_id: epoch(),
                        seq: ManifestSeq(2),
                        prev_head: head(0xbb),
                    }),
                "case {name}"
            );
        }
    }

    #[test]
    fn next_chain_stamp_starts_new_epoch_when_no_receipt_decodes() {
        let fresh = EpochId(Uuid::from_u128(0xfeed));
        let expected = ChainStamp {
            epoch_id: fresh,
            seq: ManifestSeq(0),
            prev_head: ChainHead::GENESIS,
        };
        for (name, custom) in [
            ("no custom metadata at all", None),
            ("empty custom metadata", Some(CustomMetadata(Vec::new()))),
            (
                "custom metadata from another backend",
                Some(CustomMetadata(b"s3://bucket/key".to_vec())),
            ),
            (
                "JSON that is not a chain record",
                Some(CustomMetadata(br#"{"key":"value"}"#.to_vec())),
            ),
            (
                "chain record with a bad head",
                Some(CustomMetadata(
                    br#"{"epoch_id":"00000000-0000-0000-0000-000000000001","seq":1,"prev_head":"zz","head":null,"manifest_version_id":null}"#
                        .to_vec(),
                )),
            ),
        ] {
            let segments = [sample_metadata(
                100,
                RemoteLogSegmentState::CopySegmentFinished,
                custom,
            )];
            check!(next_chain_stamp(&segments, fresh) == Some(expected), "case {name}");
        }
    }

    #[test]
    fn next_chain_stamp_rejects_sequence_exhaustion() {
        let exhausted = WormChainRecord::request(ChainStamp {
            epoch_id: epoch(),
            seq: ManifestSeq(u64::MAX),
            prev_head: head(0xaa),
        })
        .with_head(head(0xbb))
        .to_custom_metadata();
        let segments = [sample_metadata(
            100,
            RemoteLogSegmentState::CopySegmentFinished,
            Some(exhausted),
        )];

        check!(next_chain_stamp(&segments, EpochId(Uuid::from_u128(0xfeed))) == None);
    }

    #[test]
    fn chain_record_round_trips_through_custom_metadata() {
        let stamp = ChainStamp {
            epoch_id: epoch(),
            seq: ManifestSeq(11),
            prev_head: head(0x5a),
        };
        let request = WormChainRecord::request(stamp);
        let full = request
            .clone()
            .with_head(head(0x6b))
            .with_manifest_version(Some("3HL4kqtJlcpXroDTDmjVBH40Nrjfkd".to_string()));

        check!(
            request
                == WormChainRecord {
                    epoch_id: epoch(),
                    seq: ManifestSeq(11),
                    prev_head: head(0x5a),
                    head: None,
                    manifest_version_id: None,
                }
        );
        check!(request.next_stamp() == None);
        check!(
            full.next_stamp()
                == Some(ChainStamp {
                    epoch_id: epoch(),
                    seq: ManifestSeq(12),
                    prev_head: head(0x6b),
                })
        );

        for (name, record) in [("request form", request), ("receipt form", full)] {
            let encoded = record.to_custom_metadata();
            check!(
                WormChainRecord::from_custom_metadata(&encoded).unwrap() == record,
                "case {name}"
            );
        }
    }

    #[test]
    fn malformed_chain_record_is_an_error_not_a_panic() {
        let err = WormChainRecord::from_custom_metadata(&CustomMetadata(b"not json".to_vec()))
            .unwrap_err();
        check!(matches!(err, WormError::MalformedChainRecord(_)));
    }

    proptest! {
        #[test]
        fn from_custom_metadata_never_panics_on_arbitrary_bytes(
            bytes in prop_vec(ANY_U8, 0..512usize),
        ) {
            let _ = WormChainRecord::from_custom_metadata(&CustomMetadata(bytes));
        }
    }
}
