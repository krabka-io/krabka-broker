//! An audit partition as a broker leaves it on disk, for an auditor to verify.
//!
//! [`AuditPartition`] keeps the stored history as the list of chained records a
//! broker wrote, in offset order. An [`Alteration`] edits that list the way an
//! attacker with disk access would. [`AuditPartition::verify`] lays the list
//! out at consecutive offsets in a fresh partition log and runs the offline
//! verifier over its directory.

use {
    crate::{
        AuditEventClass, AuditRecord, ChainState, Checkpoint, EpochMs, FileEd25519Signer,
        HEADER_SEQ, OcsfVersion, ProductInfo, Seq, TrustedKeys, VerifyReport,
        chain::ChainLink,
        test_support::{TestSigner, admin, denied, signer_with_key_id},
        verify_partition_dir,
    },
    bytes::Bytes,
    krabka_log::{Log, LogConfig},
    krabka_protocol::records::{Record, RecordBatch, RecordHeader},
    std::str,
};

/// The key id a broker signs with when no rotation is in play.
const BROKER_KEY_ID: &str = "audit-key";

/// A key id that none of the broker's checkpoints names.
const UNRELATED_KEY_ID: &str = "audit-unrelated";

/// How the stored copy of one audit event has been altered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_bdd::StepParam)]
pub(super) enum Alteration {
    /// Its payload is replaced; its chain headers are kept.
    Rewritten,
    /// It is gone from the partition.
    Removed,
    /// It has traded places with the record after it.
    Swapped,
    /// It is stored twice, one copy after the other.
    Repeated,
}

/// The public keys an auditor brings to a verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, krabka_bdd::StepParam)]
pub(super) enum AuditorTrust {
    /// The key of every epoch the broker signed in.
    #[step_param("every key of the broker", "the old and the new key")]
    EveryBrokerKey,
    /// The key of the broker's latest epoch alone.
    #[step_param("only the new key")]
    LatestKeyOnly,
    /// A freshly generated key under each key id the broker signed with.
    #[step_param("a different key under the same key id")]
    ImpostorKey,
    /// A key under a key id no checkpoint names.
    #[step_param("no key for its key id")]
    NoKeyForKeyId,
}

/// The stored history of one broker's audit partition.
pub(super) struct AuditPartition {
    chain: ChainState,
    stored: Vec<AuditRecord>,
    /// The broker's signing keys, in the order it first signed with each.
    keys: Vec<TestSigner<FileEd25519Signer>>,
    clock: i64,
    loss_generation: u64,
}

impl AuditPartition {
    pub(super) fn new() -> Self {
        Self {
            chain: ChainState::new(),
            stored: Vec::new(),
            keys: Vec::new(),
            clock: 1_750_000_000_000,
            loss_generation: 0,
        }
    }

    /// Append `count` chained audit events, written below `krabka.version` 3.
    pub(super) fn write_events(&mut self, count: u64) {
        self.write_events_at(count, OcsfVersion::V1);
    }

    /// Append `count` chained audit events in record `version`.
    pub(super) fn write_events_at(&mut self, count: u64, version: OcsfVersion) {
        for _ in 0..count {
            self.clock += 1;
            let record = AuditRecord::from_event(&admin(self.clock), &product(), version);
            self.append_chained(record);
        }
    }

    /// Append a checkpoint over the current chain head, signed under the
    /// broker's usual key id.
    pub(super) fn write_signed_checkpoint(&mut self) {
        self.write_checkpoint_signed_by(BROKER_KEY_ID);
    }

    /// Append a checkpoint over the current chain head, signed with the
    /// broker's key for `key_id`. The first checkpoint under a key id
    /// generates that key, which is how a rotation starts a new epoch.
    pub(super) fn write_checkpoint_signed_by(&mut self, key_id: &str) {
        if !self.keys.iter().any(|key| key.signer.key_id() == key_id) {
            self.keys.push(signer_with_key_id(key_id));
        }
        let signer = &self
            .keys
            .iter()
            .find(|key| key.signer.key_id() == key_id)
            .expect("the key for this key id was just generated")
            .signer;
        let seq_high = self
            .chain
            .next_seq()
            .checked_sub(1)
            .expect("a checkpoint covers at least one audit event");
        self.clock += 1;
        let checkpoint = Checkpoint::signed(
            signer,
            Seq(seq_high),
            &self.chain.head(),
            EpochMs(self.clock),
        );
        self.stored.push(checkpoint.to_record());
    }

    /// Append the chained marker by which a broker declares `count` audit
    /// records lost.
    pub(super) fn declare_lost(&mut self, count: u64) {
        self.loss_generation += 1;
        self.append_chained(AuditRecord::records_lost(count, self.loss_generation));
    }

    /// Alter the stored copy of the event chained at `seq`.
    pub(super) fn alter(&mut self, alteration: Alteration, seq: Seq) {
        let index = self
            .stored
            .iter()
            .position(|record| stored_seq(record) == Some(seq))
            .unwrap_or_else(|| panic!("the partition stores no event at sequence {seq}"));
        match alteration {
            Alteration::Rewritten => {
                self.stored[index].value =
                    AuditRecord::from_event(&denied(self.clock), &product(), OcsfVersion::V1).value;
            }
            Alteration::Removed => {
                self.stored.remove(index);
            }
            Alteration::Swapped => self.stored.swap(index, index + 1),
            Alteration::Repeated => {
                let copy = self.stored[index].clone();
                self.stored.insert(index + 1, copy);
            }
        }
    }

    /// The public keys an auditor holds under `trust`, by key id.
    pub(super) fn trusted_keys(&self, trust: AuditorTrust) -> TrustedKeys {
        let key_ids = self.keys.iter().map(|key| key.signer.key_id().to_owned());
        match trust {
            AuditorTrust::EveryBrokerKey => self
                .keys
                .iter()
                .map(|key| (key.signer.key_id().to_owned(), key.public_key.clone()))
                .collect(),
            AuditorTrust::LatestKeyOnly => self
                .keys
                .last()
                .map(|key| (key.signer.key_id().to_owned(), key.public_key.clone()))
                .into_iter()
                .collect(),
            AuditorTrust::ImpostorKey => key_ids
                .map(|key_id| {
                    let impostor = signer_with_key_id(&key_id).public_key;
                    (key_id, impostor)
                })
                .collect(),
            AuditorTrust::NoKeyForKeyId => {
                let unrelated = signer_with_key_id(UNRELATED_KEY_ID).public_key;
                TrustedKeys::single(UNRELATED_KEY_ID.to_owned(), unrelated)
            }
        }
    }

    /// The offset of the first checkpoint the partition stores.
    pub(super) fn first_checkpoint_offset(&self) -> i64 {
        let index = self
            .stored
            .iter()
            .position(|record| record.class == AuditEventClass::Checkpoint)
            .expect("the partition stores a checkpoint");
        offset_of(index)
    }

    /// The offset and sequence number of every stored loss marker.
    pub(super) fn loss_markers(&self) -> Vec<(i64, Seq)> {
        self.stored
            .iter()
            .enumerate()
            .filter(|(_, record)| record.class == AuditEventClass::RecordsLost)
            .map(|(index, record)| {
                let seq = stored_seq(record).expect("a loss marker is chained");
                (offset_of(index), seq)
            })
            .collect()
    }

    /// Store the history in a fresh partition log and verify it offline.
    pub(super) fn verify(&self, trusted: &TrustedKeys) -> VerifyReport {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), LogConfig::default()).unwrap();
        for (index, record) in self.stored.iter().enumerate() {
            log.append(&mut batch_at(record, offset_of(index))).unwrap();
        }
        verify_partition_dir(dir.path(), trusted).unwrap()
    }

    fn append_chained(&mut self, mut record: AuditRecord) {
        let ChainLink { seq, prev_head } = self.chain.extend(&record.value);
        record.push_chain_headers(seq, &prev_head);
        self.stored.push(record);
    }
}

fn product() -> ProductInfo {
    ProductInfo {
        vendor_name: "Krabka".to_owned(),
        name: "krabka-broker".to_owned(),
        version: "1.0.0".to_owned(),
    }
}

fn offset_of(index: usize) -> i64 { i64::try_from(index).unwrap() }

fn stored_seq(record: &AuditRecord) -> Option<Seq> {
    record
        .headers
        .iter()
        .find(|(key, _)| key == HEADER_SEQ)
        .and_then(|(_, value)| str::from_utf8(value).ok())
        .and_then(|text| text.parse().ok())
        .map(Seq)
}

/// One record in a batch of its own at `offset`, as the broker appends it.
fn batch_at(record: &AuditRecord, offset: i64) -> RecordBatch {
    let headers = record
        .headers
        .iter()
        .map(|(key, value)| RecordHeader {
            key: key.clone(),
            value: Some(Bytes::from(value.clone())),
        })
        .collect();
    RecordBatch {
        base_offset: offset,
        last_offset_delta: 0,
        records: vec![Record {
            value: Some(Bytes::from(record.value.clone())),
            headers,
            ..Record::default()
        }],
        ..RecordBatch::default()
    }
}
