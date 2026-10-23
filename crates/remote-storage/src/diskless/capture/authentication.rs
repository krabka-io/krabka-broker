//! Capture signing and external pinned-head authentication using WORM manifests.

use {
    super::{CAPTURE_HEAD_NAME, CAPTURE_STATE_KEY, DisklessWalCapture},
    crate::worm::{
        ChainHead, ChainStamp, EpochId, HexBytes, MANIFEST_FORMAT_VERSION, ManifestBody,
        ManifestSeq, ManifestSignature, ObjectEntry, SegmentIdentity, SegmentManifest,
        Sha256Digest, TrustedManifestKeys, manifest_head, manifest_signing_bytes,
        verify_manifest_signature,
    },
    krabka_audit::FileEd25519Signer,
    std::collections::BTreeMap,
    uuid::Uuid,
};

impl DisklessWalCapture {
    fn unsigned_bytes(&self) -> Result<Vec<u8>, String> {
        let mut unsigned = self.clone();
        unsigned.authentication = None;
        serde_json::to_vec(&unsigned).map_err(|error| error.to_string())
    }

    /// Sign the exact capture state and referenced WAL object claims.
    ///
    /// # Errors
    /// Returns an error when capture state is invalid, object claims do not exactly cover it,
    /// or its deterministic encoding fails.
    pub fn seal(
        &mut self,
        mut objects: Vec<ObjectEntry>,
        signer: &FileEd25519Signer,
    ) -> Result<ChainHead, String> {
        self.validate()?;
        let expected = self
            .partitions
            .iter()
            .flat_map(|partition| &partition.ranges)
            .map(|range| range.object_key.clone())
            .collect::<std::collections::BTreeSet<_>>();
        objects.sort_by(|a, b| a.key.cmp(&b.key));
        let actual = objects
            .iter()
            .map(|object| object.key.clone())
            .collect::<std::collections::BTreeSet<_>>();
        if expected != actual || objects.len() != actual.len() {
            return Err("signed WAL claims do not exactly cover capture references".to_owned());
        }
        let state = self.unsigned_bytes()?;
        let state_digest = Sha256Digest::of(&state);
        objects.push(ObjectEntry {
            suffix: ".capture-state".to_owned(),
            key: CAPTURE_STATE_KEY.to_owned(),
            size_bytes: state.len() as u64,
            sha256: state_digest,
            e_tag: None,
            version_id: None,
            create_precondition: false,
        });
        let mut segment_id = [0; 16];
        segment_id.copy_from_slice(&state_digest.0[..16]);
        let identity = SegmentIdentity {
            topic: CAPTURE_HEAD_NAME.to_owned(),
            topic_id: Uuid::nil(),
            partition: 0,
            segment_id: Uuid::from_bytes(segment_id),
            start_offset: 0,
            end_offset: 0,
            max_timestamp_ms: i64::try_from(self.captured_at_ms).unwrap_or(i64::MAX),
            broker_id: -1,
            event_timestamp_ms: i64::try_from(self.captured_at_ms).unwrap_or(i64::MAX),
            segment_size_bytes: objects
                .iter()
                .map(|object| object.size_bytes)
                .sum::<u64>()
                .try_into()
                .unwrap_or(i64::MAX),
            leader_epochs: BTreeMap::new(),
            txn_index_empty: true,
        };
        let chain = ChainStamp {
            epoch_id: EpochId(Uuid::nil()),
            seq: ManifestSeq(0),
            prev_head: ChainHead::GENESIS,
        };
        let body = ManifestBody {
            format_version: MANIFEST_FORMAT_VERSION,
            segment: identity,
            objects,
            chain,
        };
        let head = manifest_head(&body);
        let message = manifest_signing_bytes(signer.key_id(), chain.epoch_id, chain.seq, head);
        self.authentication = Some(SegmentManifest {
            body,
            signature: Some(ManifestSignature {
                key_id: signer.key_id().to_owned(),
                public_key: HexBytes(signer.public_key()),
                signature: HexBytes(signer.sign(&message)),
            }),
        });
        Ok(head)
    }

    /// Verify the embedded signed boundary and return trusted WAL claims.
    ///
    /// # Errors
    /// Returns an error for invalid capture state, missing or untrusted evidence, a bad
    /// signature, a head mismatch, or incomplete object claims.
    pub fn authenticate(
        &self,
        trusted: &TrustedManifestKeys,
        expected_head: Option<&str>,
    ) -> Result<BTreeMap<String, ObjectEntry>, String> {
        self.validate()?;
        let manifest = self
            .authentication
            .as_ref()
            .ok_or_else(|| "diskless capture has no signed WORM boundary".to_owned())?;
        if manifest.body.format_version != MANIFEST_FORMAT_VERSION {
            return Err("unsupported diskless capture manifest version".to_owned());
        }
        let signature = manifest
            .signature
            .as_ref()
            .ok_or_else(|| "diskless capture WORM boundary is unsigned".to_owned())?;
        let key = trusted
            .get(&signature.key_id)
            .ok_or_else(|| format!("diskless capture uses untrusted key `{}`", signature.key_id))?;
        if !verify_manifest_signature(manifest, key) {
            return Err("diskless capture WORM signature is invalid".to_owned());
        }
        let head = manifest_head(&manifest.body).to_string();
        let expected_head = expected_head.ok_or_else(|| {
            format!("trusted diskless restore requires --worm-expect-head {CAPTURE_HEAD_NAME}=HEX")
        })?;
        if !head.eq_ignore_ascii_case(expected_head) {
            return Err(format!(
                "pinned diskless capture head is {expected_head}, authenticated head is {head}"
            ));
        }
        let state = self.unsigned_bytes()?;
        let mut claims = BTreeMap::new();
        let mut state_claim = None;
        for object in &manifest.body.objects {
            if object.key == CAPTURE_STATE_KEY {
                if state_claim.replace(object).is_some() {
                    return Err("duplicate signed diskless capture-state claim".to_owned());
                }
            } else if claims.insert(object.key.clone(), object.clone()).is_some() {
                return Err(format!("duplicate signed WAL claim `{}`", object.key));
            }
        }
        let state_claim = state_claim
            .ok_or_else(|| "signed diskless capture-state claim is missing".to_owned())?;
        if state_claim.size_bytes != state.len() as u64
            || state_claim.sha256 != Sha256Digest::of(&state)
        {
            return Err("diskless capture state changed after signing".to_owned());
        }
        let expected = self
            .partitions
            .iter()
            .flat_map(|partition| &partition.ranges)
            .map(|range| range.object_key.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        if expected != claims.keys().map(String::as_str).collect() {
            return Err("signed WAL claims do not exactly cover capture references".to_owned());
        }
        Ok(claims)
    }
}
