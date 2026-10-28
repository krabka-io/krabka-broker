//! The KIP-932 share-group member metadata record at key version 10.
//!
//! [`ShareGroupMemberMetadataValue`] holds what a share consumer told the
//! coordinator about itself: its optional rack, its client identity, and the
//! topics it subscribed to by name. Share groups have no regex subscription, no
//! server assignor, and no rebalance timeout, so those consumer-only fields are
//! absent.
//!
//! # Layout
//!
//! From `ShareGroupMemberMetadataValue.json` at Apache Kafka tag `4.3.1`, which
//! declares `"flexibleVersions": "0+"`: `RackId` (nullable string), `ClientId`
//! (string), `ClientHost` (string) and `SubscribedTopicNames` (`[]string`).
//! Every string is compact, the array is compact, and the message ends with its
//! tagged-field count.

use {
    crate::{
        coordinator::unified::persistence::flex::{
            get_member_client, get_string_array, put_member_client, put_string_array,
        },
        error::BrokerError,
    },
    bytes::BytesMut,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShareGroupMemberMetadataValue {
    pub rack_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub subscribed_topic_names: Vec<String>,
}

#[krabka_macros::flexible_value(record = "ShareGroupMemberMetadataValue")]
impl ShareGroupMemberMetadataValue {
    fn encode_fields(&self, buf: &mut BytesMut) {
        put_member_client(
            buf,
            self.rack_id.as_deref(),
            &self.client_id,
            &self.client_host,
        );
        put_string_array(buf, &self.subscribed_topic_names);
    }

    fn decode_fields(buf: &mut &[u8]) -> Result<Self, BrokerError> {
        let (rack_id, client_id, client_host) = get_member_client(buf)?;
        let subscribed_topic_names = get_string_array(buf)?;
        Ok(Self {
            rack_id,
            client_id,
            client_host,
            subscribed_topic_names,
        })
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::coordinator::unified::{
            codec_observations::EncodedRecord,
            share::persistence::{KEY_SHARE_MEMBER_METADATA, ShareGroupKey},
        },
        assert2::assert,
    };

    #[test]
    fn member_metadata_round_trip() {
        let key = ShareGroupKey::MemberMetadata {
            group_id: "g1".into(),
            member_id: "m1".into(),
        };
        super::super::check_key_round_trip(
            &key,
            super::super::ShareKeyVersion(KEY_SHARE_MEMBER_METADATA),
        );

        let v = ShareGroupMemberMetadataValue {
            rack_id: Some("us-east-1a".into()),
            client_id: "c1".into(),
            client_host: "/127.0.0.1".into(),
            subscribed_topic_names: vec!["a".into(), "b".into()],
        };
        assert!(ShareGroupMemberMetadataValue::decode(&v.encode()).unwrap() == v);
    }

    #[test]
    fn member_metadata_rejects_a_missing_tagged_trailer() {
        let v = ShareGroupMemberMetadataValue {
            rack_id: None,
            client_id: "c".into(),
            client_host: "h".into(),
            subscribed_topic_names: vec![],
        };
        let full = v.encode();
        assert!(ShareGroupMemberMetadataValue::decode(&full[..full.len() - 1]).is_err());
    }

    #[test]
    fn member_metadata_null_rack_round_trip() {
        let v = ShareGroupMemberMetadataValue {
            rack_id: None,
            client_id: "c1".into(),
            client_host: "/127.0.0.1".into(),
            subscribed_topic_names: vec![],
        };
        assert!(ShareGroupMemberMetadataValue::decode(&v.encode()).unwrap() == v);
    }
    mod bdd_member_metadata {
        use {
            super::*,
            assert2::assert,
            rstest_bdd_macros::{given, scenarios, then, when},
        };
        type EncodedValue = EncodedRecord<ShareGroupMemberMetadataValue>;
        #[rstest::fixture]
        fn stored_value() -> EncodedValue {
            // Start this scenario with fresh inputs and observations.
            EncodedValue::default()
        }
        #[given("a Share member c at host h in rack r subscribes to topic a")]
        fn prepared_value(stored_value: &mut EncodedValue) {
            let v = ShareGroupMemberMetadataValue {
                rack_id: Some("r".into()),
                client_id: "c".into(),
                client_host: "h".into(),
                subscribed_topic_names: vec!["a".into()],
            };

            stored_value.value = Some(v);
        }
        #[when("the coordinator persists the Share member metadata in the compacted group log")]
        fn encode_group_record(stored_value: &mut EncodedValue) {
            stored_value.encoded = Some(
                stored_value
                    .value
                    .as_ref()
                    .expect("group record prepared")
                    .encode(),
            );
        }
        #[then(
            "the persisted Share member metadata matches Kafka schema bytes including compact lengths and tagged trailers"
        )]
        fn kafka_record_layout(stored_value: &EncodedValue) {
            let want = b"\x00\x00\x02r\x02c\x02h\x02\x02a\x00";

            assert!(&stored_value.encoded.as_ref().expect("record encoded")[..] == &want[..]);
        }
        scenarios!("tests/features/migrated/src/coordinator/unified/share/persistence/member.feature",tags="@member_metadata",fixtures=[stored_value: EncodedValue]);
    }
}
