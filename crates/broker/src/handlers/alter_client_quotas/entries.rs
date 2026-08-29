//! Validation of one `AlterClientQuotas` entry and the metadata records it
//! becomes.
//!
//! The quota keys and the entity types Krabka accepts are the vocabulary of
//! that validation, so they live here beside the checks that use them rather
//! than in the request entry point.

use std::collections::HashSet;

use krabka_metadata::{ClientQuotaRecord, MetadataRecord, QuotaEntity};
use krabka_protocol::owned::alter_client_quotas_request::EntryData;

use crate::codes::{INVALID_CONFIG, INVALID_REQUEST};

/// Quota key: produce-side bandwidth cap in bytes/sec (KIP-13).
const PRODUCER_BYTE_RATE_KEY: &str = "producer_byte_rate";
/// Quota key: fetch-side bandwidth cap in bytes/sec (KIP-13).
const CONSUMER_BYTE_RATE_KEY: &str = "consumer_byte_rate";
/// Quota key: request-handler time cap as a percentage of one thread (KIP-124).
const REQUEST_PERCENTAGE_KEY: &str = "request_percentage";
/// Quota key: per-IP connection creation rate (KIP-612).
const CONNECTION_CREATION_RATE_KEY: &str = "connection_creation_rate";
/// Quota key: controller mutation rate for topic/partition creation and deletion (KIP-599).
const CONTROLLER_MUTATION_RATE_KEY: &str = "controller_mutation_rate";
/// Upper bound for `request_percentage`, a percentage of one request-handler
/// thread.
const REQUEST_PERCENTAGE_MAX: f64 = 100.0;

/// Quota keys Krabka accepts in `AlterClientQuotas` ops.
const KNOWN_QUOTA_KEYS: &[&str] = &[
    PRODUCER_BYTE_RATE_KEY,
    CONSUMER_BYTE_RATE_KEY,
    REQUEST_PERCENTAGE_KEY,
    CONNECTION_CREATION_RATE_KEY, // KIP-612 — only enforced when paired with ip entity
    CONTROLLER_MUTATION_RATE_KEY, // KIP-599
];

/// Quota entity type: authenticated user principal (KIP-257).
const ENTITY_TYPE_USER: &str = "user";
/// Quota entity type: client id (KIP-257).
const ENTITY_TYPE_CLIENT_ID: &str = "client-id";
/// Quota entity type: client source IP address (KIP-612).
const ENTITY_TYPE_IP: &str = "ip";

/// Quota entity types Krabka accepts in `AlterClientQuotas` entries.
const SUPPORTED_ENTITY_TYPES: &[&str] = &[ENTITY_TYPE_USER, ENTITY_TYPE_CLIENT_ID, ENTITY_TYPE_IP];

/// Validates one `EntryData` and transforms it into a list of
/// `MetadataRecord` values to submit. It returns the wire `(code, message)`
/// pair on a validation failure.
pub(crate) fn process_one_entry(entry: &EntryData) -> Result<Vec<MetadataRecord>, (i16, String)> {
    if entry.entity.is_empty() {
        return Err((INVALID_REQUEST, "empty entity tuple".into()));
    }
    let mut seen_types: HashSet<&str> = HashSet::new();
    for e in &entry.entity {
        if !SUPPORTED_ENTITY_TYPES.contains(&e.entity_type.as_str()) {
            return Err((
                INVALID_REQUEST,
                format!("unsupported entity_type {:?}", e.entity_type),
            ));
        }
        if !seen_types.insert(e.entity_type.as_str()) {
            return Err((
                INVALID_REQUEST,
                format!("duplicate entity_type {:?}", e.entity_type),
            ));
        }
        // entity_name == None is fine for ip — that means the default ip entity.
        if e.entity_type == ENTITY_TYPE_IP
            && let Some(name) = &e.entity_name
            && name.parse::<std::net::Ipv4Addr>().is_err()
        {
            return Err((INVALID_REQUEST, format!("invalid IPv4 address {name:?}")));
        }
    }
    let mut records = Vec::with_capacity(entry.ops.len());
    for op in &entry.ops {
        if !KNOWN_QUOTA_KEYS.contains(&op.key.as_str()) {
            return Err((INVALID_CONFIG, format!("unknown quota key {:?}", op.key)));
        }
        if !op.remove {
            if !op.value.is_finite() || op.value < 0.0 {
                return Err((
                    INVALID_CONFIG,
                    format!("invalid value {} for {}", op.value, op.key),
                ));
            }
            if op.key == REQUEST_PERCENTAGE_KEY && op.value > REQUEST_PERCENTAGE_MAX {
                return Err((
                    INVALID_CONFIG,
                    format!("request_percentage > 100.0: {}", op.value),
                ));
            }
        }
        records.push(MetadataRecord::V1ClientQuota(ClientQuotaRecord {
            entity: entry
                .entity
                .iter()
                .map(|e| QuotaEntity {
                    entity_type: e.entity_type.clone(),
                    entity_name: e.entity_name.clone(),
                })
                .collect(),
            config_key: op.key.clone(),
            config_value: if op.remove { None } else { Some(op.value) },
        }));
    }
    Ok(records)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::handlers::alter_client_quotas::test_support::entry;

    #[test]
    fn start_writes_v1_client_quota_record() {
        let e = entry(
            vec![("user", Some("alice"))],
            vec![("producer_byte_rate", 1024.0, false)],
        );
        let records = process_one_entry(&e).expect("ok");
        assert!(records.len() == 1);
        let MetadataRecord::V1ClientQuota(r) = &records[0] else {
            panic!("wrong variant")
        };
        assert!(r.config_key == "producer_byte_rate");
        assert!(r.config_value == Some(1024.0));
    }

    #[test]
    fn validate_only_does_not_submit() {
        // This is exercised at the handler level; process_one_entry has no notion.
        // The test below verifies that the record-building step works regardless.
        let e = entry(
            vec![("user", Some("alice"))],
            vec![("producer_byte_rate", 1024.0, false)],
        );
        assert!(process_one_entry(&e).is_ok());
    }

    #[test]
    fn remove_writes_none_value() {
        let e = entry(
            vec![("user", Some("alice"))],
            vec![("producer_byte_rate", 0.0, true)],
        );
        let records = process_one_entry(&e).expect("ok");
        let MetadataRecord::V1ClientQuota(r) = &records[0] else {
            panic!()
        };
        assert!(r.config_value == None);
    }

    #[test]
    fn inclusive_boundary_values_are_accepted() {
        let e = entry(
            vec![("user", Some("alice"))],
            vec![
                ("producer_byte_rate", 0.0, false),
                ("request_percentage", 100.0, false),
            ],
        );

        let records = process_one_entry(&e).expect("boundary values are valid");
        let alice_entity = vec![QuotaEntity {
            entity_type: "user".into(),
            entity_name: Some("alice".into()),
        }];
        let expected = vec![
            MetadataRecord::V1ClientQuota(ClientQuotaRecord {
                entity: alice_entity.clone(),
                config_key: "producer_byte_rate".into(),
                config_value: Some(0.0),
            }),
            MetadataRecord::V1ClientQuota(ClientQuotaRecord {
                entity: alice_entity,
                config_key: "request_percentage".into(),
                config_value: Some(100.0),
            }),
        ];
        assert!(records == expected);
    }

    #[test]
    fn unsupported_entity_type_rejected() {
        let e = entry(
            vec![("group", Some("g1"))],
            vec![("producer_byte_rate", 1024.0, false)],
        );
        let err = process_one_entry(&e).unwrap_err();
        assert!(err.0 == INVALID_REQUEST);
    }

    #[test]
    fn duplicate_entity_type_rejected() {
        let e = entry(
            vec![("user", Some("alice")), ("user", Some("bob"))],
            vec![("producer_byte_rate", 1024.0, false)],
        );
        let err = process_one_entry(&e).unwrap_err();
        assert!(err.0 == INVALID_REQUEST);
    }

    #[test]
    fn out_of_range_value_rejected() {
        let cases = [
            ("producer_byte_rate", -100.0),   // negative
            ("request_percentage", 250.0),    // > 100.0 cap
            ("producer_byte_rate", f64::NAN), // non-finite
        ];
        for (quota_key, value) in cases {
            let e = entry(
                vec![("user", Some("alice"))],
                vec![(quota_key, value, false)],
            );
            let err = process_one_entry(&e).unwrap_err();
            assert!(err.0 == INVALID_CONFIG, "key {quota_key}, value {value}");
        }
    }

    #[test]
    fn ip_entity_with_valid_ipv4_accepted() {
        let e = entry(
            vec![("ip", Some("10.0.0.1"))],
            vec![("connection_creation_rate", 1.0, false)],
        );
        let records = process_one_entry(&e).expect("ok");
        assert!(records.len() == 1);
        let MetadataRecord::V1ClientQuota(r) = &records[0] else {
            panic!()
        };
        assert!(r.config_key == "connection_creation_rate");
        assert!(r.config_value == Some(1.0));
    }

    #[test]
    fn ip_entity_with_invalid_address_rejected() {
        let e = entry(
            vec![("ip", Some("not-an-ip"))],
            vec![("connection_creation_rate", 1.0, false)],
        );
        let err = process_one_entry(&e).unwrap_err();
        assert!(err.0 == INVALID_REQUEST);
    }

    #[test]
    fn controller_mutation_rate_key_accepted() {
        let e = entry(
            vec![("user", Some("alice"))],
            vec![("controller_mutation_rate", 2.0, false)],
        );
        let records = process_one_entry(&e).expect("ok");
        assert!(records.len() == 1);
        let MetadataRecord::V1ClientQuota(r) = &records[0] else {
            panic!("wrong variant");
        };
        assert!(r.config_key == "controller_mutation_rate");
        assert!(r.config_value == Some(2.0));
    }
}
