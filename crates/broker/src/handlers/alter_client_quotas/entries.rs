//! Validation of the `AlterClientQuotas` entries and the metadata records
//! they become.
//!
//! This is a port of Kafka's `ClientQuotaControlManager.alterClientQuotas`:
//! the same checks, in the same order, with the same `INVALID_REQUEST`
//! messages. The quota keys and the entity types are the vocabulary of that
//! validation, so they live here beside the checks that use them.

use {
    crate::codes::INVALID_REQUEST,
    krabka_metadata::{ClientQuotaRecord, EntityKey, MetadataRecord, QuotaEntity},
    krabka_protocol::owned::alter_client_quotas_request::EntryData,
    std::collections::{BTreeMap, HashMap},
};

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

/// The `ConfigDef` type Kafka's `QuotaConfig` gives a quota key. It decides
/// the range and fraction checks a new value goes through.
#[derive(Clone, Copy)]
enum QuotaValueType {
    Int,
    Long,
    Double,
}

/// One quota key of an entity type's key set, with its `ConfigDef` type.
struct QuotaKey {
    name: &'static str,
    value_type: QuotaValueType,
}

/// `QuotaConfig.userAndClientQuotaConfigs()`: the keys a `user` and/or
/// `client-id` entity accepts.
const USER_AND_CLIENT_KEYS: &[QuotaKey] = &[
    QuotaKey {
        name: PRODUCER_BYTE_RATE_KEY,
        value_type: QuotaValueType::Long,
    },
    QuotaKey {
        name: CONSUMER_BYTE_RATE_KEY,
        value_type: QuotaValueType::Long,
    },
    QuotaKey {
        name: REQUEST_PERCENTAGE_KEY,
        value_type: QuotaValueType::Double,
    },
    QuotaKey {
        name: CONTROLLER_MUTATION_RATE_KEY,
        value_type: QuotaValueType::Double,
    },
];

/// `QuotaConfig.ipConfigs()`: the keys an `ip` entity accepts.
const IP_KEYS: &[QuotaKey] = &[QuotaKey {
    name: CONNECTION_CREATION_RATE_KEY,
    value_type: QuotaValueType::Int,
}];

/// One requested change to a quota key: a set to a value, or a removal.
#[derive(Clone, Copy)]
struct QuotaChange<'a> {
    key: &'a str,
    value: Option<f64>,
}

/// `Integer.MAX_VALUE` as a double, the bound of an `INT` quota.
const INT_MAX: f64 = 2_147_483_647.0;
/// `Long.MAX_VALUE` as Java widens it to a double (2^63), the bound of a
/// `LONG` quota.
const LONG_MAX: f64 = 9.223_372_036_854_776e18;

/// Quota entity type: authenticated user principal (KIP-257).
const ENTITY_TYPE_USER: &str = "user";
/// Quota entity type: client id (KIP-257).
const ENTITY_TYPE_CLIENT_ID: &str = "client-id";
/// Quota entity type: client source IP address (KIP-612).
const ENTITY_TYPE_IP: &str = "ip";

/// The wire `(code, message)` pair of a rejected entity.
pub type EntityError = (i16, String);

/// The outcome of one `AlterClientQuotas` request.
#[derive(Debug, PartialEq)]
pub struct Alteration {
    /// One result per distinct entity, in the order each entity first
    /// appears. Kafka keys its result map by entity, so a repeated entity
    /// answers one row.
    pub results: Vec<(EntityKey, Result<(), EntityError>)>,
    /// The records to write. They are empty for a request that changes
    /// nothing.
    pub records: Vec<MetadataRecord>,
}

/// The entity an entry names, sorted by entity type.
///
/// Kafka's `AlterClientQuotasRequest.entries` builds a map from type to
/// name, so an entry that repeats a type keeps its last name.
pub fn entry_entity(entry: &EntryData) -> EntityKey {
    let mut by_type: BTreeMap<&str, Option<&str>> = BTreeMap::new();
    for e in &entry.entity {
        by_type.insert(e.entity_type.as_str(), e.entity_name.as_deref());
    }
    by_type
        .into_iter()
        .map(|(t, n)| (t.to_owned(), n.map(str::to_owned)))
        .collect()
}

/// Kafka's `ClientQuotaEntity.toString`, which its error messages embed.
///
/// A Java `HashMap` over `user`, `client-id` and `ip` iterates `client-id`
/// before `user`, the same order as the sorted key.
fn describe_entity(entity: &[(String, Option<String>)]) -> String {
    let entries = entity
        .iter()
        .map(|(t, n)| format!("{t}={}", n.as_deref().unwrap_or("null")))
        .collect::<Vec<_>>()
        .join(", ");
    format!("ClientQuotaEntity(entries={{{entries}}})")
}

fn invalid(message: String) -> EntityError { (INVALID_REQUEST, message) }

/// Validates every entry and builds the records they write, as Kafka's
/// `ClientQuotaControlManager.alterClientQuotas` does.
///
/// `current` is the quota state before the request: a removal writes a
/// record only for a key that is set, and a set writes one only for a value
/// that changes. `ip_is_valid` answers Kafka's `InetAddress.getByName` check
/// for a named `ip` entity.
pub fn alter_client_quotas(
    entries: &[EntryData],
    current: &HashMap<EntityKey, BTreeMap<String, f64>>,
    ip_is_valid: &dyn Fn(&str) -> bool,
) -> Alteration {
    let mut results: Vec<(EntityKey, Result<(), EntityError>)> = Vec::new();
    let mut records = Vec::new();
    for entry in entries {
        let entity = entry_entity(entry);
        let mut alterations: Vec<QuotaChange<'_>> = Vec::with_capacity(entry.ops.len());
        let mut duplicate_key = false;
        for op in &entry.ops {
            if alterations.iter().any(|change| change.key == op.key) {
                duplicate_key = true;
            } else {
                alterations.push(QuotaChange {
                    key: op.key.as_str(),
                    value: (!op.remove).then_some(op.value),
                });
            }
        }
        let slot = results.iter().position(|(seen, _)| *seen == entity);
        // Kafka records the "Duplicate quota key" error under the entity and
        // then finds the entity already in its result map, so both a
        // duplicate key and a repeated entity end as "Ignoring duplicate
        // entity" with no records for this entry.
        let outcome = if slot.is_some() || duplicate_key {
            Err(invalid(format!(
                "Ignoring duplicate entity {}",
                describe_entity(&entity)
            )))
        } else {
            alter_entity(
                &entity,
                &alterations,
                current.get(&entity),
                ip_is_valid,
                &mut records,
            )
        };
        if let Some(entry) = slot.and_then(|index| results.get_mut(index)) {
            entry.1 = outcome;
        } else {
            results.push((entity, outcome));
        }
    }
    Alteration { results, records }
}

/// Kafka's `alterClientQuotaEntity`: the entity checks, the key set they
/// select, then each alteration. A removal is not validated.
fn alter_entity(
    entity: &[(String, Option<String>)],
    alterations: &[QuotaChange<'_>],
    current: Option<&BTreeMap<String, f64>>,
    ip_is_valid: &dyn Fn(&str) -> bool,
    records: &mut Vec<MetadataRecord>,
) -> Result<(), EntityError> {
    validate_entity(entity)?;
    let keys = config_keys_for_entity(entity, ip_is_valid)?;
    let mut new_records = Vec::with_capacity(alterations.len());
    for &QuotaChange { key, value } in alterations {
        let current_value = current.and_then(|quotas| quotas.get(key)).copied();
        match value {
            None => {
                if current_value.is_some() {
                    new_records.push(record(entity, key, None));
                }
            }
            Some(value) => {
                validate_quota_key_value(keys, key, value)?;
                // Java's `Objects.equals` on two `Double`s compares bits.
                if current_value.map(f64::to_bits) != Some(value.to_bits()) {
                    new_records.push(record(entity, key, Some(value)));
                }
            }
        }
    }
    records.extend(new_records);
    Ok(())
}

fn record(entity: &[(String, Option<String>)], key: &str, value: Option<f64>) -> MetadataRecord {
    MetadataRecord::V1ClientQuota(ClientQuotaRecord {
        entity: entity
            .iter()
            .map(|(entity_type, entity_name)| QuotaEntity {
                entity_type: entity_type.clone(),
                entity_name: entity_name.clone(),
            })
            .collect(),
        config_key: key.to_owned(),
        config_value: value,
    })
}

/// Kafka's `validateEntity`: a known type and a non-empty name for each
/// component.
fn validate_entity(entity: &[(String, Option<String>)]) -> Result<(), EntityError> {
    if entity.is_empty() {
        return Err(invalid("Invalid empty client quota entity".to_owned()));
    }
    for (entity_type, entity_name) in entity {
        if ![ENTITY_TYPE_USER, ENTITY_TYPE_CLIENT_ID, ENTITY_TYPE_IP]
            .contains(&entity_type.as_str())
        {
            return Err(invalid(format!(
                "Unhandled client quota entity type: {entity_type}"
            )));
        }
        if entity_name.as_deref() == Some("") {
            return Err(invalid(format!("Empty {entity_type} not supported")));
        }
    }
    Ok(())
}

/// Kafka's `configKeysForEntityType`: the allowed combination of entity
/// types and the quota keys it accepts.
fn config_keys_for_entity(
    entity: &[(String, Option<String>)],
    ip_is_valid: &dyn Fn(&str) -> bool,
) -> Result<&'static [QuotaKey], EntityError> {
    let has = |wanted: &str| entity.iter().any(|(t, _)| t == wanted);
    let ip = entity.iter().find(|(t, _)| t == ENTITY_TYPE_IP);
    if let Some((_, ip_name)) = ip {
        if has(ENTITY_TYPE_USER) || has(ENTITY_TYPE_CLIENT_ID) {
            // Kafka's message has no space between "should" and "not".
            return Err(invalid(
                "Invalid quota entity combination, IP entity shouldnot be combined with User or \
                 ClientId"
                    .to_owned(),
            ));
        }
        match ip_name {
            Some(name) if !ip_is_valid(name) => Err(invalid(format!(
                "{name} is not a valid IP or resolvable host."
            ))),
            _ => Ok(IP_KEYS),
        }
    } else if has(ENTITY_TYPE_USER) || has(ENTITY_TYPE_CLIENT_ID) {
        Ok(USER_AND_CLIENT_KEYS)
    } else {
        Err(invalid("Invalid empty client quota entity".to_owned()))
    }
}

/// Kafka's `validateQuotaKeyValue`: a key of the entity's set, a positive
/// value, and for an integral key a whole value inside the type's range. A
/// `DOUBLE` key has no further check.
fn validate_quota_key_value(keys: &[QuotaKey], key: &str, value: f64) -> Result<(), EntityError> {
    let Some(&QuotaKey { value_type, .. }) = keys.iter().find(|known| known.name == key) else {
        return Err(invalid(format!("Invalid configuration key {key}")));
    };
    if value <= 0.0 {
        return Err(invalid(format!("Quota {key} must be greater than 0")));
    }
    match value_type {
        QuotaValueType::Double => Ok(()),
        QuotaValueType::Int if value > INT_MAX => Err(invalid(format!(
            "Proposed value for {key} is too large for an INT."
        ))),
        QuotaValueType::Long if value > LONG_MAX => Err(invalid(format!(
            "Proposed value for {key} is too large for a LONG."
        ))),
        QuotaValueType::Int | QuotaValueType::Long => {
            if (value % 1.0).abs() > 1.0e-6 {
                Err(invalid(format!("{key} cannot be a fractional value.")))
            } else {
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::handlers::alter_client_quotas::test_support::{EntityPart, entry, part},
        assert2::check,
        std::net::IpAddr,
    };

    #[derive(Default)]
    struct ClientQuotaEntryScenario {
        entries: Vec<EntryData>,
        current: HashMap<EntityKey, BTreeMap<String, f64>>,
        outcome: Option<Alteration>,
    }
    #[rstest::fixture]
    fn client_quota_entries() -> ClientQuotaEntryScenario {
        let entries = Vec::new();
        ClientQuotaEntryScenario {
            entries,
            ..Default::default()
        }
    }
    fn quota_entity_parts(text: &str) -> Vec<EntityPart<'_>> {
        if text == "none" {
            return Vec::new();
        }
        text.split('+')
            .map(|field| {
                let (kind, name) = field.split_once('=').unwrap();
                part(kind, match name {
                    "null" => None,
                    "empty" => Some(""),
                    _ => Some(name),
                })
            })
            .collect()
    }
    fn quota_entity_key(text: &str) -> EntityKey {
        krabka_metadata::canonicalize_entity(
            quota_entity_parts(text)
                .into_iter()
                .map(|part| {
                    (
                        part.entity_type.to_owned(),
                        part.entity_name.map(str::to_owned),
                    )
                })
                .collect(),
        )
    }
    #[rstest_bdd_macros::given("user carol currently has producer byte rate 1024")]
    fn existing_carol_quota(client_quota_entries: &mut ClientQuotaEntryScenario) {
        client_quota_entries.current = HashMap::from([(
            quota_entity_key("user=carol"),
            BTreeMap::from([("producer_byte_rate".into(), 1024.0)]),
        )]);
    }
    #[rstest_bdd_macros::given(
        "quota entries name entities {entities} with operations {operations}"
    )]
    fn requested_quota_entries(
        client_quota_entries: &mut ClientQuotaEntryScenario,
        entities: String,
        operations: String,
    ) {
        let entities: Vec<_> = entities.split(';').collect();
        let operations: Vec<_> = operations.split(';').collect();
        assert2::assert!(entities.len() == operations.len());
        use krabka_protocol::owned::alter_client_quotas_request::OpData;
        client_quota_entries.entries = entities
            .into_iter()
            .zip(operations)
            .map(|(entity, operations)| {
                let mut request = entry(quota_entity_parts(entity), Vec::new());
                request.ops = operations
                    .split(',')
                    .map(|operation| {
                        let (action, assignment) = operation.split_once(':').unwrap();
                        let (key, value) = assignment.split_once('=').unwrap();
                        let remove = match action {
                            "set" => false,
                            "remove" => true,
                            _ => panic!("unknown quota action: {action}"),
                        };
                        OpData {
                            key: key.into(),
                            value: value.parse::<f64>().unwrap(),
                            remove,
                            ..Default::default()
                        }
                    })
                    .collect();
                request
            })
            .collect();
    }
    #[rstest_bdd_macros::when(
        "the quota controller validates entries with IP literals and localhost resolvable"
    )]
    fn alter_requested_quotas(client_quota_entries: &mut ClientQuotaEntryScenario) {
        client_quota_entries.outcome = Some(alter_client_quotas(
            &client_quota_entries.entries,
            &client_quota_entries.current,
            &|name| name.parse::<IpAddr>().is_ok() || name == "localhost",
        ));
    }
    #[rstest_bdd_macros::then(
        "the quota results name entities {entities} with outcome {outcome} and records {records}"
    )]
    fn check_client_quota_entries(
        client_quota_entries: &ClientQuotaEntryScenario,
        entities: String,
        outcome: String,
        records: String,
    ) {
        let result = if outcome == "accepted" {
            Ok(())
        } else {
            Err((INVALID_REQUEST, outcome))
        };
        let results = entities
            .split(';')
            .map(|entity| (quota_entity_key(entity), result.clone()))
            .collect();
        let records = if records == "none" {
            Vec::new()
        } else {
            records
                .split(';')
                .map(|spec| {
                    let (entity, assignment) = spec.split_once('/').unwrap();
                    let (key, value) = assignment.split_once('=').unwrap();
                    record(
                        &quota_entity_key(entity),
                        key,
                        if value == "null" {
                            None
                        } else {
                            Some(value.parse::<f64>().unwrap())
                        },
                    )
                })
                .collect()
        };
        check!(client_quota_entries.outcome.as_ref().unwrap() == &Alteration { results, records });
    }
    rstest_bdd_macros::scenarios!("tests/features/handler_client_quota_entries",fixtures=[client_quota_entries:ClientQuotaEntryScenario]);
}
