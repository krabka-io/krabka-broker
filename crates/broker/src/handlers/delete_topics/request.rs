//! Resolving a `DeleteTopics` request into the list of topics the handler
//! will act on, across the two request shapes the protocol has carried.
//!
//! v0-5 sends `topic_names` and knows nothing about topic ids. v6+ sends
//! `topics`, where KIP-516 lets a client identify a topic by name or by UUID.
//! Whether a row was requested by id decides which error code a miss reports,
//! so that flag travels alongside the resolved name.
//!
//! Kafka's `ControllerApis.deleteTopics` rejects some rows with
//! `INVALID_REQUEST` before it authorizes or deletes anything: a row with no
//! name and the zero id, a row with a name and a non-zero id, a name that
//! more than one row carries, and an id that more than one row carries. After
//! authorization it also rejects a name whose topic id another row carries.
//! This module applies the same rules.

use {
    super::{authz::TopicAccess, wire::invalid_topic_result},
    krabka_protocol::{
        owned::{
            delete_topics_request::DeleteTopicsRequest,
            delete_topics_response::DeletableTopicResult,
        },
        primitives::uuid::Uuid as WireUuid,
    },
    std::{
        collections::{HashMap, HashSet},
        mem,
    },
};

/// One requested topic: the name resolved from the metadata image (`None` when
/// the image does not know it), whether the client identified the topic by id,
/// and the topic id the client sent.
pub(super) type TopicNameRequest = (Option<String>, bool, WireUuid);

/// The message of a v6 row with no name and the zero id.
pub(super) const NO_NAME_OR_ID: &str = "Neither topic name nor id were specified.";

/// The message of a v6 row with a name and a non-zero id.
pub(super) const NAME_AND_ID: &str = "You may not specify both topic name and topic id.";

/// The message of a name that more than one row carries.
pub(super) const DUPLICATE_NAME: &str = "Duplicate topic name.";

/// The message of an id that more than one row carries.
pub(super) const DUPLICATE_ID: &str = "Duplicate topic id.";

/// The message of a name whose topic id another row carries.
pub(super) const NAME_OF_SUPPLIED_ID: &str =
    "The provided topic name maps to an ID that was already supplied.";

/// The rows of one request after validation.
#[derive(Debug, PartialEq)]
pub(super) struct ValidatedTopics {
    /// The rows to authorize and delete, in request order.
    pub(super) topics: Vec<TopicNameRequest>,
    /// The `INVALID_REQUEST` rows of the response.
    pub(super) invalid: Vec<DeletableTopicResult>,
    /// The ids that more than one row carries.
    duplicate_ids: HashSet<WireUuid>,
}

/// One reference that a request row makes.
enum Reference<'a> {
    Name(&'a str),
    Id(WireUuid),
}

/// Validates every topic row of the request, and collects
/// `(resolved_name, requested_by_id, requested_topic_id)` for each valid row.
///
/// When the client sent only a topic id, the name is resolved from the current
/// image and the entry is marked id-based so that a miss returns
/// `UNKNOWN_TOPIC_ID` (KIP-516) rather than `UNKNOWN_TOPIC_OR_PARTITION`.
///
/// Kafka keys a row by its name only when the name is not null. An empty name
/// is a name.
pub(super) fn resolve_topic_names(
    request: &DeleteTopicsRequest,
    image: &krabka_metadata::MetadataImage,
) -> ValidatedTopics {
    let mut invalid = Vec::new();
    let mut references: Vec<Reference<'_>> = request
        .topic_names
        .iter()
        .map(|name| Reference::Name(name))
        .collect();
    for state in &request.topics {
        match (state.name.as_deref(), state.topic_id == WireUuid::ZERO) {
            (None, true) => {
                invalid.push(invalid_topic_result(None, WireUuid::ZERO, NO_NAME_OR_ID));
            }
            (None, false) => references.push(Reference::Id(state.topic_id)),
            (Some(name), true) => references.push(Reference::Name(name)),
            (Some(name), false) => {
                invalid.push(invalid_topic_result(
                    Some(name.to_owned()),
                    state.topic_id,
                    NAME_AND_ID,
                ));
            }
        }
    }

    let mut name_rows: HashMap<&str, usize> = HashMap::new();
    let mut id_rows: HashMap<WireUuid, usize> = HashMap::new();
    for reference in &references {
        match reference {
            Reference::Name(name) => *name_rows.entry(name).or_default() += 1,
            Reference::Id(id) => *id_rows.entry(*id).or_default() += 1,
        }
    }

    // Each duplicate name or id answers once, in the order of its first row.
    // The sets make that check O(1) per row, so a request full of duplicates
    // costs O(n) before authorization.
    let mut topics = Vec::with_capacity(references.len());
    let mut duplicate_names: Vec<&str> = Vec::new();
    let mut seen_duplicate_names: HashSet<&str> = HashSet::new();
    let mut duplicate_ids: Vec<WireUuid> = Vec::new();
    let mut seen_duplicate_ids: HashSet<WireUuid> = HashSet::new();
    for reference in references {
        match reference {
            Reference::Name(name) if name_rows.get(name).is_some_and(|&rows| rows > 1) => {
                if seen_duplicate_names.insert(name) {
                    duplicate_names.push(name);
                }
            }
            Reference::Name(name) => topics.push((Some(name.to_owned()), false, WireUuid::ZERO)),
            Reference::Id(id) if id_rows.get(&id).is_some_and(|&rows| rows > 1) => {
                if seen_duplicate_ids.insert(id) {
                    duplicate_ids.push(id);
                }
            }
            Reference::Id(id) => {
                let name = image
                    .topic_by_id(&uuid::Uuid::from_bytes(id.0))
                    .map(|topic| topic.name.clone());
                topics.push((name, true, id));
            }
        }
    }
    invalid.extend(duplicate_names.iter().map(|name| {
        invalid_topic_result(Some((*name).to_owned()), WireUuid::ZERO, DUPLICATE_NAME)
    }));
    invalid.extend(
        duplicate_ids
            .iter()
            .map(|id| invalid_topic_result(None, *id, DUPLICATE_ID)),
    );

    ValidatedTopics {
        topics,
        invalid,
        duplicate_ids: seen_duplicate_ids,
    }
}

impl ValidatedTopics {
    /// Rejects a name row whose topic id another row carries, when the
    /// principal may describe and delete that topic.
    ///
    /// The name row answers `INVALID_REQUEST` with the name and the id. A
    /// valid id row for the same topic leaves the request and gets no response
    /// row, as in Kafka's `ControllerApis.deleteTopics`. A denied topic keeps
    /// both rows, so the response does not tell the caller which id a name has.
    pub(super) fn reject_names_of_supplied_ids(
        &mut self,
        image: &krabka_metadata::MetadataImage,
        access: &TopicAccess,
    ) {
        let supplied_ids: HashSet<WireUuid> = self
            .topics
            .iter()
            .filter(|(name, by_id, _)| *by_id && name.is_some())
            .map(|(_, _, id)| *id)
            .collect();
        let mut dropped_ids = HashSet::new();
        let mut kept = Vec::with_capacity(self.topics.len());
        for row in mem::take(&mut self.topics) {
            let (Some(name), false, _) = &row else {
                kept.push(row);
                continue;
            };
            let topic_id = image
                .topic(name)
                .map(|topic| WireUuid(topic.topic_id.into_bytes()));
            match topic_id {
                Some(id)
                    if access.may_describe(name)
                        && access.may_delete(name)
                        && (supplied_ids.contains(&id) || self.duplicate_ids.contains(&id)) =>
                {
                    dropped_ids.insert(id);
                    self.invalid.push(invalid_topic_result(
                        Some(name.clone()),
                        id,
                        NAME_OF_SUPPLIED_ID,
                    ));
                }
                _ => kept.push(row),
            }
        }
        kept.retain(|(_, by_id, id)| !(*by_id && dropped_ids.contains(id)));
        self.topics = kept;
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::assert,
        krabka_metadata::{MetadataImage, MetadataRecord, TopicRecord},
        krabka_protocol::owned::delete_topics_request::DeleteTopicState,
    };

    const ORDERS_ID: WireUuid = WireUuid([7; 16]);
    const OTHER_ID: WireUuid = WireUuid([9; 16]);

    fn image() -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        image.apply(&MetadataRecord::V1Topic(TopicRecord {
            name: "orders".into(),
            topic_id: uuid::Uuid::from_bytes(ORDERS_ID.0),
            partitions: 1,
            replication_factor: 1,
        }));
        image
    }

    fn state(name: Option<&str>, topic_id: WireUuid) -> DeleteTopicState {
        DeleteTopicState {
            name: name.map(str::to_owned),
            topic_id,
            ..Default::default()
        }
    }

    fn invalid(name: Option<&str>, topic_id: WireUuid, message: &str) -> DeletableTopicResult {
        invalid_topic_result(name.map(str::to_owned), topic_id, message)
    }

    fn v6(rows: Vec<DeleteTopicState>) -> DeleteTopicsRequest {
        DeleteTopicsRequest {
            topics: rows,
            ..Default::default()
        }
    }

    #[derive(Default)]
    struct DeleteTopicValidationScenario {
        request: Option<DeleteTopicsRequest>,
        image: Option<MetadataImage>,
        denied: HashSet<String>,
        validated: Option<ValidatedTopics>,
    }
    #[rstest::fixture]
    fn delete_topic_validation() -> DeleteTopicValidationScenario {
        let request = None;
        DeleteTopicValidationScenario {
            request,
            ..Default::default()
        }
    }
    fn requested_delete_uuid(text: &str) -> WireUuid {
        match text {
            "zero" => WireUuid::ZERO,
            "orders" => ORDERS_ID,
            "other" => OTHER_ID,
            _ => panic!("unknown delete topic UUID: {text}"),
        }
    }
    fn requested_delete_name(text: &str) -> Option<&str> {
        match text {
            "null" => None,
            "empty" => Some(""),
            _ => Some(text),
        }
    }
    #[rstest_bdd_macros::given("DeleteTopics uses {format} rows {rows} and denies names {denied}")]
    fn delete_topic_validation_input(
        delete_topic_validation: &mut DeleteTopicValidationScenario,
        format: String,
        rows: String,
        denied: String,
    ) {
        delete_topic_validation.image = Some(image());
        delete_topic_validation.denied = if denied == "none" {
            HashSet::new()
        } else {
            denied.split(',').map(str::to_owned).collect()
        };
        delete_topic_validation.request = Some(match format.as_str() {
            "legacy" => DeleteTopicsRequest {
                topic_names: rows.split(',').map(str::to_owned).collect(),
                ..Default::default()
            },
            "UUID" => v6(rows
                .split(';')
                .map(|row| {
                    let (name, id) = row.split_once('/').unwrap();
                    state(requested_delete_name(name), requested_delete_uuid(id))
                })
                .collect()),
            _ => panic!("unknown deletion request format: {format}"),
        });
    }
    #[rstest_bdd_macros::when(
        "deletion resolves topic identities and rejects names also supplied by authorized UUID"
    )]
    fn validate_delete_topic_rows(delete_topic_validation: &mut DeleteTopicValidationScenario) {
        let image = delete_topic_validation.image.as_ref().unwrap();
        let mut validated =
            resolve_topic_names(delete_topic_validation.request.as_ref().unwrap(), image);
        let allowed: HashSet<String> = ["orders", "other", "a", "b", "c"]
            .into_iter()
            .filter(|name| !delete_topic_validation.denied.contains(*name))
            .map(str::to_owned)
            .collect();
        let access = TopicAccess::Topics {
            describable: allowed.clone(),
            deletable: allowed,
        };
        validated.reject_names_of_supplied_ids(image, &access);
        delete_topic_validation.validated = Some(validated);
    }
    #[rstest_bdd_macros::then("the ordered deletable topic identities are {topics}")]
    fn check_valid_delete_topic_rows(
        delete_topic_validation: &DeleteTopicValidationScenario,
        topics: String,
    ) {
        let expected: Vec<TopicNameRequest> = if topics == "none" {
            Vec::new()
        } else {
            topics
                .split(';')
                .map(|row| {
                    let columns: Vec<_> = row.split('/').collect();
                    let [name, mode, id] = columns.as_slice() else {
                        panic!("name lookup mode UUID required")
                    };
                    (
                        requested_delete_name(name).map(str::to_owned),
                        match *mode {
                            "id" => true,
                            "name" => false,
                            _ => panic!("unknown topic lookup mode"),
                        },
                        requested_delete_uuid(id),
                    )
                })
                .collect()
        };
        assert!(delete_topic_validation.validated.as_ref().unwrap().topics == expected);
    }
    #[rstest_bdd_macros::then("the ordered invalid deletion rows are {rows}")]
    fn check_invalid_delete_topic_rows(
        delete_topic_validation: &DeleteTopicValidationScenario,
        rows: String,
    ) {
        let expected: Vec<_> = if rows == "none" {
            Vec::new()
        } else {
            rows.split(';')
                .map(|row| {
                    let columns: Vec<_> = row.split('/').collect();
                    let [name, id, reason] = columns.as_slice() else {
                        panic!("name UUID refusal required")
                    };
                    let message = match *reason {
                        "no-name-or-id" => NO_NAME_OR_ID,
                        "name-and-id" => NAME_AND_ID,
                        "duplicate-name" => DUPLICATE_NAME,
                        "duplicate-id" => DUPLICATE_ID,
                        "name-of-id" => NAME_OF_SUPPLIED_ID,
                        _ => panic!("unknown delete topic refusal"),
                    };
                    invalid(
                        requested_delete_name(name),
                        requested_delete_uuid(id),
                        message,
                    )
                })
                .collect()
        };
        assert!(delete_topic_validation.validated.as_ref().unwrap().invalid == expected);
    }
    rstest_bdd_macros::scenarios!("tests/features/handler_delete_topic_validation",fixtures=[delete_topic_validation:DeleteTopicValidationScenario]);

    /// A request of many distinct names and ids, each sent twice, answers one
    /// `INVALID_REQUEST` row per name and per id, in first-row order, and
    /// keeps no row to delete.
    #[test]
    fn many_duplicates_answer_once_each_in_first_row_order() {
        const DISTINCT: u16 = 5_000;
        let names: Vec<String> = (0..DISTINCT).map(|i| format!("topic-{i}")).collect();
        let ids: Vec<WireUuid> = (0..DISTINCT)
            .map(|i| {
                let mut bytes = [0xee; 16];
                bytes[..2].copy_from_slice(&i.to_be_bytes());
                WireUuid(bytes)
            })
            .collect();
        let rows: Vec<DeleteTopicState> = (0..2)
            .flat_map(|_| {
                names
                    .iter()
                    .zip(&ids)
                    .flat_map(|(name, id)| [state(Some(name), WireUuid::ZERO), state(None, *id)])
            })
            .collect();

        let validated = resolve_topic_names(&v6(rows), &image());

        let expected: Vec<DeletableTopicResult> = names
            .iter()
            .map(|name| invalid(Some(name), WireUuid::ZERO, DUPLICATE_NAME))
            .chain(ids.iter().map(|id| invalid(None, *id, DUPLICATE_ID)))
            .collect();
        assert!((validated.topics, validated.invalid) == (Vec::new(), expected));
    }
}
