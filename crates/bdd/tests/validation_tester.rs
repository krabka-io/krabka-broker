#![cfg(test)]
//! Validation Tester: start from a valid topic request, override the fields
//! a rule is about with `#[derive(Overrides)]`, then validate.
//!
//! The valid request comes from the `defaults` module below, which follows
//! the defaults convention: a bon builder Default Instance with valid domain
//! defaults, and named Object Mothers on top of it. A step overrides only
//! what its scenario's rule needs.

use {
    assert2::assert,
    krabka_bdd::{
        Given, Overrides, TableError,
        overrides::{self, apply_table},
    },
    rstest::rstest,
    std::fmt,
};

// A `CreateTopics` request, as a validator sees it.
#[derive(Debug, Clone, PartialEq, Eq, Overrides, bon::Builder)]
struct TopicRequest {
    #[builder(into, default = "orders")]
    name: String,
    #[builder(default = 3)]
    partitions: u32,
    #[builder(default = 3)]
    replication_factor: u16,
    #[column("min insync replicas")]
    min_isr: Option<u16>,
}

// Domain defaults for topic requests.
mod defaults {
    use super::{TopicRequest, TopicRequestBuilder};

    // The Default Instance: a request every validator accepts.
    pub fn a_topic_request() -> TopicRequestBuilder { TopicRequest::builder() }

    // An Object Mother: a valid request that tolerates one lost replica.
    pub fn a_durable_topic_request() -> TopicRequest {
        a_topic_request().replication_factor(3).min_isr(2).build()
    }
}

const FIELDS: &[&str] = &[
    "name",
    "partitions",
    "replication factor",
    "min insync replicas",
];

#[derive(Debug, Clone, PartialEq, Eq)]
enum Invalid {
    NameCharacter { name: String, character: char },
    NoPartitions,
    NoReplicas,
    NoInsyncReplicas,
    InsyncAboveReplicas { min_isr: u16, replicas: u16 },
}

impl fmt::Display for Invalid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NameCharacter { name, character } => {
                write!(f, "topic name {name} contains {character}")
            }
            Self::NoPartitions => f.write_str("a topic needs at least 1 partition"),
            Self::NoReplicas => f.write_str("a topic needs a replication factor of at least 1"),
            Self::NoInsyncReplicas => f.write_str("min insync replicas must be at least 1"),
            Self::InsyncAboveReplicas { min_isr, replicas } => write!(
                f,
                "min insync replicas {min_isr} exceeds replication factor {replicas}"
            ),
        }
    }
}

fn validate(request: &TopicRequest) -> Result<(), Invalid> {
    if let Some(character) = request
        .name
        .chars()
        .find(|character| !(character.is_ascii_alphanumeric() || "._-".contains(*character)))
    {
        return Err(Invalid::NameCharacter {
            name: request.name.clone(),
            character,
        });
    }
    if request.partitions == 0 {
        return Err(Invalid::NoPartitions);
    }
    if request.replication_factor == 0 {
        return Err(Invalid::NoReplicas);
    }
    match request.min_isr {
        Some(0) => Err(Invalid::NoInsyncReplicas),
        Some(min_isr) if min_isr > request.replication_factor => {
            Err(Invalid::InsyncAboveReplicas {
                min_isr,
                replicas: request.replication_factor,
            })
        }
        _ => Ok(()),
    }
}

fn table(text: &str) -> Vec<Vec<String>> {
    text.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| {
            let inner = line.strip_prefix('|').unwrap().strip_suffix('|').unwrap();
            inner
                .split('|')
                .map(|cell| cell.trim().to_owned())
                .collect()
        })
        .collect()
}

#[test]
fn the_default_instance_and_the_mother_are_valid() {
    assert!(validate(&defaults::a_topic_request().build()) == Ok(()));
    assert!(validate(&defaults::a_durable_topic_request()) == Ok(()));
}

#[rstest]
#[case::text("name", "payments", TopicRequest { name: "payments".to_owned(), ..defaults::a_durable_topic_request() })]
#[case::empty_text("name", "", TopicRequest { name: String::new(), ..defaults::a_durable_topic_request() })]
#[case::empty_marker_on_a_required_text("name", "-", TopicRequest { name: "-".to_owned(), ..defaults::a_durable_topic_request() })]
#[case::number("partitions", "0", TopicRequest { partitions: 0, ..defaults::a_durable_topic_request() })]
#[case::field_name_with_spaces("replication factor", "1", TopicRequest { replication_factor: 1, ..defaults::a_durable_topic_request() })]
#[case::renamed_optional("min insync replicas", "3", TopicRequest { min_isr: Some(3), ..defaults::a_durable_topic_request() })]
#[case::empty_marker_clears_an_option("min insync replicas", "-", TopicRequest { min_isr: None, ..defaults::a_durable_topic_request() })]
fn apply_sets_one_field_from_its_step_text(
    #[case] field: &str,
    #[case] value: &str,
    #[case] expected: TopicRequest,
) {
    let mut request = defaults::a_durable_topic_request();
    request.apply(field, value).unwrap();
    assert!(request == expected);
}

#[rstest]
#[case::unknown_field(
    "partiton",
    "1",
    TableError::UnknownField { field: "partiton".to_owned(), known: FIELDS },
)]
#[case::column_name_differs_from_the_rust_name(
    "min isr",
    "1",
    TableError::UnknownField { field: "min isr".to_owned(), known: FIELDS },
)]
#[case::unparsable(
    "partitions",
    "many",
    TableError::InvalidField {
        field: "partitions",
        value: "many".to_owned(),
        reason: "invalid digit found in string".to_owned(),
    },
)]
#[case::out_of_range(
    "replication factor",
    "70000",
    TableError::InvalidField {
        field: "replication factor",
        value: "70000".to_owned(),
        reason: "number too large to fit in target type".to_owned(),
    },
)]
#[case::empty_on_an_option(
    "min insync replicas",
    "",
    TableError::InvalidField {
        field: "min insync replicas",
        value: String::new(),
        reason: "cannot parse integer from empty string".to_owned(),
    },
)]
fn apply_refuses_an_override_that_does_not_fit(
    #[case] field: &str,
    #[case] value: &str,
    #[case] expected: TableError,
) {
    let mut request = defaults::a_durable_topic_request();
    assert!(request.apply(field, value) == Err(expected));
    assert!(request == defaults::a_durable_topic_request());
}

#[test]
fn fields_list_every_name_in_order() {
    assert!(TopicRequest::FIELDS == FIELDS);
}

#[rstest]
#[case::vertical(
    "| field              | value |
     | partitions         | 6     |
     | replication factor | 1     |
     | min insync replicas| -     |"
)]
#[case::horizontal(
    "| partitions | replication factor | min insync replicas |
     | 6          | 1                  | -                   |"
)]
fn apply_table_sets_several_fields(#[case] datatable: &str) {
    let mut request = defaults::a_durable_topic_request();
    apply_table(&mut request, &table(datatable)).unwrap();
    let expected = TopicRequest {
        partitions: 6,
        replication_factor: 1,
        min_isr: None,
        ..defaults::a_durable_topic_request()
    };
    assert!(request == expected);
}

#[rstest]
#[case::empty("", TableError::NoHeader)]
#[case::header_without_values("| partitions |", TableError::NotOneValueRow { rows: 0 })]
#[case::two_rows_of_values(
    "| partitions |
     | 1          |
     | 2          |",
    TableError::NotOneValueRow { rows: 2 },
)]
#[case::ragged_horizontal(
    "| partitions | name |
     | 1          |",
    TableError::RaggedRow { row: Some(2), cells: 1, columns: 2 },
)]
#[case::not_a_pair(
    "| field      | value |
     | partitions | 1     |
     | name       |",
    TableError::NotAPair { row: 3, cells: 1 },
)]
#[case::vertical_duplicate(
    "| field      | value |
     | partitions | 1     |
     | partitions | 2     |",
    TableError::DuplicateField { field: "partitions".to_owned() },
)]
#[case::horizontal_duplicate(
    "| name | name |
     | a    | b    |",
    TableError::DuplicateField { field: "name".to_owned() },
)]
#[case::unknown_field(
    "| field     | value |
     | partitons | 1     |",
    TableError::UnknownField { field: "partitons".to_owned(), known: FIELDS },
)]
fn apply_table_refuses_a_table_of_neither_shape(
    #[case] datatable: &str,
    #[case] expected: TableError,
) {
    let mut request = defaults::a_durable_topic_request();
    assert!(apply_table(&mut request, &table(datatable)) == Err(expected));
}

#[rstest]
#[case::unknown_field_near_miss(
    TableError::UnknownField { field: "partiton".to_owned(), known: FIELDS },
    "unknown field `partiton`; did you mean `partitions`? known fields: `name`, `partitions`, \
     `replication factor`, `min insync replicas`",
)]
#[case::unknown_field_far_off(
    TableError::UnknownField { field: "retention".to_owned(), known: FIELDS },
    "unknown field `retention`; known fields: `name`, `partitions`, `replication factor`, \
     `min insync replicas`",
)]
#[case::invalid_field(
    TableError::InvalidField {
        field: "partitions",
        value: "many".to_owned(),
        reason: "invalid digit found in string".to_owned(),
    },
    "field `partitions`: cannot parse `many`: invalid digit found in string",
)]
#[case::duplicate_field(
    TableError::DuplicateField { field: "partitions".to_owned() },
    "field `partitions` is set more than once",
)]
#[case::not_one_value_row(
    TableError::NotOneValueRow { rows: 2 },
    "a horizontal overrides table has a header and one row of values, but this one has 2; set \
     one field per row under a `| field | value |` header instead",
)]
fn override_error_message_names_the_field_and_value(
    #[case] error: TableError,
    #[case] expected: &str,
) {
    assert!(error.to_string() == expected);
}

#[test]
fn the_helpers_are_reachable_through_the_module() {
    let mut request = defaults::a_topic_request().build();
    let datatable = table("| field | value |\n| name | audit |");
    overrides::apply_table(&mut request, &datatable).unwrap();
    assert!(request.name == "audit");
}

struct Validation {
    request: Given<TopicRequest>,
    outcome: Given<Result<(), Invalid>>,
}

#[rstest::fixture]
fn validation() -> Validation {
    Validation {
        request: Given::new("request", "Given a valid topic request changed to"),
        outcome: Given::new("outcome", "When the request is validated"),
    }
}

#[krabka_bdd::given("a valid topic request with its {field} set to {value}")]
fn a_request_with_one_override(validation: &mut Validation, field: String, value: String) {
    let mut request = defaults::a_durable_topic_request();
    request.apply(&field, &value).unwrap();
    validation.request.set(request);
}

#[krabka_bdd::given("a valid topic request changed to")]
fn a_request_with_overrides(validation: &mut Validation, datatable: Vec<Vec<String>>) {
    let mut request = defaults::a_topic_request().build();
    apply_table(&mut request, &datatable).unwrap();
    validation.request.set(request);
}

#[krabka_bdd::when("the request is validated")]
fn the_request_is_validated(validation: &mut Validation) {
    let outcome = validate(validation.request.get());
    validation.outcome.set(outcome);
}

#[krabka_bdd::then("the request is refused because {reason}")]
fn the_request_is_refused(validation: &Validation, reason: String) {
    let outcome = validation.outcome.get();
    assert!(let Err(error) = outcome);
    assert!(error.to_string() == reason);
}

#[krabka_bdd::then("the request is accepted")]
fn the_request_is_accepted(validation: &Validation) {
    assert!(*validation.outcome.get() == Ok(()));
}

krabka_bdd::scenarios!(
    "tests/features/validation_tester",
    fixtures = [validation: Validation],
);
