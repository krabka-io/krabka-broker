#![cfg(test)]
//! Data Table Accessor: `#[derive(TableRow)]` reads horizontal and vertical
//! tables into typed rows, and refuses a table that does not fit with a
//! message naming the row, the column and the value.

mod datatable;

use {
    assert2::assert,
    datatable::table,
    krabka_bdd::{
        Attempt, Given, TableError, TableRow,
        table::{self, TableCells},
    },
    rstest::rstest,
};

#[derive(Debug, Clone, PartialEq, Eq, TableRow)]
struct Partition {
    topic: String,
    #[column("partition")]
    number: u32,
    leader_epoch: Option<i32>,
}

#[derive(Debug, Clone, PartialEq, Eq, TableRow)]
struct Topic {
    name: String,
    partitions: u32,
    #[column("replication factor")]
    replicas: u16,
}

const PARTITION_COLUMNS: &[&str] = &["topic", "partition", "leader epoch"];
const TOPIC_COLUMNS: &[&str] = &["name", "partitions", "replication factor"];

fn partition(topic: &str, number: u32, leader_epoch: Option<i32>) -> Partition {
    Partition {
        topic: topic.to_owned(),
        number,
        leader_epoch,
    }
}

#[rstest]
#[case::every_column(
    "| topic | partition | leader epoch |\n| orders | 0 | 5 |\n| orders | 1 | 7 |",
    vec![partition("orders", 0, Some(5)), partition("orders", 1, Some(7))],
)]
#[case::columns_in_another_order(
    "| leader epoch | partition | topic |\n| 5 | 0 | orders |",
    vec![partition("orders", 0, Some(5))],
)]
#[case::empty_marker_is_none(
    "| topic | partition | leader epoch |\n| orders | 0 | - |",
    vec![partition("orders", 0, None)],
)]
#[case::empty_cell_is_none(
    "| topic | partition | leader epoch |\n| orders | 0 |  |",
    vec![partition("orders", 0, None)],
)]
#[case::optional_column_left_out(
    "| topic | partition |\n| orders | 0 |",
    vec![partition("orders", 0, None)],
)]
#[case::header_only("| topic | partition |", vec![])]
fn rows_reads_a_horizontal_table(#[case] datatable: &str, #[case] expected: Vec<Partition>) {
    assert!(table::rows::<Partition>(&table(datatable)) == Ok(expected));
}

#[rstest]
#[case::empty_table("", TableError::NoHeader)]
#[case::unknown_column_near_miss(
    "| topic | partiton |\n| orders | 0 |",
    TableError::UnknownColumn { column: "partiton".to_owned(), known: PARTITION_COLUMNS },
)]
#[case::unknown_column_far_off(
    "| topic | partition | replicas |\n| orders | 0 | 3 |",
    TableError::UnknownColumn { column: "replicas".to_owned(), known: PARTITION_COLUMNS },
)]
#[case::unknown_column_with_no_rows(
    "| topic | partiton |",
    TableError::UnknownColumn { column: "partiton".to_owned(), known: PARTITION_COLUMNS },
)]
#[case::duplicate_column(
    "| topic | topic | partition |\n| orders | orders | 0 |",
    TableError::DuplicateColumn { column: "topic".to_owned() },
)]
#[case::duplicate_column_with_no_rows(
    "| topic | topic | partition |",
    TableError::DuplicateColumn { column: "topic".to_owned() },
)]
#[case::missing_required_column(
    "| topic |\n| orders |",
    TableError::MissingColumn { column: "partition" },
)]
#[case::empty_required_cell(
    "| topic | partition |\n| orders | 0 |\n|  | 1 |",
    TableError::EmptyCell { row: Some(3), column: "topic", value: String::new() },
)]
#[case::empty_marker_in_required_cell(
    "| topic | partition |\n| orders | - |",
    TableError::EmptyCell { row: Some(2), column: "partition", value: "-".to_owned() },
)]
#[case::unparsable_cell(
    "| topic | partition |\n| orders | 0 |\n| orders | three |",
    TableError::InvalidCell {
        row: Some(3),
        column: "partition",
        value: "three".to_owned(),
        reason: "invalid digit found in string".to_owned(),
    },
)]
#[case::unparsable_optional_cell(
    "| topic | partition | leader epoch |\n| orders | 0 | new |",
    TableError::InvalidCell {
        row: Some(2),
        column: "leader epoch",
        value: "new".to_owned(),
        reason: "invalid digit found in string".to_owned(),
    },
)]
#[case::ragged_row(
    "| topic | partition |\n| orders |",
    TableError::RaggedRow { row: Some(2), cells: 1, columns: 2 },
)]
fn rows_refuses_a_table_that_does_not_fit(#[case] datatable: &str, #[case] expected: TableError) {
    assert!(table::rows::<Partition>(&table(datatable)) == Err(expected));
}

#[rstest]
#[case::without_header("| name | orders |\n| partitions | 3 |\n| replication factor | 2 |")]
#[case::with_field_value_header(
    "| field | value |\n| name | orders |\n| partitions | 3 |\n| replication factor | 2 |"
)]
#[case::fields_in_another_order(
    "| replication factor | 2 |\n| partitions | 3 |\n| name | orders |"
)]
fn vertical_reads_one_value_from_field_value_rows(#[case] datatable: &str) {
    let expected = Topic {
        name: "orders".to_owned(),
        partitions: 3,
        replicas: 2,
    };
    assert!(table::vertical::<Topic>(&table(datatable)) == Ok(expected));
}

#[rstest]
#[case::empty_table("", TableError::NoHeader)]
#[case::not_a_pair(
    "| field | value |\n| name | orders |\n| partitions | 3 | 4 |",
    TableError::NotAPair { row: 3, cells: 3 },
)]
#[case::unparsable_value_names_its_row(
    "| name | orders |\n| partitions | three |\n| replication factor | 2 |",
    TableError::InvalidCell {
        row: Some(2),
        column: "partitions",
        value: "three".to_owned(),
        reason: "invalid digit found in string".to_owned(),
    },
)]
#[case::row_counts_the_header(
    "| field | value |\n| name | orders |\n| partitions | 3 |\n| replication factor | - |",
    TableError::EmptyCell { row: Some(4), column: "replication factor", value: "-".to_owned() },
)]
#[case::unknown_field(
    "| name | orders |\n| partitons | 3 |",
    TableError::UnknownColumn { column: "partitons".to_owned(), known: TOPIC_COLUMNS },
)]
#[case::missing_field(
    "| name | orders |\n| partitions | 3 |",
    TableError::MissingColumn { column: "replication factor" },
)]
fn vertical_refuses_a_table_that_does_not_fit(
    #[case] datatable: &str,
    #[case] expected: TableError,
) {
    assert!(table::vertical::<Topic>(&table(datatable)) == Err(expected));
}

#[test]
fn from_row_reads_one_row_without_a_row_number() {
    let headers = table("| topic | partition |").remove(0);
    let cells = table("| orders | x |").remove(0);
    assert!(
        Partition::from_row(&headers, &cells)
            == Err(TableError::InvalidCell {
                row: None,
                column: "partition",
                value: "x".to_owned(),
                reason: "invalid digit found in string".to_owned(),
            })
    );
}

#[test]
fn columns_list_every_field_in_order() {
    assert!(Partition::COLUMNS == PARTITION_COLUMNS);
    assert!(Topic::COLUMNS == TOPIC_COLUMNS);
}

#[rstest]
#[case::no_header(TableError::NoHeader, "the data table is empty: it needs a header row")]
#[case::unknown_column_near_miss(
    TableError::UnknownColumn { column: "leadr epoch".to_owned(), known: PARTITION_COLUMNS },
    "unknown column `leadr epoch`; did you mean `leader epoch`? known columns: `topic`, \
     `partition`, `leader epoch`",
)]
#[case::unknown_column_far_off(
    TableError::UnknownColumn { column: "replicas".to_owned(), known: PARTITION_COLUMNS },
    "unknown column `replicas`; known columns: `topic`, `partition`, `leader epoch`",
)]
#[case::duplicate_column(
    TableError::DuplicateColumn { column: "topic".to_owned() },
    "column `topic` appears more than once",
)]
#[case::missing_column(
    TableError::MissingColumn { column: "partition" },
    "missing required column `partition`",
)]
#[case::empty_cell(
    TableError::EmptyCell { row: Some(3), column: "topic", value: "-".to_owned() },
    "row 3, column `topic`: the cell is empty (`-`), but the column is required",
)]
#[case::invalid_cell(
    TableError::InvalidCell {
        row: Some(3),
        column: "partition",
        value: "three".to_owned(),
        reason: "invalid digit found in string".to_owned(),
    },
    "row 3, column `partition`: cannot parse `three`: invalid digit found in string",
)]
#[case::invalid_cell_without_row(
    TableError::InvalidCell {
        row: None,
        column: "partition",
        value: "three".to_owned(),
        reason: "invalid digit found in string".to_owned(),
    },
    "column `partition`: cannot parse `three`: invalid digit found in string",
)]
#[case::ragged_row(
    TableError::RaggedRow { row: Some(2), cells: 1, columns: 2 },
    "row 2 has 1 cells, but the header has 2 columns",
)]
#[case::ragged_row_without_row(
    TableError::RaggedRow { row: None, cells: 1, columns: 2 },
    "a row has 1 cells, but the header has 2 columns",
)]
#[case::not_a_pair(
    TableError::NotAPair { row: 3, cells: 3 },
    "row 3 has 3 cells, but a vertical table has two per row: `name | value`",
)]
fn table_error_message_names_row_column_and_value(
    #[case] error: TableError,
    #[case] expected: &str,
) {
    assert!(error.to_string() == expected);
}

#[rstest]
#[case::required("topic", Some("orders"))]
#[case::number("partition", Some("0"))]
#[case::optional_set("leader epoch", Some("5"))]
#[case::unknown("replicas", None)]
fn table_cells_shows_each_column_through_display(
    #[case] column: &str,
    #[case] expected: Option<&str>,
) {
    let row = partition("orders", 0, Some(5));
    assert!(row.cell(column).as_deref() == expected);
}

#[test]
fn table_cells_shows_none_as_the_empty_marker() {
    assert!(partition("orders", 0, None).cell("leader epoch").as_deref() == Some("-"));
}

struct Catalog {
    partitions: Given<Vec<Partition>>,
    topic: Given<Topic>,
}

#[rstest::fixture]
fn catalog() -> Catalog {
    Catalog {
        partitions: Given::new("partitions", "When these partitions are created"),
        topic: Given::new("topic", "When a topic is created from"),
    }
}

#[krabka_bdd::when("these partitions are created")]
fn these_partitions_are_created(catalog: &mut Catalog, datatable: Vec<Vec<String>>) {
    catalog.partitions.set(table::rows(&datatable).unwrap());
}

#[krabka_bdd::when("a topic is created from")]
fn a_topic_is_created_from(catalog: &mut Catalog, datatable: Vec<Vec<String>>) {
    catalog.topic.set(table::vertical(&datatable).unwrap());
}

type ReadAttempt = Attempt<Vec<Partition>, TableError>;

#[rstest::fixture]
fn read_attempt() -> ReadAttempt {
    // Until the When step returns its attempt in this fixture's place.
    Attempt::NotExecuted
}

#[krabka_bdd::when("a step attempts to read these partitions")]
fn attempts_to_read_partitions(datatable: Vec<Vec<String>>) -> Attempt<Vec<Partition>, TableError> {
    Attempt::of(|| table::rows(&datatable))
}

#[krabka_bdd::then("partition {topic}-{number} has leader epoch {epoch}")]
fn partition_has_leader_epoch(catalog: &Catalog, topic: String, number: u32, epoch: i32) {
    let expected = partition(&topic, number, Some(epoch));
    assert!(catalog.partitions.get().contains(&expected));
}

#[krabka_bdd::then("partition {topic}-{number} has no leader epoch")]
fn partition_has_no_leader_epoch(catalog: &Catalog, topic: String, number: u32) {
    let expected = partition(&topic, number, None);
    assert!(catalog.partitions.get().contains(&expected));
}

#[krabka_bdd::then("topic {name} has {partitions} partitions and replication factor {replicas}")]
fn topic_has(catalog: &Catalog, name: String, partitions: u32, replicas: u16) {
    let expected = Topic {
        name,
        partitions,
        replicas,
    };
    assert!(*catalog.topic.get() == expected);
}

#[krabka_bdd::then("reading the table fails on unknown column {column}, suggesting {suggestion}")]
fn reading_fails_on_unknown_column(read_attempt: &ReadAttempt, column: String, suggestion: String) {
    let error = read_attempt.failed();
    assert!(
        *error
            == TableError::UnknownColumn {
                column,
                known: PARTITION_COLUMNS,
            }
    );
    assert!(error.suggestion() == Some(suggestion.as_str()));
}

krabka_bdd::scenarios!(
    "tests/features/table_accessor",
    fixtures = [catalog: Catalog, read_attempt: ReadAttempt],
);
