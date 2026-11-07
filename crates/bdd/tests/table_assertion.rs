#![cfg(test)]
//! Data Table Assertion: `assert_table!` compares rows against an expected
//! table on the table's columns, and one failure reports every differing,
//! missing and extra row.

use {
    assert2::assert,
    krabka_bdd::{
        Given, TableRow,
        table::{self, RowDiff, TableMismatch},
    },
    rstest::rstest,
    std::panic::{self, AssertUnwindSafe},
};

#[derive(Debug, Clone, PartialEq, Eq, TableRow)]
struct Partition {
    topic: String,
    #[column("partition")]
    number: u32,
    leader_epoch: Option<i32>,
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

fn cells(row: &[&str]) -> Vec<String> { row.iter().map(ToString::to_string).collect() }

fn partition(topic: &str, number: u32, leader_epoch: Option<i32>) -> Partition {
    Partition {
        topic: topic.to_owned(),
        number,
        leader_epoch,
    }
}

fn listing() -> Vec<Partition> {
    vec![
        partition("orders", 0, Some(5)),
        partition("orders", 1, None),
        partition("payments", 0, Some(2)),
    ]
}

#[rstest]
#[case::every_column(
    "| topic    | partition | leader epoch |
     | orders   | 0         | 5            |
     | orders   | 1         | -            |
     | payments | 0         | 2            |"
)]
#[case::some_columns(
    "| topic    | partition |
     | orders   | 0         |
     | orders   | 1         |
     | payments | 0         |"
)]
#[case::columns_in_another_order(
    "| leader epoch | topic    |
     | 5            | orders   |
     | -            | orders   |
     | 2            | payments |"
)]
fn compare_accepts_rows_that_match_on_the_table_columns(#[case] expected: &str) {
    assert!(table::compare(listing(), &table(expected)) == Ok(()));
}

#[test]
fn compare_accepts_no_rows_against_a_header_only_table() {
    assert!(table::compare(Vec::<Partition>::new(), &table("| topic |")) == Ok(()));
}

#[rstest]
#[case::differing_cell(
    "| topic    | partition |
     | orders   | 0         |
     | orders   | 2         |
     | payments | 0         |",
    TableMismatch {
        columns: cells(&["topic", "partition"]),
        rows: vec![
            RowDiff::Same { row: 2, cells: cells(&["orders", "0"]) },
            RowDiff::Differs {
                row: 3,
                expected: cells(&["orders", "2"]),
                actual: cells(&["orders", "1"]),
            },
            RowDiff::Same { row: 4, cells: cells(&["payments", "0"]) },
        ],
        reordered: false,
        unknown_columns: vec![],
    },
)]
#[case::missing_row(
    "| topic    | partition |
     | orders   | 0         |
     | orders   | 1         |
     | payments | 0         |
     | payments | 1         |",
    TableMismatch {
        columns: cells(&["topic", "partition"]),
        rows: vec![
            RowDiff::Same { row: 2, cells: cells(&["orders", "0"]) },
            RowDiff::Same { row: 3, cells: cells(&["orders", "1"]) },
            RowDiff::Same { row: 4, cells: cells(&["payments", "0"]) },
            RowDiff::Missing { row: 5, expected: cells(&["payments", "1"]) },
        ],
        reordered: false,
        unknown_columns: vec![],
    },
)]
#[case::extra_row(
    "| topic  | partition |
     | orders | 0         |
     | orders | 1         |",
    TableMismatch {
        columns: cells(&["topic", "partition"]),
        rows: vec![
            RowDiff::Same { row: 2, cells: cells(&["orders", "0"]) },
            RowDiff::Same { row: 3, cells: cells(&["orders", "1"]) },
            RowDiff::Extra { row: 4, actual: cells(&["payments", "0"]) },
        ],
        reordered: false,
        unknown_columns: vec![],
    },
)]
#[case::reordered(
    "| topic    | partition |
     | payments | 0         |
     | orders   | 0         |
     | orders   | 1         |",
    TableMismatch {
        columns: cells(&["topic", "partition"]),
        rows: vec![
            RowDiff::Differs {
                row: 2,
                expected: cells(&["payments", "0"]),
                actual: cells(&["orders", "0"]),
            },
            RowDiff::Differs {
                row: 3,
                expected: cells(&["orders", "0"]),
                actual: cells(&["orders", "1"]),
            },
            RowDiff::Differs {
                row: 4,
                expected: cells(&["orders", "1"]),
                actual: cells(&["payments", "0"]),
            },
        ],
        reordered: true,
        unknown_columns: vec![],
    },
)]
#[case::unknown_column(
    "| topic  | replicas |
     | orders | 3        |",
    TableMismatch {
        columns: cells(&["topic", "replicas"]),
        rows: vec![],
        reordered: false,
        unknown_columns: cells(&["replicas"]),
    },
)]
fn compare_reports_every_difference(#[case] expected: &str, #[case] mismatch: TableMismatch) {
    assert!(table::compare(listing(), &table(expected)) == Err(mismatch));
}

#[rstest]
#[case::differing_missing_and_extra(
    listing(),
    "| topic    | partition | leader epoch |
     | orders   | 0         | 5            |
     | orders   | 1         | 4            |",
    "the rows differ from the expected table: 1 row differs, 1 unexpected row\n  \
     | topic    | partition | leader epoch |\n  \
     | orders   | 0         | 5            |\n\
     - | orders   | 1         | 4            |\n\
     + | orders   | 1         | -            |\n\
     + | payments | 0         | 2            |\n\
     row 3, column `leader epoch`: expected `4`, actual `-`\n\
     row 4: unexpected row\n"
)]
#[case::missing_rows(
    vec![partition("orders", 0, None)],
    "| topic    | partition |
     | orders   | 0         |
     | orders   | 1         |
     | payments | 0         |",
    "the rows differ from the expected table: 2 expected rows are missing\n  \
     | topic    | partition |\n  \
     | orders   | 0         |\n\
     - | orders   | 1         |\n\
     - | payments | 0         |\n\
     row 3: expected row is missing\n\
     row 4: expected row is missing\n",
)]
#[case::reordered(
    vec![partition("orders", 1, None), partition("orders", 0, None)],
    "| partition |
     | 0         |
     | 1         |",
    "the rows match the expected table, but in a different order: 2 rows differ\n  \
     | partition |\n\
     - | 0         |\n\
     + | 1         |\n\
     - | 1         |\n\
     + | 0         |\n\
     row 2, column `partition`: expected `0`, actual `1`\n\
     row 3, column `partition`: expected `1`, actual `0`\n",
)]
#[case::unknown_columns(
    listing(),
    "| topic  | replicas | leader |
     | orders | 3        | 1      |",
    "the actual rows have no column `replicas`, `leader`, which the expected table compares"
)]
fn mismatch_renders_a_table_diff(
    #[case] actual: Vec<Partition>,
    #[case] expected: &str,
    #[case] message: &str,
) {
    let mismatch = table::compare(&actual, &table(expected)).unwrap_err();
    assert!(mismatch.to_string() == message);
}

#[test]
fn assert_table_passes_matching_rows() {
    let datatable = table(
        "| topic    | partition |
         | orders   | 0         |
         | orders   | 1         |
         | payments | 0         |",
    );
    krabka_bdd::assert_table!(listing(), &datatable);
}

#[test]
fn assert_table_panics_with_the_expressions_and_the_diff() {
    let datatable = table(
        "| topic  | partition |
         | orders | 0         |",
    );
    let actual = vec![partition("orders", 1, None)];
    let payload = panic::catch_unwind(AssertUnwindSafe(|| {
        krabka_bdd::assert_table!(&actual, &datatable);
    }))
    .unwrap_err();
    let message = payload.downcast_ref::<String>().unwrap();
    assert!(
        message
            == "Assertion failed: assert_table!(&actual, &datatable)\nthe rows differ from the \
                expected table: 1 row differs\n  | topic  | partition |\n- | orders | 0         \
                |\n+ | orders | 1         |\nrow 2, column `partition`: expected `0`, actual `1`\n"
    );
}

struct Catalog {
    partitions: Given<Vec<Partition>>,
    listing: Given<Vec<Partition>>,
}

#[rstest::fixture]
fn catalog() -> Catalog {
    Catalog {
        partitions: Given::new("partitions", "Given these partitions exist"),
        listing: Given::new("listing", "When the partitions are listed"),
    }
}

#[krabka_bdd::given("these partitions exist")]
fn these_partitions_exist(catalog: &mut Catalog, datatable: Vec<Vec<String>>) {
    catalog.partitions.set(table::rows(&datatable).unwrap());
}

#[krabka_bdd::when("the partitions are listed")]
fn the_partitions_are_listed(catalog: &mut Catalog) {
    let mut listing = catalog.partitions.get().clone();
    listing.sort_by(|left, right| (&left.topic, left.number).cmp(&(&right.topic, right.number)));
    catalog.listing.set(listing);
}

#[krabka_bdd::then("the listing shows")]
fn the_listing_shows(catalog: &Catalog, datatable: Vec<Vec<String>>) {
    krabka_bdd::assert_table!(catalog.listing.get(), &datatable);
}

krabka_bdd::scenarios!("tests/features/table_assertion", fixtures = [catalog: Catalog]);
