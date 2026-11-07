//! Data Table Accessor and Data Table Assertion: typed rows out of a step's
//! data table, and one assertion that compares rows against a table.
//!
//! rstest-bdd hands a step its data table as `datatable: Vec<Vec<String>>`,
//! header row first. `#[derive(TableRow)]` turns each row into a struct:
//!
//! - Each named field is a column. The column name is the field name with
//!   `_` replaced by a space, so `leader_epoch` reads the column
//!   `leader epoch`. `#[column("epoch")]` names it explicitly.
//! - A cell parses into its field's type with `FromStr`.
//! - An `Option<T>` field is an optional column. A missing column, an empty
//!   cell and the empty marker `-` all give `None`. A required field rejects
//!   an empty cell and `-`.
//! - The header is strict: a column that no field reads is an error that
//!   lists the known columns, with a "did you mean" for a near miss.
//!
//! The derive also implements [`TableCells`] through each field's `Display`,
//! with `-` for a `None`, so [`assert_table!`](crate::assert_table) can
//! compare the same type against an expected table.
//!
//! Rows count from 1 at the header row, as the feature file shows them: the
//! first data row of a horizontal table is row 2.
//!
//! # Examples
//!
//! ```
//! use {
//!     assert2::assert,
//!     krabka_bdd::{TableRow, table},
//! };
//!
//! #[derive(Debug, PartialEq, Eq, TableRow)]
//! struct Assignment {
//!     topic: String,
//!     partition: u32,
//!     #[column("epoch")]
//!     leader_epoch: Option<i32>,
//! }
//!
//! let row = |cells: [&str; 3]| cells.map(String::from).to_vec();
//! let datatable = vec![
//!     row(["topic", "partition", "epoch"]),
//!     row(["orders", "0", "5"]),
//!     row(["orders", "1", "-"]),
//! ];
//!
//! let assignments: Vec<Assignment> = table::rows(&datatable).unwrap();
//! assert!(
//!     assignments
//!         == [
//!             Assignment {
//!                 topic: "orders".to_owned(),
//!                 partition: 0,
//!                 leader_epoch: Some(5),
//!             },
//!             Assignment {
//!                 topic: "orders".to_owned(),
//!                 partition: 1,
//!                 leader_epoch: None,
//!             },
//!         ]
//! );
//! krabka_bdd::assert_table!(&assignments, &datatable);
//! ```

use std::{
    error::Error,
    fmt::{self, Display, Write as _},
    str::FromStr,
};

/// The marker a cell holds for "no value".
pub const EMPTY: &str = "-";

/// A struct that one row of a data table fills.
///
/// Derive it with `#[derive(krabka_bdd::TableRow)]`, then read a table with
/// [`rows`] or [`vertical`].
pub trait TableRow: Sized {
    /// Every column the type reads, in field order.
    const COLUMNS: &'static [&'static str];

    /// Builds one value from a row of `cells` under `headers`.
    ///
    /// # Errors
    ///
    /// Returns a [`TableError`] for an unknown, repeated or missing column,
    /// an empty required cell, a cell that does not parse, or a row whose
    /// length differs from the header's. The error carries no row number;
    /// [`rows`] and [`vertical`] add it.
    fn from_row(headers: &[String], cells: &[String]) -> Result<Self, TableError>;
}

/// A value that shows its cells by column name, for
/// [`assert_table!`](crate::assert_table).
///
/// `#[derive(TableRow)]` implements it through each field's `Display`.
pub trait TableCells {
    /// The text of `column`, or `None` if the value has no such column.
    fn cell(&self, column: &str) -> Option<String>;
}

impl<T: TableCells + ?Sized> TableCells for &T {
    fn cell(&self, column: &str) -> Option<String> { (**self).cell(column) }
}

/// Reads a horizontal table: a header row, then one value per row.
///
/// # Errors
///
/// Returns a [`TableError`] for an empty table or for the first row that
/// [`TableRow::from_row`] rejects, with that row's number.
pub fn rows<T: TableRow>(datatable: &[Vec<String>]) -> Result<Vec<T>, TableError> {
    let Some((headers, body)) = datatable.split_first() else {
        return Err(TableError::NoHeader);
    };
    body.iter()
        .zip(2..)
        .map(|(cells, row)| T::from_row(headers, cells).map_err(|error| error.at_row(row)))
        .collect()
}

/// Reads a vertical table: one `| column | value |` row per field.
///
/// A first row of `| field | value |` is a header and is skipped.
///
/// # Errors
///
/// Returns a [`TableError`] for an empty table, a row that is not a pair,
/// or what [`TableRow::from_row`] rejects, with the row number of the
/// column it names.
pub fn vertical<T: TableRow>(datatable: &[Vec<String>]) -> Result<T, TableError> {
    let (offset, body) = match datatable.split_first() {
        None => return Err(TableError::NoHeader),
        Some((first, body)) if is_field_value_header(first) => (2, body),
        Some(_) => (1, datatable),
    };
    let mut headers = Vec::with_capacity(body.len());
    let mut cells = Vec::with_capacity(body.len());
    for (pair, row) in body.iter().zip(offset..) {
        let [name, value] = pair.as_slice() else {
            return Err(TableError::NotAPair {
                row,
                cells: pair.len(),
            });
        };
        headers.push(name.clone());
        cells.push(value.clone());
    }
    T::from_row(&headers, &cells).map_err(|error| {
        let row = error
            .cell_column()
            .and_then(|column| headers.iter().position(|header| header == column))
            .map_or(offset, |index| index + offset);
        error.at_row(row)
    })
}

/// Reports whether `row` is the `| field | value |` header of a vertical
/// table.
pub(crate) fn is_field_value_header(row: &[String]) -> bool {
    matches!(row, [field, value] if field == "field" && value == "value")
}

/// Compares `actual` rows against the `expected` table, header row first.
///
/// Only the expected table's columns are compared, in order: row 2 of the
/// table against the first actual row, and so on.
///
/// # Errors
///
/// Returns a [`TableMismatch`] listing every differing, missing and extra
/// row when the rows differ, or the expected columns that the actual rows do
/// not have.
pub fn compare<I>(actual: I, expected: &[Vec<String>]) -> Result<(), TableMismatch>
where
    I: IntoIterator,
    I::Item: TableCells,
{
    let columns: Vec<String> = expected.first().cloned().unwrap_or_default();
    let body = expected.get(1..).unwrap_or_default();
    let actual: Vec<I::Item> = actual.into_iter().collect();
    let unknown_columns: Vec<String> = columns
        .iter()
        .filter(|column| actual.iter().any(|row| row.cell(column).is_none()))
        .cloned()
        .collect();
    if !unknown_columns.is_empty() {
        return Err(TableMismatch {
            columns,
            rows: Vec::new(),
            reordered: false,
            unknown_columns,
        });
    }
    let actual: Vec<Vec<String>> = actual
        .iter()
        .map(|row| {
            columns
                .iter()
                .map(|column| row.cell(column).unwrap_or_default())
                .collect()
        })
        .collect();
    let expected: Vec<Vec<String>> = body
        .iter()
        .map(|row| {
            (0..columns.len())
                .map(|index| row.get(index).cloned().unwrap_or_default())
                .collect()
        })
        .collect();
    let length = actual.len().max(expected.len());
    let rows: Vec<RowDiff> = (0..length)
        .zip(2..)
        .filter_map(|(index, row)| {
            match (expected.get(index).cloned(), actual.get(index).cloned()) {
                (Some(expected), Some(actual)) if expected == actual => {
                    Some(RowDiff::Same { row, cells: actual })
                }
                (Some(expected), Some(actual)) => Some(RowDiff::Differs {
                    row,
                    expected,
                    actual,
                }),
                (Some(expected), None) => Some(RowDiff::Missing { row, expected }),
                (None, Some(actual)) => Some(RowDiff::Extra { row, actual }),
                (None, None) => None,
            }
        })
        .collect();
    if rows.iter().all(|row| matches!(row, RowDiff::Same { .. })) {
        return Ok(());
    }
    let mut sorted_expected = expected;
    let mut sorted_actual = actual;
    sorted_expected.sort();
    sorted_actual.sort();
    Err(TableMismatch {
        columns,
        rows,
        reordered: sorted_expected == sorted_actual,
        unknown_columns,
    })
}

/// How the actual rows differ from an expected table.
///
/// `Display` renders a table diff: `-` marks an expected row, `+` an actual
/// one, and a line per differing cell, missing row and extra row follows.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableMismatch {
    /// The expected table's header row.
    pub columns: Vec<String>,
    /// Every row position, in order, the matching ones included.
    pub rows: Vec<RowDiff>,
    /// The rows hold the same values, in a different order.
    pub reordered: bool,
    /// Expected columns that the actual rows do not have. When there are
    /// any, no row is compared.
    pub unknown_columns: Vec<String>,
}

/// One row position of a [`TableMismatch`]. `row` counts from 1 at the
/// expected table's header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowDiff {
    /// The actual row matches the expected one.
    Same {
        /// The row number.
        row: usize,
        /// The row's cells.
        cells: Vec<String>,
    },
    /// The actual row differs from the expected one in at least one cell.
    Differs {
        /// The row number.
        row: usize,
        /// The expected cells.
        expected: Vec<String>,
        /// The actual cells.
        actual: Vec<String>,
    },
    /// An expected row with no actual row at its position.
    Missing {
        /// The row number.
        row: usize,
        /// The expected cells.
        expected: Vec<String>,
    },
    /// An actual row past the end of the expected table.
    Extra {
        /// The row number it would have.
        row: usize,
        /// The actual cells.
        actual: Vec<String>,
    },
}

impl TableMismatch {
    fn count(&self, matches: fn(&RowDiff) -> bool) -> usize {
        self.rows.iter().filter(|row| matches(row)).count()
    }

    fn widths(&self) -> Vec<usize> {
        let mut widths: Vec<usize> = self.columns.iter().map(|cell| width(cell)).collect();
        for diff in &self.rows {
            let lines: [Option<&Vec<String>>; 2] = match diff {
                RowDiff::Same { cells, .. } => [Some(cells), None],
                RowDiff::Differs {
                    expected, actual, ..
                } => [Some(expected), Some(actual)],
                RowDiff::Missing { expected, .. } => [Some(expected), None],
                RowDiff::Extra { actual, .. } => [Some(actual), None],
            };
            for cells in lines.into_iter().flatten() {
                for (slot, cell) in widths.iter_mut().zip(cells) {
                    *slot = (*slot).max(width(cell));
                }
            }
        }
        widths
    }

    fn summary(&self) -> String {
        let parts: Vec<String> = [
            (
                self.count(|row| matches!(row, RowDiff::Differs { .. })),
                "row differs",
                "rows differ",
            ),
            (
                self.count(|row| matches!(row, RowDiff::Missing { .. })),
                "expected row is missing",
                "expected rows are missing",
            ),
            (
                self.count(|row| matches!(row, RowDiff::Extra { .. })),
                "unexpected row",
                "unexpected rows",
            ),
        ]
        .into_iter()
        .filter(|(count, ..)| *count > 0)
        .map(|(count, one, many)| format!("{count} {}", if count == 1 { one } else { many }))
        .collect();
        parts.join(", ")
    }
}

fn width(cell: &str) -> usize { cell.chars().count() }

fn write_row(
    f: &mut fmt::Formatter<'_>,
    marker: char,
    cells: &[String],
    widths: &[usize],
) -> fmt::Result {
    f.write_char(marker)?;
    f.write_str(" |")?;
    for (cell, width) in cells.iter().zip(widths) {
        write!(f, " {cell:<width$} |")?;
    }
    f.write_char('\n')
}

impl Display for TableMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if !self.unknown_columns.is_empty() {
            f.write_str("the actual rows have no column ")?;
            write_names(f, &self.unknown_columns)?;
            return f.write_str(", which the expected table compares");
        }
        if self.reordered {
            writeln!(
                f,
                "the rows match the expected table, but in a different order: {}",
                self.summary()
            )?;
        } else {
            writeln!(
                f,
                "the rows differ from the expected table: {}",
                self.summary()
            )?;
        }
        let widths = self.widths();
        write_row(f, ' ', &self.columns, &widths)?;
        for diff in &self.rows {
            match diff {
                RowDiff::Same { cells, .. } => write_row(f, ' ', cells, &widths)?,
                RowDiff::Differs {
                    expected, actual, ..
                } => {
                    write_row(f, '-', expected, &widths)?;
                    write_row(f, '+', actual, &widths)?;
                }
                RowDiff::Missing { expected, .. } => write_row(f, '-', expected, &widths)?,
                RowDiff::Extra { actual, .. } => write_row(f, '+', actual, &widths)?,
            }
        }
        for diff in &self.rows {
            match diff {
                RowDiff::Same { .. } => {}
                RowDiff::Differs {
                    row,
                    expected,
                    actual,
                } => {
                    let cells = self.columns.iter().zip(expected.iter().zip(actual));
                    for (column, (expected, actual)) in cells {
                        if expected != actual {
                            writeln!(
                                f,
                                "row {row}, column `{column}`: expected `{expected}`, actual \
                                 `{actual}`"
                            )?;
                        }
                    }
                }
                RowDiff::Missing { row, .. } => writeln!(f, "row {row}: expected row is missing")?,
                RowDiff::Extra { row, .. } => writeln!(f, "row {row}: unexpected row")?,
            }
        }
        Ok(())
    }
}

impl Error for TableMismatch {}

/// A data table that does not fit the type a step reads it into.
///
/// Its message names the row, the column and the value, so a typo in a
/// feature file reads as a fix:
///
/// ```text
/// unknown column `leadr epoch`; did you mean `leader epoch`? known columns: `topic`, `leader epoch`
/// row 3, column `partitions`: cannot parse `three`: invalid digit found in string
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum TableError {
    /// The table has no rows at all.
    NoHeader,
    /// A row has a different number of cells than the header.
    RaggedRow {
        /// The row number, once known.
        row: Option<usize>,
        /// The row's cell count.
        cells: usize,
        /// The header's column count.
        columns: usize,
    },
    /// A row of a vertical table is not a `name | value` pair.
    NotAPair {
        /// The row number.
        row: usize,
        /// The row's cell count.
        cells: usize,
    },
    /// A horizontal overrides table has other than one row of values.
    NotOneValueRow {
        /// How many rows of values it has.
        rows: usize,
    },
    /// The header names a column that the type does not read.
    UnknownColumn {
        /// The column as written.
        column: String,
        /// Every column the type reads.
        known: &'static [&'static str],
    },
    /// The header names a column twice.
    DuplicateColumn {
        /// The repeated column.
        column: String,
    },
    /// The header lacks a column that a required field reads.
    MissingColumn {
        /// The missing column.
        column: &'static str,
    },
    /// A required column's cell is empty or `-`.
    EmptyCell {
        /// The row number, once known.
        row: Option<usize>,
        /// The column.
        column: &'static str,
        /// The cell as written.
        value: String,
    },
    /// A cell does not parse into its field's type.
    InvalidCell {
        /// The row number, once known.
        row: Option<usize>,
        /// The column.
        column: &'static str,
        /// The cell as written.
        value: String,
        /// The `FromStr` error's message.
        reason: String,
    },
    /// An override names a field that the type does not have.
    UnknownField {
        /// The field as written.
        field: String,
        /// Every field the type has.
        known: &'static [&'static str],
    },
    /// An overrides table sets a field twice.
    DuplicateField {
        /// The repeated field.
        field: String,
    },
    /// An override value does not parse into its field's type.
    InvalidField {
        /// The field.
        field: &'static str,
        /// The value as written.
        value: String,
        /// The `FromStr` error's message.
        reason: String,
    },
}

impl TableError {
    /// The known name closest to an unknown column or field, if one is
    /// within an edit distance of 2.
    #[must_use]
    pub fn suggestion(&self) -> Option<&'static str> {
        match self {
            Self::UnknownColumn {
                column: name,
                known,
            }
            | Self::UnknownField { field: name, known } => closest(name, known),
            _ => None,
        }
    }

    /// Fills in the row number of an error about one row, and leaves an
    /// error about the header as it is.
    #[must_use]
    pub fn at_row(self, number: usize) -> Self {
        match self {
            Self::RaggedRow { cells, columns, .. } => Self::RaggedRow {
                row: Some(number),
                cells,
                columns,
            },
            Self::EmptyCell { column, value, .. } => Self::EmptyCell {
                row: Some(number),
                column,
                value,
            },
            Self::InvalidCell {
                column,
                value,
                reason,
                ..
            } => Self::InvalidCell {
                row: Some(number),
                column,
                value,
                reason,
            },
            other => other,
        }
    }

    fn cell_column(&self) -> Option<&'static str> {
        match self {
            Self::EmptyCell { column, .. } | Self::InvalidCell { column, .. } => Some(column),
            _ => None,
        }
    }
}

fn write_names<S: AsRef<str>>(f: &mut fmt::Formatter<'_>, names: &[S]) -> fmt::Result {
    for (index, name) in names.iter().enumerate() {
        if index > 0 {
            f.write_str(", ")?;
        }
        write!(f, "`{}`", name.as_ref())?;
    }
    Ok(())
}

fn write_cell(f: &mut fmt::Formatter<'_>, row: Option<usize>, column: &str) -> fmt::Result {
    if let Some(row) = row {
        write!(f, "row {row}, ")?;
    }
    write!(f, "column `{column}`: ")
}

fn write_unknown(
    f: &mut fmt::Formatter<'_>,
    noun: &str,
    name: &str,
    suggestion: Option<&str>,
    known: &[&str],
) -> fmt::Result {
    write!(f, "unknown {noun} `{name}`; ")?;
    if let Some(suggestion) = suggestion {
        write!(f, "did you mean `{suggestion}`? ")?;
    }
    write!(f, "known {noun}s: ")?;
    write_names(f, known)
}

impl Display for TableError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoHeader => f.write_str("the data table is empty: it needs a header row"),
            Self::RaggedRow {
                row,
                cells,
                columns,
            } => {
                match row {
                    Some(row) => write!(f, "row {row}")?,
                    None => f.write_str("a row")?,
                }
                write!(
                    f,
                    " has {cells} cells, but the header has {columns} columns"
                )
            }
            Self::NotAPair { row, cells } => write!(
                f,
                "row {row} has {cells} cells, but a vertical table has two per row: `name | value`"
            ),
            Self::NotOneValueRow { rows } => write!(
                f,
                "a horizontal overrides table has a header and one row of values, but this one \
                 has {rows}; set one field per row under a `| field | value |` header instead"
            ),
            Self::UnknownColumn { column, known } => {
                write_unknown(f, "column", column, self.suggestion(), known)
            }
            Self::UnknownField { field, known } => {
                write_unknown(f, "field", field, self.suggestion(), known)
            }
            Self::DuplicateColumn { column } => {
                write!(f, "column `{column}` appears more than once")
            }
            Self::DuplicateField { field } => write!(f, "field `{field}` is set more than once"),
            Self::MissingColumn { column } => write!(f, "missing required column `{column}`"),
            Self::EmptyCell { row, column, value } => {
                write_cell(f, *row, column)?;
                write!(
                    f,
                    "the cell is empty (`{value}`), but the column is required"
                )
            }
            Self::InvalidCell {
                row,
                column,
                value,
                reason,
            } => {
                write_cell(f, *row, column)?;
                write!(f, "cannot parse `{value}`: {reason}")
            }
            Self::InvalidField {
                field,
                value,
                reason,
            } => write!(f, "field `{field}`: cannot parse `{value}`: {reason}"),
        }
    }
}

impl Error for TableError {}

/// The `known` name closest to `name`, within an edit distance of 2. The
/// earlier of two equally close names wins.
fn closest(name: &str, known: &[&'static str]) -> Option<&'static str> {
    known
        .iter()
        .map(|candidate| (edit_distance(name, candidate), *candidate))
        .enumerate()
        .filter(|(_, (distance, _))| *distance <= 2)
        .min_by_key(|(index, (distance, _))| (*distance, *index))
        .map(|(_, (_, candidate))| candidate)
}

/// The Levenshtein distance between `a` and `b`, in characters.
fn edit_distance(a: &str, b: &str) -> usize {
    let b: Vec<char> = b.chars().collect();
    let mut previous: Vec<usize> = (0..=b.len()).collect();
    for (row, left) in a.chars().enumerate() {
        let mut current = Vec::with_capacity(b.len() + 1);
        current.push(row + 1);
        for (right, (diagonal, above)) in b.iter().zip(previous.iter().zip(previous.iter().skip(1)))
        {
            let before = current.last().copied().unwrap_or(usize::MAX);
            let substitute = diagonal + usize::from(left != *right);
            current.push(substitute.min(above + 1).min(before.saturating_add(1)));
        }
        previous = current;
    }
    previous.last().copied().unwrap_or_default()
}

/// Whether `cell` holds no value: empty, or the marker `-`.
fn is_empty(cell: &str) -> bool { cell.is_empty() || cell == EMPTY }

/// One row of a table as a `#[derive(TableRow)]` expansion reads it. Not
/// part of the API.
#[doc(hidden)]
pub struct RowCells<'a> {
    headers: &'a [String],
    cells: &'a [String],
}

impl<'a> RowCells<'a> {
    /// Checks the row against the header and the header against `columns`.
    ///
    /// # Errors
    ///
    /// Returns a [`TableError`] for a ragged row, a repeated column or an
    /// unknown one.
    pub fn new(
        headers: &'a [String],
        cells: &'a [String],
        columns: &'static [&'static str],
    ) -> Result<Self, TableError> {
        if headers.len() != cells.len() {
            return Err(TableError::RaggedRow {
                row: None,
                cells: cells.len(),
                columns: headers.len(),
            });
        }
        for (index, header) in headers.iter().enumerate() {
            if headers.iter().take(index).any(|earlier| earlier == header) {
                return Err(TableError::DuplicateColumn {
                    column: header.clone(),
                });
            }
            if !columns.contains(&header.as_str()) {
                return Err(TableError::UnknownColumn {
                    column: header.clone(),
                    known: columns,
                });
            }
        }
        Ok(Self { headers, cells })
    }

    fn get(&self, column: &str) -> Option<&'a String> {
        let index = self.headers.iter().position(|header| header == column)?;
        self.cells.get(index)
    }

    /// Parses the cell of a required `column`.
    ///
    /// # Errors
    ///
    /// Returns a [`TableError`] if the column is missing, the cell is empty,
    /// or the cell does not parse.
    pub fn required<T>(&self, column: &'static str) -> Result<T, TableError>
    where
        T: FromStr,
        T::Err: Display,
    {
        let Some(value) = self.get(column) else {
            return Err(TableError::MissingColumn { column });
        };
        if is_empty(value) {
            return Err(TableError::EmptyCell {
                row: None,
                column,
                value: value.clone(),
            });
        }
        parse_cell(column, value)
    }

    /// Parses the cell of an optional `column`: `None` if the column is
    /// missing or the cell is empty or `-`.
    ///
    /// # Errors
    ///
    /// Returns a [`TableError`] if the cell does not parse.
    pub fn optional<T>(&self, column: &'static str) -> Result<Option<T>, TableError>
    where
        T: FromStr,
        T::Err: Display,
    {
        match self.get(column) {
            Some(value) if !is_empty(value) => parse_cell(column, value).map(Some),
            _ => Ok(None),
        }
    }
}

fn parse_cell<T>(column: &'static str, value: &str) -> Result<T, TableError>
where
    T: FromStr,
    T::Err: Display,
{
    value
        .parse()
        .map_err(|error: T::Err| TableError::InvalidCell {
            row: None,
            column,
            value: value.to_owned(),
            reason: error.to_string(),
        })
}

/// A required field's cell text. Not part of the API.
#[doc(hidden)]
pub fn cell<T: Display>(value: &T) -> String { value.to_string() }

/// An optional field's cell text: `-` for `None`. Not part of the API.
#[doc(hidden)]
pub fn optional_cell<T: Display>(value: &Option<T>) -> String {
    value
        .as_ref()
        .map_or_else(|| EMPTY.to_owned(), ToString::to_string)
}

/// Panics with the table diff unless `actual` matches `expected`. Not part
/// of the API: call [`assert_table!`](crate::assert_table).
///
/// # Panics
///
/// Panics if [`compare`] finds a mismatch.
#[doc(hidden)]
#[track_caller]
pub fn assert_table<I>(actual: I, expected: &[Vec<String>], actual_text: &str, expected_text: &str)
where
    I: IntoIterator,
    I::Item: TableCells,
{
    if let Err(mismatch) = compare(actual, expected) {
        panic!("Assertion failed: assert_table!({actual_text}, {expected_text})\n{mismatch}");
    }
}

/// Asserts that rows match an expected data table, header row first.
///
/// `actual` is anything that iterates over [`TableCells`](crate::table::TableCells)
/// values, such as a `Vec` or a slice of a `#[derive(TableRow)]` type. Only
/// the expected table's columns are compared, row by row in order. On a
/// mismatch it panics at the caller with one table diff that shows every
/// differing row and cell, every missing expected row and every unexpected
/// extra row, and says so when the rows only differ in order:
///
/// ```text
/// Assertion failed: assert_table!(topics, &datatable)
/// the rows differ from the expected table: 1 row differs, 1 unexpected row
///   | name     | partitions |
///   | orders   | 3          |
/// - | payments | 2          |
/// + | payments | 4          |
/// + | audit    | 1          |
/// row 3, column `partitions`: expected `2`, actual `4`
/// row 4: unexpected row
/// ```
///
/// [`compare`](crate::table::compare) returns the same diff as a value.
#[macro_export]
macro_rules! assert_table {
    ($actual:expr, $expected:expr $(,)?) => {
        $crate::__private::assert_table(
            $actual,
            $expected,
            ::core::stringify!($actual),
            ::core::stringify!($expected),
        )
    };
}

#[cfg(test)]
mod tests {
    use {
        super::{closest, edit_distance},
        assert2::assert,
        rstest::rstest,
    };

    #[rstest]
    #[case::equal("epoch", "epoch", 0)]
    #[case::one_substitution("epoch", "epoxh", 1)]
    #[case::one_deletion("epoch", "epch", 1)]
    #[case::one_insertion("epoch", "epochs", 1)]
    #[case::transposition("leader", "laeder", 2)]
    #[case::empty_left("", "abc", 3)]
    #[case::empty_right("abc", "", 3)]
    #[case::unrelated("name", "partitions", 9)]
    fn edit_distance_counts_character_edits(
        #[case] a: &str,
        #[case] b: &str,
        #[case] expected: usize,
    ) {
        assert!(edit_distance(a, b) == expected);
    }

    #[rstest]
    #[case::near_miss("partiton", Some("partitions"))]
    #[case::closest_wins("nam", Some("name"))]
    #[case::too_far("replicas", None)]
    #[case::tie_takes_the_first("ab", Some("aa"))]
    fn closest_finds_a_near_miss(#[case] name: &str, #[case] expected: Option<&str>) {
        assert!(closest(name, &["aa", "ac", "name", "names", "partitions"]) == expected);
    }
}
