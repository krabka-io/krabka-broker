//! Validation Tester: start from a valid value, override the fields a
//! scenario's rule is about, then validate.
//!
//! `#[derive(Overrides)]` gives a struct [`Overrides::apply`], which sets one
//! field from its step text. Field names map as [`TableRow`] columns do: the
//! field name with `_` replaced by a space, or `#[column("...")]`. A value
//! parses with `FromStr`, and `-` sets an `Option` field to `None`. Any other
//! value, the empty one included, parses as written, because an override
//! exists to set the invalid values a validator must refuse.
//!
//! [`apply_table`] applies a data table of several overrides at once, for a
//! rule that spans fields. It reads either shape:
//!
//! ```gherkin
//! When the request is changed to
//!   | field              | value |
//!   | partitions         | 0     |
//!   | replication factor | 0     |
//! When the request is changed to
//!   | partitions | replication factor |
//!   | 0          | 0                  |
//! ```
//!
//! The `| field | value |` header marks the vertical shape; any other first row
//! is the header of a horizontal table with exactly one row of values.
//!
//! The starting value comes from a valid default instance: a `Default`, an
//! Object Mother, or a bon builder with valid domain defaults. The scenario
//! then states only what makes it invalid.
//!
//! # Examples
//!
//! ```
//! use {
//!     assert2::assert,
//!     krabka_bdd::{Overrides, TableError},
//! };
//!
//! #[derive(Debug, PartialEq, Eq, Overrides)]
//! struct TopicRequest {
//!     name: String,
//!     partitions: u32,
//!     retention_ms: Option<u64>,
//! }
//!
//! let mut request = TopicRequest {
//!     name: "orders".to_owned(),
//!     partitions: 3,
//!     retention_ms: Some(60_000),
//! };
//! request.apply("partitions", "0").unwrap();
//! request.apply("retention ms", "-").unwrap();
//! assert!(
//!     request
//!         == TopicRequest {
//!             name: "orders".to_owned(),
//!             partitions: 0,
//!             retention_ms: None,
//!         }
//! );
//! assert!(
//!     request.apply("partiton", "1")
//!         == Err(TableError::UnknownField {
//!             field: "partiton".to_owned(),
//!             known: &["name", "partitions", "retention ms"],
//!         })
//! );
//! ```
//!
//! [`TableRow`]: crate::TableRow

use {
    crate::table::{EMPTY, TableError, is_field_value_header},
    std::{fmt::Display, str::FromStr},
};

/// A struct whose fields a scenario overrides by name.
///
/// Derive it with `#[derive(krabka_bdd::Overrides)]`.
pub trait Overrides {
    /// Every field name, in field order.
    const FIELDS: &'static [&'static str];

    /// Sets `field` from its step text `value`.
    ///
    /// # Errors
    ///
    /// Returns [`TableError::UnknownField`] for a name the type does not
    /// have, and [`TableError::InvalidField`] for a value that does not
    /// parse. The value is unchanged then.
    fn apply(&mut self, field: &str, value: &str) -> Result<(), TableError>;
}

/// Applies every override in `datatable` to `value`, in table order.
///
/// # Errors
///
/// Returns a [`TableError`] for an empty table, a table of neither shape, a
/// field set twice, or the first override that [`Overrides::apply`]
/// rejects. Overrides before that one stay applied.
pub fn apply_table<T: Overrides + ?Sized>(
    value: &mut T,
    datatable: &[Vec<String>],
) -> Result<(), TableError> {
    let pairs = overrides(datatable)?;
    for (index, (field, _)) in pairs.iter().enumerate() {
        if pairs
            .iter()
            .take(index)
            .any(|(earlier, _)| earlier == field)
        {
            return Err(TableError::DuplicateField {
                field: (*field).clone(),
            });
        }
    }
    pairs
        .into_iter()
        .try_for_each(|(field, text)| value.apply(field, text))
}

/// The `(field, value)` pairs of either shape of overrides table.
fn overrides(datatable: &[Vec<String>]) -> Result<Vec<(&String, &String)>, TableError> {
    match datatable {
        [] => Err(TableError::NoHeader),
        [header, body @ ..] if is_field_value_header(header) => body
            .iter()
            .zip(2..)
            .map(|(pair, row)| match pair.as_slice() {
                [field, value] => Ok((field, value)),
                _ => Err(TableError::NotAPair {
                    row,
                    cells: pair.len(),
                }),
            })
            .collect(),
        [header, values] if header.len() == values.len() => Ok(header.iter().zip(values).collect()),
        [header, values] => Err(TableError::RaggedRow {
            row: Some(2),
            cells: values.len(),
            columns: header.len(),
        }),
        [_, body @ ..] => Err(TableError::NotOneValueRow { rows: body.len() }),
    }
}

/// A required field's value parsed from its override. Not part of the API.
///
/// # Errors
///
/// Returns [`TableError::InvalidField`] if `value` does not parse.
#[doc(hidden)]
pub fn field<T>(field: &'static str, value: &str) -> Result<T, TableError>
where
    T: FromStr,
    T::Err: Display,
{
    value
        .parse()
        .map_err(|error: T::Err| TableError::InvalidField {
            field,
            value: value.to_owned(),
            reason: error.to_string(),
        })
}

/// An optional field's value parsed from its override: `None` for `-`. Not
/// part of the API.
///
/// # Errors
///
/// Returns [`TableError::InvalidField`] if `value` does not parse.
#[doc(hidden)]
pub fn optional_field<T>(name: &'static str, value: &str) -> Result<Option<T>, TableError>
where
    T: FromStr,
    T::Err: Display,
{
    if value == EMPTY {
        Ok(None)
    } else {
        field(name, value).map(Some)
    }
}

/// The error for an override of a field the type does not have. Not part of
/// the API.
#[doc(hidden)]
#[must_use]
pub fn unknown_field(field: &str, known: &'static [&'static str]) -> TableError {
    TableError::UnknownField {
        field: field.to_owned(),
        known,
    }
}
