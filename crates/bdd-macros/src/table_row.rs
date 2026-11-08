//! `#[derive(TableRow)]`: a struct filled from one row of a data table, and
//! shown back as cells for `assert_table!`.
//!
//! Each named field is a column. Its name is the field name with `_`
//! replaced by a space, unless `#[column("name")]` names it. An `Option<T>`
//! field is an optional column. `#[derive(Overrides)]` maps names the same
//! way, through [`columns`].

use {
    crate::step_param::string_literals,
    moxy::{
        ast::{
            Attributed, FromMeta, GenericArgument, ItemStruct, Meta, ParseError, PathArguments,
            Type,
        },
        token::{Ident, LitStr, Spanner, TokenStream},
    },
    std::collections::HashSet,
};

/// The attribute that names a field's column.
const ATTRIBUTE: &str = "column";

/// The column name of `#[column("name")]`.
struct ColumnName(LitStr);

impl FromMeta for ColumnName {
    fn from_meta(meta: &Meta) -> Result<Self, ParseError> {
        let malformed = || {
            ParseError::new(
                meta.span(),
                "expected `#[column(\"name\")]`: one string literal",
            )
        };
        match string_literals(meta, malformed)?.as_slice() {
            [name] => Ok(Self(name.clone())),
            _ => Err(malformed()),
        }
    }
}

/// One field: its name in Rust, its column name, and whether it is an
/// `Option`.
pub struct Column {
    pub ident: Ident,
    pub name: LitStr,
    pub optional: bool,
}

/// The default column name of a field: `leader_epoch` reads `leader epoch`.
/// A raw identifier's `r#` and leading or repeated underscores drop out.
fn column_name(ident: &str) -> String {
    let ident = ident.strip_prefix("r#").unwrap_or(ident);
    ident
        .split('_')
        .filter(|word| !word.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

/// Whether `ty` is `Option<T>`, by the last segment of its path.
fn is_option(ty: &Type) -> bool {
    let Type::Path(path) = ty else {
        return false;
    };
    if path.qself.is_some() {
        return false;
    }
    path.path.last().is_some_and(|segment| {
        segment.ident == "Option"
            && matches!(
                &segment.args,
                PathArguments::AngleBracketed(arguments)
                    if arguments.args.len() == 1
                        && matches!(arguments.args.first(), Some(GenericArgument::Type(_)))
            )
    })
}

/// The columns of `item`, in field order, or the first error.
///
/// `derive` names the derive in messages.
pub fn columns(item: &ItemStruct, derive: &str) -> Result<Vec<Column>, ParseError> {
    let Some(named) = item.fields.as_named() else {
        return Err(ParseError::new(
            item.ident.span(),
            format!("`{derive}` needs a struct with named fields"),
        ));
    };
    let mut columns = Vec::new();
    let mut seen = HashSet::new();
    for field in &named.fields.inner {
        let Some(ident) = &field.ident else {
            return Err(ParseError::new(
                field.span(),
                format!("`{derive}` needs named fields"),
            ));
        };
        let name = match field.parse_meta::<ColumnName>(ATTRIBUTE)? {
            Some(ColumnName(name)) => name,
            None => LitStr::new(&column_name(&ident.to_string()), ident.span()),
        };
        if name.value().is_empty() {
            return Err(ParseError::new(
                name.span(),
                "a column name must not be empty",
            ));
        }
        if !seen.insert(name.value().to_owned()) {
            return Err(ParseError::new(
                name.span(),
                format!(
                    "column `{}` names two fields: each column must fill one field",
                    name.value()
                ),
            ));
        }
        columns.push(Column {
            ident: ident.clone(),
            name,
            optional: is_option(&field.ty),
        });
    }
    if columns.is_empty() {
        return Err(ParseError::new(
            item.ident.span(),
            format!("`{derive}` needs at least one field"),
        ));
    }
    Ok(columns)
}

/// Expands `#[derive(TableRow)]` on `item`.
pub fn expand(item: ItemStruct) -> Result<TokenStream, ParseError> {
    derive_impls(item, "TableRow", |columns| {
        vec![
            (
                moxy::template! { ::krabka_bdd::TableRow },
                table_row_body(columns),
            ),
            (
                moxy::template! { ::krabka_bdd::table::TableCells },
                table_cells_body(columns),
            ),
        ]
    })
}

/// One `#[automatically_derived] impl Trait for Item` per `(Trait, body)`
/// that `impls` builds from the struct's columns, with the struct's generics.
///
/// # Errors
///
/// Returns what [`columns`] rejects.
pub fn derive_impls(
    item: ItemStruct,
    derive: &str,
    impls: impl FnOnce(&[Column]) -> Vec<(TokenStream, TokenStream)>,
) -> Result<TokenStream, ParseError> {
    let columns = columns(&item, derive)?;
    let ItemStruct {
        ident, generics, ..
    } = item;
    let (impl_generics, type_generics, where_clause) = generics.split();
    let impls: Vec<TokenStream> = impls(&columns)
        .into_iter()
        .map(|(trait_path, body)| {
            moxy::template! {
                #[automatically_derived]
                impl {{ impl_generics }} {{ trait_path }} for {{ ident }} {{ type_generics }} {{ where_clause }} {
                    {{ body }}
                }
            }
        })
        .collect();
    Ok(moxy::template! { @for derived in &impls { {{ derived }} } })
}

/// The items of `impl TableRow`.
fn table_row_body(columns: &[Column]) -> TokenStream {
    moxy::template! {
            const COLUMNS: &'static [&'static str] = &[
                @for column in columns { {{ column.name }}, }
            ];

            fn from_row(
                headers: &[::std::string::String],
                cells: &[::std::string::String],
            ) -> ::core::result::Result<Self, ::krabka_bdd::TableError> {
                let row = ::krabka_bdd::__private::RowCells::new(headers, cells, Self::COLUMNS)?;
                ::core::result::Result::Ok(Self {
                    @for column in columns {
                        {{ column.ident }}: @if column.optional {
                            row.optional({{ column.name }})?
                        } @else {
                            row.required({{ column.name }})?
                        },
                    }
                })
            }
    }
}

/// The items of `impl TableCells`.
fn table_cells_body(columns: &[Column]) -> TokenStream {
    moxy::template! {
            fn cell(&self, column: &str) -> ::core::option::Option<::std::string::String> {
                match column {
                    @for column in columns {
                        {{ column.name }} => ::core::option::Option::Some(
                            @if column.optional {
                                ::krabka_bdd::__private::optional_cell(&self.{{ column.ident }})
                            } @else {
                                ::krabka_bdd::__private::cell(&self.{{ column.ident }})
                            }
                        ),
                    }
                    _ => ::core::option::Option::None,
                }
            }
    }
}

/// What `expand` makes of `source`, with any error as text, for the derive
/// tests.
#[cfg(test)]
pub fn expanded(
    expand: impl FnOnce(ItemStruct) -> Result<TokenStream, ParseError>,
    source: &str,
) -> Result<TokenStream, String> {
    let item: ItemStruct = moxy::parse!({ source }).map_err(|error| error.to_string())?;
    expand(item).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use {
        super::{column_name, expand, expanded},
        assert2::assert,
        moxy::token::TokenStream,
        rstest::rstest,
    };

    #[rstest]
    #[case::one_word("name", "name")]
    #[case::two_words("leader_epoch", "leader epoch")]
    #[case::raw_identifier("r#type", "type")]
    #[case::leading_underscore("_hidden", "hidden")]
    #[case::repeated_underscore("log__dir", "log dir")]
    fn column_name_spaces_words(#[case] ident: &str, #[case] expected: &str) {
        assert!(column_name(ident) == expected);
    }

    fn expand_source(source: &str) -> Result<TokenStream, String> { expanded(expand, source) }

    #[rstest]
    #[case::plain("struct Row { name: String, partitions: u32 }")]
    #[case::optional("struct Row { name: String, epoch: Option<i32> }")]
    #[case::qualified_option("struct Row { epoch: core::option::Option<i32> }")]
    #[case::renamed("struct Row { #[column(\"epoch\")] leader_epoch: i32 }")]
    #[case::generic("struct Row<T> { value: T }")]
    #[case::other_attributes("#[derive(Debug)] struct Row { #[doc = \"x\"] name: String }")]
    fn accepts_named_field_structs(#[case] source: &str) {
        assert!(let Ok(_) = expand_source(source));
    }

    #[rstest]
    #[case::tuple_struct("struct Row(String);", "`TableRow` needs a struct with named fields")]
    #[case::unit_struct("struct Row;", "`TableRow` needs a struct with named fields")]
    #[case::no_fields("struct Row {}", "`TableRow` needs at least one field")]
    #[case::bare_attribute(
        "struct Row { #[column] name: String }",
        "expected `#[column(\"name\")]`: one string literal"
    )]
    #[case::two_names(
        "struct Row { #[column(\"a\", \"b\")] name: String }",
        "expected `#[column(\"name\")]`: one string literal"
    )]
    #[case::not_a_literal(
        "struct Row { #[column(name)] name: String }",
        "expected `#[column(\"name\")]`: one string literal"
    )]
    #[case::empty_name(
        "struct Row { #[column(\"\")] name: String }",
        "a column name must not be empty"
    )]
    #[case::duplicate_name(
        "struct Row { leader_epoch: i32, #[column(\"leader epoch\")] epoch: i32 }",
        "column `leader epoch` names two fields: each column must fill one field"
    )]
    fn rejects_malformed_structs(#[case] source: &str, #[case] message: &str) {
        assert!(let Err(error) = expand_source(source));
        assert!(error == message);
    }
}
