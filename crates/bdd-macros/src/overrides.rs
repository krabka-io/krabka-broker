//! `#[derive(Overrides)]`: set a struct's fields by name from step text, for
//! the Validation Tester.
//!
//! Field names map as `#[derive(TableRow)]` columns do, through
//! [`columns`]. `-` sets an `Option` field to `None`.

use {
    crate::table_row::{Column, derive_impls},
    moxy::{
        ast::{ItemStruct, ParseError},
        token::TokenStream,
    },
};

/// Expands `#[derive(Overrides)]` on `item`.
pub fn expand(item: ItemStruct) -> Result<TokenStream, ParseError> {
    derive_impls(item, "Overrides", |fields| {
        vec![(
            moxy::template! { ::krabka_bdd::Overrides },
            overrides_body(fields),
        )]
    })
}

/// The items of `impl Overrides`.
fn overrides_body(fields: &[Column]) -> TokenStream {
    moxy::template! {
            const FIELDS: &'static [&'static str] = &[
                @for field in fields { {{ field.name }}, }
            ];

            fn apply(
                &mut self,
                field: &str,
                value: &str,
            ) -> ::core::result::Result<(), ::krabka_bdd::TableError> {
                match field {
                    @for field in fields {
                        {{ field.name }} => {
                            self.{{ field.ident }} = @if field.optional {
                                ::krabka_bdd::__private::optional_field({{ field.name }}, value)?
                            } @else {
                                ::krabka_bdd::__private::field({{ field.name }}, value)?
                            };
                        }
                    }
                    _ => {
                        return ::core::result::Result::Err(
                            ::krabka_bdd::__private::unknown_field(field, Self::FIELDS),
                        );
                    }
                }
                ::core::result::Result::Ok(())
            }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::expand, crate::table_row::expanded, assert2::assert, moxy::token::TokenStream,
        rstest::rstest,
    };

    fn expand_source(source: &str) -> Result<TokenStream, String> { expanded(expand, source) }

    #[rstest]
    #[case::plain("struct Request { name: String, partitions: u32 }")]
    #[case::optional("struct Request { retention_ms: Option<u64> }")]
    #[case::renamed("struct Request { #[column(\"rf\")] replication_factor: u16 }")]
    fn accepts_named_field_structs(#[case] source: &str) {
        assert!(let Ok(_) = expand_source(source));
    }

    #[rstest]
    #[case::tuple_struct("struct Request(u32);", "`Overrides` needs a struct with named fields")]
    #[case::no_fields("struct Request {}", "`Overrides` needs at least one field")]
    #[case::duplicate_name(
        "struct Request { a_b: u32, #[column(\"a b\")] ab: u32 }",
        "column `a b` names two fields: each column must fill one field"
    )]
    fn rejects_malformed_structs(#[case] source: &str, #[case] message: &str) {
        assert!(let Err(error) = expand_source(source));
        assert!(error == message);
    }
}
