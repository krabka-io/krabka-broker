//! `#[derive(Overrides)]`: set a struct's fields by name from step text, for
//! the Validation Tester.
//!
//! Field names map as `#[derive(TableRow)]` columns do, through
//! [`columns`]. `-` sets an `Option` field to `None`.

use {
    crate::table_row::columns,
    moxy::{
        ast::{ItemStruct, ParseError},
        token::TokenStream,
    },
};

/// Expands `#[derive(Overrides)]` on `item`.
pub fn expand(item: ItemStruct) -> Result<TokenStream, ParseError> {
    let fields = columns(&item, "Overrides")?;
    let ItemStruct {
        ident, generics, ..
    } = item;
    let (impl_generics, type_generics, where_clause) = generics.split();
    Ok(moxy::template! {
        #[automatically_derived]
        impl {{ impl_generics }} ::krabka_bdd::Overrides for {{ ident }} {{ type_generics }} {{ where_clause }} {
            const FIELDS: &'static [&'static str] = &[
                @for field in &fields { {{ field.name }}, }
            ];

            fn apply(
                &mut self,
                field: &str,
                value: &str,
            ) -> ::core::result::Result<(), ::krabka_bdd::TableError> {
                match field {
                    @for field in &fields {
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
    })
}

#[cfg(test)]
mod tests {
    use {
        super::expand,
        assert2::assert,
        moxy::{ast::ItemStruct, token::TokenStream},
        rstest::rstest,
    };

    fn expand_source(source: &str) -> Result<TokenStream, String> {
        let item: ItemStruct = moxy::parse!({ source }).map_err(|error| error.to_string())?;
        expand(item).map_err(|error| error.to_string())
    }

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
