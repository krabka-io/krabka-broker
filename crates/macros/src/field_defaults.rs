//! `#[derive(FieldDefaults)]`: see the crate documentation.

use moxy::{
    ast::{Attributed, Field, ItemStruct, ParseError},
    token::{Spanner, TokenStream},
};

/// The expression inside a field's `#[default(...)]`.
struct DefaultExpr(TokenStream);

impl moxy::ast::FromMeta for DefaultExpr {
    fn from_meta(meta: &moxy::ast::Meta) -> Result<Self, ParseError> {
        match &meta.content {
            moxy::ast::MetaContent::List(group) if !group.tokens.is_empty() => {
                Ok(Self(group.tokens.clone()))
            }
            _ => Err(ParseError::new(
                meta.span(),
                "expected `#[default(<expression>)]`",
            )),
        }
    }
}

/// `name: <value>` for `field`: its `#[default(...)]` expression, or
/// `Default::default()`.
fn initializer(field: &Field) -> Result<TokenStream, ParseError> {
    let Some(ident) = &field.ident else {
        return Err(ParseError::new(
            field.span(),
            "`FieldDefaults` needs named fields",
        ));
    };
    let value = field.parse_meta::<DefaultExpr>("default")?.map_or_else(
        || moxy::template! { ::core::default::Default::default() },
        |DefaultExpr(value)| value,
    );
    Ok(moxy::template! { {{ ident }}: {{ value }}, })
}

/// Expands `#[derive(FieldDefaults)]` on `item`.
pub(crate) fn expand(item: ItemStruct) -> Result<TokenStream, ParseError> {
    let ident = item.ident;
    let Some(named) = item.fields.as_named() else {
        return Err(ParseError::new(
            ident.span(),
            "`FieldDefaults` needs a struct with named fields",
        ));
    };
    let initializers = named
        .fields
        .iter()
        .map(initializer)
        .collect::<Result<Vec<_>, _>>()?;

    let (impl_generics, type_generics, where_clause) = item.generics.split();
    Ok(moxy::template! {
        impl {{ impl_generics }} ::core::default::Default
            for {{ ident }} {{ type_generics }} {{ where_clause }}
        {
            fn default() -> Self {
                Self {
                    @for initializer in &initializers { {{ initializer }} }
                }
            }
        }
    })
}
