//! Syntax and attribute helpers shared by the macros.

use moxy::{
    ast::{Field, ItemStruct, List, ParseError, Token, Type},
    token::{Ident, Spanner, TokenStream},
};

/// The named fields of a struct, or an error naming the derive.
pub(crate) fn named_fields<'a>(
    item: &'a ItemStruct,
    derive: &str,
) -> Result<&'a List<Field, Token![,]>, ParseError> {
    item.fields
        .as_named()
        .map(|named| &named.fields.inner)
        .ok_or_else(|| {
            ParseError::new(
                item.ident.span(),
                format!("`{derive}` needs a struct with named fields"),
            )
        })
}

/// A field's name, or an error naming the derive.
pub(crate) fn field_ident<'a>(field: &'a Field, derive: &str) -> Result<&'a Ident, ParseError> {
    field
        .ident
        .as_ref()
        .ok_or_else(|| ParseError::new(field.span(), format!("`{derive}` needs named fields")))
}

/// The single field of a newtype, or an error naming the derive.
pub(crate) fn inner_type(item: &ItemStruct, derive: &str) -> Result<Type, ParseError> {
    item.fields
        .as_unnamed()
        .filter(|unnamed| unnamed.fields.len() == 1)
        .and_then(|unnamed| unnamed.fields.first())
        .map(|field| field.ty.clone())
        .ok_or_else(|| {
            ParseError::new(
                item.ident.span(),
                format!("`{derive}` needs a tuple struct with exactly one field"),
            )
        })
}

/// Token text with every whitespace character removed.
pub(crate) fn compact(tokens: &(impl ToString + ?Sized)) -> String {
    tokens
        .to_string()
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect()
}

/// The value of a `key = <expression>` argument, as the expression's tokens.
pub(crate) struct Tokens(pub(crate) TokenStream);

impl moxy::ast::FromMeta for Tokens {
    fn from_meta(meta: &moxy::ast::Meta) -> Result<Self, ParseError> {
        match &meta.content {
            moxy::ast::MetaContent::Expr { expr, .. } => Ok(Self(expr.clone())),
            _ => Err(ParseError::new(
                meta.span(),
                "expected `key = <expression>`",
            )),
        }
    }
}

/// Locate an item body while preserving attributes, visibility and generics.
pub(crate) fn item_body(
    item: TokenStream,
    macro_name: &str,
    is_kind: fn(&moxy::token::TokenTree) -> bool,
) -> Result<(Vec<moxy::token::TokenTree>, usize), ParseError> {
    use moxy::token::Span;
    let tokens: Vec<_> = item.into_iter().collect();
    let body = tokens
        .iter()
        .any(is_kind)
        .then(|| {
            tokens
                .iter()
                .rposition(|token| token.as_group().is_some_and(|group| group.delim.is_brace()))
        })
        .flatten();
    match body {
        Some(body) => Ok((tokens, body)),
        None => Err(ParseError::new(
            tokens.first().map_or_else(Span::call_site, Spanner::span),
            format!("`#[{macro_name}]` needs a braced item of the expected kind"),
        )),
    }
}

pub(crate) fn named_body(
    item: TokenStream,
    macro_name: &str,
) -> Result<(Vec<moxy::token::TokenTree>, usize), ParseError> {
    item_body(item, macro_name, moxy::token::TokenTree::is_keyword_struct)
}
