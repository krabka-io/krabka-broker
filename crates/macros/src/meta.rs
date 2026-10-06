//! Attribute-argument helpers shared by the macros.

use moxy::{
    ast::ParseError,
    token::{Spanner, TokenStream},
};

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

/// The tokens inside a `key(...)` argument.
pub(crate) struct Parenthesized(pub(crate) TokenStream);

impl moxy::ast::FromMeta for Parenthesized {
    fn from_meta(meta: &moxy::ast::Meta) -> Result<Self, ParseError> {
        match &meta.content {
            moxy::ast::MetaContent::List(group) if !group.tokens.is_empty() => {
                Ok(Self(group.tokens.clone()))
            }
            _ => Err(ParseError::new(meta.span(), "expected `key(...)`")),
        }
    }
}

