//! Syntax and attribute helpers: the ones both macro crates use, and the ones
//! only this crate's macros use.

use moxy::{
    ast::{ItemStruct, ParseError, Type},
    token::{LitStr, Span, TokenStream, TokenTree},
};

#[path = "shared/meta.rs"]
mod shared;

pub use shared::*;

/// The single field of a newtype, or an error naming the derive.
pub fn inner_type(item: &ItemStruct, derive: &str) -> Result<Type, ParseError> {
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

/// Whether `tokens` declare a function called `name` at their top level. Raw
/// identifiers match their unprefixed name.
pub fn declares_fn(tokens: &[TokenTree], name: &str) -> bool {
    tokens.windows(2).any(|pair| {
        matches!(pair, [keyword, item] if keyword.is_keyword_fn()
            && item.as_ident().is_some_and(|item| item.text() == name))
    })
}

/// Transform a named struct's brace-delimited body and preserve the item header.
pub fn edit_named_body(
    item: TokenStream,
    macro_name: &str,
    edit: impl FnOnce(TokenStream) -> Result<TokenStream, ParseError>,
) -> Result<TokenStream, ParseError> {
    let ItemBody {
        mut tokens,
        body_index: body,
    } = named_body(item, macro_name)?;
    let group = body_group(&mut tokens, body)?;
    group.tokens = edit(group.tokens.clone())?;
    Ok(tokens.into())
}

/// Require a nonempty token argument without changing its tokens or error span.
pub fn required_tokens(tokens: TokenStream, message: &str) -> Result<TokenStream, ParseError> {
    if tokens.is_empty() {
        Err(ParseError::new(Span::call_site(), message))
    } else {
        Ok(tokens)
    }
}

/// Select file/runtime documentation and attach it to the generated fields.
pub fn mode_docs<const N: usize>(
    meta: TokenStream,
    file: [&'static str; N],
    runtime: [&'static str; N],
) -> Result<[LitStr; N], ParseError> {
    let docs = mode(meta, [("file", file), ("runtime", runtime)])?;
    Ok(docs.map(|doc| LitStr::new(doc, Span::call_site())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[yare::parameterized(
        declared = { "fn other() {} async fn publish(&self) {}", true },
        raw_identifier = { "fn r#publish() {}", true },
        absent = { "fn publish_keyed() {}", false },
        not_after_fn = { "const publish: () = ();", false },
        nested = { "mod inner { fn publish() {} }", false },
        empty = { "", false },
    )]
    fn declared_functions_are_found_only_at_the_top_level(tokens: &str, expected: bool) {
        let tokens = Vec::from(tokens.parse::<TokenStream>().unwrap());
        assert2::assert!(declares_fn(&tokens, "publish") == expected);
    }
}
