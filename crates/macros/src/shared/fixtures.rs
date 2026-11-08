//! Fixture-generator argument helpers that both macro crates use.

use {
    crate::meta::argument_array,
    moxy::{
        ast::ParseError,
        token::{Ident, Span, TokenStream, TokenTree},
    },
};

/// Top-level comma splitting that retains the input's diagnostic span.
pub struct CommaArguments {
    pub(crate) span: Span,
    parts: Vec<Vec<TokenTree>>,
}

impl CommaArguments {
    pub(crate) fn new(input: TokenStream) -> Self {
        let span = input.span();
        let tokens = Vec::from(input);
        Self {
            span,
            parts: tokens
                .split(TokenTree::is_punct_comma)
                .map(<[TokenTree]>::to_vec)
                .collect(),
        }
    }

    pub(crate) fn parts(&self) -> Vec<&[TokenTree]> {
        self.parts.iter().map(Vec::as_slice).collect()
    }
}

pub fn name(tokens: TokenStream) -> Result<Ident, ParseError> {
    let tokens: Vec<_> = tokens.into_iter().collect();
    match tokens.as_slice() {
        [TokenTree::Ident(name)] => Ok(name.clone()),
        _ => Err(ParseError::new(
            Span::call_site(),
            "fixture generator needs one item name",
        )),
    }
}

/// Build named fixture items after validating the generator's item name.
pub fn named_items(
    input: TokenStream,
    build: impl FnOnce(Ident) -> TokenStream,
) -> Result<TokenStream, ParseError> {
    Ok(build(name(input)?))
}

/// Parse the common item-name and crate-path pair of fixture generators.
pub fn named_root(input: TokenStream) -> Result<(Ident, TokenStream), ParseError> {
    let [name, root]: [TokenStream; 2] = argument_array(input)?;
    Ok((self::name(name)?, root))
}

/// Assemble a fixture function while leaving its signature and body explicit.
pub fn function(
    input: TokenStream,
    visibility: &TokenStream,
    parameters: &TokenStream,
    returns: &TokenStream,
    body: &TokenStream,
) -> Result<TokenStream, ParseError> {
    let name = self::name(input)?;
    Ok(moxy::template! {
        {{ visibility }} fn {{ name }}({{ parameters }}) -> {{ returns }} {
            {{ body }}
        }
    })
}
