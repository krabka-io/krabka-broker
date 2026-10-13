//! Exhaustive wire constructors with protocol-only fields supplied once.

use moxy::{
    ast::{Expr, Parse, ParseError, Parser, Token},
    token::{Spanner, TokenStream, TokenTree},
};

pub(crate) fn expand(input: &TokenStream, unthrottled: bool) -> Result<TokenStream, ParseError> {
    let mut tokens = input.clone().into_inner();
    let Some(TokenTree::Group(body)) = tokens.last_mut() else {
        return Err(ParseError::new(input.span(), "expected a struct body"));
    };
    if !body.delim.is_brace() {
        return Err(ParseError::new(
            body.span(),
            "expected a brace-delimited struct body",
        ));
    }
    // Validate only the header; rustc handles closure patterns and other field expressions.
    let fields = std::mem::take(&mut body.tokens);
    let header = TokenStream::from(tokens.clone());
    let parser = Parser::from_tokens(&header);
    let Expr::Struct(_) = Expr::parse(&parser)? else {
        return Err(parser.error("expected an exhaustive struct literal"));
    };
    if !parser.is_empty() {
        return Err(parser.error("unexpected tokens after the struct literal"));
    }
    let field_parser = Parser::from_tokens(&fields);
    let mut field_start = true;
    while !field_parser.is_empty() {
        if field_start && <Token![..]>::peek(field_parser.cursor()) {
            return Err(field_parser
                .error("wire constructors require every field; struct updates are not allowed"));
        }
        field_start = field_parser
            .advance()
            .is_some_and(TokenTree::is_punct_comma);
    }

    let Some(TokenTree::Group(body)) = tokens.last_mut() else {
        return Err(parser.error("expected a brace-delimited struct body"));
    };
    let needs_comma = fields
        .iter()
        .last()
        .is_some_and(|last| !last.is_punct_comma());
    // Preserve expressions and opaque macro fragments instead of rebuilding them from the AST.
    body.tokens = moxy::template! {
        @if unthrottled { throttle_time_ms: 0, }
        {{ fields }}
        @if needs_comma { , }
        unknown_tagged_fields: ::krabka_protocol::UnknownTaggedFields::default(),
    };
    Ok(tokens.into())
}

#[cfg(test)]
mod tests {
    use {super::expand, assert2::assert, moxy::ast::Expr};

    #[test]
    fn rejects_updates_and_non_struct_inputs() {
        for input in [
            "Response { ..other }",
            "Response { version: 2, ..other }",
            "Response",
            "Response {} extra",
            "42",
        ] {
            assert!(expand(&input.parse().unwrap(), false).is_err(), "{input}");
        }
    }

    #[test]
    fn expansion_has_no_default_fallback_or_constructor_macros() {
        for unthrottled in [false, true] {
            let input = "module::Response { version: 2, error_code }"
                .parse()
                .unwrap();
            let output = expand(&input, unthrottled).unwrap();
            let Expr::Struct(value) = moxy::parse!({ output.clone() } as Expr).unwrap() else {
                panic!("constructor must emit a struct literal")
            };
            assert!(value.body.rest.is_none());
            assert!(value.body.fields.len() == if unthrottled { 4 } else { 3 });
            let text = output.to_string();
            assert!(!text.contains("tagged_wire"));
            assert!(!text.contains("macro_rules"));
        }
    }

    #[test]
    fn nested_struct_updates_are_field_expressions() {
        let input = "Response { value: Inner { ..other } }".parse().unwrap();
        assert!(expand(&input, false).is_ok());
    }
}
