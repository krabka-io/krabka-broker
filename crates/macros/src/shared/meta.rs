//! Syntax and attribute helpers shared by the macros.

use {
    moxy::{
        ast::{
            Attributes, Field, FnParam, FromMeta, ItemFn, ItemStruct, List, Meta, MetaContent,
            Parse, ParseError, Parser, Path, PathArguments, Pattern, Signature, Token, Visibility,
        },
        token::{Group, Ident, Span, Spanner, TokenStream, TokenTree},
    },
    std::mem,
};

#[cfg(test)]
pub struct RejectedStruct<'a> {
    pub(crate) label: &'a str,
    pub(crate) declaration: &'a str,
}

#[cfg(test)]
pub fn assert_rejected_structs(
    cases: &[RejectedStruct<'_>],
    expand: impl Fn(ItemStruct) -> Result<TokenStream, ParseError>,
) {
    for case in cases {
        let item: ItemStruct = moxy::parse!({ case.declaration }).expect(case.label);
        assert2::assert!(expand(item).is_err(), "{}", case.label);
    }
}

/// A safe, synchronous Rust signature without constness or generic parameters.
pub fn is_plain_sync(signature: &Signature) -> bool {
    signature.asyncness.is_none() && has_plain_qualifiers(signature)
}

pub fn has_plain_qualifiers(signature: &Signature) -> bool {
    signature.constness.is_none()
        && signature.safety.is_none()
        && signature.abi.is_none()
        && signature.generics.params.is_empty()
}

pub fn first_missing_named_field<'a>(
    fields: &List<Field, Token![,]>,
    required: &[&'a str],
) -> Option<&'a str> {
    required.iter().copied().find(|required| {
        !fields
            .iter()
            .any(|field| field.ident.as_ref().is_some_and(|name| name == *required))
    })
}

/// Read a nonempty external-schema table with the caller's domain error.
pub fn nonempty_paths(
    input: &TokenStream,
    message: &str,
) -> Result<List<Path, Token![,]>, ParseError> {
    let parser = Parser::from_tokens(input);
    let paths: List<Path, Token![,]> = List::parse_all(&parser)?;
    if paths.is_empty() {
        return Err(parser.error(message));
    }
    Ok(paths)
}

pub fn parameter_ident<'a>(parameter: &'a FnParam, message: &str) -> Result<&'a Ident, ParseError> {
    if let FnParam::Typed(parameter) = parameter
        && let Pattern::Ident(name) = parameter.pat.as_ref()
        && name.ident.text() != "_"
        && name.by_ref.is_none()
        && name.subpat.is_none()
    {
        Ok(&name.ident)
    } else {
        Err(ParseError::new(parameter.span(), message))
    }
}

/// Parse an ordinary function's header while keeping its operation body opaque.
pub fn function_signature(item: &TokenStream) -> Result<Signature, ParseError> {
    let parser = Parser::from_tokens(item);
    let _: Attributes = Parse::parse(&parser)?;
    let _: Visibility = Parse::parse(&parser)?;
    let signature = Signature::parse(&parser)?;
    finish_braced_body(&parser, "expected one function with a body")?;
    Ok(signature)
}

/// Parse a complete path without generic arguments for a concrete type or constant.
pub fn plain_path(tokens: &TokenStream, span: Span, message: &str) -> Result<Path, ParseError> {
    let parser = Parser::from_tokens(tokens);
    let path = Path::parse(&parser)?;
    if !parser.is_empty()
        || path.is_empty()
        || path
            .iter()
            .any(|segment| !matches!(segment.args, PathArguments::None))
    {
        return Err(ParseError::new(span, message));
    }
    Ok(path)
}

/// Consume the final native body without parsing its operations.
pub fn finish_braced_body(parser: &Parser, message: &str) -> Result<(), ParseError> {
    if !parser
        .advance()
        .is_some_and(|token| token.as_group().is_some_and(|group| group.delim.is_brace()))
        || !parser.is_empty()
    {
        return Err(parser.error(message));
    }
    Ok(())
}

/// Split a complete declaration's trailing metadata and operation body as native tokens.
pub struct BracketedFunction {
    pub(crate) header: proc_macro::TokenStream,
    pub(crate) metadata: proc_macro::TokenStream,
    pub(crate) body: proc_macro::Group,
}

pub fn bracketed_function(
    input: proc_macro::TokenStream,
    macro_name: &str,
) -> Result<BracketedFunction, ParseError> {
    use proc_macro::{Delimiter, TokenTree};

    let mut tokens: Vec<_> = input.into_iter().collect();
    let Some(TokenTree::Group(body)) = tokens.pop() else {
        return Err(ParseError::new(
            Span::call_site(),
            format!("{macro_name} needs a braced body"),
        ));
    };
    if body.delimiter() != Delimiter::Brace {
        return Err(ParseError::new(
            body.span().into(),
            format!("{macro_name} needs a braced body"),
        ));
    }
    let Some(TokenTree::Group(metadata)) = tokens.pop() else {
        return Err(ParseError::new(
            body.span().into(),
            format!("{macro_name} needs bracketed metadata before its body"),
        ));
    };
    if metadata.delimiter() != Delimiter::Bracket {
        return Err(ParseError::new(
            metadata.span().into(),
            format!("{macro_name} needs bracketed metadata before its body"),
        ));
    }
    Ok(BracketedFunction {
        header: tokens.into_iter().collect(),
        metadata: metadata.stream(),
        body,
    })
}

/// Wrap generated statements as one native brace expression.
pub fn native_expression(body: proc_macro::TokenStream) -> proc_macro::TokenStream {
    proc_macro::TokenStream::from_iter([proc_macro::TokenTree::Group(proc_macro::Group::new(
        proc_macro::Delimiter::Brace,
        body,
    ))])
}

/// Read a named declaration clause and retain its identifier span for errors.
pub fn expect_name(parser: &Parser, expected: &str, message: &str) -> Result<Ident, ParseError> {
    let name = Ident::parse(parser)?;
    if name != expected {
        return Err(ParseError::new(name.span(), message));
    }
    Ok(name)
}

/// Read an opaque clause group; each caller validates its delimiter and emptiness.
pub fn grouped_value(parser: &Parser, message: &str) -> Result<Group, ParseError> {
    let token = parser.advance().ok_or_else(|| parser.error(message))?;
    token
        .as_group()
        .cloned()
        .ok_or_else(|| ParseError::new(token.span(), message))
}

/// The named fields of a struct, or an error naming the derive.
pub fn named_fields<'a>(
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
pub fn field_ident<'a>(field: &'a Field, derive: &str) -> Result<&'a Ident, ParseError> {
    field
        .ident
        .as_ref()
        .ok_or_else(|| ParseError::new(field.span(), format!("`{derive}` needs named fields")))
}

/// Token text with every whitespace character removed.
pub fn compact(tokens: &(impl ToString + ?Sized)) -> String {
    tokens
        .to_string()
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect()
}

/// The value of a `key = <expression>` argument, as the expression's tokens.
pub struct Tokens {
    pub(crate) tokens: TokenStream,
}

pub fn expression_tokens(meta: &Meta, expected: &'static str) -> Result<TokenStream, ParseError> {
    match &meta.content {
        MetaContent::Expr { expr, .. } => Ok(expr.clone()),
        _ => Err(ParseError::new(meta.span(), expected)),
    }
}

pub fn parenthesized_list(
    meta: &Meta,
    expected: &str,
    delimiter: &str,
) -> Result<(TokenStream, Span), ParseError> {
    let MetaContent::List(group) = &meta.content else {
        return Err(ParseError::new(meta.span(), expected));
    };
    if !group.delim.is_paren() {
        return Err(ParseError::new(group.span(), delimiter));
    }
    Ok((group.tokens.clone(), group.span()))
}

impl FromMeta for Tokens {
    fn from_meta(meta: &Meta) -> Result<Self, ParseError> {
        expression_tokens(meta, "expected `key = <expression>`").map(|tokens| Self { tokens })
    }
}

/// An item's complete tokens and the index of its brace-delimited body.
pub struct ItemBody {
    pub(crate) tokens: Vec<TokenTree>,
    pub(crate) body_index: usize,
}

/// The braced body group that [`item_body`] located at `index`.
pub fn body_group(tokens: &mut [TokenTree], index: usize) -> Result<&mut Group, ParseError> {
    match tokens.get_mut(index) {
        Some(TokenTree::Group(group)) => Ok(group),
        _ => Err(ParseError::new(
            Span::call_site(),
            "expected a braced item body",
        )),
    }
}

/// Locate an item body while preserving attributes, visibility and generics.
pub fn item_body(
    item: TokenStream,
    macro_name: &str,
    is_kind: fn(&TokenTree) -> bool,
) -> Result<ItemBody, ParseError> {
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
        Some(body) => Ok(ItemBody {
            tokens,
            body_index: body,
        }),
        None => Err(ParseError::new(
            tokens.first().map_or_else(Span::call_site, Spanner::span),
            format!("`#[{macro_name}]` needs a braced item of the expected kind"),
        )),
    }
}

pub fn function_header(
    item: TokenStream,
    macro_name: &str,
    expected: &str,
) -> Result<ItemFn, ParseError> {
    let span = item.span();
    let ItemBody {
        mut tokens,
        body_index: body,
    } = item_body(item, macro_name, TokenTree::is_keyword_fn)?;
    if body + 1 != tokens.len() {
        return Err(ParseError::new(span, expected));
    }
    let group = body_group(&mut tokens, body)?;
    group.tokens = TokenStream::new();
    moxy::parse!({ TokenStream::from(tokens) })
}

pub fn named_body(item: TokenStream, macro_name: &str) -> Result<ItemBody, ParseError> {
    item_body(item, macro_name, TokenTree::is_keyword_struct)
}

/// Read one of the explicit modes accepted by a field-group attribute, as the
/// value paired with its name.
pub fn mode<T, const N: usize>(
    tokens: TokenStream,
    modes: [(&'static str, T); N],
) -> Result<T, ParseError> {
    let names = modes.each_ref().map(|(name, _)| *name);
    let tokens: Vec<_> = tokens.into_iter().collect();
    if let [TokenTree::Ident(name)] = tokens.as_slice()
        && let Some((_, value)) = modes.into_iter().find(|(mode, _)| name == *mode)
    {
        return Ok(value);
    }
    Err(ParseError::new(
        Span::call_site(),
        format!(
            "expected one of {}",
            names.map(|mode| format!("`{mode}`")).join(", ")
        ),
    ))
}

/// Wrap generated members in the declaration's original generics and where clause.
pub fn impl_block(
    item: ItemStruct,
    trait_prefix: &TokenStream,
    members: &TokenStream,
) -> TokenStream {
    let ident = item.ident;
    let (impl_generics, type_generics, where_clause) = item.generics.split();
    moxy::template! {
        impl {{ impl_generics }} {{ trait_prefix }} {{ ident }} {{ type_generics }} {{ where_clause }} {
            {{ members }}
        }
    }
}

/// Split nonempty top-level token arguments, preserving grouped expressions.
pub fn arguments(input: TokenStream, count: usize) -> Result<Vec<TokenStream>, ParseError> {
    let mut arguments = Vec::new();
    let mut current = TokenStream::new();
    for token in input {
        if token.is_punct_comma() {
            arguments.push(mem::take(&mut current));
        } else {
            current.extend([token]);
        }
    }
    arguments.push(current);
    if arguments.len() != count || arguments.iter().any(TokenStream::is_empty) {
        return Err(ParseError::new(
            Span::call_site(),
            format!("expected {count} nonempty comma-separated arguments"),
        ));
    }
    Ok(arguments)
}

/// Split exactly `N` nonempty top-level token arguments into an array.
pub fn argument_array<const N: usize>(input: TokenStream) -> Result<[TokenStream; N], ParseError> {
    arguments(input, N)?.try_into().map_err(|_| {
        ParseError::new(
            Span::call_site(),
            format!("expected {N} nonempty comma-separated arguments"),
        )
    })
}

/// Reject arguments on an attribute macro that accepts only its annotated item.
pub fn no_arguments(tokens: TokenStream, macro_name: &str) -> Result<(), ParseError> {
    if let Some(token) = tokens.into_iter().next() {
        return Err(ParseError::new(
            token.span(),
            format!("{macro_name} takes no arguments"),
        ));
    }
    Ok(())
}

/// One function-like macro's expansion entry point, for the export tests.
#[cfg(test)]
pub struct FunctionMacro {
    pub name: &'static str,
    pub expand: fn(TokenStream) -> Result<TokenStream, ParseError>,
}

/// The names of the macros in `macros` that accept empty input.
#[cfg(test)]
pub fn empty_input_accepted(macros: impl IntoIterator<Item = FunctionMacro>) -> Vec<&'static str> {
    macros
        .into_iter()
        .filter_map(|entry| {
            (entry.expand)(TokenStream::new())
                .is_ok()
                .then_some(entry.name)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert, moxy::token::ToTokenStream};

    #[yare::parameterized(
        first = { "strict", "strict" },
        second = { "open", "open" },
    )]
    fn modes_select_each_accepted_name(tokens: &str, expected: &str) {
        let modes = [("strict", "strict"), ("open", "open")];
        assert2::assert!(mode(tokens.parse().unwrap(), modes).unwrap() == expected);
    }

    #[yare::parameterized(
        unknown_name = { "lenient" },
        multiple_tokens = { "strict open" },
        empty = { "" },
        literal = { "\"strict\"" },
    )]
    fn modes_name_every_accepted_choice_when_rejecting(tokens: &str) {
        let error = mode(tokens.parse().unwrap(), [("strict", ()), ("open", ())])
            .expect_err("mode rejected");
        assert2::assert!(
            error.to_string() == "expected one of `strict`, `open`",
            "{tokens}"
        );
    }

    #[test]
    fn schema_tables_preserve_paths_and_domain_errors() {
        let paths = nonempty_paths(
            &"wire::First, ::wire::Second,".parse().unwrap(),
            "response schemas",
        )
        .unwrap();
        assert!(paths.len() == 2);
        assert!(compact(&paths[0].to_token_stream()) == "wire::First");
        assert!(compact(&paths[1].to_token_stream()) == "::wire::Second");
        let error = nonempty_paths(&TokenStream::new(), "response schemas")
            .err()
            .expect("empty schema table rejected");
        assert!(error.to_string().contains("response schemas"));
    }

    #[yare::parameterized(
        trailing_tokens = { "Type extra" },
        semicolon = { "Type;" },
        non_path_item = { "Type, 42" },
    )]
    fn schema_paths_reject_invalid_entries(invalid: &str) {
        assert2::assert!(nonempty_paths(&invalid.parse().unwrap(), "response schemas").is_err());
    }

    #[yare::parameterized(
        simple_type = { "Type" },
        qualified_type = { "crate::module::Type" },
        leading_colon = { "::module::VALUE" },
    )]
    fn concrete_paths_accept_plain_paths(valid: &str) {
        let tokens = valid.parse().unwrap();
        assert2::assert!(plain_path(&tokens, Span::call_site(), "concrete path").is_ok());
    }

    #[yare::parameterized(
        empty = { "" },
        generic = { "Type<T>" },
        sum_type = { "Type + Other" },
        call = { "Type::method()" },
    )]
    fn concrete_paths_reject_generics_and_trailing_expressions(invalid: &str) {
        let tokens = invalid.parse().unwrap();
        assert2::assert!(plain_path(&tokens, Span::call_site(), "concrete path").is_err());
    }

    #[test]
    fn named_clauses_keep_opaque_groups_and_reject_missing_values() {
        let tokens = "body { nested!(if ready { a() } else { b() }); }"
            .parse()
            .unwrap();
        let parser = Parser::from_tokens(&tokens);
        assert!(expect_name(&parser, "body", "body clause").unwrap() == "body");
        let body = grouped_value(&parser, "operation body").unwrap();
        assert!(body.delim.is_brace());
        assert!(compact(&body.tokens) == "nested!(ifready{a()}else{b()});");
        assert!(parser.is_empty());
        assert!(grouped_value(&parser, "missing value").is_err());
        let tokens = "wrong literal".parse().unwrap();
        let parser = Parser::from_tokens(&tokens);
        assert!(expect_name(&parser, "body", "body clause").is_err());
        assert!(grouped_value(&parser, "group expected").is_err());
    }
}
