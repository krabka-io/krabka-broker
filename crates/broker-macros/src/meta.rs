//! Syntax and attribute helpers: the ones both macro crates use, and the ones
//! only this crate's macros use.

use moxy::{
    ast::{Cursor, FnParam, Parse, ParseError, Parser, Path, Signature},
    token::{Span, Spanner, TokenStream},
};

#[path = "../../macros/src/shared/meta.rs"]
mod shared;

pub use shared::*;

pub fn parse_skip<T: Parse>(cursor: Cursor<'_>) -> Option<Cursor<'_>> {
    let parser = Parser::from_cursor(cursor);
    T::parse(&parser).ok()?;
    Some(parser.cursor())
}

/// The common broker request-handler signature, with explicit wire schemas.
pub fn broker_handler_signature(request: &Path, response: &Path) -> TokenStream {
    moxy::template! {
        pub(crate) async fn handle(
            broker: &crate::broker::Broker,
            req: {{ request }},
            _version: i16,
            ctx: &crate::handlers::RequestContext<'_>,
        ) -> Result<{{ response }}, crate::error::BrokerError>
    }
}

/// An async signature without constness, unsafety, an ABI or generic parameters.
pub fn is_plain_async(signature: &Signature) -> bool {
    signature.asyncness.is_some() && has_plain_qualifiers(signature)
}

/// A plain async signature that also has no where clause and no variadic
/// parameter, so its parameters are concrete.
pub fn is_concrete_async(signature: &Signature) -> bool {
    is_plain_async(signature)
        && signature.generics.where_clause.is_none()
        && signature.params.inner.variadic.is_none()
}

/// The signature's parameter at `index`, or `message` at the signature.
pub fn parameter_at<'a>(
    signature: &'a Signature,
    index: usize,
    message: &str,
) -> Result<&'a FnParam, ParseError> {
    signature
        .params
        .inner
        .inputs
        .get(index)
        .ok_or_else(|| ParseError::new(signature.span(), message))
}

/// Validate a concrete async declaration and return its named parameter types.
pub fn named_async_parameter_types(
    signature: &Signature,
    names: &[&str],
    message: &str,
) -> Result<Vec<String>, ParseError> {
    let parameters = &signature.params.inner.inputs;
    if !is_concrete_async(signature) || parameters.len() != names.len() {
        return Err(ParseError::new(signature.span(), message));
    }
    parameters
        .iter()
        .zip(names)
        .map(|(parameter, name)| {
            let FnParam::Typed(parameter) = parameter else {
                return Err(ParseError::new(signature.span(), message));
            };
            if !parameter.attrs.is_empty()
                || compact(&moxy::template! { {{ parameter.pat }} }) != *name
            {
                return Err(ParseError::new(signature.span(), message));
            }
            Ok(compact(&moxy::template! { {{ parameter.ty }} }))
        })
        .collect()
}

/// Collect top-level native declaration bodies without traversing caller operations.
pub fn native_braced_bodies(input: proc_macro::TokenStream) -> Vec<proc_macro::Group> {
    native_groups(input)
        .filter(|group| group.delimiter() == proc_macro::Delimiter::Brace)
        .collect()
}

/// Yield top-level native groups without looking inside a caller's expressions.
pub fn native_groups(input: proc_macro::TokenStream) -> impl Iterator<Item = proc_macro::Group> {
    input.into_iter().filter_map(|token| match token {
        proc_macro::TokenTree::Group(group) => Some(group),
        _ => None,
    })
}

/// The next validated native group, or an error naming the missing `what`.
pub fn next_group(
    groups: &mut impl Iterator<Item = proc_macro::Group>,
    what: &str,
) -> Result<proc_macro::Group, ParseError> {
    groups
        .next()
        .ok_or_else(|| ParseError::new(Span::call_site(), format!("expected {what}")))
}

/// Edit a validated item's final native body without rebuilding nested groups.
pub fn edit_native_body(
    item: proc_macro::TokenStream,
    edit: impl FnOnce(proc_macro::TokenStream) -> proc_macro::TokenStream,
) -> proc_macro::TokenStream {
    let mut tokens: Vec<_> = item.into_iter().collect();
    let Some(proc_macro::TokenTree::Group(body)) = tokens.pop() else {
        unreachable!("validated item body")
    };
    let mut replacement = proc_macro::Group::new(body.delimiter(), edit(body.stream()));
    replacement.set_span(body.span());
    tokens.push(proc_macro::TokenTree::Group(replacement));
    tokens.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    #[test]
    fn async_and_sync_policies_share_only_qualifier_checks() {
        let asynchronous = function_signature(&"async fn perform() {}".parse().unwrap()).unwrap();
        assert!(is_plain_async(&asynchronous));
        assert!(!is_plain_sync(&asynchronous));
        let synchronous = function_signature(&"fn perform() {}".parse().unwrap()).unwrap();
        assert!(is_plain_sync(&synchronous));
        assert!(!is_plain_async(&synchronous));
        let signature = function_signature(
            &"async fn perform() where Concrete: Trait {}"
                .parse()
                .unwrap(),
        )
        .unwrap();
        assert!(is_plain_async(&signature));
    }

    #[yare::parameterized(
        const_fn = { "const fn perform() {}" },
        unsafe_fn = { "unsafe fn perform() {}" },
        async_unsafe_fn = { "async unsafe fn perform() {}" },
        extern_fn = { "extern \"C\" fn perform() {}" },
        async_extern_fn = { "async extern \"C\" fn perform() {}" },
        generic_fn = { "fn perform<T>() {}" },
        async_generic_fn = { "async fn perform<T>() {}" },
    )]
    fn other_qualifiers_are_not_plain_sync_or_async(source: &str) {
        let signature = function_signature(&source.parse().unwrap()).unwrap();
        assert2::assert!(
            !is_plain_sync(&signature) && !is_plain_async(&signature),
            "{source}"
        );
    }

    #[yare::parameterized(
        plain = { "async fn perform(value: u8) {}", true },
        sync = { "fn perform(value: u8) {}", false },
        const_fn = { "const async fn perform() {}", false },
        unsafe_fn = { "async unsafe fn perform() {}", false },
        extern_fn = { "async extern \"C\" fn perform() {}", false },
        generic = { "async fn perform<T>(value: T) {}", false },
        where_clause = { "async fn perform() where Concrete: Trait {}", false },
        variadic = { "async fn perform(value: u8, ...) {}", false },
    )]
    fn concrete_async_rejects_qualifiers_generics_where_clauses_and_variadics(
        source: &str,
        expected: bool,
    ) {
        let signature = function_signature(&source.parse().unwrap()).unwrap();
        assert2::assert!(is_concrete_async(&signature) == expected, "{source}");
    }
}
