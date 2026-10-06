//! `#[krabka_env]`: see the crate documentation.

use moxy::{
    ast::{Attribute, Fields, ItemStruct, ParseError},
    token::{LitStr, Spanner, ToTokenStream, TokenStream},
};

/// The name of the environment variable that sets the flag of `field`.
fn env_name(field: &str) -> String {
    format!("KRABKA_{}", field.to_ascii_uppercase())
}

/// The arguments after `long, env = ...` that a field of type `ty` gets, as
/// tokens, or `None` when no type-derived default exists for `ty`.
///
/// `ty` is the field type with its whitespace removed. Each entry is the
/// parser that most fields of that type already used; a field of the type
/// that needs another one keeps its own `#[arg(...)]`.
fn type_arguments(ty: &str) -> Option<TokenStream> {
    Some(match ty {
        "Option<Time>" => moxy::template! { value_parser = ::krabka_units::parse::positive_time },
        "Option<ByteSize>" => {
            moxy::template! { value_parser = ::krabka_units::parse::positive_byte_size }
        }
        "Option<Ratio>" => moxy::template! { value_parser = ::krabka_units::parse::positive_ratio },
        "Option<PositiveCount>" => moxy::template! {
            value_parser = ::krabka_broker::config_value::parse_positive_count
        },
        "Option<PositiveI16>" => moxy::template! {
            value_parser = ::krabka_broker::config_value::parse_positive_i16
        },
        "Option<PositiveI32>" => moxy::template! {
            value_parser = ::krabka_broker::config_value::parse_positive_i32
        },
        "Option<PositiveI64>" => moxy::template! {
            value_parser = ::krabka_broker::config_value::parse_positive_i64
        },
        "Option<u32>" => moxy::template! { value_parser = ::clap::value_parser!(u32).range(1..) },
        "Option<i64>" => moxy::template! { value_parser = ::clap::value_parser!(i64).range(0..) },
        "Option<bool>" => moxy::template! { action = ::clap::ArgAction::Set },
        "Option<i16>" | "Option<i32>" | "Option<String>" => TokenStream::new(),
        _ => return None,
    })
}

/// The type of a field as written, without whitespace.
fn type_key(ty: &impl ToTokenStream) -> String {
    ty.to_token_stream()
        .to_string()
        .chars()
        .filter(|c| !c.is_whitespace())
        .collect()
}

/// Expands `#[krabka_env]` on `item`.
pub(crate) fn expand(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    if let Some(argument) = meta.into_iter().next() {
        return Err(ParseError::new(
            argument.span(),
            "`krabka_env` takes no arguments",
        ));
    }
    let mut item = moxy::parse!({ item } as ItemStruct)?;
    let Fields::Named(named) = &mut item.fields else {
        return Err(ParseError::new(
            item.ident.span(),
            "`krabka_env` needs a struct with named fields",
        ));
    };
    for field in &mut named.fields.inner {
        if field.attrs.iter().any(|attr| attr.path.is_ident("arg")) {
            continue;
        }
        let Some(ident) = field.ident.as_ref() else {
            continue;
        };
        let ty = type_key(&field.ty);
        let Some(arguments) = type_arguments(&ty) else {
            return Err(ParseError::new(
                field.ty.span(),
                format!(
                    "`krabka_env` has no value parser for `{ty}`; give the field its own `#[arg(...)]`"
                ),
            ));
        };
        let env = LitStr::new(&env_name(ident.text()), ident.span());
        let attribute = moxy::template! {
            #[arg(long, env = {{ env }} @if !arguments.is_empty() { , {{ arguments }} })]
        };
        field.attrs.push(moxy::parse!(attribute as Attribute)?);
    }
    Ok(item.to_token_stream())
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::{env_name, type_arguments};

    #[test]
    fn env_name_is_the_upper_case_field_name_under_krabka() {
        assert!(env_name("cleaner_interval") == "KRABKA_CLEANER_INTERVAL");
    }

    #[test]
    fn types_without_a_single_parser_have_no_default() {
        for ty in ["usize", "Option<usize>", "Option<Vec<String>>", "Time"] {
            assert!(type_arguments(ty).is_none(), "{ty}");
        }
    }
}
