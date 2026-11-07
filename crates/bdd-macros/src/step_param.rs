//! `#[derive(StepParam)]`: `FromStr` and `Display` over a unit-variant enum's
//! step words.
//!
//! A variant's word is the kebab-case of its name, unless a
//! `#[step_param("word", "alias", ...)]` attribute names it. `Display` writes
//! the first word; `FromStr` accepts every word, and its error lists them all.

use {
    moxy::{
        ast::{Attributed, FromMeta, ItemEnum, List, Meta, MetaContent, ParseError, Parser, Token},
        token::{Ident, LitStr, Spanner, TokenStream},
    },
    std::{collections::HashSet, iter},
};

/// The attribute that names a variant's words.
const ATTRIBUTE: &str = "step_param";

/// A variant's words: the one `Display` writes, then the other words that
/// `FromStr` also accepts, in source order.
struct Words {
    primary: LitStr,
    aliases: Vec<LitStr>,
}

impl Words {
    fn iter(&self) -> impl Iterator<Item = &LitStr> {
        iter::once(&self.primary).chain(&self.aliases)
    }
}

impl FromMeta for Words {
    fn from_meta(meta: &Meta) -> Result<Self, ParseError> {
        let malformed = || {
            ParseError::new(
                meta.span(),
                "expected `#[step_param(\"word\", ...)]`: one or more string literals",
            )
        };
        let MetaContent::List(group) = &meta.content else {
            return Err(malformed());
        };
        let parser = Parser::from_tokens(&group.tokens);
        let list = List::<LitStr, Token![,]>::parse_all(&parser).map_err(|_| malformed())?;
        let mut words = list.iter().cloned();
        let primary = words.next().ok_or_else(malformed)?;
        Ok(Self {
            primary,
            aliases: words.collect(),
        })
    }
}

/// One variant: its name and its words.
struct StepVariant {
    ident: Ident,
    words: Words,
}

/// `ident` in kebab-case: `CompactAndDelete` becomes `compact-and-delete`.
///
/// A word starts at an uppercase letter that follows a lowercase letter or a
/// digit, and at the last uppercase letter of a run that a lowercase letter
/// follows, so `HTTPServer` becomes `http-server`. An underscore becomes a
/// hyphen.
fn kebab_case(ident: &str) -> String {
    let mut text = String::with_capacity(ident.len() + 4);
    let mut previous: Option<char> = None;
    let mut characters = ident.chars().peekable();
    while let Some(character) = characters.next() {
        let next = characters.peek().copied();
        let boundary = character == '_'
            || (character.is_uppercase()
                && previous.is_some_and(|before| {
                    before.is_lowercase()
                        || before.is_ascii_digit()
                        || (before.is_uppercase() && next.is_some_and(char::is_lowercase))
                }));
        if boundary && !text.is_empty() && !text.ends_with('-') {
            text.push('-');
        }
        if character != '_' {
            text.extend(character.to_lowercase());
        }
        previous = Some(character);
    }
    text
}

/// The index, in `words`, of the first word that an earlier word repeats.
fn first_duplicate<S: AsRef<str>>(words: &[S]) -> Option<usize> {
    let mut seen = HashSet::new();
    words.iter().position(|word| !seen.insert(word.as_ref()))
}

/// The variants of `item`, each with its words, or the first error in
/// declaration order.
fn step_variants(item: &ItemEnum) -> Result<Vec<StepVariant>, ParseError> {
    let mut variants = Vec::new();
    for variant in &item.variants.inner {
        if !variant.fields.is_unit() {
            return Err(ParseError::new(
                variant.fields.span(),
                "`StepParam` needs unit variants: a step word cannot carry fields",
            ));
        }
        let words = variant
            .parse_meta::<Words>(ATTRIBUTE)?
            .unwrap_or_else(|| Words {
                primary: LitStr::new(
                    &kebab_case(&variant.ident.to_string()),
                    variant.ident.span(),
                ),
                aliases: Vec::new(),
            });
        if let Some(empty) = words.iter().find(|word| word.value().is_empty()) {
            return Err(ParseError::new(
                empty.span(),
                "a step word must not be empty",
            ));
        }
        variants.push(StepVariant {
            ident: variant.ident.clone(),
            words,
        });
    }
    if variants.is_empty() {
        return Err(ParseError::new(
            item.ident.span(),
            "`StepParam` needs at least one variant",
        ));
    }
    Ok(variants)
}

/// Expands `#[derive(StepParam)]` on `item`.
pub fn expand(item: ItemEnum) -> Result<TokenStream, ParseError> {
    let variants = step_variants(&item)?;
    let accepted: Vec<&LitStr> = variants
        .iter()
        .flat_map(|variant| variant.words.iter())
        .collect();
    let values: Vec<&str> = accepted.iter().map(|word| word.value()).collect();
    if let Some(word) = first_duplicate(&values).and_then(|index| accepted.get(index)) {
        return Err(ParseError::new(
            word.span(),
            format!(
                "step word `{}` names two variants: each word must parse to one variant",
                word.value()
            ),
        ));
    }

    let ItemEnum {
        ident, generics, ..
    } = item;
    let type_name = LitStr::new(&ident.to_string(), ident.span());
    let (impl_generics, type_generics, where_clause) = generics.split();
    Ok(moxy::template! {
        #[automatically_derived]
        impl {{ impl_generics }} ::core::str::FromStr for {{ ident }} {{ type_generics }} {{ where_clause }} {
            type Err = ::krabka_bdd::StepParamError;

            fn from_str(value: &str) -> ::core::result::Result<Self, Self::Err> {
                const ACCEPTED: &[&str] = &[
                    @for word in &accepted { {{ word }}, }
                ];
                match value {
                    @for variant in &variants {
                        @for (index, word) in variant.words.iter().enumerate() {
                            @if index > 0 { | }
                            {{ word }}
                        }
                        => ::core::result::Result::Ok(Self::{{ variant.ident }}),
                    }
                    _ => ::core::result::Result::Err(
                        ::krabka_bdd::StepParamError::new({{ type_name }}, value, ACCEPTED),
                    ),
                }
            }
        }

        #[automatically_derived]
        impl {{ impl_generics }} ::core::fmt::Display for {{ ident }} {{ type_generics }} {{ where_clause }} {
            fn fmt(&self, f: &mut ::core::fmt::Formatter<'_>) -> ::core::fmt::Result {
                f.pad(match self {
                    @for variant in &variants {
                        Self::{{ variant.ident }} => {{ variant.words.primary }},
                    }
                })
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use {
        super::{expand, first_duplicate, kebab_case},
        assert2::assert,
        moxy::{ast::ItemEnum, token::TokenStream},
        rstest::rstest,
    };

    #[rstest]
    #[case::one_word("Delete", "delete")]
    #[case::two_words("LogCompaction", "log-compaction")]
    #[case::three_words("CompactAndDelete", "compact-and-delete")]
    #[case::acronym_then_word("HTTPServer", "http-server")]
    #[case::word_then_acronym("ReadTLS", "read-tls")]
    #[case::digit_boundary("V2Topic", "v2-topic")]
    #[case::trailing_digit("Version3", "version3")]
    #[case::underscore("Not_Camel", "not-camel")]
    #[case::single_letter("A", "a")]
    fn kebab_case_splits_words(#[case] ident: &str, #[case] expected: &str) {
        assert!(kebab_case(ident) == expected);
    }

    #[rstest]
    #[case::none(&["delete", "compact"], None)]
    #[case::adjacent(&["delete", "delete"], Some(1))]
    #[case::later(&["delete", "compact", "none", "compact"], Some(3))]
    #[case::first_repeat_wins(&["a", "b", "b", "a"], Some(2))]
    #[case::empty(&[], None)]
    fn first_duplicate_finds_the_repeat(#[case] words: &[&str], #[case] expected: Option<usize>) {
        assert!(first_duplicate(words) == expected);
    }

    fn expand_source(source: &str) -> Result<TokenStream, String> {
        let item: ItemEnum = moxy::parse!({ source }).map_err(|error| error.to_string())?;
        expand(item).map_err(|error| error.to_string())
    }

    #[rstest]
    #[case::default_words("enum Policy { Delete, LogCompaction }")]
    #[case::explicit_word("enum Policy { #[step_param(\"compact,delete\")] CompactAndDelete }")]
    #[case::aliases("enum Policy { #[step_param(\"none\", \"off\")] NoCleanup, Delete }")]
    #[case::discriminant("enum Policy { Delete = 1, Compact = 2 }")]
    #[case::other_attributes("#[repr(u8)] enum Policy { #[doc = \"x\"] Delete }")]
    fn accepts_unit_variant_enums(#[case] source: &str) {
        assert!(let Ok(_) = expand_source(source));
    }

    #[rstest]
    #[case::tuple_variant(
        "enum Policy { Delete(u8) }",
        "`StepParam` needs unit variants: a step word cannot carry fields"
    )]
    #[case::struct_variant(
        "enum Policy { Delete { after: u8 } }",
        "`StepParam` needs unit variants: a step word cannot carry fields"
    )]
    #[case::empty_enum("enum Policy {}", "`StepParam` needs at least one variant")]
    #[case::duplicate_default(
        "enum Policy { #[step_param(\"delete\")] Remove, Delete }",
        "step word `delete` names two variants: each word must parse to one variant"
    )]
    #[case::duplicate_alias(
        "enum Policy { #[step_param(\"none\", \"off\")] NoCleanup, #[step_param(\"off\")] Disabled }",
        "step word `off` names two variants: each word must parse to one variant"
    )]
    #[case::duplicate_within_variant(
        "enum Policy { #[step_param(\"none\", \"none\")] NoCleanup }",
        "step word `none` names two variants: each word must parse to one variant"
    )]
    #[case::bare_attribute(
        "enum Policy { #[step_param] Delete }",
        "expected `#[step_param(\"word\", ...)]`: one or more string literals"
    )]
    #[case::name_value(
        "enum Policy { #[step_param = \"delete\"] Delete }",
        "expected `#[step_param(\"word\", ...)]`: one or more string literals"
    )]
    #[case::empty_list(
        "enum Policy { #[step_param()] Delete }",
        "expected `#[step_param(\"word\", ...)]`: one or more string literals"
    )]
    #[case::not_a_literal(
        "enum Policy { #[step_param(delete)] Delete }",
        "expected `#[step_param(\"word\", ...)]`: one or more string literals"
    )]
    #[case::key_value(
        "enum Policy { #[step_param(name = \"delete\")] Delete }",
        "expected `#[step_param(\"word\", ...)]`: one or more string literals"
    )]
    #[case::empty_word(
        "enum Policy { #[step_param(\"\")] Delete }",
        "a step word must not be empty"
    )]
    fn rejects_malformed_enums(#[case] source: &str, #[case] message: &str) {
        assert!(let Err(error) = expand_source(source));
        assert!(error == message);
    }
}
