//! `#[derive(ScenarioContext)]`: accessors and a `Default` for a scenario
//! context whose fields are `Given<T>` slots.
//!
//! A field with `#[introduced_by("Given ...")]` is a slot. It gets
//! `the_<field>()`, `the_<field>_mut()` and `set_<field>(value)`, and
//! `Default` builds it as `Given::new("<field>", "<step>")`. Any other field is
//! plain state, such as an entity registry or a stub clock: `Default` fills it
//! with its own `Default`, and it gets no accessors.

use moxy::{
    ast::{Attributed, FromMeta, ItemStruct, Meta, MetaContent, Parse, ParseError, Parser},
    token::{Ident, LitStr, Spanner, ToTokenStream, TokenStream, TokenTree},
};

/// The attribute that names the step introducing a slot.
const ATTRIBUTE: &str = "introduced_by";

/// The keywords a step named by `#[introduced_by]` may start with.
const KEYWORDS: &[&str] = &["Given ", "When ", "Then "];

/// The step that `#[introduced_by("...")]` names.
struct IntroducedBy(LitStr);

impl FromMeta for IntroducedBy {
    fn from_meta(meta: &Meta) -> Result<Self, ParseError> {
        let malformed = || {
            ParseError::new(
                meta.span(),
                "expected `#[introduced_by(\"Given ...\")]`: one string literal naming the step",
            )
        };
        let MetaContent::List(group) = &meta.content else {
            return Err(malformed());
        };
        let parser = Parser::from_tokens(&group.tokens);
        let step = <LitStr as Parse>::parse(&parser).map_err(|_| malformed())?;
        if !parser.is_empty() {
            return Err(malformed());
        }
        if !starts_with_keyword(step.value()) {
            return Err(ParseError::new(
                step.span(),
                format!(
                    "`#[introduced_by]` names the step with its keyword, such as `Given {}`",
                    step.value()
                ),
            ));
        }
        Ok(Self(step))
    }
}

/// Reports whether `step` starts with `Given`, `When` or `Then` and a space,
/// with step text after it.
fn starts_with_keyword(step: &str) -> bool {
    KEYWORDS.iter().any(|keyword| {
        step.strip_prefix(keyword)
            .is_some_and(|text| !text.trim().is_empty())
    })
}

/// Reports whether `ty`'s tokens name `Given<..>` as their outer type, as in
/// `Given<u32>` or `krabka_bdd::Given<u32>`.
fn names_given(ty: &TokenStream) -> bool {
    let trees: Vec<&TokenTree> = ty.iter().collect();
    let Some(open) = trees
        .iter()
        .position(|tree| matches!(tree, TokenTree::Punct(punct) if punct.as_str() == "<"))
    else {
        return false;
    };
    open.checked_sub(1)
        .and_then(|before| trees.get(before))
        .and_then(|tree| tree.text())
        .is_some_and(|text| text == "Given")
}

/// One field of the context: its name, its type and, for a slot, its step.
struct ContextField {
    ident: Ident,
    name: LitStr,
    ty: TokenStream,
    step: Option<LitStr>,
}

/// A slot field and the names and docs of its accessors.
struct Slot<'a> {
    field: &'a ContextField,
    the: Ident,
    the_mut: Ident,
    set: Ident,
    the_doc: LitStr,
    the_mut_doc: LitStr,
    set_doc: LitStr,
    panics_doc: LitStr,
}

impl<'a> Slot<'a> {
    fn new(field: &'a ContextField, step: &LitStr) -> Self {
        let name = field.ident.text();
        let span = field.ident.span();
        let ident = |prefix: &str, suffix: &str| {
            Ident::new(format!("{prefix}{name}{suffix}")).with_span(span)
        };
        let doc = |text: String| LitStr::new(&text, span);
        let step = step.value();
        Self {
            field,
            the: ident("the_", ""),
            the_mut: ident("the_", "_mut"),
            set: ident("set_", ""),
            the_doc: doc(format!(" Returns `{name}`, which `{step}` sets.")),
            the_mut_doc: doc(format!(" Returns `{name}` mutably; `{step}` sets it.")),
            set_doc: doc(format!(" Stores `{name}`, as the step `{step}` does.")),
            panics_doc: doc(format!(
                " Panics, naming `{name}` and its step, when no step has set it."
            )),
        }
    }
}

/// The fields of `item`, or the first error in declaration order.
fn context_fields(item: &ItemStruct) -> Result<Vec<ContextField>, ParseError> {
    let Some(named) = item.fields.as_named() else {
        return Err(ParseError::new(
            item.ident.span(),
            "`ScenarioContext` needs a struct with named fields: each field is one piece of \
             scenario state",
        ));
    };
    let mut fields = Vec::new();
    for field in named.fields.iter() {
        let Some(ident) = field.ident.clone() else {
            continue;
        };
        let ty = field.ty.to_token_stream();
        let step = field
            .parse_meta::<IntroducedBy>(ATTRIBUTE)?
            .map(|IntroducedBy(step)| step);
        if step.is_none() && names_given(&ty) {
            return Err(ParseError::new(
                ident.span(),
                format!(
                    "`Given` field `{}` needs `#[introduced_by(\"Given ...\")]`, naming the step \
                     that sets it",
                    ident.text()
                ),
            ));
        }
        fields.push(ContextField {
            name: LitStr::new(ident.text(), ident.span()),
            ident,
            ty,
            step,
        });
    }
    Ok(fields)
}

/// Expands `#[derive(ScenarioContext)]` on `item`.
pub fn expand(item: ItemStruct) -> Result<TokenStream, ParseError> {
    let fields = context_fields(&item)?;
    let slots: Vec<Slot<'_>> = fields
        .iter()
        .filter_map(|field| field.step.as_ref().map(|step| Slot::new(field, step)))
        .collect();

    let ItemStruct {
        vis,
        ident,
        generics,
        ..
    } = item;
    let (impl_generics, type_generics, where_clause) = generics.split();
    Ok(moxy::template! {
        #[automatically_derived]
        impl {{ impl_generics }} ::core::default::Default for {{ ident }} {{ type_generics }} {{ where_clause }} {
            fn default() -> Self {
                Self {
                    @for field in &fields {
                        {{ field.ident }}:
                        @if let Some(step) = &field.step {
                            ::krabka_bdd::Given::new({{ field.name }}, {{ step }}),
                        } @else {
                            ::core::default::Default::default(),
                        }
                    }
                }
            }
        }

        #[automatically_derived]
        impl {{ impl_generics }} {{ ident }} {{ type_generics }} {{ where_clause }} {
            @for slot in &slots {
                #[doc = {{ slot.the_doc }}]
                #[doc = ""]
                #[doc = " # Panics"]
                #[doc = ""]
                #[doc = {{ slot.panics_doc }}]
                #[must_use]
                #[track_caller]
                {{ vis }} fn {{ slot.the }}(&self)
                    -> &<{{ slot.field.ty }} as ::krabka_bdd::__private::ContextSlot>::Value
                {
                    ::krabka_bdd::__private::ContextSlot::slot(&self.{{ slot.field.ident }}).get()
                }

                #[doc = {{ slot.the_mut_doc }}]
                #[doc = ""]
                #[doc = " # Panics"]
                #[doc = ""]
                #[doc = {{ slot.panics_doc }}]
                #[must_use]
                #[track_caller]
                {{ vis }} fn {{ slot.the_mut }}(&mut self)
                    -> &mut <{{ slot.field.ty }} as ::krabka_bdd::__private::ContextSlot>::Value
                {
                    ::krabka_bdd::__private::ContextSlot::slot_mut(&mut self.{{ slot.field.ident }})
                        .get_mut()
                }

                #[doc = {{ slot.set_doc }}]
                {{ vis }} fn {{ slot.set }}(
                    &mut self,
                    value: <{{ slot.field.ty }} as ::krabka_bdd::__private::ContextSlot>::Value,
                ) {
                    ::krabka_bdd::__private::ContextSlot::slot_mut(&mut self.{{ slot.field.ident }})
                        .set(value);
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use {
        super::{expand, names_given, starts_with_keyword},
        crate::table_row::expanded,
        assert2::assert,
        moxy::token::TokenStream,
        rstest::rstest,
    };

    #[rstest]
    #[case::given("Given a topic with {n} partitions", true)]
    #[case::when("When an operator deletes the topic", true)]
    #[case::then("Then the topic is gone", true)]
    #[case::no_keyword("a topic with {n} partitions", false)]
    #[case::and("And a topic", false)]
    #[case::lowercase("given a topic", false)]
    #[case::keyword_only("Given ", false)]
    #[case::keyword_no_space("Givena topic", false)]
    fn starts_with_keyword_needs_a_keyword_and_text(#[case] step: &str, #[case] expected: bool) {
        assert!(starts_with_keyword(step) == expected);
    }

    #[rstest]
    #[case::bare("Given<u32>", true)]
    #[case::qualified("krabka_bdd::Given<String>", true)]
    #[case::nested_given("Vec<Given<u32>>", false)]
    #[case::other_generic("Option<u32>", false)]
    #[case::plain("u32", false)]
    fn names_given_reads_the_outer_type(#[case] ty: &str, #[case] expected: bool) {
        let tokens: TokenStream = ty.parse().expect("a type");
        assert!(names_given(&tokens) == expected);
    }

    fn expand_source(source: &str) -> Result<TokenStream, String> { expanded(expand, source) }

    #[rstest]
    #[case::slots("struct Ctx { #[introduced_by(\"Given a topic\")] topic: Given<u32> }")]
    #[case::mixed(
        "struct Ctx { #[introduced_by(\"Given a topic\")] topic: Given<u32>, registry: Vec<u32> }"
    )]
    #[case::when_step(
        "struct Ctx { #[introduced_by(\"When a producer appends\")] offset: Given<u64> }"
    )]
    #[case::generic("struct Ctx<T> { #[introduced_by(\"Given a value\")] value: Given<T> }")]
    #[case::empty("struct Ctx {}")]
    fn accepts_named_field_structs(#[case] source: &str) {
        assert!(let Ok(_) = expand_source(source));
    }

    #[rstest]
    #[case::tuple(
        "struct Ctx(Given<u32>);",
        "`ScenarioContext` needs a struct with named fields: each field is one piece of \
         scenario state"
    )]
    #[case::unit(
        "struct Ctx;",
        "`ScenarioContext` needs a struct with named fields: each field is one piece of \
         scenario state"
    )]
    #[case::given_without_step(
        "struct Ctx { topic: krabka_bdd::Given<u32> }",
        "`Given` field `topic` needs `#[introduced_by(\"Given ...\")]`, naming the step that \
         sets it"
    )]
    #[case::bare_attribute(
        "struct Ctx { #[introduced_by] topic: Given<u32> }",
        "expected `#[introduced_by(\"Given ...\")]`: one string literal naming the step"
    )]
    #[case::two_literals(
        "struct Ctx { #[introduced_by(\"Given a\", \"Given b\")] topic: Given<u32> }",
        "expected `#[introduced_by(\"Given ...\")]`: one string literal naming the step"
    )]
    #[case::not_a_literal(
        "struct Ctx { #[introduced_by(topic)] topic: Given<u32> }",
        "expected `#[introduced_by(\"Given ...\")]`: one string literal naming the step"
    )]
    #[case::no_keyword(
        "struct Ctx { #[introduced_by(\"a topic\")] topic: Given<u32> }",
        "`#[introduced_by]` names the step with its keyword, such as `Given a topic`"
    )]
    fn rejects_malformed_contexts(#[case] source: &str, #[case] message: &str) {
        assert!(let Err(error) = expand_source(source));
        assert!(error == message);
    }
}
