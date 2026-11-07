//! `#[before]` and `#[after]`: scenario hooks.
//!
//! The attribute keeps the `async fn` as written and registers a
//! `krabka_bdd::__private::Hook` for it with `inventory`. A `before` hook
//! takes `(scenario: &ScenarioSpec)`; an `after` hook takes
//! `(scenario: &ScenarioSpec, outcome: &Outcome)`. `tags = "..."` limits the
//! hook to scenarios whose tags match the expression, which is checked here
//! so a malformed one fails the build. `order = N` sorts the hooks, lowest
//! first.

use {
    krabka_bdd_model::model::parse_tag_expression,
    moxy::{
        ast::{FromMeta, ItemFn, Meta, ParseError},
        token::{Ident, Lit, LitStr, Spanner, TokenStream},
    },
    proc_macro::TokenStream as NativeTokenStream,
};

/// When a hook runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HookPhase {
    /// Before a scenario's first step.
    Before,
    /// After a scenario, whatever its outcome.
    After,
}

impl HookPhase {
    /// The attribute's name.
    const fn attribute(self) -> &'static str {
        match self {
            Self::Before => "before",
            Self::After => "after",
        }
    }

    /// How many parameters the hook function takes.
    const fn arity(self) -> usize {
        match self {
            Self::Before => 1,
            Self::After => 2,
        }
    }

    /// The signature the hook function needs, for messages.
    const fn signature(self) -> &'static str {
        match self {
            Self::Before => "`async fn name(scenario: &ScenarioSpec)`",
            Self::After => "`async fn name(scenario: &ScenarioSpec, outcome: &Outcome)`",
        }
    }
}

/// The arguments of `#[before(...)]` and `#[after(...)]`.
#[derive(Debug, Default, PartialEq, Eq, moxy::FromMeta)]
struct HookArgs {
    /// The tag expression a scenario's tags must match.
    #[meta(default)]
    tags: Option<String>,
    /// Where the hook sorts among the others, lowest first.
    #[meta(default)]
    order: Option<i32>,
}

/// The hook's arguments, its tag expression checked.
fn hook_args(phase: HookPhase, meta: &TokenStream) -> Result<HookArgs, ParseError> {
    if meta.is_empty() {
        return Ok(HookArgs::default());
    }
    let name = Ident::new(phase.attribute());
    let meta: Meta = moxy::parse!({ moxy::template! { {{ name }}({{ meta }}) } })?;
    let args = HookArgs::from_meta(&meta)?;
    if let Some(tags) = &args.tags {
        parse_tag_expression(tags).map_err(|error| {
            ParseError::new(
                meta.span(),
                format!("`tags = \"{tags}\"` is not a tag expression: {error}"),
            )
        })?;
    }
    Ok(args)
}

/// Checks that `item` is an `async fn` with the hook's parameters.
fn check_signature(phase: HookPhase, item: &ItemFn) -> Result<(), ParseError> {
    let parameters = item.sig.params.inner.inputs.len();
    if item.sig.asyncness.is_none()
        || parameters != phase.arity()
        || !item.sig.generics.params.is_empty()
        || !item.sig.output.is_default()
    {
        return Err(ParseError::new(
            item.sig.ident.span(),
            format!(
                "a `#[{}]` hook is {}, returning nothing",
                phase.attribute(),
                phase.signature()
            ),
        ));
    }
    Ok(())
}

/// The hook registration that follows `item`.
pub fn expand(
    phase: HookPhase,
    meta: &TokenStream,
    item: &TokenStream,
) -> Result<TokenStream, ParseError> {
    let args = hook_args(phase, meta)?;
    let item: ItemFn = moxy::parse!(item as ItemFn)?;
    check_signature(phase, &item)?;
    let target = &item.sig.ident;
    let span = target.span();
    let tags = args.tags.map(|tags| LitStr::new(&tags, span));
    let order = Lit::i32_suffixed(args.order.unwrap_or(0));
    let wrapper = Ident::new(format!("__krabka_bdd_hook_{}", target.text())).with_span(span);
    Ok(moxy::template! {
        const _: () = {
            fn {{ wrapper }}<'a>(
                scenario: &'a ::krabka_bdd::__private::ScenarioSpec,
                outcome: ::core::option::Option<&'a ::krabka_bdd::__private::Outcome>,
            ) -> ::core::pin::Pin<
                ::std::boxed::Box<dyn ::core::future::Future<Output = ()> + 'a>,
            > {
                @match (phase) {
                    HookPhase::Before => {
                        let _ = outcome;
                        ::std::boxed::Box::pin({{ target }}(scenario))
                    }
                    HookPhase::After => {
                        ::std::boxed::Box::pin(async move {
                            if let ::core::option::Option::Some(outcome) = outcome {
                                {{ target }}(scenario, outcome).await;
                            }
                        })
                    }
                }
            }

            ::krabka_bdd::__private::inventory::submit! {
                ::krabka_bdd::__private::Hook {
                    phase: @match (phase) {
                        HookPhase::Before => { ::krabka_bdd::__private::HookPhase::BeforeScenario },
                        HookPhase::After => { ::krabka_bdd::__private::HookPhase::AfterScenario },
                    },
                    tags: @match (&tags) {
                        Some(tags) => { ::core::option::Option::Some({{ tags }}) },
                        None => { ::core::option::Option::None },
                    },
                    order: {{ order }},
                    file: ::core::file!(),
                    line: ::core::line!(),
                    run: {{ wrapper }},
                }
            }
        };
    })
}

/// Expands the attribute after the caller's native tokens, so the hook
/// function keeps its own spans.
pub fn expand_native(
    phase: HookPhase,
    meta: &TokenStream,
    item: NativeTokenStream,
) -> Result<NativeTokenStream, ParseError> {
    let registration = expand(phase, meta, &item.clone().into())?;
    let mut tokens = item;
    tokens.extend(NativeTokenStream::from(registration));
    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use {
        super::{HookArgs, HookPhase, expand, hook_args},
        assert2::assert,
        moxy::token::{TokenStream, ToTokenStream},
        rstest::rstest,
    };

    fn tokens(source: &str) -> TokenStream {
        moxy::parse!({ source }).unwrap()
    }

    #[rstest]
    #[case::none("", HookArgs::default())]
    #[case::tags(
        r#"tags = "@docker and not @slow""#,
        HookArgs { tags: Some("@docker and not @slow".to_owned()), order: None }
    )]
    #[case::order("order = 10", HookArgs { tags: None, order: Some(10) })]
    #[case::negative_order("order = -3", HookArgs { tags: None, order: Some(-3) })]
    #[case::both(
        r#"tags = "@docker", order = 2"#,
        HookArgs { tags: Some("@docker".to_owned()), order: Some(2) }
    )]
    fn parses_hook_arguments(#[case] meta: &str, #[case] expected: HookArgs) {
        assert!(hook_args(HookPhase::Before, &tokens(meta)).ok() == Some(expected));
    }

    #[test]
    fn rejects_a_malformed_tag_expression() {
        let meta = tokens(r#"tags = "@docker and""#);
        assert!(let Err(error) = hook_args(HookPhase::After, &meta));
        assert!(
            error
                .to_string()
                .starts_with("`tags = \"@docker and\"` is not a tag expression: ")
        );
    }

    #[rstest]
    #[case::order_not_a_number(r#"order = "first""#)]
    #[case::tags_not_a_string("tags = docker")]
    fn rejects_malformed_arguments(#[case] meta: &str) {
        assert!(let Err(_) = hook_args(HookPhase::Before, &tokens(meta)));
    }

    #[rstest]
    #[case::sync_before(
        HookPhase::Before,
        "fn start(scenario: &ScenarioSpec) {}",
        "a `#[before]` hook is `async fn name(scenario: &ScenarioSpec)`, returning nothing"
    )]
    #[case::before_with_outcome(
        HookPhase::Before,
        "async fn start(scenario: &ScenarioSpec, outcome: &Outcome) {}",
        "a `#[before]` hook is `async fn name(scenario: &ScenarioSpec)`, returning nothing"
    )]
    #[case::after_without_outcome(
        HookPhase::After,
        "async fn stop(scenario: &ScenarioSpec) {}",
        "a `#[after]` hook is `async fn name(scenario: &ScenarioSpec, outcome: &Outcome)`, \
         returning nothing"
    )]
    #[case::returns_value(
        HookPhase::Before,
        "async fn start(scenario: &ScenarioSpec) -> u8 { 1 }",
        "a `#[before]` hook is `async fn name(scenario: &ScenarioSpec)`, returning nothing"
    )]
    fn rejects_other_signatures(
        #[case] phase: HookPhase,
        #[case] item: &str,
        #[case] message: &str,
    ) {
        assert!(let Err(error) = expand(phase, &TokenStream::new(), &tokens(item)));
        assert!(error.to_string() == message);
    }

    #[test]
    fn registers_a_before_hook() {
        let expanded = expand(
            HookPhase::Before,
            &tokens(r#"tags = "@docker", order = 10"#),
            &tokens("async fn start(scenario: &ScenarioSpec) {}"),
        );
        let expected = tokens(
            r#"
            const _: () = {
                fn __krabka_bdd_hook_start<'a>(
                    scenario: &'a ::krabka_bdd::__private::ScenarioSpec,
                    outcome: ::core::option::Option<&'a ::krabka_bdd::__private::Outcome>,
                ) -> ::core::pin::Pin<
                    ::std::boxed::Box<dyn ::core::future::Future<Output = ()> + 'a>,
                > {
                    let _ = outcome;
                    ::std::boxed::Box::pin(start(scenario))
                }

                ::krabka_bdd::__private::inventory::submit! {
                    ::krabka_bdd::__private::Hook {
                        phase: ::krabka_bdd::__private::HookPhase::BeforeScenario,
                        tags: ::core::option::Option::Some("@docker"),
                        order: 10i32,
                        file: ::core::file!(),
                        line: ::core::line!(),
                        run: __krabka_bdd_hook_start,
                    }
                }
            };
            "#,
        );
        assert!(
            expanded.map(|tokens| tokens.to_token_stream().to_string()).ok()
                == Some(expected.to_string())
        );
    }

    #[test]
    fn registers_an_after_hook() {
        let expanded = expand(
            HookPhase::After,
            &TokenStream::new(),
            &tokens("async fn stop(scenario: &ScenarioSpec, outcome: &Outcome) {}"),
        );
        let expected = tokens(
            r#"
            const _: () = {
                fn __krabka_bdd_hook_stop<'a>(
                    scenario: &'a ::krabka_bdd::__private::ScenarioSpec,
                    outcome: ::core::option::Option<&'a ::krabka_bdd::__private::Outcome>,
                ) -> ::core::pin::Pin<
                    ::std::boxed::Box<dyn ::core::future::Future<Output = ()> + 'a>,
                > {
                    ::std::boxed::Box::pin(async move {
                        if let ::core::option::Option::Some(outcome) = outcome {
                            stop(scenario, outcome).await;
                        }
                    })
                }

                ::krabka_bdd::__private::inventory::submit! {
                    ::krabka_bdd::__private::Hook {
                        phase: ::krabka_bdd::__private::HookPhase::AfterScenario,
                        tags: ::core::option::Option::None,
                        order: 0i32,
                        file: ::core::file!(),
                        line: ::core::line!(),
                        run: __krabka_bdd_hook_stop,
                    }
                }
            };
            "#,
        );
        assert!(
            expanded.map(|tokens| tokens.to_token_stream().to_string()).ok()
                == Some(expected.to_string())
        );
    }
}
