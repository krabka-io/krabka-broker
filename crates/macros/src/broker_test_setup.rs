//! Bind retained broker resources with explicit starter, readiness and optional caller policy.

use {
    crate::{
        meta::{expect_name, grouped_value, native_groups, next_group},
        request_test_identity::user_bindings,
        sweep_loop::{SpliceValue, splice},
    },
    moxy::{
        ast::{List, Parse, ParseError, Parser, Path, Token},
        token::{Ident, Spanner, TokenStream, TokenTree},
    },
    proc_macro::{Delimiter, Group, TokenStream as NativeTokenStream},
};

struct Setup {
    handle: Ident,
    directory: Ident,
    broker: Ident,
    /// The optional context bindings after the three broker bindings.
    context: Vec<Ident>,
    policy: Option<String>,
    ready: bool,
    leader: bool,
    user: UserPolicy,
}

enum UserPolicy {
    Absent,
    Default,
    Client,
}

fn nonempty_bracketed(token: &TokenTree, message: &str) -> Result<(), ParseError> {
    if token
        .as_group()
        .is_some_and(|group| group.delim.is_bracket() && !group.tokens.is_empty())
    {
        Ok(())
    } else {
        Err(ParseError::new(token.span(), message))
    }
}

fn declaration(input: &TokenStream) -> Result<Setup, ParseError> {
    let parser = Parser::from_tokens(input);
    expect_name(
        &parser,
        "bind",
        "expected broker bindings and optional context or principal/peer/context",
    )?;
    let group = grouped_value(&parser, "expected broker binding tuple")?;
    if !group.delim.is_paren() {
        return Err(ParseError::new(
            group.span(),
            "broker bindings need parentheses",
        ));
    }
    let names = List::<Ident, Token![,]>::parse_all(&Parser::from_tokens(&group.tokens))?;
    if !matches!(names.len(), 3..=6) {
        return Err(ParseError::new(
            group.span(),
            "bind handle/directory/broker and optional context or principal/peer/context",
        ));
    }
    let _: Token![;] = Parse::parse(&parser)?;
    expect_name(&parser, "boot", "expected boot clause")?;
    let boot = grouped_value(&parser, "expected native starter expression")?;
    if !boot.delim.is_bracket() || boot.tokens.is_empty() {
        return Err(ParseError::new(
            boot.span(),
            "boot needs a nonempty bracketed expression",
        ));
    }
    let policy = if <Token![;]>::peek(parser.cursor()) {
        None
    } else {
        let policy = Ident::parse(&parser)?;
        if !matches!(policy.text(), "allow_all" | "deny_all" | "controller_peer") {
            return Err(ParseError::new(
                policy.span(),
                "starter policy must be allow_all, deny_all or controller_peer",
            ));
        }
        let _: Path = moxy::parse!({ boot.tokens })?;
        Some(policy.text().to_owned())
    };
    let _: Token![;] = Parse::parse(&parser)?;
    let mut ready = false;
    let mut leader = false;
    let mut user = UserPolicy::Absent;
    while !parser.is_empty() {
        let name = Ident::parse(&parser)?;
        let token = parser
            .advance()
            .ok_or_else(|| parser.error("expected readiness or user value"))?;
        if name == "ready" && !ready && !leader && matches!(user, UserPolicy::Absent) {
            if token.as_group().is_some_and(|group| group.delim.is_brace()) {
                ready = true;
            } else if token
                .as_ident()
                .is_some_and(|name| name == "controller_leader")
            {
                leader = true;
            } else {
                return Err(ParseError::new(
                    token.span(),
                    "ready needs native statements or controller_leader",
                ));
            }
        } else if name == "user" && matches!(user, UserPolicy::Absent) && names.len() > 3 {
            nonempty_bracketed(token, "user needs a nonempty bracketed name expression")?;
            user = UserPolicy::Default;
        } else if name == "client"
            && matches!(user, UserPolicy::Default)
            && matches!(names.len(), 4 | 6)
        {
            nonempty_bracketed(token, "client needs a nonempty bracketed expression")?;
            user = UserPolicy::Client;
        } else {
            return Err(ParseError::new(
                name.span(),
                "expected optional readiness, then user and optional context client",
            ));
        }
        let _: Token![;] = Parse::parse(&parser)?;
    }
    if (names.len() > 3) == matches!(user, UserPolicy::Absent) {
        return Err(ParseError::new(
            input.span(),
            "a context binding requires its user policy",
        ));
    }
    let mut names = names.into_iter();
    let (Some(handle), Some(directory), Some(broker)) = (names.next(), names.next(), names.next())
    else {
        return Err(ParseError::new(
            input.span(),
            "bind handle/directory/broker and optional context or principal/peer/context",
        ));
    };
    Ok(Setup {
        handle,
        directory,
        broker,
        context: names.collect(),
        policy,
        ready,
        leader,
        user,
    })
}

fn template(setup: &Setup) -> TokenStream {
    let Setup {
        handle,
        directory,
        broker,
        ..
    } = setup;
    let deny = setup.policy.as_deref() == Some("deny_all");
    let controller_peer = setup.policy.as_deref() == Some("controller_peer");
    let policy = setup.policy.is_some();
    let leader = setup.leader;
    moxy::template! {
        let crate::handlers::test_support::StartedBroker { broker: {{ handle }}, dir: {{ directory }} } = @if policy {
            __broker_boot(@if deny { crate::handlers::test_support::deny_all_authorizer() } @else if controller_peer { crate::handlers::test_support::controller_peer_authorizer() } @else { crate::handlers::test_support::allow_all_authorizer() }).await
        } @else { __broker_boot.await };
        let {{ broker }} = {{ handle }}.broker_arc_for_test();
        @if leader { crate::test_support::wait_for_controller_leader(&{{ broker }}).await; }
        __broker_ready
    }
}

pub fn expand(input: NativeTokenStream) -> Result<NativeTokenStream, ParseError> {
    let setup = declaration(&input.clone().into())?;
    let mut groups = native_groups(input).skip(1);
    let boot = next_group(&mut groups, "a starter")?;
    let ready = if setup.ready {
        next_group(&mut groups, "a readiness block")?.stream()
    } else {
        NativeTokenStream::new()
    };
    let boot_delimiter = if setup.policy.is_some() {
        Delimiter::None
    } else {
        Delimiter::Parenthesis
    };
    let mut output = splice(
        template(&setup).into(),
        &[
            SpliceValue {
                name: "__broker_boot".to_owned(),
                group: Group::new(boot_delimiter, boot.stream()),
            },
            SpliceValue {
                name: "__broker_ready".to_owned(),
                group: Group::new(Delimiter::None, ready),
            },
        ],
        &[],
    );
    if !matches!(setup.user, UserPolicy::Absent) {
        let user = next_group(&mut groups, "a user expression")?.stream();
        let client = if matches!(setup.user, UserPolicy::Client) {
            Some(next_group(&mut groups, "a client expression")?.stream())
        } else {
            None
        };
        output.extend(user_bindings(&setup.context, user, client)?);
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use {
        super::{declaration, template},
        crate::meta::compact,
    };

    #[yare::parameterized(
        typed_starter = { "bind(handle, dir, broker); boot [start::<1, 2>(authorizer)];" },
        ready_block = {
            "bind(handle, dir, broker); boot [{ start(config) }]; ready { let part = local_partition(&broker, dir.path()); };",
        },
        allow_all_controller = {
            "bind(handle, dir, broker, ctx); boot [start_broker] allow_all; ready controller_leader; user [\"admin\"];",
        },
        deny_all = { "bind(handle,dir,broker); boot [start_broker] deny_all;" },
        controller_peer = {
            "bind(h,d,b,p,peer); boot [start_broker] controller_peer; user [\"alice\"];",
        },
        native_client = {
            "bind(h,d,b,p,peer,ctx); boot [start_broker] allow_all; user [\"admin\"]; client [native_client::<1, 2>()];",
        },
    )]
    fn keeps_native_starters_and_combined_policy_order_explicit(valid: &str) {
        let setup = declaration(&valid.parse().unwrap()).unwrap();
        assert2::assert!(compact(&template(&setup)).contains("broker_arc_for_test"));
    }

    #[yare::parameterized(
        empty = { "" },
        wrong_binding_keyword = { "bindings(handle,dir,broker);" },
        too_few_bindings = { "bind(handle,dir); boot [start()];" },
        empty_boot_list = { "bind(handle,dir,broker); boot [];" },
        braced_boot = { "bind(handle,dir,broker); boot {};" },
        missing_semicolon = { "bind(handle,dir,broker); boot [start()]" },
        invalid_ready_list = { "bind(handle,dir,broker); boot [start()]; ready [];" },
        unexpected_clause = { "bind(handle,dir,broker); boot [start()]; extra {};" },
        four_bindings = { "bind(h,d,b,c); boot [start()];" },
        unknown_policy = { "bind(h,d,b); boot [start] unknown;" },
        user_without_policy = { "bind(h,d,b); boot [start()]; user [\"admin\"];" },
        malformed_client = {
            "bind(h,d,b,p,peer); boot [start()]; user [\"admin\"]; client [\"bad\"];",
        },
        empty_user = { "bind(h,d,b,ctx); boot [start()]; user [];" },
        empty_client = { "bind(h,d,b,p,peer,ctx); boot [start()]; user [\"admin\"]; client [];" },
        client_without_peers = { "bind(h,d,b,ctx); boot [start()]; client [\"bad\"];" },
        invalid_starter = {
            "bind(h,d,b,c); boot [start]; user [\"admin\"]; ready controller_leader;",
        },
    )]
    fn rejects_invalid_combined_setups(invalid: &str) {
        assert2::assert!(declaration(&invalid.parse().unwrap()).is_err(), "{invalid}");
    }
}
