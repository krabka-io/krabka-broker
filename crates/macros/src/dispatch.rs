//! `dispatch_table!`: see the crate documentation.

use moxy::{
    ast::ParseError,
    token::{Ident, LitStr, TokenStream},
};

use crate::api_names::{Entry, Section, Suffixed, parse_sections, section};

/// What the adapter of a section does with the request before it reaches the
/// handler.
#[derive(Clone, Copy)]
enum Adapter {
    /// Hands the handler the raw body and awaits it.
    Context,
    /// Hands the handler the raw body and wraps its result in a ready future.
    SyncContext,
    /// Decodes the body into the request type, awaits the handler's response
    /// struct and encodes it. `group` decodes through
    /// `decode_group_request`, which also refuses a record string over the
    /// coordinator bound.
    Typed { group: bool },
    /// Decodes the body into the request type, calls the handler, encodes its
    /// response struct and wraps the result in a ready future.
    TypedSync,
    /// Hands a telemetry handler the raw body and wraps its result in a ready
    /// future.
    Telemetry,
    /// The adapter is hand-written; the table only registers it.
    HandWritten,
}

/// One section the table accepts: its label, its adapter, and the
/// `DispatchEntry` constructor that registers it.
struct Kind {
    label: &'static str,
    adapter: Adapter,
    /// Whether the generated adapter opens the `handle_<snake>` span. A
    /// section whose handler opens a span of its own sets this false, so the
    /// request is not traced twice.
    traced: bool,
    constructor: &'static str,
}

const KINDS: [Kind; 9] = [
    Kind {
        label: "context",
        adapter: Adapter::Context,
        traced: true,
        constructor: "context",
    },
    Kind {
        label: "sync_context",
        adapter: Adapter::SyncContext,
        traced: true,
        constructor: "context",
    },
    Kind {
        label: "typed",
        adapter: Adapter::Typed { group: false },
        traced: true,
        constructor: "context",
    },
    Kind {
        label: "typed_own_span",
        adapter: Adapter::Typed { group: false },
        traced: false,
        constructor: "context",
    },
    Kind {
        label: "typed_group",
        adapter: Adapter::Typed { group: true },
        traced: true,
        constructor: "context",
    },
    Kind {
        label: "typed_sync",
        adapter: Adapter::TypedSync,
        traced: true,
        constructor: "context",
    },
    Kind {
        label: "typed_sync_own_span",
        adapter: Adapter::TypedSync,
        traced: false,
        constructor: "context",
    },
    Kind {
        label: "auth",
        adapter: Adapter::HandWritten,
        traced: false,
        constructor: "auth",
    },
    Kind {
        label: "telemetry",
        adapter: Adapter::Telemetry,
        traced: true,
        constructor: "telemetry",
    },
];

/// The adapter function for one generated entry.
///
/// Unless the entry's section says the handler opens its own span, the adapter
/// runs the decode, the handler and the encode inside an `info` span named
/// `handle_<snake_name>` with `api`, `version` and, for an adapter that hands
/// the handler the raw body, `req_bytes`. On `Err` it emits the `ERROR` event
/// `#[tracing::instrument(err)]` would: `error` set to the error's `Display`,
/// inside the span.
fn adapter(
    kind: Adapter,
    traced: bool,
    adapter: &Ident,
    handler: &TokenStream,
    entry: &Entry,
) -> TokenStream {
    let request_module = &entry.names.request_module;
    let request_type = &entry.names.request_type;
    let (raw_body, body) = match kind {
        Adapter::Context => (
            true,
            Body::Future(moxy::template! { {{ handler }}(broker, version, body, ctx) }),
        ),
        Adapter::SyncContext => (
            true,
            Body::Ready(moxy::template! { {{ handler }}(broker, version, body, ctx) }),
        ),
        Adapter::Typed { group } => {
            let decode = if group {
                moxy::template! {
                    crate::handlers::decode_group_request::<
                        krabka_protocol::owned::{{ request_module }}::{{ request_type }},
                    >(&mut cur, version)?
                }
            } else {
                moxy::template! {
                    {
                        use krabka_protocol::Decode as _;

                        krabka_protocol::owned::{{ request_module }}::{{ request_type }}::decode(
                            &mut cur, version,
                        )?
                    }
                }
            };
            (
                false,
                Body::Future(moxy::template! {
                    async move {
                        let mut cur = body;
                        let req = {{ decode }};
                        let resp = {{ handler }}(broker, req, version, ctx).await?;
                        crate::handlers::encode_response(&resp, version)
                    }
                }),
            )
        }
        Adapter::TypedSync => (
            false,
            Body::Ready(moxy::template! {
                (|| {
                    use krabka_protocol::Decode as _;

                    let mut cur = body;
                    let req = krabka_protocol::owned::{{ request_module }}::{{ request_type }}::decode(
                        &mut cur, version,
                    )?;
                    let resp = {{ handler }}(broker, req, version, ctx)?;
                    crate::handlers::encode_response(&resp, version)
                })()
            }),
        ),
        Adapter::Telemetry => (
            true,
            Body::Ready(moxy::template! {
                {{ handler }}(broker, version, correlation_id, body, ctx)
            }),
        ),
        Adapter::HandWritten => return TokenStream::new(),
    };
    let signature = if matches!(kind, Adapter::Telemetry) {
        moxy::template! {
            fn {{ adapter }}<'a>(
                broker: &'a Broker,
                version: ApiVersion,
                correlation_id: CorrelationId,
                body: &'a [u8],
                ctx: &'a TelemetryContext<'a>,
            ) -> BoxFuture<'a, Result<Bytes, BrokerError>>
        }
    } else {
        moxy::template! {
            fn {{ adapter }}<'a>(
                broker: &'a Broker,
                version: ApiVersion,
                body: &'a [u8],
                ctx: &'a RequestContext<'a>,
            ) -> BoxFuture<'a, Result<Bytes, BrokerError>>
        }
    };
    let block = if traced {
        traced_block(entry, raw_body, body)
    } else {
        match body {
            Body::Future(future) => moxy::template! { Box::pin({{ future }}) },
            Body::Ready(result) => moxy::template! { Box::pin(::std::future::ready({{ result }})) },
        }
    };
    moxy::template! {
        {{ signature }} {
            {{ block }}
        }
    }
}

/// What an adapter computes before it boxes the result.
enum Body {
    /// A future of the handler's encoded result.
    Future(TokenStream),
    /// The handler's encoded result, computed when the adapter is called.
    Ready(TokenStream),
}

/// An adapter's block that runs `body` inside the entry's `handle_<snake>`
/// span and emits an `ERROR` event with the error's `Display` on `Err`.
fn traced_block(entry: &Entry, raw_body: bool, body: Body) -> TokenStream {
    let names = &entry.names;
    let name = LitStr::new(
        &format!("handle_{}", names.snake.text()),
        names.snake.span(),
    );
    let api = LitStr::new(names.api.text(), names.api.span());
    let req_bytes = if raw_body {
        moxy::template! { req_bytes = body.len(), }
    } else {
        TokenStream::new()
    };
    let span = moxy::template! {
        ::tracing::info_span!({{ name }}, api = {{ api }}, version, {{ req_bytes }})
    };
    match body {
        Body::Future(future) => moxy::template! {
            Box::pin(::tracing::Instrument::instrument(
                async move {
                    ({{ future }})
                        .await
                        .inspect_err(|error| ::tracing::error!(error = %error))
                },
                {{ span }},
            ))
        },
        Body::Ready(result) => moxy::template! {
            let span = {{ span }};
            let _entered = span.enter();
            Box::pin(::std::future::ready(
                ({{ result }}).inspect_err(|error| ::tracing::error!(error = %error)),
            ))
        },
    }
}

/// One registration call of the generated `register_dispatch_table`.
struct Registration {
    constructor: Ident,
    api: Ident,
    request_module: Ident,
    adapter: Ident,
}

/// Expands `dispatch_table! { ... }`.
pub(crate) fn expand(tokens: TokenStream) -> Result<TokenStream, ParseError> {
    let labels = KINDS.map(|kind| kind.label);
    let sections: Vec<Section> = parse_sections(tokens, "=>", &labels)?;

    let mut adapters = Vec::new();
    let mut registrations = Vec::new();
    for kind in &KINDS {
        for entry in section(&sections, kind.label) {
            let names = &entry.names;
            let adapter_ident = names.snake.suffixed("_adapter");
            match (kind.adapter, &entry.value) {
                (Adapter::HandWritten, Some(value)) => {
                    return Err(ParseError::new(
                        value.span(),
                        format!(
                            "`{}` entries name no handler: the table registers the \
                             hand-written `{}`",
                            kind.label,
                            adapter_ident.text()
                        ),
                    ));
                }
                (Adapter::HandWritten, None) => {}
                (generated, value) => {
                    let handler = value.clone().unwrap_or_else(|| {
                        let snake = &names.snake;
                        moxy::template! { crate::handlers::{{ snake }}::handle }
                    });
                    adapters.push(adapter(
                        generated,
                        kind.traced,
                        &adapter_ident,
                        &handler,
                        entry,
                    ));
                }
            }
            registrations.push(Registration {
                constructor: Ident::new(kind.constructor).with_span(names.api.span()),
                api: names.api.clone(),
                request_module: names.request_module.clone(),
                adapter: adapter_ident,
            });
        }
    }

    Ok(moxy::template! {
        @for adapter in &adapters {
            {{ adapter }}
        }

        fn register_dispatch_table(registry: &mut DispatchRegistry) {
            @for r in &registrations {
                assert2::assert!(
                    registry.register(DispatchEntry::{{ r.constructor }}(
                        ApiKey::{{ r.api }} as i16,
                        krabka_protocol::owned::{{ r.request_module }}::FLEXIBLE_MIN,
                        {{ r.adapter }},
                    )),
                    "duplicate dispatch registration for {:?}",
                    ApiKey::{{ r.api }}
                );
            }
        }
    })
}
