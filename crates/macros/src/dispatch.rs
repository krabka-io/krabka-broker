//! `dispatch_table!`: see the crate documentation.

use moxy::{
    ast::ParseError,
    token::{Ident, TokenStream},
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
    /// Decodes the body into the request type and awaits the handler.
    Decoded,
    /// Decodes the body into the request type and wraps the handler's result
    /// in a ready future.
    DecodedSync,
    /// Decodes the body into the request type, awaits the handler's response
    /// struct and encodes it. `group` decodes through
    /// `decode_group_request`, which also refuses a record string over the
    /// coordinator bound.
    Typed { group: bool },
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
    constructor: &'static str,
}

const KINDS: [Kind; 9] = [
    Kind {
        label: "context",
        adapter: Adapter::Context,
        constructor: "context",
    },
    Kind {
        label: "sync_context",
        adapter: Adapter::SyncContext,
        constructor: "context",
    },
    Kind {
        label: "decoded",
        adapter: Adapter::Decoded,
        constructor: "context",
    },
    Kind {
        label: "decoded_sync",
        adapter: Adapter::DecodedSync,
        constructor: "context",
    },
    Kind {
        label: "typed",
        adapter: Adapter::Typed { group: false },
        constructor: "context",
    },
    Kind {
        label: "typed_group",
        adapter: Adapter::Typed { group: true },
        constructor: "context",
    },
    Kind {
        label: "custom_context",
        adapter: Adapter::HandWritten,
        constructor: "context",
    },
    Kind {
        label: "auth",
        adapter: Adapter::HandWritten,
        constructor: "auth",
    },
    Kind {
        label: "telemetry",
        adapter: Adapter::Telemetry,
        constructor: "telemetry",
    },
];

/// The adapter function for one generated entry.
fn adapter(kind: Adapter, adapter: &Ident, handler: &TokenStream, entry: &Entry) -> TokenStream {
    let request_module = &entry.names.request_module;
    let request_type = &entry.names.request_type;
    match kind {
        Adapter::Context => moxy::template! {
            fn {{ adapter }}<'a>(
                broker: &'a Broker,
                version: ApiVersion,
                correlation_id: CorrelationId,
                body: &'a [u8],
                ctx: &'a RequestContext<'a>,
            ) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
                Box::pin({{ handler }}(broker, version, correlation_id, body, ctx))
            }
        },
        Adapter::SyncContext => moxy::template! {
            fn {{ adapter }}<'a>(
                broker: &'a Broker,
                version: ApiVersion,
                correlation_id: CorrelationId,
                body: &'a [u8],
                ctx: &'a RequestContext<'a>,
            ) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
                Box::pin(::std::future::ready({{ handler }}(
                    broker, version, correlation_id, body, ctx,
                )))
            }
        },
        Adapter::Decoded => moxy::template! {
            fn {{ adapter }}<'a>(
                broker: &'a Broker,
                version: ApiVersion,
                _correlation_id: CorrelationId,
                body: &'a [u8],
                ctx: &'a RequestContext<'a>,
            ) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
                Box::pin(async move {
                    use krabka_protocol::Decode;

                    let mut cur = body;
                    let req = krabka_protocol::owned::{{ request_module }}::{{ request_type }}::decode(
                        &mut cur, version,
                    )?;
                    {{ handler }}(broker, req, ctx, version).await
                })
            }
        },
        Adapter::DecodedSync => moxy::template! {
            fn {{ adapter }}<'a>(
                broker: &'a Broker,
                version: ApiVersion,
                _correlation_id: CorrelationId,
                body: &'a [u8],
                ctx: &'a RequestContext<'a>,
            ) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
                Box::pin(::std::future::ready((|| {
                    use krabka_protocol::Decode;

                    let mut cur = body;
                    let req = krabka_protocol::owned::{{ request_module }}::{{ request_type }}::decode(
                        &mut cur, version,
                    )?;
                    {{ handler }}(broker, req, ctx, version)
                })()))
            }
        },
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
            moxy::template! {
                fn {{ adapter }}<'a>(
                    broker: &'a Broker,
                    version: ApiVersion,
                    _correlation_id: CorrelationId,
                    body: &'a [u8],
                    ctx: &'a RequestContext<'a>,
                ) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
                    Box::pin(async move {
                        let mut cur = body;
                        let req = {{ decode }};
                        let resp = {{ handler }}(broker, req, version, ctx).await?;
                        crate::handlers::encode_response(&resp, version)
                    })
                }
            }
        }
        Adapter::Telemetry => moxy::template! {
            fn {{ adapter }}<'a>(
                broker: &'a Broker,
                version: ApiVersion,
                correlation_id: CorrelationId,
                body: &'a [u8],
                ctx: &'a TelemetryContext<'a>,
            ) -> BoxFuture<'a, Result<Bytes, BrokerError>> {
                Box::pin(::std::future::ready({{ handler }}(
                    broker, version, correlation_id, body, ctx,
                )))
            }
        },
        Adapter::HandWritten => TokenStream::new(),
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
                    adapters.push(adapter(generated, &adapter_ident, &handler, entry));
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
