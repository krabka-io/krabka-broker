//! Typed wire drivers select framing policy without nested fixture macros.

use {
    crate::meta::{expect_name, grouped_value},
    moxy::{
        ast::{Attributes, Expr, Parse, ParseError, Parser, Path, Token, Visibility},
        token::{Ident, Spanner, TokenStream},
    },
};

struct WireSchema {
    request: Path,
    response: Path,
    api: Expr,
}

fn wire_schema(parser: &Parser) -> Result<WireSchema, ParseError> {
    expect_name(parser, "request", "expected request path")?;
    <Token![:]>::parse(parser)?;
    let request = Path::parse(parser)?;
    <Token![,]>::parse(parser)?;
    expect_name(parser, "response", "expected response path")?;
    <Token![:]>::parse(parser)?;
    let response = Path::parse(parser)?;
    <Token![,]>::parse(parser)?;
    expect_name(parser, "api", "expected wire API expression")?;
    <Token![:]>::parse(parser)?;
    let api = Expr::parse(parser)?;
    Ok(WireSchema {
        request,
        response,
        api,
    })
}

pub fn expand(input: &TokenStream) -> Result<TokenStream, ParseError> {
    let parser = Parser::from_tokens(input);
    let mode = Ident::parse(&parser)?;
    let attributes = Attributes::parse(&parser)?;
    let visibility = Visibility::parse(&parser)?;
    let name = Ident::parse(&parser)?;
    let output = match mode.text() {
        "socket" => {
            <Token![,]>::parse(&parser)?;
            let client = Expr::parse(&parser)?;
            moxy::template! {
                {{ attributes }} #[::bon::builder] {{ visibility }} async fn {{ name }}<S: ::tokio::io::AsyncRead + ::tokio::io::AsyncWrite + Unpin>(
                    stream: &mut S,
                    #[builder(into)] api_key: Option<::krabka_ids::ApiKey>,
                    #[builder(into)] version: Option<::krabka_ids::ApiVersion>,
                    #[builder(into)] correlation: Option<crate::support::wire::CorrelationId>,
                    header: Option<crate::support::wire::HeaderEncoding>,
                    body: &[u8],
                ) -> ::std::io::Result<Vec<u8>> {
                    crate::kafka_wire::round_trip()
                        .stream(stream)
                        .maybe_api_key(api_key)
                        .maybe_version(version)
                        .maybe_correlation(correlation)
                        .client_id({{ client }})
                        .maybe_header(header)
                        .body(body)
                        .call()
                        .await
                }
            }
        }
        "flexible" => {
            <Token![,]>::parse(&parser)?;
            let client = Expr::parse(&parser)?;
            <Token![,]>::parse(&parser)?;
            let correlation = Expr::parse(&parser)?;
            moxy::template! {
                {{ attributes }} {{ visibility }} async fn {{ name }}(
                    stream: &mut ::tokio::net::TcpStream, setup: crate::support::wire::ApiRequestSetup, body: &[u8],
                ) -> ::std::io::Result<Vec<u8>> {
                    crate::kafka_wire::round_trip()
                        .stream(stream)
                        .api_key(setup.api_key)
                        .version(setup.version)
                        .correlation(crate::support::wire::CorrelationId({{ correlation }}))
                        .client_id({{ client }})
                        .header(crate::support::wire::HeaderEncoding::Flexible)
                        .body(body)
                        .call()
                        .await
                }
            }
        }
        "plain" | "broker" => {
            let fields = grouped_value(&parser, "expected wire driver schema")?;
            if !fields.delim.is_brace() {
                return Err(ParseError::new(
                    fields.span(),
                    "expected wire driver schema",
                ));
            }
            let schema = Parser::from_tokens(&fields.tokens);
            let WireSchema {
                request,
                response,
                api,
            } = wire_schema(&schema)?;
            if mode.text() == "broker" {
                finish(&schema)?;
                moxy::template! {
                    {{ attributes }} {{ visibility }} async fn {{ name }}(
                        broker: &crate::broker::Broker, version: i16, request: &{{ request }},
                        context: &crate::handlers::RequestContext<'_>,
                    ) -> {{ response }} {
                        crate::test_support::dispatch_wire(broker, {{ api }}, version, request, context).await
                    }
                }
            } else {
                <Token![,]>::parse(&schema)?;
                expect_name(&schema, "operation", "expected decode diagnostic")?;
                <Token![:]>::parse(&schema)?;
                let operation = Expr::parse(&schema)?;
                finish(&schema)?;
                moxy::template! {
                    {{ attributes }} {{ visibility }} async fn {{ name }}(
                        addr: ::std::net::SocketAddr, request: {{ request }},
                        authentication: &crate::kafka_wire::PlainAuthenticationSetup<'_>,
                    ) -> ::std::io::Result<{{ response }}> {
                        crate::kafka_wire::request_as_plain(addr, &request, &crate::kafka_wire::PlainRequestSetup {
                            authentication: authentication.clone(), api: {{ api }}, operation: {{ operation }},
                        }).await
                    }
                }
            }
        }
        _ => {
            return Err(ParseError::new(
                mode.span(),
                "expected socket, flexible, plain, or broker wire driver",
            ));
        }
    };
    finish(&parser)?;
    Ok(output)
}

fn finish(parser: &Parser) -> Result<(), ParseError> {
    if <Token![,]>::peek(parser.cursor()) {
        <Token![,]>::parse(parser)?;
    }
    if parser.is_empty() {
        Ok(())
    } else {
        Err(parser.error("unexpected wire driver input"))
    }
}

#[cfg(test)]
mod tests {
    use {super::expand, crate::meta::compact, assert2::assert};

    #[test]
    fn framing_and_codec_adapters_keep_their_selected_header_and_diagnostic() {
        let socket = compact(&expand(&"socket pub exchange, CLIENT".parse().unwrap()).unwrap());
        assert!(socket.contains("::bon::builder") && socket.contains("asyncfnexchange"));
        assert!(
            socket
                .contains(".maybe_correlation(correlation).client_id(CLIENT).maybe_header(header)")
        );
        let flexible = compact(&expand(&"flexible exchange, CLIENT, 7".parse().unwrap()).unwrap());
        assert!(
            flexible.contains("CorrelationId(7)") && flexible.contains("HeaderEncoding::Flexible")
        );
        assert!(flexible.contains(".client_id(CLIENT)"));
        let plain_tokens = expand(&"plain pub send { request: schema::Request, response: schema::Response, api: API, operation: \"decode response\", }".parse().unwrap()).unwrap();
        let plain = compact(&plain_tokens);
        assert!(
            plain.contains("request:schema::Request") && plain.contains("Result<schema::Response>")
        );
        assert!(plain.contains("authentication:authentication.clone(),api:API,operation:"));
        assert!(plain_tokens.to_string().contains("\"decode response\""));
    }

    #[yare::parameterized(
        empty = { "" },
        unknown = { "other exchange" },
        missing_socket_arguments = { "socket exchange" },
        missing_flexible_version = { "flexible exchange, CLIENT" },
        empty_plain = { "plain exchange []" },
        incomplete_plain = { "plain exchange { request: Request }" },
        trailing = { "socket exchange, CLIENT extra" },
    )]
    fn exchange_driver_rejects_invalid_inputs(input: &str) {
        assert2::assert!(expand(&input.parse().unwrap()).is_err());
    }

    #[test]
    fn broker_driver_borrows_request_and_context_and_forwards_wire_arguments() {
        let output = compact(&expand(&"broker pub(super) answer { request: wire::Request, response: wire::Response, api: wire::API_KEY, }".parse().unwrap()).unwrap());
        assert!(output.contains("request:&wire::Request"));
        assert!(output.contains("context:&crate::handlers::RequestContext<'_>"));
        assert!(
            output.contains("dispatch_wire(broker,wire::API_KEY,version,request,context).await")
        );
    }

    #[yare::parameterized(
        empty = { "broker answer {}" },
        extra_field = {
            "broker answer { request: Request, response: Response, api: API, operation: \"extra\" }",
        },
        tuple = { "broker answer (request: Request)" },
    )]
    fn broker_driver_rejects_invalid_inputs(input: &str) {
        assert2::assert!(expand(&input.parse().unwrap()).is_err(), "{input}");
    }
}
