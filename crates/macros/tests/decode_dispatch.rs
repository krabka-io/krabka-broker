//! Decoding keeps version, request ownership and the native operation's control flow.

use {assert2::assert, std::cell::Cell};

type ApiVersion = i16;
type DispatchResult = Result<Vec<u8>, &'static str>;

struct Broker {
    operations: Cell<usize>,
}

struct RequestContext<'a> {
    principal: &'a str,
    correlation_id: u8,
}

struct Request {
    value: u8,
}

trait Decode: Sized {
    fn decode(bytes: &[u8], version: ApiVersion) -> Result<Self, &'static str>;
}

impl Decode for Request {
    fn decode(bytes: &[u8], version: ApiVersion) -> Result<Self, &'static str> {
        if version != 2 {
            return Err("unsupported version");
        }
        let &value = bytes.first().ok_or("truncated request")?;
        if value == 0xff {
            return Err("invalid request");
        }
        Ok(Self { value })
    }
}

mod handlers {
    use super::{ApiVersion, Decode};

    pub(crate) fn decode_request<R: Decode>(
        bytes: &[u8],
        version: ApiVersion,
    ) -> Result<R, &'static str> {
        R::decode(bytes, version)
    }
}

mod immutable {
    use super::{ApiVersion, Broker, DispatchResult, Request, RequestContext};

    /// Native documentation and tracing remain attached to the entry point.
    #[krabka_macros::decode_dispatch(request = Request, binding(req))]
    #[tracing::instrument(skip_all)]
    pub(super) async fn dispatch(
        broker: &Broker,
        version: ApiVersion,
        req_bytes: &[u8],
        ctx: &RequestContext<'_>,
    ) -> DispatchResult {
        broker.operations.set(broker.operations.get() + 1);
        if ctx.principal != "admin" {
            return Err("operation denied");
        }
        Ok(vec![req.value, req_bytes[0], ctx.correlation_id])
    }
}

#[tokio::test]
async fn successful_decode_preserves_context_raw_bytes_and_early_answers() {
    let broker = Broker {
        operations: Cell::new(0),
    };
    for (principal, expected) in [
        ("admin", Ok(vec![7, 7, 42])),
        ("guest", Err("operation denied")),
    ] {
        let ctx = RequestContext {
            principal,
            correlation_id: 42,
        };
        assert!(immutable::dispatch(&broker, 2, &[7, 99], &ctx).await == expected);
    }
    assert!(broker.operations.get() == 2);
}

#[tokio::test]
async fn failed_decoding_never_enters_the_original_body() {
    let broker = Broker {
        operations: Cell::new(0),
    };
    let ctx = RequestContext {
        principal: "admin",
        correlation_id: 1,
    };
    for (version, bytes, error) in [
        (1, &b"\x07"[..], "unsupported version"),
        (2, &b""[..], "truncated request"),
        (2, &b"\xff"[..], "invalid request"),
    ] {
        assert!(immutable::dispatch(&broker, version, bytes, &ctx).await == Err(error));
    }
    assert!(broker.operations.get() == 0);
}

#[tokio::test]
async fn mutable_request_binding_remains_an_owned_local() {
    krabka_macros::decoded_dispatch! {
        async fn dispatch(
            source: &Broker,
            negotiated: ApiVersion,
            body: &[u8],
            caller: &RequestContext<'_>,
        )
        [request = Request, binding(mut req)] -> DispatchResult
        {
            tokio::task::yield_now().await;
            req.value += u8::try_from(negotiated).unwrap();
            source.operations.set(usize::from(req.value));
            Ok(vec![req.value, caller.correlation_id])
        }
    }

    let broker = Broker {
        operations: Cell::new(0),
    };
    let ctx = RequestContext {
        principal: "admin",
        correlation_id: 42,
    };
    assert!(dispatch(&broker, 2, &[7], &ctx).await == Ok(vec![9, 42]));
    assert!(broker.operations.get() == 9);
}

#[test]
fn synchronous_handle_preserves_its_correlation_parameter() {
    #[krabka_macros::decode_dispatch(request = Request, binding(req))]
    fn handle(
        broker: &Broker,
        version: ApiVersion,
        correlation: u8,
        req_bytes: &[u8],
        ctx: &RequestContext<'_>,
    ) -> DispatchResult {
        broker.operations.set(usize::from(req.value));
        Ok(vec![req.value, correlation, ctx.correlation_id])
    }

    let broker = Broker {
        operations: Cell::new(0),
    };
    let ctx = RequestContext {
        principal: "admin",
        correlation_id: 42,
    };
    assert!(handle(&broker, 2, 99, &[7], &ctx) == Ok(vec![7, 99, 42]));
    assert!(handle(&broker, 2, 99, &[], &ctx) == Err("truncated request"));
    assert!(broker.operations.get() == 7);
}
