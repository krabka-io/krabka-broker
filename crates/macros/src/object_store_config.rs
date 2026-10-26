//! Shared retry and HTTP timeout fields of object-store configurations.

use crate::{
    meta::{ItemBody, body_group, named_body, no_arguments},
    prelude::*,
};

pub fn expand(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    no_arguments(meta, "object_store_config")?;
    let ItemBody {
        mut tokens,
        body_index: body,
    } = named_body(item, "object_store_config")?;
    let group = body_group(&mut tokens, body)?;
    group.tokens.extend(moxy::template! {
        /// How many times one request is retried before the error surfaces.
        /// Defaults to [`DEFAULT_MAX_RETRIES`]; `0` disables retries.
        #[default(DEFAULT_MAX_RETRIES)]
        pub max_retries: usize,
        /// Ceiling on the wall-clock time one request may spend across all of its
        /// retries. Defaults to [`DEFAULT_RETRY_TIMEOUT`].
        #[default(DEFAULT_RETRY_TIMEOUT)]
        pub retry_timeout: Duration,
        /// Ceiling on one HTTP request. Defaults to [`DEFAULT_REQUEST_TIMEOUT`].
        #[default(DEFAULT_REQUEST_TIMEOUT)]
        pub request_timeout: Duration,
        /// Ceiling on the connect phase alone. Defaults to
        /// [`DEFAULT_CONNECT_TIMEOUT`].
        #[default(DEFAULT_CONNECT_TIMEOUT)]
        pub connect_timeout: Duration,
    });
    Ok(tokens.into())
}
