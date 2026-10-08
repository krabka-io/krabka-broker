//! The fixed legacy wire input used by decompression-policy regressions.

use moxy::{ast::ParseError, token::TokenStream};

pub(crate) fn policy(input: TokenStream) -> Result<TokenStream, ParseError> {
    let [name, root]: [TokenStream; 2] = crate::meta::arguments(input, 2)?
        .try_into()
        .expect("two arguments");
    let name = crate::fixtures::name(name)?;
    Ok(moxy::template! {
        fn {{ name }}() -> ::bytes::BytesMut {
            let records = vec![{{ root }}::ParsedRecord {
                offset: ::krabka_ids::Offset(0),
                timestamp: Some(1),
                key: None,
                value: Some(::bytes::Bytes::from(vec![b'x'; 4096])),
            }];
            let mut wire = ::bytes::BytesMut::new();
            {{ root }}::encode_compressed_message_set(
                &records,
                {{ root }}::Magic::V1,
                ::krabka_compression::CompressionType::Lz4,
                &mut wire,
            ).unwrap();
            wire
        }
    })
}
