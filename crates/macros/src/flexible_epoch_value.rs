//! Version-zero flexible epoch records with one optional tag-zero i64 field.

use {
    crate::meta::{field_ident, impl_block, named_fields},
    moxy::{
        ast::{Attributed, ItemStruct, ParseError},
        token::{LitStr, Spanner, TokenStream},
    },
};

#[derive(moxy::FromMeta)]
struct EpochArgs {
    record: String,
}

pub(crate) fn expand(item: ItemStruct) -> Result<TokenStream, ParseError> {
    let args = item
        .parse_meta::<EpochArgs>("epoch_value")?
        .ok_or_else(|| {
            ParseError::new(
                item.ident.span(),
                "`FlexibleEpochValue` needs `#[epoch_value(record = \"KafkaRecordName\")]`",
            )
        })?;
    let fields = named_fields(&item, "FlexibleEpochValue")?;
    if fields.len() != 2 {
        return Err(ParseError::new(
            item.ident.span(),
            "`FlexibleEpochValue` needs an i32 epoch followed by an i64 tag-zero field",
        ));
    }
    for (field, ty) in fields.iter().zip(["i32", "i64"]) {
        if !field
            .ty
            .as_path()
            .is_some_and(|path| path.path.is_ident(ty))
        {
            return Err(ParseError::new(field.ty.span(), format!("expected `{ty}`")));
        }
    }
    let epoch = field_ident(fields.first().expect("two fields"), "FlexibleEpochValue")?.clone();
    let tagged = field_ident(fields.last().expect("two fields"), "FlexibleEpochValue")?.clone();
    let record = LitStr::new(&args.record, item.ident.span());
    Ok(impl_block(item, &TokenStream::new(), &moxy::template! {
        #[must_use]
        pub fn encode(self) -> bytes::Bytes {
            let mut buf = bytes::BytesMut::new();
            bytes::BufMut::put_i16(&mut buf, 0);
            bytes::BufMut::put_i32(&mut buf, self.{{ epoch }});
            crate::coordinator::unified::persistence::flex::put_tag0_i64(
                &mut buf, self.{{ tagged }},
            );
            buf.freeze()
        }

        /// # Errors
        /// Returns an error when `buf` ends before a field, when the value
        /// version is not 0, or when the tag 0 payload is shorter than
        /// eight bytes.
        pub fn decode(mut buf: &[u8]) -> Result<Self, crate::error::BrokerError> {
            crate::coordinator::unified::persistence::flex::get_value_version(
                &mut buf, 0, concat!("unknown ", {{ record }}, " version"),
            )?;
            let {{ epoch }} = crate::coordinator::unified::persistence::get_i32(&mut buf)?;
            let {{ tagged }} = crate::coordinator::unified::persistence::flex::get_tag0_i64(&mut buf)?;
            Ok(Self { {{ epoch }}, {{ tagged }} })
        }
    }))
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::assert};

    #[test]
    fn refuses_missing_record_names_and_incorrect_field_layouts() {
        for input in [
            "struct Value { epoch: i32, timestamp: i64 }",
            "#[epoch_value(record = 0)] struct Value { epoch: i32, timestamp: i64 }",
            "#[epoch_value(record = \"Record\")] struct Value(i32, i64);",
            "#[epoch_value(record = \"Record\")] struct Value { epoch: i32 }",
            "#[epoch_value(record = \"Record\")] struct Value { timestamp: i64, epoch: i32 }",
            "#[epoch_value(record = \"Record\")] struct Value { epoch: i32, timestamp: u64 }",
        ] {
            let item: ItemStruct = moxy::parse!({ input }).unwrap();
            assert!(expand(item).is_err(), "{input}");
        }
    }
}
