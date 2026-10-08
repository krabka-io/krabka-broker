//! The common schema and deserialization recipe for TOML configuration tables.

use moxy::{
    ast::ParseError,
    token::{TokenStream, TokenTree},
};

pub(crate) fn expand(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    let mode = crate::meta::mode(meta, ["strict", "open"])?;
    let (item, _) = crate::meta::item_body(item, "config_table", TokenTree::is_keyword_struct)?;
    let mut output = moxy::template! {
        #[derive(Debug, Clone, Default, ::serde::Deserialize, ::schemars::JsonSchema, PartialEq)]
    };
    if mode == "strict" {
        output.extend(moxy::template! { #[serde(deny_unknown_fields)] });
    }
    output.extend(item);
    Ok(output)
}
