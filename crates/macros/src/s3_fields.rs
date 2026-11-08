//! S3 endpoint and redacted credentials, preserving each configuration's docs.

use {
    crate::meta::{edit_named_body, mode_docs},
    moxy::{
        ast::ParseError,
        token::{Span, TokenStream, TokenTree},
    },
};

pub fn expand(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    let [endpoint_doc, access_doc, secret_doc] = mode_docs(
        meta,
        [
            "Optional custom endpoint URL (e.g. `MinIO` or Cloudflare R2). When\nomitted, `AWS_ENDPOINT_URL_S3`, then `AWS_ENDPOINT_URL` or `AWS_ENDPOINT`,\napplies, and with none of them the AWS endpoint for the region.",
            "Explicit access key id. Set it together with `secret_access_key`; the\npair wins over every `AWS_*` credential variable. When both are omitted,\nthe AWS credential chain applies; see `[remote_storage.s3]`.",
            "Explicit secret access key. Set it together with `access_key_id`.\nWhen both are omitted, the AWS credential chain applies.",
        ],
        [
            "Optional custom endpoint URL, for example `http://minio:9000` or an R2\nendpoint. Without it, `AWS_ENDPOINT_URL_S3`, then `AWS_ENDPOINT_URL` or\n`AWS_ENDPOINT`, applies.",
            "Optional explicit access key id. With `secret_access_key` it wins over\nevery `AWS_*` credential variable. Without either, the backend falls back\nto the AWS credential chain.",
            "Optional explicit secret access key. Without it and `access_key_id`,\nthe backend falls back to the AWS credential chain.",
        ],
    )?;
    let new_fields = moxy::template! {
        #[doc = {{ endpoint_doc }}]
        pub endpoint: Option<String>,
        #[doc = {{ access_doc }}]
        #[debug("{:?}", access_key_id.as_ref().map(|_| "***"))]
        pub access_key_id: Option<String>,
        #[doc = {{ secret_doc }}]
        #[debug("{:?}", secret_access_key.as_ref().map(|_| "***"))]
        pub secret_access_key: Option<String>,
    };
    edit_named_body(item, "s3_fields", |body| {
        let mut fields: Vec<_> = body.into_iter().collect();
        let marker = fields
            .iter()
            .position(|token| token.as_ident().is_some_and(|id| id == "allow_http"))
            .ok_or_else(|| {
                ParseError::new(Span::call_site(), "s3_fields requires an allow_http field")
            })?;
        let insertion = fields
            .iter()
            .take(marker)
            .rposition(TokenTree::is_punct_comma)
            .map_or(0, |comma| comma + 1);
        fields.splice(insertion..insertion, new_fields);
        Ok(fields.into())
    })
}

#[cfg(test)]
mod tests {
    use super::expand;

    #[yare::parameterized(
        file = { "file", "Explicit access key id." },
        runtime = { "runtime", "Optional explicit access key id." },
    )]
    fn keeps_mode_specific_docs_and_fields(mode: &'static str, access_doc: &'static str) {
        let output = expand(
            mode.parse().unwrap(),
            "struct Config { allow_http: bool }".parse().unwrap(),
        )
        .unwrap()
        .to_string();
        assert2::assert!(output.contains(access_doc), "{output}");
        assert2::assert!(output.contains("pub endpoint"), "{output}");
        assert2::assert!(output.contains("pub access_key_id"), "{output}");
        assert2::assert!(output.contains("pub secret_access_key"), "{output}");
        assert2::assert!(output.contains("allow_http"), "{output}");
    }
}
