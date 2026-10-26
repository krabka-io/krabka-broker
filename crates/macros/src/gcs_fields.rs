//! Shared credential fields of the runtime and file GCS configurations.

use {
    crate::meta::{edit_named_body, mode_docs},
    moxy::{ast::ParseError, token::TokenStream},
};

pub fn expand(meta: TokenStream, item: TokenStream) -> Result<TokenStream, ParseError> {
    let [prefix_doc, path_doc, key_doc, adc_doc, endpoint_doc] = mode_docs(
        meta,
        [
            "Optional key prefix inside the bucket (lets multiple clusters\nshare a bucket). Tiered segments and new diskless WAL objects are\nwritten under it, and the diskless reclaimer lists and deletes only\nbelow `<prefix>/diskless-wal/`. WAL objects written at the bucket root\nbefore a prefix was set stay readable at their recorded keys, and the\nreclaimer of a prefixed cluster never deletes them.",
            "Path to a service-account JSON key file. Omit (along with the\nother credential fields) to use Workload Identity / ADC.",
            "Inline service-account JSON key. Omit (along with the other\ncredential fields) to use Workload Identity / ADC.",
            "Path to an Application Default Credentials JSON file. Omit (along\nwith the other credential fields) to use Workload Identity / ADC.",
            "Optional custom GCS API base URL (for emulators / fakes).",
        ],
        [
            "Optional key prefix inside the bucket. No leading slash and no trailing\nslash.",
            "Optional path to a service-account JSON key file.",
            "Optional inline service-account JSON key. It is mutually exclusive with\nthe path.",
            "Optional path to an application-default-credentials JSON file.",
            "Optional custom GCS API base URL, for example `http://fake-gcs:4443`.",
        ],
    )?;
    let fields = moxy::template! {
        /// GCS bucket name.
        pub bucket: String,
        #[doc = {{ prefix_doc }}]
        pub prefix: Option<String>,
        #[doc = {{ path_doc }}]
        #[debug("{:?}", service_account_path.as_ref().map(|_| "***"))]
        pub service_account_path: Option<String>,
        #[doc = {{ key_doc }}]
        #[debug("{:?}", service_account_key.as_ref().map(|_| "***"))]
        pub service_account_key: Option<String>,
        #[doc = {{ adc_doc }}]
        #[debug("{:?}", application_credentials_path.as_ref().map(|_| "***"))]
        pub application_credentials_path: Option<String>,
        #[doc = {{ endpoint_doc }}]
        pub endpoint: Option<String>,
    };
    edit_named_body(item, "gcs_fields", |body| {
        Ok(fields.into_iter().chain(body).collect())
    })
}
