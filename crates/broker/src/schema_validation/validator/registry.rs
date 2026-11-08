//! The registry reads a schema lookup makes, as a seam.
//!
//! Production reads through [`RegistryClient`]. A test can read through a fake
//! whose latency runs on tokio's clock, so a deadline test steps virtual time
//! instead of racing real sockets against a real timer.

use {
    krabka_schema_serde::{
        error::SchemaSerdeError,
        registry::{FetchedSchema, RegistryClient, model::SubjectVersion},
    },
    std::future::Future,
};

/// The three registry reads one schema lookup can make.
pub(super) trait RegistryReads: Sync {
    /// Every subject and version the schema id is registered under.
    fn subject_versions_for_id(
        &self,
        id: u32,
    ) -> impl Future<Output = Result<Vec<SubjectVersion>, SchemaSerdeError>> + Send;

    /// The schema text, format and references of a schema id.
    fn schema_by_id(
        &self,
        id: u32,
    ) -> impl Future<Output = Result<FetchedSchema, SchemaSerdeError>> + Send;

    /// The schema a reference names, by subject and version.
    fn schema_by_subject_version(
        &self,
        subject: &str,
        version: i32,
    ) -> impl Future<Output = Result<FetchedSchema, SchemaSerdeError>> + Send;
}

impl RegistryReads for RegistryClient {
    fn subject_versions_for_id(
        &self,
        id: u32,
    ) -> impl Future<Output = Result<Vec<SubjectVersion>, SchemaSerdeError>> + Send {
        Self::subject_versions_for_id(self, id)
    }

    fn schema_by_id(
        &self,
        id: u32,
    ) -> impl Future<Output = Result<FetchedSchema, SchemaSerdeError>> + Send {
        Self::schema_by_id(self, id)
    }

    fn schema_by_subject_version(
        &self,
        subject: &str,
        version: i32,
    ) -> impl Future<Output = Result<FetchedSchema, SchemaSerdeError>> + Send {
        Self::schema_by_subject_version(self, subject, version)
    }
}
