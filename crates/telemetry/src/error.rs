//! The error type that the OTLP pipeline build returns.
//!
//! The crate keeps one error enum here, so a caller matches a single type for a
//! malformed configuration value and for an exporter build failure.

/// An error from the build of the OTLP pipeline.
///
/// The error carries the exporter build failure. A misconfigured endpoint thus
/// gives a clear message and does not become a silent no-export.
#[derive(Debug, thiserror::Error)]
pub enum TelemetryError {
    #[error("invalid {name}: {message}")]
    InvalidConfig { name: &'static str, message: String },
    #[error("failed to build OTLP span exporter: {0}")]
    Exporter(#[from] opentelemetry_otlp::ExporterBuildError),
}
