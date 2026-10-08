//! Construction of the OTLP exporters from a resolved `OtlpConfig`.
//!
//! The span exporter, the log exporter, the OpenTelemetry `Resource`, and the
//! head sampler are all built here, so the transport details stay out of the
//! environment parsing and out of the subscriber install.

use krabka_units::prelude::TimeExt as _;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{LogExporter, Protocol, SpanExporter, WithExportConfig};
use opentelemetry_sdk::{Resource, trace::Sampler};

use crate::{
    config::{OtlpConfig, OtlpProtocol},
    error::TelemetryError,
};

macro_rules! exporter_method {
    ($(#[$doc:meta])* fn $method:ident -> $exporter:ident) => {
        $(#[$doc])*
        pub(crate) fn $method(&self) -> Result<$exporter, TelemetryError> {
            let builder = $exporter::builder();
            let exporter = match self.protocol {
                OtlpProtocol::Grpc => self.configure_exporter(builder.with_tonic()).build()?,
                OtlpProtocol::HttpProtobuf => self
                    .configure_exporter(builder.with_http().with_protocol(Protocol::HttpBinary))
                    .build()?,
            };
            Ok(exporter)
        }
    };
}

impl OtlpConfig {
    fn configure_exporter<B: WithExportConfig>(&self, builder: B) -> B {
        builder
            .with_endpoint(self.endpoint.clone())
            .with_timeout(self.timeout.to_std())
    }

    exporter_method!(fn build_exporter -> SpanExporter);

    exporter_method! {
        /// Build the OTLP **log** exporter.
        ///
        /// This function mirrors [`Self::build_exporter`], which builds the span
        /// exporter. Services can thus send their `tracing` logs over OTLP to the
        /// logs pipeline, and they do not depend on container-stdout tailing.
        fn build_log_exporter -> LogExporter
    }

    pub(crate) fn resource(&self) -> Resource {
        Resource::builder()
            .with_service_name(self.service_name.clone())
            .with_attributes([
                KeyValue::new("service.version", self.service_version.clone()),
                KeyValue::new("service.instance.id", self.service_instance_id.clone()),
            ])
            .build()
    }

    pub(crate) fn sampler(&self) -> Sampler {
        Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(self.sample_ratio)))
    }
}
