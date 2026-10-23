//! Validation and sink delivery for admitted client telemetry payloads.
//!
//! Admission and Kafka error mapping belong to the wire handler. This module
//! validates the entire payload before either sink receives any metrics.

use {
    self::prometheus::flatten_for_prometheus,
    super::{ClientMetrics, config::ALL_METRICS},
    krabka_compression::{CompressionError, CompressionType},
    opentelemetry_proto::tonic::metrics::v1::MetricsData,
    prost::Message as _,
    uuid::Uuid,
};

mod prometheus;

/// Payload failures independent of Kafka's version-specific error mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IngestionError {
    InvalidRecord,
    TooLarge,
}

impl ClientMetrics {
    /// Validates and delivers a nonempty, admitted payload. Conversion must
    /// succeed before updating Prometheus or enqueueing the original OTLP data.
    pub(crate) fn ingest_payload(
        &self,
        codec: CompressionType,
        payload: &[u8],
        subscribed: &[String],
        instance: Uuid,
        client_id: &str,
    ) -> Result<(), IngestionError> {
        let raw = krabka_compression::decompress(codec, payload, self.manager.telemetry_max())
            .map_err(|error| match error {
                CompressionError::TooLarge { limit } => {
                    tracing::debug!(limit, "client-metrics payload decompresses past the limit");
                    IngestionError::TooLarge
                }
                error => {
                    tracing::debug!(error = %error, "client-metrics decompress failed");
                    IngestionError::InvalidRecord
                }
            })?;
        let md = MetricsData::decode(&raw[..]).map_err(|error| {
            tracing::debug!(error = %error, "client-metrics OTLP decode failed");
            IngestionError::InvalidRecord
        })?;
        if !md
            .resource_metrics
            .iter()
            .flat_map(|resource| &resource.scope_metrics)
            .flat_map(|scope| &scope.metrics)
            .all(|metric| {
                subscribed
                    .iter()
                    .any(|prefix| prefix == ALL_METRICS || metric.name.starts_with(prefix))
            })
        {
            return Err(IngestionError::InvalidRecord);
        }
        let instance = instance.to_string();
        let points = flatten_for_prometheus(&md, &instance, client_id)
            .map_err(|()| IngestionError::InvalidRecord)?;
        self.prometheus.ingest(&points);
        self.otlp.forward(md, &instance);
        Ok(())
    }
}

#[cfg(all(test, not(target_family = "wasm")))]
mod tests {
    use {
        super::*,
        crate::client_metrics::{
            ClientMetricsManager,
            config::INTERVAL_MS_DEFAULT,
            otlp_sink::{OtlpForwarder, build_export_request},
            prometheus_sink::{ClientMetricsCollector, SharedClientMetricsCollector},
            test_support::{gauge_metric, metrics_data},
        },
        assert2::assert,
        krabka_telemetry::OtlpProtocol,
        krabka_units::kibibytes,
        opentelemetry_proto::tonic::{
            collector::metrics::v1::ExportMetricsServiceRequest, metrics::v1::number_data_point,
        },
        prometheus_client::{metrics::counter::Counter, registry::Registry},
        std::{sync::Arc, time::Duration},
        wiremock::{Mock, MockServer, ResponseTemplate, matchers::path},
    };

    fn receiver(endpoint: String) -> ClientMetrics {
        ClientMetrics {
            manager: ClientMetricsManager::new(kibibytes(1), INTERVAL_MS_DEFAULT),
            prometheus: Arc::new(ClientMetricsCollector::new(Duration::from_secs(60))),
            otlp: OtlpForwarder::spawn(
                endpoint,
                OtlpProtocol::HttpProtobuf,
                8,
                Counter::default(),
                Counter::default(),
            ),
        }
    }

    fn scrape(metrics: &ClientMetrics) -> String {
        let mut registry = Registry::default();
        registry.register_collector(Box::new(SharedClientMetricsCollector(Arc::clone(
            &metrics.prometheus,
        ))));
        let mut output = String::new();
        prometheus_client::encoding::text::encode(&mut output, &registry).unwrap();
        output
    }

    #[tokio::test]
    async fn rejected_payloads_reach_neither_sink() {
        let server = MockServer::start().await;
        let valid = metrics_data(vec![gauge_metric(
            "allowed.requests",
            number_data_point::Value::AsInt(7),
        )]);
        let invalid_name = metrics_data(vec![gauge_metric(
            &"a".repeat(257),
            number_data_point::Value::AsInt(7),
        )]);
        let oversized = krabka_compression::compress(CompressionType::Gzip, &[0; 1025])
            .expect("compress oversized payload")
            .to_vec();
        for (codec, payload, prefixes, expected) in [
            (
                CompressionType::None,
                vec![0xff],
                vec![ALL_METRICS.into()],
                IngestionError::InvalidRecord,
            ),
            (
                CompressionType::Gzip,
                vec![0xff],
                vec![ALL_METRICS.into()],
                IngestionError::InvalidRecord,
            ),
            (
                CompressionType::Gzip,
                oversized,
                vec![ALL_METRICS.into()],
                IngestionError::TooLarge,
            ),
            (
                CompressionType::None,
                valid.encode_to_vec(),
                vec!["other.".into()],
                IngestionError::InvalidRecord,
            ),
            (
                CompressionType::None,
                invalid_name.encode_to_vec(),
                vec![ALL_METRICS.into()],
                IngestionError::InvalidRecord,
            ),
        ] {
            let metrics = receiver(server.uri());
            assert!(
                metrics.ingest_payload(codec, &payload, &prefixes, Uuid::nil(), "client",)
                    == Err(expected)
            );
            assert!(!scrape(&metrics).contains("krabka_client_"));
            metrics.shutdown().await;
        }
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn accepted_payload_updates_prometheus_and_forwards_original_metrics() {
        let server = MockServer::start().await;
        Mock::given(path("/v1/metrics"))
            .respond_with(ResponseTemplate::new(200))
            .expect(1)
            .mount(&server)
            .await;
        let metrics = receiver(server.uri());
        let md = metrics_data(vec![gauge_metric(
            "allowed.requests",
            number_data_point::Value::AsInt(7),
        )]);
        let instance = Uuid::nil();
        assert!(
            metrics.ingest_payload(
                CompressionType::None,
                &md.encode_to_vec(),
                &["allowed.".into()],
                instance,
                "client-a",
            ) == Ok(())
        );
        let output = scrape(&metrics);
        assert!(output.contains("krabka_client_allowed_requests"));
        assert!(output.contains("client_id=\"client-a\""));
        assert!(output.contains(&format!("client_instance_id=\"{instance}\"")));
        metrics.shutdown().await;
        let requests = server.received_requests().await.unwrap();
        assert!(requests.len() == 1);
        let forwarded = ExportMetricsServiceRequest::decode(requests[0].body.as_slice()).unwrap();
        assert!(forwarded == build_export_request(md, &instance.to_string()));
    }
}
