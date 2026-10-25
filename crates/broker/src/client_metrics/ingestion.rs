//! Validation and sink delivery for admitted client telemetry payloads.
//!
//! Admission and Kafka error mapping belong to the wire handler. This module
//! validates the entire payload before either sink receives any metrics.

use {
    self::prometheus::{flatten_for_prometheus, point_count},
    super::{ClientMetrics, config::ALL_METRICS},
    crate::metrics::ClientMetricsRejection,
    krabka_compression::{CompressionError, CompressionType},
    krabka_security::Principal,
    opentelemetry_proto::tonic::metrics::v1::MetricsData,
    prost::Message as _,
    uuid::Uuid,
};

mod prometheus;

/// Payload failures independent of Kafka's version-specific error mapping.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestionError {
    InvalidRecord,
    TooLarge,
}

/// Who pushed a payload: the client's own identifiers, and the authenticated
/// principal of its connection, which the client cannot choose.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PushSource<'a> {
    pub instance: Uuid,
    pub client_id: &'a str,
    pub principal: &'a Principal,
}

impl ClientMetrics {
    /// Validates and delivers a nonempty, admitted payload. Conversion must
    /// succeed before updating Prometheus or enqueueing the original OTLP data.
    ///
    /// A payload that fails validation counts as `validation` rejections: one
    /// per OTLP data point it decoded to, or one when it did not decode.
    pub(crate) fn ingest_payload(
        &self,
        codec: CompressionType,
        payload: &[u8],
        subscribed: &[String],
        source: PushSource<'_>,
    ) -> Result<(), IngestionError> {
        let mut decoded = None;
        let result = self.validate_and_deliver(codec, payload, subscribed, source, &mut decoded);
        if result.is_err() {
            let rejected = decoded.as_ref().map_or(1, point_count).max(1);
            self.prometheus
                .stats()
                .reject(ClientMetricsRejection::Validation, rejected);
        }
        result
    }

    /// The pipeline of [`Self::ingest_payload`]. It leaves the decoded payload
    /// in `decoded`, so a rejection can be counted in data points.
    fn validate_and_deliver(
        &self,
        codec: CompressionType,
        payload: &[u8],
        subscribed: &[String],
        PushSource {
            instance,
            client_id,
            principal,
        }: PushSource<'_>,
        decoded: &mut Option<MetricsData>,
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
        let md = decoded.insert(MetricsData::decode(&raw[..]).map_err(|error| {
            tracing::debug!(error = %error, "client-metrics OTLP decode failed");
            IngestionError::InvalidRecord
        })?);
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
        let points = flatten_for_prometheus(md, &instance, client_id)
            .map_err(|()| IngestionError::InvalidRecord)?;
        self.prometheus
            .ingest(&principal.to_kafka().to_string(), &points);
        let md = decoded.take().expect("decoded above");
        self.otlp.forward(md, &instance);
        Ok(())
    }
}

#[cfg(test)]
#[cfg(not(target_family = "wasm"))]
mod tests {
    use {
        super::*,
        crate::{
            client_metrics::{
                ClientMetricsManager,
                config::INTERVAL_MS_DEFAULT,
                otlp_sink::{OtlpForwarder, build_export_request},
                prometheus_sink::{ClientMetricsCollector, SharedClientMetricsCollector},
                test_support::{gauge_metric, metrics_data},
            },
            metrics::ClientMetricsRejectionLabel,
            test_support::principal,
        },
        assert2::assert,
        krabka_telemetry::OtlpProtocol,
        krabka_units::kibibytes,
        opentelemetry_proto::tonic::{
            collector::metrics::v1::ExportMetricsServiceRequest, metrics::v1::number_data_point,
        },
        prometheus_client::{encoding::text, metrics::counter::Counter, registry::Registry},
        std::{sync::Arc, time::Duration},
        wiremock::{Mock, MockServer, ResponseTemplate, matchers::path},
    };

    fn source<'a>(instance: Uuid, client_id: &'a str, principal: &'a Principal) -> PushSource<'a> {
        PushSource {
            instance,
            client_id,
            principal,
        }
    }

    fn receiver(endpoint: String) -> ClientMetrics {
        ClientMetrics {
            manager: ClientMetricsManager::new(kibibytes(1), INTERVAL_MS_DEFAULT),
            prometheus: Arc::new(
                ClientMetricsCollector::builder()
                    .ttl(Duration::from_secs(60))
                    .build(),
            ),
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
        text::encode(&mut output, &registry).unwrap();
        output
    }

    #[tokio::test]
    async fn rejected_payloads_reach_neither_sink() {
        let anonymous = principal("ANONYMOUS");
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
        let three_points = metrics_data(
            ["other.a", "other.b", "other.c"]
                .into_iter()
                .map(|name| gauge_metric(name, number_data_point::Value::AsInt(1)))
                .collect(),
        );
        // `rejected` is the `validation` count: one per decoded data point,
        // and one for a payload that never decoded.
        for (codec, payload, prefixes, expected, rejected) in [
            (
                CompressionType::None,
                vec![0xff],
                vec![ALL_METRICS.into()],
                IngestionError::InvalidRecord,
                1,
            ),
            (
                CompressionType::Gzip,
                vec![0xff],
                vec![ALL_METRICS.into()],
                IngestionError::InvalidRecord,
                1,
            ),
            (
                CompressionType::Gzip,
                oversized,
                vec![ALL_METRICS.into()],
                IngestionError::TooLarge,
                1,
            ),
            (
                CompressionType::None,
                valid.encode_to_vec(),
                vec!["other.".into()],
                IngestionError::InvalidRecord,
                1,
            ),
            (
                CompressionType::None,
                three_points.encode_to_vec(),
                vec!["allowed.".into()],
                IngestionError::InvalidRecord,
                3,
            ),
            (
                CompressionType::None,
                invalid_name.encode_to_vec(),
                vec![ALL_METRICS.into()],
                IngestionError::InvalidRecord,
                1,
            ),
        ] {
            let metrics = receiver(server.uri());
            assert!(
                metrics.ingest_payload(
                    codec,
                    &payload,
                    &prefixes,
                    source(Uuid::nil(), "client", &anonymous),
                ) == Err(expected)
            );
            assert!(!scrape(&metrics).contains("krabka_client_"));
            let stats = metrics.prometheus.stats();
            assert!(
                (
                    stats
                        .rejected
                        .get_or_create(&ClientMetricsRejectionLabel {
                            reason: ClientMetricsRejection::Validation,
                        })
                        .get(),
                    stats.accepted.get(),
                ) == (rejected, 0)
            );
            metrics.shutdown().await;
        }
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn accepted_payload_updates_prometheus_and_forwards_original_metrics() {
        let anonymous = principal("ANONYMOUS");
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
                source(instance, "client-a", &anonymous),
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
