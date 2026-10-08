//! Fixture builders shared by the `PushTelemetry` handler's test modules.
//!
//! Both the live-broker handler tests and the Prometheus flattening tests need
//! the same minimal OTLP payload shapes, so the builders live here rather than
//! being duplicated in each module.

use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, MetricsData, NumberDataPoint, ResourceMetrics, ScopeMetrics, metric,
    number_data_point,
};

pub(super) fn number_point(value: number_data_point::Value) -> NumberDataPoint {
    NumberDataPoint {
        value: Some(value),
        ..Default::default()
    }
}

pub(super) fn metrics_data(metrics: Vec<Metric>) -> MetricsData {
    MetricsData {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// A one-point gauge for the OTLP input fixtures; expected Prometheus output stays explicit.
pub(super) fn gauge_metric(name: &str, value: number_data_point::Value) -> Metric {
    Metric {
        name: name.into(),
        data: Some(metric::Data::Gauge(Gauge {
            data_points: vec![number_point(value)],
        })),
        ..Default::default()
    }
}
