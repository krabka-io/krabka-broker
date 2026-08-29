//! Fixture builders shared by the `PushTelemetry` handler's test modules.
//!
//! Both the live-broker handler tests and the Prometheus flattening tests need
//! the same minimal OTLP payload shapes, so the builders live here rather than
//! being duplicated in each module.

use opentelemetry_proto::tonic::metrics::v1::{
    Metric, MetricsData, NumberDataPoint, ResourceMetrics, ScopeMetrics, number_data_point,
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
