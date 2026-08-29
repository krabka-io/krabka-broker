//! The value model for one client-reported metric point: the three shapes a
//! point can take, and how a point folds a delta into a running total.
//!
//! The shapes stay together because every operation on them is a match over
//! all three, and because a delta only folds into a total of its own shape,
//! which is the check that keeps a counter and a histogram from merging.

use prometheus_client::{
    encoding::{EncodeMetric, MetricEncoder},
    metrics::{MetricType, counter::ConstCounter, gauge::ConstGauge},
};

#[derive(Debug, Clone)]
pub(crate) enum PointValue {
    Gauge(f64),
    Counter(f64),
    Histogram {
        count: u64,
        sum: f64,
        buckets: Vec<(f64, u64)>,
    },
}

impl PointValue {
    pub(super) fn accumulate(&mut self, delta: &Self) -> bool {
        match (self, delta) {
            (Self::Gauge(total), Self::Gauge(value))
            | (Self::Counter(total), Self::Counter(value)) => {
                *total += *value;
                true
            }
            (
                Self::Histogram {
                    count,
                    sum,
                    buckets,
                },
                Self::Histogram {
                    count: delta_count,
                    sum: delta_sum,
                    buckets: delta_buckets,
                },
            ) if buckets
                .iter()
                .map(|(bound, _)| bound)
                .eq(delta_buckets.iter().map(|(bound, _)| bound)) =>
            {
                *count = count.saturating_add(*delta_count);
                *sum += *delta_sum;
                for ((_, count), (_, delta_count)) in buckets.iter_mut().zip(delta_buckets) {
                    *count = count.saturating_add(*delta_count);
                }
                true
            }
            _ => false,
        }
    }

    pub(super) fn same_type(&self, other: &Self) -> bool {
        matches!(
            (self, other),
            (Self::Gauge(_), Self::Gauge(_))
                | (Self::Counter(_), Self::Counter(_))
                | (Self::Histogram { .. }, Self::Histogram { .. })
        )
    }

    pub(super) fn metric_type(&self) -> MetricType {
        match self {
            Self::Gauge(_) => MetricType::Gauge,
            Self::Counter(_) => MetricType::Counter,
            Self::Histogram { .. } => MetricType::Histogram,
        }
    }

    pub(super) fn encode(&self, encoder: MetricEncoder) -> Result<(), std::fmt::Error> {
        match self {
            Self::Gauge(value) => ConstGauge::new(*value).encode(encoder),
            Self::Counter(value) => ConstCounter::new(*value).encode(encoder),
            Self::Histogram {
                count,
                sum,
                buckets,
            } => {
                let mut encoder = encoder;
                encoder.encode_histogram::<[(&str, &str); 0]>(*sum, *count, buckets, None)
            }
        }
    }
}

/// A single decoded client metric data point destined for Prometheus.
#[derive(Debug, Clone)]
pub(crate) struct DataPoint {
    pub metric: String,
    pub client_instance_id: String,
    pub client_id: String,
    pub attributes: Vec<(String, String)>,
    pub value: PointValue,
    pub delta_start: Option<u64>,
}
