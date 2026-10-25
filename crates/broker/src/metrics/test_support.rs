//! Metric exposition fixtures that retain the registry guard at the call site.

use prometheus_client::encoding::text;

pub fn encode_metrics(metrics: &super::BrokerMetrics) -> String {
    let mut body = String::new();
    text::encode(&mut body, &metrics.registry.try_lock().expect("registry")).expect("encode");
    body
}
