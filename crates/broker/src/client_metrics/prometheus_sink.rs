//! Prometheus sink for KIP-714 client metrics.
//!
//! Client metric *names* are dynamic, so this module registers a custom
//! `Collector` instead of static `Family` values. That collector renders a
//! live snapshot at scrape time, with stale points removed, as
//! `krabka_client_*` series.
//!
//! The point shapes and the ingest input type live in `point`, the live
//! snapshot and its ingest path in `collector`, and the scrape-time
//! rendering in `render`.

mod collector;
mod point;
mod render;

pub(crate) use self::{
    collector::ClientMetricsCollector,
    point::{DataPoint, PointValue},
    render::SharedClientMetricsCollector,
};
