//! Metric exposition fixtures that retain the registry guard at the call site.

krabka_macros::metric_registry_fixture!(render_registry);
pub(crate) use render_registry;
