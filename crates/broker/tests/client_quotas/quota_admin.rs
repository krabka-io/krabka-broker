//! Quota RPC drivers shared with the raw Kafka wire fixtures.

pub use crate::kafka_wire::quotas::{
    drive_alter_client_quotas_sasl, drive_describe_client_quotas_sasl,
};
