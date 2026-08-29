//! The audit trail: whether audit records are written at all, the internal
//! topic that carries them, the key that signs the checkpoints and how often
//! one is cut, and the durable spool that holds records the broker could not
//! append yet.

// Link 9 of the `BrokerConfig` field chain, and its last: it adds this
// group and hands the whole set to `define_broker_config`.
macro_rules! audit_fields {
    ($($collected:tt)*) => {
        define_broker_config! {
            $($collected)*
            /// Whether the audit subsystem is active (`FedRAMP` MLA).
            pub audit_enabled: bool,
            /// Internal topic name for audit records.
            pub audit_topic: String,
            /// Path to the PKCS#8 Ed25519 audit checkpoint signing key. `None` means
            /// no checkpoints.
            pub audit_signing_key_path: Option<std::path::PathBuf>,
            /// Key id recorded on checkpoints (for rotation).
            pub audit_signing_key_id: Option<String>,
            /// Emit a checkpoint after this many audit records.
            pub audit_checkpoint_every_n: u64,
            /// Emit a checkpoint at least this often.
            pub audit_checkpoint_every: Time,
            /// Directory for the durable audit spool. A relative path resolves under
            /// the broker's log dir.
            pub audit_spool_dir: std::path::PathBuf,
            /// Cap on the audit spool size.
            pub audit_spool_max: ByteSize,
        }
    };
}
