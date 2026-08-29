//! The `set_runtime_*` assignment macros that the `[runtime]` appliers expand.
//!
//! `macro_rules!` definitions are textually scoped, so the module root declares
//! this module `#[macro_use]` ahead of every module that expands these macros.
//! Each macro reads one optional `RuntimeFileConfig` field, validates it
//! through a named validator, and assigns the result to a `BrokerConfig` field.

/// Assigns a validated dimensioned time value.
macro_rules! set_runtime_time_millis {
    ($runtime:ident, $field:ident, $target:expr) => {
        if let Some(value) = $runtime.$field {
            $target = positive_time(stringify!($field), value)?;
        }
    };
    ($runtime:ident, $field:ident, $target:expr, positive_i32) => {
        if let Some(value) = $runtime.$field {
            $target = whole_millis_i32_time(stringify!($field), value)?;
        }
    };
    ($runtime:ident, $field:ident, $target:expr, positive_i64) => {
        if let Some(value) = $runtime.$field {
            $target = whole_millis_i64_time(stringify!($field), value)?;
        }
    };
}

/// Assigns a `_ms` key into a [`std::time::Duration`] field.
///
/// For the group-coordinator configs, which are still `Duration`-typed: two of
/// the four (`StreamsGroupConfig`, `ShareCoordinatorConfig`) derive `Eq` and so
/// cannot hold an `f64`-backed quantity, and keeping all four in one
/// representation is what lets `BrokerConfig::validate` compare them uniformly.
macro_rules! set_runtime_duration {
    ($runtime:ident, $field:ident, $target:expr) => {
        if let Some(value) = $runtime.$field {
            $target = positive_time(stringify!($field), value)?.to_std();
        }
    };
}

/// Assigns a validated dimensioned time value.
macro_rules! set_runtime_time_secs {
    ($runtime:ident, $field:ident, $target:expr) => {
        if let Some(value) = $runtime.$field {
            $target = positive_time(stringify!($field), value)?;
        }
    };
}

/// Assigns a validated dimensioned byte size.
macro_rules! set_runtime_size_bytes {
    ($runtime:ident, $field:ident, $target:expr, $validator:ident) => {
        if let Some(value) = $runtime.$field {
            $target = $validator(stringify!($field), value)?;
        }
    };
}

macro_rules! set_runtime_validated {
    ($runtime:ident, $field:ident, $target:expr, $validator:ident) => {
        if let Some(value) = $runtime.$field {
            $target = $validator(stringify!($field), value)?;
        }
    };
}

macro_rules! set_runtime_i32 {
    ($runtime:ident, $field:ident, $target:expr) => {
        set_runtime_validated!($runtime, $field, $target, positive_i32);
    };
}

macro_rules! set_runtime_i64 {
    ($runtime:ident, $field:ident, $target:expr) => {
        set_runtime_validated!($runtime, $field, $target, positive_i64);
    };
}

macro_rules! set_runtime_usize {
    ($runtime:ident, $field:ident, $target:expr) => {
        set_runtime_validated!($runtime, $field, $target, positive_usize);
    };
}

macro_rules! set_runtime_u32 {
    ($runtime:ident, $field:ident, $target:expr) => {
        set_runtime_validated!($runtime, $field, $target, positive_u32);
    };
}

macro_rules! set_runtime_positive_u64 {
    ($runtime:ident, $field:ident, $target:expr) => {
        set_runtime_validated!($runtime, $field, $target, positive_u64);
    };
}

macro_rules! set_runtime_plain {
    ($runtime:ident, $field:ident, $target:expr) => {
        if let Some(value) = $runtime.$field {
            $target = value;
        }
    };
}
