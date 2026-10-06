//! The `set_runtime*` assignment macros that the `[runtime]` appliers expand.
//!
//! `macro_rules!` definitions are textually scoped, so the module root declares
//! this module `#[macro_use]` ahead of every module that expands these macros.
//! Each macro reads one optional `RuntimeFileConfig` field, validates it
//! through a named validator, and assigns the result to a `BrokerConfig` field.

/// Assigns each listed field to the `BrokerConfig` field of the same name.
///
/// Each `validator: field, ...;` group passes every field through the named
/// validator, keyed by the field's own name; a `plain:` group assigns the
/// value unvalidated. Fields apply in the order listed, so the first invalid
/// one is the error reported.
macro_rules! set_runtime {
    ($runtime:ident => $cfg:ident;) => {};
    ($runtime:ident => $cfg:ident; plain: $($field:ident),+; $($rest:tt)*) => {
        $(set_runtime_plain!($runtime, $field, $cfg.$field);)+
        set_runtime!($runtime => $cfg; $($rest)*);
    };
    ($runtime:ident => $cfg:ident; $validator:ident: $($field:ident),+; $($rest:tt)*) => {
        $(set_runtime_validated!($runtime, $field, $cfg.$field, $validator);)+
        set_runtime!($runtime => $cfg; $($rest)*);
    };
}

/// Assigns a positive dimensioned time value to a differently named field.
macro_rules! set_runtime_time_millis {
    ($runtime:ident, $field:ident, $target:expr) => {
        set_runtime_validated!($runtime, $field, $target, positive_time);
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

/// Assigns `$field` through the named `$validator`.
macro_rules! set_runtime_validated {
    ($runtime:ident, $field:ident, $target:expr, $validator:ident) => {
        if let Some(value) = $runtime.$field {
            $target = $validator(stringify!($field), value)?;
        }
    };
}

/// Assigns `$field` unvalidated.
macro_rules! set_runtime_plain {
    ($runtime:ident, $field:ident, $target:expr) => {
        if let Some(value) = $runtime.$field {
            $target = value;
        }
    };
}
