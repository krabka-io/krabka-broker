//! The `set_runtime*` assignment macros that the `[runtime]` appliers expand.
//!
//! `macro_rules!` definitions are textually scoped, so the module root declares
//! this module `#[macro_use]` ahead of every module that expands these macros.
//! Each macro reads one optional `RuntimeFileConfig` field, validates it
//! through a named validator, and assigns the result to a field of the target.

/// Assigns each listed `[runtime]` field to a field of `$target`.
///
/// Each `kind: item, ...;` group applies every item through one kind of
/// assignment, and an item is one of
///
/// - `field`, which assigns `$target.field`
/// - `field => path.to.target`, which assigns `$target.path.to.target`
/// - either of those followed by `in range`, which passes `range` to the
///   validator as a third argument
///
/// A `plain:` group assigns the value unvalidated, and a `duration:` group
/// assigns a positive time as a [`std::time::Duration`]. Any other kind names
/// the validator, which is keyed by the `[runtime]` field's own name whatever
/// the target is called. Fields apply in the order listed, so the first invalid
/// one is the error reported.
///
/// `duration:` is for the group-coordinator configs, which are still
/// `Duration`-typed: two of the four (`StreamsGroupConfig`,
/// `ShareCoordinatorConfig`) derive `Eq` and so cannot hold an `f64`-backed
/// quantity, and keeping all four in one representation is what lets
/// `BrokerConfig::validate` compare them uniformly.
macro_rules! set_runtime {
    ($runtime:ident => $target:ident;) => {};
    (
        $runtime:ident => $target:ident;
        $kind:ident: $($field:ident $(=> $($path:ident).+)? $(in $range:expr)?),+;
        $($rest:tt)*
    ) => {
        $(set_runtime_field!($kind; $runtime => $target; $field $(=> $($path).+)? $(in $range)?);)+
        set_runtime!($runtime => $target; $($rest)*);
    };
}

/// Assigns one `set_runtime!` item; see there for the kinds.
macro_rules! set_runtime_field {
    ($kind:ident; $runtime:ident => $target:ident; $field:ident $(in $range:expr)?) => {
        set_runtime_field!($kind; $runtime => $target; $field => $field $(in $range)?);
    };
    (plain; $runtime:ident => $target:ident; $field:ident => $($path:ident).+) => {
        if let Some(value) = $runtime.$field {
            $target.$($path).+ = value;
        }
    };
    (duration; $runtime:ident => $target:ident; $field:ident => $($path:ident).+) => {
        if let Some(value) = $runtime.$field {
            $target.$($path).+ = positive_time(stringify!($field), value)?.to_std();
        }
    };
    (
        $validator:ident; $runtime:ident => $target:ident;
        $field:ident => $($path:ident).+ $(in $range:expr)?
    ) => {
        if let Some(value) = $runtime.$field {
            $target.$($path).+ = $validator(stringify!($field), value $(, $range)?)?;
        }
    };
}
