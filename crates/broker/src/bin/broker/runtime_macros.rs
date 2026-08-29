//! The two field-copy macros that build a `RuntimeFileConfig` from the parsed
//! command line.
//!
//! `macro_rules!` is textually scoped, so the macros need a module that the
//! crate root declares with `#[macro_use]` ahead of every module that expands
//! them.

macro_rules! copy_refined_runtime {
    ($source:ident, $target:ident, $($field:ident),+ $(,)?) => {
        $(
            $target.$field = $source.$field.map(|value| value.into_value());
        )+
    };
}

macro_rules! copy_plain_runtime {
    ($source:ident, $target:ident, $($field:ident),+ $(,)?) => {
        $(
            $target.$field = $source.$field;
        )+
    };
}
