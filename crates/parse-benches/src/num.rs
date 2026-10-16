//! The one numeric conversion the budget code needs.

use num_traits::ToPrimitive;

/// The nearest `f64` to a count. Counts past 2^53 lose precision, which a
/// record count or a byte total never reaches.
pub(crate) fn to_f64(value: u64) -> f64 { value.to_f64().expect("every u64 has a nearest f64") }
