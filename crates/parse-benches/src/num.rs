//! The one numeric conversion the budget code needs.

/// The nearest `f64` to a count. Counts past 2^53 lose precision, which a
/// record count or a byte total never reaches.
///
/// Both halves of `value` convert exactly, and so does the high half's
/// scaling, so the one rounding is in the final sum: the result is the
/// nearest `f64`.
pub fn to_f64(value: u64) -> f64 {
    let [h0, h1, h2, h3, l0, l1, l2, l3] = value.to_be_bytes();
    let high = f64::from(u32::from_be_bytes([h0, h1, h2, h3]));
    let low = f64::from(u32::from_be_bytes([l0, l1, l2, l3]));
    high.mul_add(4_294_967_296.0, low)
}
