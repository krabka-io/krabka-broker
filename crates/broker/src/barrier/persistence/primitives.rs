//! The three reads that every `__barrier_state` record kind shares.
//!
//! The leading version check, the trailing-byte check, and the `i32`-counted
//! array appear in all four decoders, so they live here rather than once per
//! record kind.

use krabka_protocol::{
    ProtocolError,
    primitives::{array::get_array_len, fixed::get_i16},
};

use super::RECORD_VERSION;

/// Read and check the leading record version.
pub(super) fn expect_version(cur: &mut &[u8]) -> Result<(), ProtocolError> {
    if get_i16(cur)? == RECORD_VERSION {
        Ok(())
    } else {
        Err(ProtocolError::InvalidValue(
            "unsupported barrier record version",
        ))
    }
}

/// Reject a record that carries bytes past its last field.
pub(super) fn expect_end(cur: &[u8]) -> Result<(), ProtocolError> {
    if cur.is_empty() {
        Ok(())
    } else {
        Err(ProtocolError::InvalidValue(
            "trailing bytes after barrier record",
        ))
    }
}

/// Read an `i32`-counted array, and read each element with `element`.
pub(super) fn decode_vec<T>(
    cur: &mut &[u8],
    element: impl Fn(&mut &[u8]) -> Result<T, ProtocolError>,
) -> Result<Vec<T>, ProtocolError> {
    let len = get_array_len(cur, false)?;
    let mut out = Vec::with_capacity(len);
    for _ in 0..len {
        out.push(element(cur)?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use crate::barrier::persistence::{
        RecordKey, decode_cut, decode_group, decode_injection_start, decode_key, encode_cut,
        encode_group, encode_injection_start, encode_key,
        test_support::{sample_cut, sample_group, sample_injection_start},
    };

    #[test]
    fn every_decoder_rejects_a_wrong_version() {
        let mut key = encode_key(&RecordKey::cut("orders-cut", 7));
        key[1] = 1;
        assert!(decode_key(&key).is_err());

        let mut group = encode_group(&sample_group());
        group[1] = 1;
        assert!(decode_group(&group).is_err());

        let mut start = encode_injection_start(&sample_injection_start());
        start[1] = 1;
        assert!(decode_injection_start(&start).is_err());

        let mut cut = encode_cut(&sample_cut());
        cut[1] = 1;
        assert!(decode_cut(&cut).is_err());
    }

    #[test]
    fn every_decoder_rejects_trailing_bytes() {
        let mut key = encode_key(&RecordKey::cut("orders-cut", 7));
        key.push(0);
        assert!(decode_key(&key).is_err());

        let mut group = encode_group(&sample_group());
        group.push(0);
        assert!(decode_group(&group).is_err());

        let mut start = encode_injection_start(&sample_injection_start());
        start.push(0);
        assert!(decode_injection_start(&start).is_err());

        let mut cut = encode_cut(&sample_cut());
        cut.push(0);
        assert!(decode_cut(&cut).is_err());
    }
}
