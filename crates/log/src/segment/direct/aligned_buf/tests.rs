//! Unit tests for the aligned buffer: alignment across growth, and zero fill.

use {super::*, crate::segment::direct::test_support::pattern, assert2::check};

#[test]
fn aligned_buf_stays_aligned_and_keeps_its_contents_as_it_grows() {
    for align in [1, 2, 8, 64, 512, 4096] {
        let mut buf = AlignedBuf::new(align);
        check!(buf.as_slice().is_empty());
        let mut expected = Vec::new();
        for (seed, size) in [3, 100, 1000, 5000, 20_000].into_iter().enumerate() {
            let bytes = pattern(u8::try_from(seed).unwrap(), size);
            buf.extend_from_slice(&bytes);
            expected.extend_from_slice(&bytes);
            check!(buf.as_slice().as_ptr().addr() % align == 0, "align {align}");
            check!(buf.as_slice().len() == expected.len());
            check!(buf.as_slice() == &expected[..], "align {align}");
        }
    }
}

#[test]
fn aligned_buf_resize_zero_fills_over_old_contents() {
    let mut buf = AlignedBuf::new(512);
    buf.extend_from_slice(&[7; 100]);
    buf.clear();
    check!(buf.as_slice().is_empty());
    buf.extend_from_slice(&[1; 10]);
    buf.resize(40);
    check!(buf.as_slice() == &[[1; 10].as_slice(), &[0; 30]].concat()[..]);
    buf.resize(5);
    check!(buf.as_slice() == &[1; 5]);
    buf.resize(3000);
    check!(buf.as_slice() == &[[1; 5].as_slice(), &[0; 2995]].concat()[..]);
    check!(buf.as_slice().as_ptr().addr() % 512 == 0);
}
