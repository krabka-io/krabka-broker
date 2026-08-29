//! The byte codec for one spooled record and the length-prefixed frame that
//! carries it.
//!
//! A frame is `[u32 len][record]` and a record is `[u8 class_tag]
//! [u32 value_len][value][u32 header_count]([u32 klen][k][u32 vlen][v])*`, all
//! lengths big-endian. The codec is deliberately its own module: the bytes it
//! writes are the on-disk format, and the decoders return `None` rather than
//! panicking so that the scan can treat a short or corrupt tail as
//! end-of-data.

use crate::{event::AuditEventClass, sink::AuditRecord};

pub(super) fn encode_frame(record: &AuditRecord) -> Vec<u8> {
    let body = encode_record(record);
    let len = u32::try_from(body.len()).expect("audit record fits u32");
    let mut frame = Vec::with_capacity(4 + body.len());
    frame.extend_from_slice(&len.to_be_bytes());
    frame.extend_from_slice(&body);
    frame
}

fn encode_record(record: &AuditRecord) -> Vec<u8> {
    let mut b = Vec::new();
    b.push(record.class.tag());
    put_bytes(&mut b, &record.value);
    let hc = u32::try_from(record.headers.len()).expect("header count fits u32");
    b.extend_from_slice(&hc.to_be_bytes());
    for (k, v) in &record.headers {
        put_bytes(&mut b, k.as_bytes());
        put_bytes(&mut b, v);
    }
    b
}

fn put_bytes(b: &mut Vec<u8>, bytes: &[u8]) {
    let len = u32::try_from(bytes.len()).expect("field fits u32");
    b.extend_from_slice(&len.to_be_bytes());
    b.extend_from_slice(bytes);
}

pub(super) fn decode_record(mut b: &[u8]) -> Option<AuditRecord> {
    let class = AuditEventClass::from_tag(*b.first()?)?;
    b = &b[1..];
    let value = take_bytes(&mut b)?;
    let hc = usize::try_from(take_u32(&mut b)?).unwrap_or(0);
    let mut headers = Vec::with_capacity(hc);
    for _ in 0..hc {
        let k = take_bytes(&mut b)?;
        let v = take_bytes(&mut b)?;
        headers.push((String::from_utf8(k).ok()?, v));
    }
    Some(AuditRecord {
        class,
        value,
        headers,
    })
}

fn take_u32(b: &mut &[u8]) -> Option<u32> {
    if b.len() < 4 {
        return None;
    }
    let n = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
    *b = &b[4..];
    Some(n)
}

fn take_bytes(b: &mut &[u8]) -> Option<Vec<u8>> {
    let len = usize::try_from(take_u32(b)?).unwrap_or(0);
    if b.len() < len {
        return None;
    }
    let out = b[..len].to_vec();
    *b = &b[len..];
    Some(out)
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    /// Four bytes is the boundary: exactly four is a readable `u32`, and fewer
    /// must yield `None` rather than index past the end.
    #[test]
    fn take_u32_reads_at_the_four_byte_boundary() {
        // (input, decoded, bytes left unread)
        let cases: &[(&[u8], Option<u32>, usize)] = &[
            (&[], None, 0),
            (&[0x00], None, 1),
            (&[0x00, 0x00, 0x01], None, 3),
            (&[0x00, 0x00, 0x01, 0x00], Some(256), 0),
            (&[0x00, 0x00, 0x01, 0x00, 0xff], Some(256), 1),
        ];
        for (input, want, want_left) in cases {
            let mut b: &[u8] = input;
            let got = take_u32(&mut b);
            check!(
                (got, b.len()) == (*want, *want_left),
                "take_u32({input:x?})"
            );
        }
    }
}
