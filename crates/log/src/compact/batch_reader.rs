//! Streaming decode of the `RecordBatch`es in a sealed segment's `.log` file,
//! one batch at a time. The offset-map pass and the rewrite pass both need it,
//! so it sits beside them instead of inside either one.
//!
//! A pass holds one framed batch and its decoded form at a time, never the
//! whole segment: a compressed segment can expand to many times its size on
//! disk, so a reader that decoded it whole held that expansion for every
//! batch at once.

use {
    crate::{error::LogError, name, segment::Segment},
    krabka_protocol::records::{RecordBatch, header::HEADER_LEN},
    krabka_verified::compaction::{CompactionDecodeStep, compaction_decode_step},
    std::{
        fs::File,
        io::{BufReader, Read},
    },
};

/// Byte offset of `batch_length` in the v2 batch header, after `base_offset`.
const BATCH_LENGTH_AT: usize = 8;

/// Header bytes that `batch_length` does not count: `base_offset` and the
/// length field itself.
const LENGTH_PREFIX: usize = 12;

/// Read buffer for the segment file. It amortises the two reads each batch
/// takes, its header and its body, over small batches.
const READ_BUFFER: usize = 64 * 1024;

/// Iterator over the `RecordBatch`es of one sealed segment, in file order.
///
/// It reads each batch's frame from the `.log` file into one reused buffer
/// and decodes it there, so a pass holds one batch, not the segment. A batch
/// that fails to decode ends the iteration with its error, which is the error
/// the whole-file decoder returned for the same bytes: the frame it decodes
/// holds every byte of the batch that the file holds.
///
/// It avoids `Segment::read`, because that path returns early when the
/// segment's in-memory `last_offset` is stale. A sealed segment loaded from
/// disk through `Segment::open_unscanned` has `last_offset = base_offset - 1`
/// until a tail scan fills it in, so `Segment::read(base_offset, ..)` would
/// short-circuit to an empty result.
pub(super) struct SegmentBatches {
    reader: BufReader<File>,
    /// Bytes of the file not yet read into a frame.
    remaining: usize,
    /// The current batch's frame, reused from batch to batch.
    frame: Vec<u8>,
    /// Set once the file is exhausted or a batch failed.
    finished: bool,
}

/// Every `RecordBatch` of `segments`, oldest segment first. Each segment's
/// file is opened when the walk reaches it, so a group of many segments holds
/// one file open at a time.
pub(super) fn group_batches<'a>(
    segments: &'a [&'a Segment],
) -> impl Iterator<Item = Result<RecordBatch, LogError>> + 'a {
    segments.iter().flat_map(|seg| {
        let (opened, failed) = match segment_batches(seg) {
            Ok(batches) => (Some(batches), None),
            Err(error) => (None, Some(Err(error))),
        };
        opened.into_iter().flatten().chain(failed)
    })
}

/// Open the `.log` file of a sealed segment for [`SegmentBatches`].
fn segment_batches(seg: &Segment) -> Result<SegmentBatches, LogError> {
    let path = name::log_path(seg.dir(), seg.base_offset().0);
    let file = File::open(&path)?;
    let remaining = usize::try_from(file.metadata()?.len()).map_err(|_| {
        LogError::Corrupt(format!(
            "segment file {} is too large to address",
            path.display()
        ))
    })?;
    Ok(SegmentBatches {
        reader: BufReader::with_capacity(READ_BUFFER, file),
        remaining,
        frame: Vec::new(),
        finished: false,
    })
}

impl SegmentBatches {
    /// Append up to `wanted` more bytes of the file to the frame. Fewer arrive
    /// only when the file ends first.
    fn read_into_frame(&mut self, wanted: usize) -> Result<(), LogError> {
        let wanted = wanted.min(self.remaining);
        self.frame.reserve_exact(wanted);
        let limit = u64::try_from(wanted).expect("usize fits in u64");
        let read = (&mut self.reader)
            .take(limit)
            .read_to_end(&mut self.frame)?;
        self.remaining -= read;
        Ok(())
    }

    /// Read and decode the next batch. `remaining_before` is the file's
    /// unread length when the batch starts, which the verified decode kernel
    /// checks the decoder's progress against.
    fn next_batch(&mut self, remaining_before: usize) -> Result<RecordBatch, LogError> {
        self.frame.clear();
        self.read_into_frame(HEADER_LEN)?;
        // A short header, a bad magic byte or a negative length fails the
        // decoder on the header alone. Any other header gives the decoder the
        // body it declares, or as much of it as the file holds.
        if self.frame.len() == HEADER_LEN {
            let length_bytes: [u8; 4] = self.frame[BATCH_LENGTH_AT..LENGTH_PREFIX]
                .try_into()
                .expect("a four-byte slice");
            if let Ok(batch_length) = usize::try_from(i32::from_be_bytes(length_bytes)) {
                let body = (LENGTH_PREFIX + batch_length).saturating_sub(HEADER_LEN);
                self.read_into_frame(body)?;
            }
        }

        let mut cursor: &[u8] = &self.frame;
        let batch = RecordBatch::decode(&mut cursor).map_err(LogError::Records)?;
        let consumed = self.frame.len() - cursor.len();
        // The frame ends where the batch's `batch_length` says it does, so a
        // decoder that succeeds consumes all of it.
        let remaining_after = remaining_before - consumed;
        match compaction_decode_step(remaining_before, true, remaining_after) {
            CompactionDecodeStep::Continue if cursor.is_empty() => Ok(batch),
            CompactionDecodeStep::Continue => Err(LogError::Corrupt(
                "record batch decoder stopped inside its frame during compaction".into(),
            )),
            CompactionDecodeStep::Done | CompactionDecodeStep::Corrupt => Err(LogError::Corrupt(
                "record batch decoder made no progress during compaction".into(),
            )),
        }
    }
}

impl Iterator for SegmentBatches {
    type Item = Result<RecordBatch, LogError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        let remaining_before = self.remaining;
        if compaction_decode_step(remaining_before, false, remaining_before)
            == CompactionDecodeStep::Done
        {
            self.finished = true;
            return None;
        }
        let batch = self.next_batch(remaining_before);
        self.finished = batch.is_err();
        Some(batch)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::compact::test_support::{make_record, write_sealed_batches},
        bytes::BytesMut,
        krabka_compression::CompressionType,
        krabka_protocol::records::Attributes,
        tempfile::tempdir,
    };

    /// A gzip batch of three records at `base_offset`.
    fn compressed_batch(base_offset: i64) -> RecordBatch {
        RecordBatch {
            base_offset,
            last_offset_delta: 2,
            attributes: Attributes::default().with_compression(CompressionType::Gzip),
            records: vec![
                make_record(0, Some(b"k0"), Some(b"v0")),
                make_record(1, Some(b"k1"), None),
                make_record(2, None, Some(b"v2")),
            ],
            ..RecordBatch::default()
        }
    }

    /// An uncompressed batch of one record at `base_offset`.
    fn plain_batch(base_offset: i64) -> RecordBatch {
        RecordBatch {
            base_offset,
            last_offset_delta: 0,
            records: vec![make_record(0, Some(b"plain"), Some(b"value"))],
            ..RecordBatch::default()
        }
    }

    fn encoded(batch: &RecordBatch) -> Vec<u8> {
        let mut bytes = BytesMut::new();
        batch.encode(&mut bytes).unwrap();
        bytes.to_vec()
    }

    /// The decoder the reader replaced: the whole file in memory, decoded
    /// batch by batch until it is consumed.
    fn decode_whole_file(bytes: &[u8]) -> Result<Vec<RecordBatch>, LogError> {
        let mut cursor = bytes;
        let mut out = Vec::new();
        while !cursor.is_empty() {
            out.push(RecordBatch::decode(&mut cursor).map_err(LogError::Records)?);
        }
        Ok(out)
    }

    /// What the streaming reader yields for a `.log` file holding `bytes`.
    fn stream(bytes: &[u8]) -> Result<Vec<RecordBatch>, LogError> {
        let dir = tempdir().unwrap();
        let segment = write_sealed_batches(dir.path(), &[plain_batch(0)]);
        std::fs::write(name::log_path(segment.dir(), 0), bytes).unwrap();
        group_batches(&[&segment]).collect()
    }

    /// A batch's outcome, comparable across the two readers.
    fn outcome(result: Result<Vec<RecordBatch>, LogError>) -> Result<Vec<RecordBatch>, String> {
        result.map_err(|error| format!("{error:?}"))
    }

    /// Two good batches, then `tail`: the end of the file that each case
    /// writes after them.
    fn file_with_tail(tail: impl FnOnce(Vec<u8>) -> Vec<u8>) -> Vec<u8> {
        let mut bytes = encoded(&compressed_batch(0));
        bytes.extend(encoded(&plain_batch(3)));
        bytes.extend(tail(encoded(&compressed_batch(4))));
        bytes
    }

    /// The streaming reader decodes what the whole-file decoder decoded, and
    /// fails where it failed with the same error, so the passes keep their
    /// behaviour on a damaged segment.
    #[yare::parameterized(
        intact = { |third| third },
        no_third_batch = { |_| Vec::new() },
        truncated_header = { |third: Vec<u8>| third[..30].to_vec() },
        truncated_body = { |third: Vec<u8>| third[..third.len() - 5].to_vec() },
        unsupported_magic = { |mut third: Vec<u8>| { third[16] = 1; third } },
        negative_length = { |mut third: Vec<u8>| { third[8..12].copy_from_slice(&(-1i32).to_be_bytes()); third } },
        length_inside_header = { |mut third: Vec<u8>| { third[8..12].copy_from_slice(&10i32.to_be_bytes()); third } },
        length_past_end = { |mut third: Vec<u8>| { third[8..12].copy_from_slice(&i32::MAX.to_be_bytes()); third } },
        crc_mismatch = { |mut third: Vec<u8>| { let last = third.len() - 1; third[last] ^= 0xff; third } },
    )]
    fn streams_what_the_whole_file_decoder_decoded(tail: fn(Vec<u8>) -> Vec<u8>) {
        let bytes = file_with_tail(tail);
        assert2::assert!(outcome(stream(&bytes)) == outcome(decode_whole_file(&bytes)));
    }

    #[test]
    fn an_empty_segment_has_no_batches() {
        assert2::assert!(outcome(stream(&[])) == Ok(vec![]));
    }

    /// A group walks its segments oldest first and opens each when it gets
    /// there, so a missing later segment fails only after the earlier
    /// segment's batches.
    #[test]
    fn a_group_yields_each_segment_in_order() {
        let dir = tempdir().unwrap();
        let first = write_sealed_batches(dir.path(), &[compressed_batch(0)]);
        let second = write_sealed_batches(dir.path(), &[plain_batch(3), plain_batch(4)]);
        std::fs::remove_file(name::log_path(dir.path(), 3)).unwrap();

        let group = [&first, &second];
        let mut walk = group_batches(&group);
        let decoded = walk.next().map(outcome_of_one);
        let missing = walk.next().map(|batch| batch.is_err());

        assert2::assert!(decoded == Some(Ok(decoded_form(&compressed_batch(0)))));
        assert2::assert!(missing == Some(true));
        assert2::assert!(walk.next().is_none());
    }

    fn outcome_of_one(batch: Result<RecordBatch, LogError>) -> Result<RecordBatch, String> {
        batch.map_err(|error| format!("{error:?}"))
    }

    /// `batch` as the decoder returns it after an encode round trip.
    fn decoded_form(batch: &RecordBatch) -> RecordBatch {
        RecordBatch::decode(&mut encoded(batch).as_slice()).unwrap()
    }
}
