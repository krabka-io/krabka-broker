//! Streaming decode of the `RecordBatch`es in a sealed segment's `.log` file,
//! one batch at a time. The offset-map pass and the rewrite pass both need it,
//! so it sits beside them instead of inside either one.
//!
//! A pass holds one framed batch and its decoded form at a time, never the
//! whole segment: a compressed segment can expand to many times its size on
//! disk, so a reader that decoded it whole held that expansion for every
//! batch at once.
//!
//! The reader works on [`SealedInput`]s, the open files of the sealed segments,
//! not on the segments themselves, so a pass can read them without holding the
//! log.

use {
    crate::{error::LogError, segment::SealedInput},
    krabka_protocol::records::{RecordBatch, header::HEADER_LEN},
    krabka_verified::compaction::{CompactionDecodeStep, compaction_decode_step},
    std::io::{BufReader, Read},
};

/// Byte offset of `batch_length` in the v2 batch header, after `base_offset`.
const BATCH_LENGTH_AT: usize = 8;

/// Header bytes that `batch_length` does not count: `base_offset` and the
/// length field itself.
const LENGTH_PREFIX: usize = 12;

/// Read buffer for the segment file. It amortises the two reads each batch
/// takes, its header and its body, over small batches.
const READ_BUFFER: usize = 64 * 1024;

/// Every `RecordBatch` of `inputs`, oldest segment first.
///
/// A walk whose log is fenced ends with [`LogError::Fenced`] at the next
/// batch, so a pass stops reading, and stops writing, soon after the fence.
pub(super) fn group_batches(
    inputs: &[SealedInput],
) -> impl Iterator<Item = Result<RecordBatch, LogError>> + '_ {
    inputs.iter().flat_map(SegmentBatches::new)
}

/// Sequential reads over a [`SealedInput`], from its first byte.
struct InputReader<'a> {
    input: &'a SealedInput,
    position: u64,
}

impl Read for InputReader<'_> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let read = self.input.read_at(self.position, buf)?;
        self.position += u64::try_from(read).expect("usize fits in u64");
        Ok(read)
    }
}

/// Iterator over the `RecordBatch`es of one sealed segment, in file order.
///
/// It reads each batch's frame from the `.log` file into one reused buffer
/// and decodes it there, so a pass holds one batch, not the segment. A batch
/// that fails to decode ends the iteration with its error, which is the error
/// the whole-file decoder returned for the same bytes: the frame it decodes
/// holds every byte of the batch that the file holds.
///
/// It reads the file through the handle the segment shares, to the file's
/// end. It avoids `Segment::read`, because that
/// path returns early when the segment's in-memory `last_offset` is stale. A
/// sealed segment loaded from disk through `Segment::open_unscanned` has
/// `last_offset = base_offset - 1` until a tail scan fills it in, so
/// `Segment::read(base_offset, ..)` would short-circuit to an empty result.
struct SegmentBatches<'a> {
    input: &'a SealedInput,
    reader: BufReader<InputReader<'a>>,
    /// Bytes of the file not yet read into a frame.
    remaining: usize,
    /// Why the file's length could not be read, yielded as the first item.
    unreadable: Option<LogError>,
    /// The current batch's frame, reused from batch to batch.
    frame: Vec<u8>,
    /// Set once the input is exhausted or a batch failed.
    finished: bool,
}

impl<'a> SegmentBatches<'a> {
    /// Walk `input` to the end of its file. A byte past the segment's own
    /// length is damage the walk has to report, as the whole-file decoder did.
    fn new(input: &'a SealedInput) -> Self {
        let (remaining, unreadable) = match input.file_len().map(usize::try_from) {
            Ok(Ok(len)) => (len, None),
            Ok(Err(_)) => (
                0,
                Some(LogError::Corrupt(
                    "segment file is too large to address".into(),
                )),
            ),
            Err(error) => (0, Some(LogError::Io(error))),
        };
        Self {
            input,
            reader: BufReader::with_capacity(READ_BUFFER, InputReader { input, position: 0 }),
            remaining,
            unreadable,
            frame: Vec::new(),
            finished: false,
        }
    }

    /// Append up to `wanted` more bytes of the input to the frame. Fewer
    /// arrive only when the input ends first.
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

    /// Read and decode the next batch. `remaining_before` is the input's
    /// unread length when the batch starts, which the verified decode kernel
    /// checks the decoder's progress against.
    fn next_batch(&mut self, remaining_before: usize) -> Result<RecordBatch, LogError> {
        if self.input.is_fenced() {
            return Err(LogError::Fenced);
        }
        self.frame.clear();
        self.read_into_frame(HEADER_LEN)?;
        // A short header, a bad magic byte or a negative length fails the
        // decoder on the header alone. Any other header gives the decoder the
        // body it declares, or as much of it as the input holds.
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

impl Iterator for SegmentBatches<'_> {
    type Item = Result<RecordBatch, LogError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        if let Some(error) = self.unreadable.take() {
            self.finished = true;
            return Some(Err(error));
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
        crate::{
            compact::test_support::{make_record, write_sealed_batches},
            name,
            segment::Segment,
        },
        bytes::BytesMut,
        krabka_compression::CompressionType,
        krabka_ids::Offset,
        krabka_protocol::records::Attributes,
        std::sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
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

    /// What the streaming reader yields for a sealed segment whose `.log`
    /// file holds `bytes`.
    fn stream(bytes: &[u8]) -> Result<Vec<RecordBatch>, LogError> {
        let dir = tempdir().unwrap();
        std::fs::write(name::log_path(dir.path(), 0), bytes).unwrap();
        let mut segment = Segment::open_unscanned(dir.path(), Offset(0)).unwrap();
        segment.seal().unwrap();
        group_batches(&[segment.sealed_input()]).collect()
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

    /// A group walks its segments oldest first.
    #[test]
    fn a_group_yields_each_segment_in_order() {
        let dir = tempdir().unwrap();
        let first = write_sealed_batches(dir.path(), &[compressed_batch(0)]);
        let second = write_sealed_batches(dir.path(), &[plain_batch(3), plain_batch(4)]);

        let walked: Result<Vec<RecordBatch>, LogError> =
            group_batches(&[first.sealed_input(), second.sealed_input()]).collect();

        let expected: Vec<RecordBatch> = [compressed_batch(0), plain_batch(3), plain_batch(4)]
            .iter()
            .map(decoded_form)
            .collect();
        assert2::assert!(outcome(walked) == Ok(expected));
    }

    /// A walk whose log is fenced ends at the next batch with
    /// [`LogError::Fenced`], and yields nothing after it.
    #[test]
    fn a_fenced_walk_stops_at_the_next_batch() {
        let dir = tempdir().unwrap();
        let segment = write_sealed_batches(dir.path(), &[plain_batch(0), plain_batch(1)]);
        let fenced = Arc::new(AtomicBool::new(false));
        let inputs = [segment.sealed_input().watched_by(Arc::clone(&fenced))];

        let mut walk = group_batches(&inputs);
        let first = walk.next().map(outcome_of_one);
        fenced.store(true, Ordering::Release);
        let second = walk.next().map(outcome_of_one);

        assert2::assert!(first == Some(Ok(decoded_form(&plain_batch(0)))));
        assert2::assert!(second == Some(Err(format!("{:?}", LogError::Fenced))));
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
