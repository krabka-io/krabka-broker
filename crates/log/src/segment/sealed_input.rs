//! A sealed segment's `.log` bytes as a compaction pass reads them, apart from
//! the [`Segment`] that holds them.
//!
//! A pass can read its input while the log goes on taking appends and reads,
//! because a sealed segment's bytes never change. It needs the bytes, and it
//! needs a way to tell afterwards whether the segment it read is still the
//! one the log holds. The open file handle answers both: the pass reads
//! through the handle the segment shares, so a file that retention or a
//! truncation unlinks meanwhile stays readable, and a segment the log reopened
//! or replaced holds a different handle.

use {
    super::{Segment, io::read_full_at},
    krabka_ids::Offset,
    std::{
        fs::File,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
    },
};

/// The `.log` file of one sealed segment and the length it held when a
/// compaction pass took it.
#[derive(Debug, Clone)]
pub(crate) struct SealedInput {
    base_offset: Offset,
    file: Arc<File>,
    len: u64,
    /// Set when the log the input came from is fenced. A pass stops reading,
    /// and writes nothing more, once it sees the flag.
    fenced: Option<Arc<AtomicBool>>,
}

impl SealedInput {
    /// The segment's base offset.
    pub(crate) fn base_offset(&self) -> Offset { self.base_offset }

    /// The file's length now, which is the length the segment held when the
    /// input was taken unless something wrote past its end behind its back.
    pub(crate) fn file_len(&self) -> std::io::Result<u64> { Ok(self.file.metadata()?.len()) }

    /// Fill `buf` from byte `offset` of the file, and return how many bytes
    /// arrived: fewer than `buf.len()` only at the end of the file.
    pub(crate) fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        read_full_at(&self.file, offset, buf)
    }

    /// Tie the input to `fenced`, the flag its log sets when it is fenced.
    pub(crate) fn watched_by(self, fenced: Arc<AtomicBool>) -> Self {
        Self {
            fenced: Some(fenced),
            ..self
        }
    }

    /// Whether the log this input came from has been fenced since.
    pub(crate) fn is_fenced(&self) -> bool {
        self.fenced
            .as_ref()
            .is_some_and(|fenced| fenced.load(Ordering::Acquire))
    }
}

impl Segment {
    /// This segment's `.log` file as a compaction pass reads it.
    pub(crate) fn sealed_input(&self) -> SealedInput {
        SealedInput {
            base_offset: self.base_offset,
            file: Arc::clone(&self.log_file),
            len: self.log_size,
            fenced: None,
        }
    }

    /// Whether this segment is still the one `input` was taken from, with the
    /// same bytes: the same open file, the same length, and still sealed.
    pub(crate) fn still_holds(&self, input: &SealedInput) -> bool {
        self.sealed
            && self.base_offset == input.base_offset
            && self.log_size == input.len
            && Arc::ptr_eq(&self.log_file, &input.file)
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::segment::test_support::{TempSegment, two_record_fixture},
        assert2::check,
        krabka_units::prelude::ByteSizeExt as _,
    };

    /// A sealed segment holding two records, and the directory that holds it.
    fn sealed_fixture() -> TempSegment {
        let mut fixture = two_record_fixture();
        fixture.segment.seal().unwrap();
        fixture
    }

    #[test]
    fn an_input_reads_the_bytes_the_segment_held() {
        let TempSegment { dir: _dir, segment } = sealed_fixture();
        let input = segment.sealed_input();
        let held = segment.size().bytes_u64();
        let mut whole = vec![0u8; usize::try_from(held).unwrap() + 8];

        let read = input.read_at(0, &mut whole).unwrap();

        check!(u64::try_from(read).unwrap() == held);
        check!(input.file_len().unwrap() == held);
        check!(input.base_offset() == segment.base_offset());
    }

    #[test]
    fn a_segment_holds_only_its_own_unchanged_input() {
        let TempSegment { dir, segment } = sealed_fixture();
        let input = segment.sealed_input();
        let mut reopened = Segment::open_unscanned(dir.path(), segment.base_offset()).unwrap();
        reopened.seal().unwrap();
        let shorter = SealedInput {
            len: input.len - 1,
            ..input.clone()
        };

        check!(segment.still_holds(&input));
        check!(!reopened.still_holds(&input));
        check!(!segment.still_holds(&shorter));
    }

    #[test]
    fn an_input_is_fenced_only_once_its_flag_is_set() {
        let TempSegment { dir: _dir, segment } = sealed_fixture();
        let flag = Arc::new(AtomicBool::new(false));
        let input = segment.sealed_input().watched_by(Arc::clone(&flag));

        check!(!segment.sealed_input().is_fenced());
        check!(!input.is_fenced());
        flag.store(true, Ordering::Release);
        check!(input.is_fenced());
    }
}
