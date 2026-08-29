//! Positioned reads and writes over a segment's `.log` file.
//!
//! A reader shares the writer's `File` handle through `&self`, so every read
//! here takes an explicit file offset and leaves the file cursor where it was.
//! The hot fetch path runs through these functions for every read, which is
//! why they live together in one small module.

use std::{
    fs::File,
    io::{IoSlice, Seek, SeekFrom, Write},
};

use super::Segment;
use crate::error::LogError;

/// Positioned read: fill `buf` from `offset` in `file` without a move of the
/// file's cursor.
///
/// This function loops over short reads until `buf` is full or it reaches EOF,
/// then returns the number of bytes read. Readers can therefore share the
/// writer's `File` handle through `&self`, with no `dup(2)` or `lseek(2)` per
/// call. The hot fetch path runs this function for every read.
pub(super) fn read_full_at(file: &File, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
    read_full_with(|at, into| read_at(file, at, into), offset, buf)
}

/// The loop itself, over any positional read.
///
/// Split out because the two things it exists to handle -- a read that returns
/// fewer bytes than asked for, and one interrupted by a signal -- are not
/// things a regular file can be persuaded to do on demand, so a test cannot
/// reach them through [`read_full_at`]. Generic rather than `dyn`, so the hot
/// path monomorphises to what it was.
fn read_full_with(
    read: impl Fn(u64, &mut [u8]) -> std::io::Result<usize>,
    mut offset: u64,
    buf: &mut [u8],
) -> std::io::Result<usize> {
    let mut total = 0;
    while total < buf.len() {
        match read(offset, &mut buf[total..]) {
            Ok(0) => break, // EOF
            Ok(n) => {
                total += n;
                offset += n as u64;
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(total)
}

pub(super) fn seek_to_log_size(file: &File, log_size: u64) -> std::io::Result<()> {
    (&*file).seek(SeekFrom::Start(log_size))?;
    Ok(())
}

pub(super) fn write_all_vectored(
    mut writer: impl Write,
    mut bufs: &mut [IoSlice<'_>],
) -> std::io::Result<()> {
    while !bufs.is_empty() {
        let written = writer.write_vectored(bufs)?;
        if written == 0 {
            return Err(std::io::ErrorKind::WriteZero.into());
        }
        IoSlice::advance_slices(&mut bufs, written);
    }
    Ok(())
}

#[cfg(unix)]
fn read_at(file: &File, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
    std::os::unix::fs::FileExt::read_at(file, buf, offset)
}

#[cfg(windows)]
fn read_at(file: &File, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
    std::os::windows::fs::FileExt::seek_read(file, buf, offset)
}

impl Segment {
    pub(super) fn read_log_range(
        &self,
        start_pos: u64,
        buf: &mut Vec<u8>,
        max_bytes: usize,
    ) -> Result<(), LogError> {
        let available = self.log_size.saturating_sub(start_pos);
        let to_read = available.min(u64::try_from(max_bytes).unwrap_or(u64::MAX));
        let to_read = usize::try_from(to_read).unwrap_or(usize::MAX);
        let base = buf.len();
        buf.resize(base + to_read, 0);
        let n = read_full_at(&self.log_file, start_pos, &mut buf[base..])?;
        buf.truncate(base + n);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    /// The read loop fills the buffer across short reads, retries an
    /// interrupted read, and stops at end of file.
    ///
    /// A regular file returns everything asked for or stops at its end, so
    /// none of this is reachable through a real one -- the loop is driven by a
    /// reader that can be told what to do.
    #[test]
    fn the_read_loop_handles_short_reads_interruptions_and_eof() {
        use std::{
            cell::RefCell,
            io::{Error, ErrorKind},
        };

        /// The offset of every call the scripted reader received, in order.
        type SeenOffsets = std::rc::Rc<RefCell<Vec<u64>>>;

        /// A reader that replays a script of results and records the offset of
        /// each call.
        fn scripted(
            script: Vec<std::io::Result<usize>>,
        ) -> (
            impl Fn(u64, &mut [u8]) -> std::io::Result<usize>,
            SeenOffsets,
        ) {
            let offsets = std::rc::Rc::new(RefCell::new(Vec::new()));
            let seen = std::rc::Rc::clone(&offsets);
            let script = RefCell::new(script.into_iter());
            let read = move |offset: u64, into: &mut [u8]| {
                seen.borrow_mut().push(offset);
                match script.borrow_mut().next() {
                    Some(Ok(n)) => {
                        into[..n].fill(b'x');
                        Ok(n)
                    }
                    Some(Err(e)) => Err(e),
                    None => Ok(0),
                }
            };
            (read, offsets)
        }

        // Three short reads fill an eight-byte buffer, each resuming where the
        // last stopped.
        let (read, offsets) = scripted(vec![Ok(3), Ok(4), Ok(1)]);
        let mut buf = [0u8; 8];
        check!(read_full_with(read, 100, &mut buf).unwrap() == 8);
        check!(
            *offsets.borrow() == vec![100, 103, 107],
            "each read resumes where the last ended, got {:?}",
            offsets.borrow()
        );

        // An interrupted read is retried at the same offset, not skipped past.
        let (read, offsets) =
            scripted(vec![Ok(2), Err(Error::from(ErrorKind::Interrupted)), Ok(2)]);
        let mut buf = [0u8; 4];
        check!(read_full_with(read, 0, &mut buf).unwrap() == 4);
        check!(
            *offsets.borrow() == vec![0, 2, 2],
            "the retry repeats the offset, got {:?}",
            offsets.borrow()
        );

        // End of file stops the loop with whatever was read so far.
        let (read, _) = scripted(vec![Ok(2), Ok(0), Ok(9)]);
        let mut buf = [0u8; 8];
        check!(
            read_full_with(read, 0, &mut buf).unwrap() == 2,
            "stops at EOF"
        );

        // Any other error is returned rather than retried.
        let (read, _) = scripted(vec![Ok(1), Err(Error::from(ErrorKind::PermissionDenied))]);
        let mut buf = [0u8; 4];
        check!(
            read_full_with(read, 0, &mut buf).is_err(),
            "a real error propagates"
        );

        // A buffer that is already full asks for nothing at all.
        let (read, offsets) = scripted(vec![Ok(1)]);
        let mut empty: [u8; 0] = [];
        check!(read_full_with(read, 0, &mut empty).unwrap() == 0);
        check!(
            offsets.borrow().is_empty(),
            "no read is issued for no bytes"
        );
    }
}
