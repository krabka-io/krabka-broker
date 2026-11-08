//! The `O_DIRECT` write path of an active segment's `.log` file.
//!
//! An `O_DIRECT` write skips the page cache, so the kernel puts three rules on
//! it: the buffer address is a multiple of the memory alignment, and the file
//! offset and the length are multiples of the block size. [`DioAlign`] holds
//! the two values for one file.
//!
//! Kafka record batches have arbitrary sizes and go to arbitrary byte
//! positions, so an append almost never starts or stops on a block boundary.
//! [`DirectWriter`] therefore keeps the partial last block of the file in
//! memory, and each append writes that block again together with the new
//! bytes. It writes `[tail_start, padded_end)`, where `tail_start` is the
//! logical end rounded down to a block and `padded_end` is the new logical end
//! rounded up to a block. Zeros fill the space after the logical end. The file
//! can thus be up to `block - 1` bytes longer than the segment. That is
//! expected: the segment knows its logical size and removes the padding when
//! it seals the file.
//!
//! An append does not wait for its write: [`DirectWriter`] queues it, and the
//! writes of every segment go to the disk from one `io_uring`, each submitted as
//! soon as it is queued. The produce path thus pays for a copy, not for the
//! device, as it does through the page cache. Where the kernel refuses an
//! `io_uring`, and for a test's [`LogIo`] that sees each write, a small pool of
//! threads shared by every segment makes the writes with synchronous
//! `pwrite`s instead. Both start a segment's writes in file order and put the
//! same bytes at the same offsets.
//!
//! The page cache does not hold what an `O_DIRECT` write put on disk, so a
//! consumer that reads right behind the producer would go to the device for
//! every fetch. [`TailCache`] keeps a bounded window of the most recently
//! appended bytes in memory to serve those reads, and every byte that is not
//! on the disk yet.
//!
//! [`LogIo`]: crate::io::LogIo

use std::io::{self, Error, ErrorKind};

mod aligned_buf;
mod tail_cache;
#[cfg(test)]
#[cfg(unix)]
mod test_support;
mod writer;

pub use self::{
    aligned_buf::AlignedBuf,
    tail_cache::{TailCache, TailWindow},
    writer::DirectWriter,
};

/// The `O_DIRECT` alignment rules of one file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DioAlign {
    /// Required alignment of a buffer's address, in bytes. A power of two, >= 1.
    pub(crate) mem: usize,
    /// Required alignment of a file offset and of a write's length, in bytes. A power of two, >= 1.
    pub(crate) block: usize,
}

impl DioAlign {
    fn validate(self) -> io::Result<()> {
        if self.mem.is_power_of_two() && self.block.is_power_of_two() {
            Ok(())
        } else {
            Err(Error::new(
                ErrorKind::InvalidInput,
                format!("O_DIRECT alignment is not a power of two: {self:?}"),
            ))
        }
    }

    /// The unit of progress that keeps the next write legal: a write that
    /// starts this many bytes further into an aligned buffer, at an aligned
    /// offset, is aligned in both address and offset. Both values are powers
    /// of two, so the larger is a multiple of the smaller.
    fn step(self) -> usize { self.mem.max(self.block) }
}

/// A `usize` widened to a `u64`; no supported target has a wider `usize`.
fn to_u64(n: usize) -> u64 { n as u64 }
