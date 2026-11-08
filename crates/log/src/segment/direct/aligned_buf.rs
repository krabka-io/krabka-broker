//! The aligned allocation the `O_DIRECT` writes are made from.

#[cfg(target_os = "linux")]
use std::mem;

/// A growable byte buffer whose contents start on an `align`-aligned address.
///
/// The buffer over-allocates a `Vec<u8>` by `align - 1` bytes and starts at
/// the first aligned byte in it, so it needs no `unsafe` allocator call. The
/// `Vec` never changes length after it is made, so its heap address, and with
/// it the alignment, stays fixed until the next growth. A growth makes a new
/// `Vec`, aligns it again, and copies the contents.
pub struct AlignedBuf {
    align: usize,
    storage: Vec<u8>,
    offset: usize,
    len: usize,
}

impl AlignedBuf {
    pub(crate) fn new(align: usize) -> Self {
        Self {
            align: align.max(1),
            storage: Vec::new(),
            offset: 0,
            len: 0,
        }
    }

    pub(crate) fn clear(&mut self) { self.len = 0; }

    pub(crate) fn extend_from_slice(&mut self, bytes: &[u8]) {
        let new_len = self.len + bytes.len();
        self.reserve(new_len);
        let at = self.offset + self.len;
        // `reserve` made room for `new_len` bytes after the aligned start.
        if let Some(room) = self.storage.get_mut(at..at + bytes.len()) {
            room.copy_from_slice(bytes);
            self.len = new_len;
        }
    }

    /// Set the length to `len`. New bytes are zero, also where an earlier
    /// use of the buffer left other data.
    pub(crate) fn resize(&mut self, len: usize) {
        if len > self.len {
            self.reserve(len);
            // `reserve` made room for `len` bytes after the aligned start.
            if let Some(fresh) = self
                .storage
                .get_mut(self.offset + self.len..self.offset + len)
            {
                fresh.fill(0);
            }
        }
        self.len = len;
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        // The contents always lie inside the storage.
        self.storage
            .get(self.offset..self.offset + self.len)
            .unwrap_or_default()
    }

    /// Lend the storage out for a write that must own its buffer: the
    /// `Vec`, and where the contents start in it and how long they are.
    /// [`Self::restore`] takes it back; until then the buffer is empty, and
    /// a buffer whose storage never comes back stays a valid empty one.
    #[cfg(target_os = "linux")]
    pub(super) fn lend(&mut self) -> (Vec<u8>, usize, usize) {
        let lent = (mem::take(&mut self.storage), self.offset, self.len);
        self.offset = 0;
        self.len = 0;
        lent
    }

    /// Take back what [`Self::lend`] lent. The `Vec` is the same allocation,
    /// so the contents are still aligned.
    #[cfg(target_os = "linux")]
    pub(super) fn restore(&mut self, (storage, offset, len): (Vec<u8>, usize, usize)) {
        self.storage = storage;
        self.offset = offset;
        self.len = len;
    }

    pub(super) fn capacity(&self) -> usize { self.storage.len() - self.offset }

    /// Make room for `needed` bytes after the aligned start.
    fn reserve(&mut self, needed: usize) {
        if needed <= self.capacity() {
            return;
        }
        let capacity = needed.max(self.capacity().saturating_mul(2));
        let storage = vec![0; capacity + self.align - 1];
        // The modulo works on the address alone, so it is right for every
        // allocation, where `align_offset` may report that it cannot help.
        let misalignment = storage.as_ptr().addr() % self.align;
        let offset = (self.align - misalignment) % self.align;
        let mut grown = Self {
            align: self.align,
            storage,
            offset,
            len: 0,
        };
        // `capacity >= needed > self.len`, so the contents fit.
        if let Some(room) = grown.storage.get_mut(offset..offset + self.len) {
            room.copy_from_slice(self.as_slice());
            grown.len = self.len;
        }
        *self = grown;
    }
}

#[cfg(test)]
#[cfg(unix)]
mod tests;
