//! I/O-primitive benchmark to decide whether bypassing the page cache would
//! help the segment read and append paths.
//!
//! `krabka-log` does buffered I/O today: appends `write` into the page cache
//! and `fdatasync` at flush, and fetches `pread` through it or hand its pages
//! to `sendfile`. Redpanda instead opens every segment `O_DIRECT` and keeps its
//! own cache. Linux 6.14 added a middle road, `RWF_DONTCACHE`: a buffered read
//! or write that drops its pages once the I/O is done, so a cold read or a
//! bulk append does not push the hot tail out of the cache. This bench races
//! the three on the same files.
//!
//! `uncached_read/*` reads a 1 MiB fetch chunk and a 16 KiB scattered read,
//! walking the file so each read lands on a new range:
//!
//! - `pread_warm`: the current behaviour, with the file resident.
//! - `pread_cold`: the current behaviour after `POSIX_FADV_DONTNEED` evicts the
//!   file, which is a consumer reading behind the tail. The eviction runs
//!   outside the timed section.
//! - `dontcache_warm`: `preadv2(RWF_DONTCACHE)` on a resident range, which
//!   shows what the flag costs on a hit.
//! - `dontcache_cold`: `preadv2(RWF_DONTCACHE)` on an evicted file.
//! - `odirect`: an aligned `pread` on an `O_DIRECT` handle. It has no warm or
//!   cold case, because it never consults the cache.
//!
//! `uncached_write/*` overwrites the same sizes in place, in a file that was
//! written out in full beforehand. That is the shape of an append into a
//! preallocated segment, so no variant pays for block allocation or a size
//! change. Each of `buffered`, `dontcache` and `odirect` runs once without a
//! sync and once followed by `fdatasync`, which is what makes an append
//! durable whichever way it was written: `O_DIRECT` skips the page cache but
//! not the drive's write cache.
//!
//! `O_DIRECT` needs a filesystem that supports it, and `RWF_DONTCACHE` needs
//! Linux 6.14 or later and a filesystem that opted in. tmpfs supports neither.
//! The bench probes both on the bench directory and skips, with a message on
//! stderr, the variants it cannot run. Point `KRABKA_IOBENCH_DIR` at a
//! directory on the disk under test; the default is the system temporary
//! directory.
//!
//! The bench is Linux-only. On every other target it builds to an empty
//! `main`.

#[cfg(target_os = "linux")]
mod linux {
    use std::{
        fs::File,
        io::{IoSlice, IoSliceMut, Write as _},
        num::NonZeroU64,
        os::unix::fs::FileExt as _,
        path::{Path, PathBuf},
    };

    use criterion::{BatchSize, Criterion, black_box, criterion_group};
    use krabka_units::prelude::{ByteSize, ByteSizeExt as _, kibibytes, mebibytes};
    use rustix::{
        fs::{Advice, AtFlags, CWD, Mode, OFlags, StatxFlags},
        io::ReadWriteFlags,
    };
    use tempfile::TempDir;

    /// The two read and write sizes `mmap_read` uses: one Kafka-default fetch
    /// chunk, and a small scattered read.
    const SIZES: [(&str, ByteSize); 2] = [("1MiB", mebibytes(1)), ("16KiB", kibibytes(16))];
    /// Size of each fixture file: 64 distinct 1 MiB ranges, so the warm and
    /// write walks spread over more than one spot on the device.
    const FILE_SIZE: ByteSize = mebibytes(64);
    /// `RWF_DONTCACHE`, from Linux 6.14's `include/uapi/linux/fs.h`. rustix
    /// 1.1 does not name it yet.
    const RWF_DONTCACHE: ReadWriteFlags = ReadWriteFlags::from_bits_retain(0x0000_0080);
    /// The alignment assumed when the kernel predates `STATX_DIOALIGN` (6.1).
    const FALLBACK_ALIGN: u32 = 4096;

    /// The `O_DIRECT` alignment rules for one file.
    #[derive(Debug, Clone, Copy)]
    struct DioAlign {
        /// Required alignment of the user buffer's address.
        mem: usize,
        /// Required alignment of the file offset and the length.
        offset: u64,
    }

    /// A buffer whose usable range starts on an `O_DIRECT`-aligned address.
    ///
    /// It over-allocates by one alignment and starts the usable range at the
    /// first aligned byte, so no `unsafe` allocator call is needed. The `Vec`
    /// is never resized, so the address stays put.
    struct AlignedBuf {
        raw: Vec<u8>,
        start: usize,
        len: usize,
    }

    impl AlignedBuf {
        fn new(len: usize, align: usize) -> Self {
            let raw = vec![0u8; len + align];
            let start = raw.as_ptr().align_offset(align);
            assert2::assert!(start + len <= raw.len());
            Self { raw, start, len }
        }

        fn filled(len: usize, align: usize) -> Self {
            let mut buf = Self::new(len, align);
            buf.as_mut_slice().copy_from_slice(&pattern(len));
            buf
        }

        fn as_slice(&self) -> &[u8] {
            &self.raw[self.start..self.start + self.len]
        }

        fn as_mut_slice(&mut self) -> &mut [u8] {
            &mut self.raw[self.start..self.start + self.len]
        }
    }

    /// Non-zero, non-repeating-per-page bytes, so no filesystem can shortcut
    /// the I/O as a hole or a run of one value.
    fn pattern(len: usize) -> Vec<u8> {
        (0..len)
            .map(|i| u8::try_from(i % 251).expect("below 256"))
            .collect()
    }

    /// The directory the fixture files go in: `KRABKA_IOBENCH_DIR` if set,
    /// else the system temporary directory.
    fn bench_dir() -> TempDir {
        match std::env::var_os("KRABKA_IOBENCH_DIR") {
            Some(dir) => tempfile::tempdir_in(dir),
            None => tempfile::tempdir(),
        }
        .expect("create bench directory")
    }

    /// Write a [`FILE_SIZE`] file in full and make it durable, so a later
    /// overwrite allocates nothing and a cold read finds real blocks.
    fn write_fixture(path: &Path) {
        let mut file = File::create(path).expect("create fixture");
        let chunk = pattern(mebibytes(1).bytes_usize());
        for _ in 0..FILE_SIZE.bytes_u64() / mebibytes(1).bytes_u64() {
            file.write_all(&chunk).expect("write fixture");
        }
        file.sync_all().expect("sync fixture");
    }

    /// The `O_DIRECT` alignment of `path`, or `None` when its filesystem does
    /// not support `O_DIRECT`.
    ///
    /// `STATX_DIOALIGN` reports both alignments, and a zero offset alignment
    /// when `O_DIRECT` is unsupported. A kernel before 6.1 does not fill the
    /// field in, so the bench assumes [`FALLBACK_ALIGN`] and lets the probe
    /// read in [`open_direct`] decide.
    fn dio_align(path: &Path) -> Option<DioAlign> {
        let stat = rustix::fs::statx(CWD, path, AtFlags::empty(), StatxFlags::DIOALIGN).ok()?;
        if stat.stx_mask & StatxFlags::DIOALIGN.bits() == 0 {
            return Some(DioAlign {
                mem: FALLBACK_ALIGN as usize,
                offset: u64::from(FALLBACK_ALIGN),
            });
        }
        (stat.stx_dio_offset_align != 0).then(|| DioAlign {
            mem: stat.stx_dio_mem_align as usize,
            offset: u64::from(stat.stx_dio_offset_align),
        })
    }

    /// Open `path` read-write with `O_DIRECT`, and prove with one aligned read
    /// that the filesystem accepts it.
    fn open_direct(path: &Path, align: DioAlign) -> std::io::Result<File> {
        let file = File::from(rustix::fs::open(
            path,
            OFlags::RDWR | OFlags::DIRECT | OFlags::CLOEXEC,
            Mode::empty(),
        )?);
        let block = usize::try_from(align.offset).expect("alignment fits usize");
        let mut probe = AlignedBuf::new(block, align.mem);
        file.read_exact_at(probe.as_mut_slice(), 0)?;
        Ok(file)
    }

    /// Check that `RWF_DONTCACHE` reads and writes work on `file`, which must
    /// be open read-write.
    ///
    /// The kernel answers `EOPNOTSUPP` both when it predates the flag and when
    /// the filesystem has not opted in. The probe write puts back the byte it
    /// read, so the fixture is unchanged.
    fn probe_dontcache(file: &File) -> rustix::io::Result<()> {
        let mut byte = [0u8; 1];
        rustix::io::preadv2(file, &mut [IoSliceMut::new(&mut byte)], 0, RWF_DONTCACHE)?;
        rustix::io::pwritev2(file, &[IoSlice::new(&byte)], 0, RWF_DONTCACHE)?;
        Ok(())
    }

    /// Drop every cached page of `file`, so the next read of it goes to the
    /// device. The fixture is clean, so every page in it can go.
    ///
    /// It drops the whole file rather than the range about to be read: the
    /// walk is sequential, so the kernel's readahead for one read has already
    /// cached the next range.
    fn evict(file: &File) {
        rustix::fs::fadvise(
            file,
            0,
            NonZeroU64::new(FILE_SIZE.bytes_u64()),
            Advice::DontNeed,
        )
        .expect("fadvise DONTNEED");
    }

    /// Write back and drop every cached page of `file`, so one write variant
    /// does not leave dirty pages for the next to flush.
    fn settle(file: &File) {
        rustix::fs::fdatasync(file).expect("fdatasync");
        evict(file);
    }

    fn read_dontcache(file: &File, offset: u64, buf: &mut [u8]) {
        let want = buf.len();
        let read = rustix::io::preadv2(file, &mut [IoSliceMut::new(buf)], offset, RWF_DONTCACHE)
            .expect("preadv2 RWF_DONTCACHE");
        assert2::assert!(read == want);
    }

    fn write_dontcache(file: &File, offset: u64, buf: &[u8]) {
        let wrote = rustix::io::pwritev2(file, &[IoSlice::new(buf)], offset, RWF_DONTCACHE)
            .expect("pwritev2 RWF_DONTCACHE");
        assert2::assert!(wrote == buf.len());
    }

    /// A walk over the file in `len`-sized steps, wrapping at the end, so each
    /// read or write lands on a new range.
    struct Walk {
        next: u64,
        len: u64,
    }

    impl Walk {
        fn new(len: u64) -> Self {
            Self { next: 0, len }
        }

        fn step(&mut self) -> u64 {
            let at = self.next;
            self.next = (self.next + self.len) % FILE_SIZE.bytes_u64();
            at
        }
    }

    /// Everything the bench needs, probed once.
    struct Fixture {
        _dir: TempDir,
        read_path: PathBuf,
        write_path: PathBuf,
        align: Option<DioAlign>,
        dontcache: bool,
    }

    impl Fixture {
        fn build() -> Self {
            let dir = bench_dir();
            let read_path = dir.path().join("00000000000000000000.log");
            let write_path = dir.path().join("00000000000000100000.log");
            write_fixture(&read_path);
            write_fixture(&write_path);

            let align = match dio_align(&read_path) {
                None => {
                    eprintln!(
                        "skipping O_DIRECT variants: {} does not support O_DIRECT",
                        dir.path().display()
                    );
                    None
                }
                Some(align) => match open_direct(&read_path, align) {
                    Ok(_) => Some(align),
                    Err(error) => {
                        eprintln!("skipping O_DIRECT variants: {error}");
                        None
                    }
                },
            };
            let probe = File::options()
                .read(true)
                .write(true)
                .open(&write_path)
                .expect("open fixture");
            let dontcache = probe_dontcache(&probe)
                .inspect_err(|error| {
                    eprintln!(
                        "skipping RWF_DONTCACHE variants: {error} (needs Linux 6.14+ and a \
                         filesystem that supports it)"
                    );
                })
                .is_ok();
            Self {
                _dir: dir,
                read_path,
                write_path,
                align,
                dontcache,
            }
        }

        /// The `O_DIRECT` alignment, if `len` satisfies it.
        fn direct_align(&self, len: u64) -> Option<DioAlign> {
            self.align.filter(|align| len.is_multiple_of(align.offset))
        }
    }

    fn bench_uncached_read(c: &mut Criterion) {
        let fixture = Fixture::build();
        let file = File::open(&fixture.read_path).expect("open fixture");

        for (label, size) in SIZES {
            let len = size.bytes_u64();
            let mut buf = vec![0u8; size.bytes_usize()];
            let mut group = c.benchmark_group(format!("uncached_read/{label}"));

            group.bench_function("pread_warm", |b| {
                let mut walk = Walk::new(len);
                b.iter(|| {
                    file.read_exact_at(&mut buf, walk.step()).expect("pread");
                    black_box(&buf);
                });
            });

            group.bench_function("pread_cold", |b| {
                let mut walk = Walk::new(len);
                b.iter_batched(
                    || {
                        let at = walk.step();
                        evict(&file);
                        at
                    },
                    |at| {
                        file.read_exact_at(&mut buf, at).expect("pread");
                        black_box(&buf);
                    },
                    BatchSize::PerIteration,
                );
            });

            if fixture.dontcache {
                group.bench_function("dontcache_warm", |b| {
                    let mut walk = Walk::new(len);
                    let mut warm = vec![0u8; size.bytes_usize()];
                    b.iter_batched(
                        || {
                            // RWF_DONTCACHE drops what it reads, so warm the
                            // range first or every read after the first is
                            // cold.
                            let at = walk.step();
                            file.read_exact_at(&mut warm, at).expect("pread");
                            at
                        },
                        |at| {
                            read_dontcache(&file, at, &mut buf);
                            black_box(&buf);
                        },
                        BatchSize::PerIteration,
                    );
                });

                group.bench_function("dontcache_cold", |b| {
                    let mut walk = Walk::new(len);
                    b.iter_batched(
                        || {
                            let at = walk.step();
                            evict(&file);
                            at
                        },
                        |at| {
                            read_dontcache(&file, at, &mut buf);
                            black_box(&buf);
                        },
                        BatchSize::PerIteration,
                    );
                });
            }

            if let Some(align) = fixture.direct_align(len) {
                let direct = open_direct(&fixture.read_path, align).expect("open O_DIRECT");
                let mut aligned = AlignedBuf::new(size.bytes_usize(), align.mem);
                group.bench_function("odirect", |b| {
                    let mut walk = Walk::new(len);
                    b.iter(|| {
                        direct
                            .read_exact_at(aligned.as_mut_slice(), walk.step())
                            .expect("O_DIRECT pread");
                        black_box(aligned.as_slice());
                    });
                });
            }

            group.finish();
        }
    }

    fn bench_uncached_write(c: &mut Criterion) {
        let fixture = Fixture::build();
        let file = File::options()
            .read(true)
            .write(true)
            .open(&fixture.write_path)
            .expect("open fixture");

        for (label, size) in SIZES {
            let len = size.bytes_u64();
            let payload = pattern(size.bytes_usize());
            let mut group = c.benchmark_group(format!("uncached_write/{label}"));

            for sync in [false, true] {
                let suffix = if sync { "_fdatasync" } else { "" };
                let finish = |file: &File| {
                    if sync {
                        rustix::fs::fdatasync(file).expect("fdatasync");
                    }
                };

                settle(&file);
                group.bench_function(format!("buffered{suffix}"), |b| {
                    let mut walk = Walk::new(len);
                    b.iter(|| {
                        file.write_all_at(&payload, walk.step()).expect("pwrite");
                        finish(&file);
                    });
                });

                if fixture.dontcache {
                    settle(&file);
                    group.bench_function(format!("dontcache{suffix}"), |b| {
                        let mut walk = Walk::new(len);
                        b.iter(|| {
                            write_dontcache(&file, walk.step(), &payload);
                            finish(&file);
                        });
                    });
                }

                if let Some(align) = fixture.direct_align(len) {
                    settle(&file);
                    let direct = open_direct(&fixture.write_path, align).expect("open O_DIRECT");
                    let aligned = AlignedBuf::filled(size.bytes_usize(), align.mem);
                    group.bench_function(format!("odirect{suffix}"), |b| {
                        let mut walk = Walk::new(len);
                        b.iter(|| {
                            direct
                                .write_all_at(aligned.as_slice(), walk.step())
                                .expect("O_DIRECT pwrite");
                            finish(&direct);
                        });
                    });
                }
            }

            group.finish();
        }
    }

    criterion_group!(benches, bench_uncached_read, bench_uncached_write);
}

#[cfg(target_os = "linux")]
criterion::criterion_main!(linux::benches);

#[cfg(not(target_os = "linux"))]
fn main() {}
