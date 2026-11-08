//! Unit tests for the `O_DIRECT` write queue: block alignment of each write,
//! queue admission, out-of-order completion, and write failures.

use {
    super::*,
    crate::{io::FileIo, segment::direct::test_support::pattern},
    assert2::{assert, check},
    std::{
        cell::{Cell, RefCell},
        fs, io, mem,
        os::unix::fs::FileExt,
        sync::atomic::{AtomicBool, AtomicUsize, Ordering},
        thread,
        time::Duration,
    },
};

const MEM: usize = 512;

/// One `write` call a writer made: where in the file, and how many bytes.
#[derive(Debug, PartialEq, Eq)]
struct WriteCall {
    offset: u64,
    len: usize,
}

/// A [`LogIo`] whose direct writes check the `O_DIRECT` rules, record
/// each call's offset and length, and write through `write_at`. It fails
/// every write once `fail` is set, and holds every write back while
/// `held` is set.
#[derive(Debug)]
struct RecordingIo {
    align: DioAlign,
    calls: Mutex<Vec<WriteCall>>,
    fail: AtomicBool,
    held: Mutex<bool>,
    released: Condvar,
}

impl RecordingIo {
    fn new(align: DioAlign) -> Arc<Self> {
        Arc::new(Self {
            align,
            calls: Mutex::default(),
            fail: false.into(),
            held: Mutex::new(false),
            released: Condvar::new(),
        })
    }

    fn take_calls(&self) -> Vec<WriteCall> { mem::take(&mut *self.calls.lock().unwrap()) }

    fn hold(&self, held: bool) {
        *self.held.lock().unwrap() = held;
        self.released.notify_all();
    }
}

impl LogIo for RecordingIo {
    fn write_direct(&self, file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
        let mut held = self.held.lock().unwrap();
        while *held {
            held = self.released.wait(held).unwrap();
        }
        drop(held);
        if self.fail.load(Ordering::SeqCst) {
            return Err(Error::other("disk on fire"));
        }
        assert!(buf.as_ptr().addr() % self.align.mem == 0);
        assert!(offset % to_u64(self.align.block) == 0);
        assert!(buf.len() % self.align.block == 0);
        self.calls.lock().unwrap().push(WriteCall {
            offset,
            len: buf.len(),
        });
        file.write_at(buf, offset)
    }
}

fn file_contents(file: &File) -> Vec<u8> {
    let len = usize::try_from(file.metadata().unwrap().len()).unwrap();
    let mut out = vec![0; len];
    file.read_exact_at(&mut out, 0).unwrap();
    out
}

/// What the file holds after appends of `logical`: those bytes, then zeros
/// up to the next block.
fn padded(logical: &[u8], block: usize) -> Vec<u8> {
    let mut expected = logical.to_vec();
    expected.resize(logical.len().div_ceil(block) * block, 0);
    expected
}

/// A writer on a new, empty temporary file that writes through `io`.
fn empty_writer(align: DioAlign, io: Arc<dyn LogIo>) -> DirectWriter {
    DirectWriter::new(tempfile::tempfile().unwrap(), align, 0, Vec::new(), io).unwrap()
}

/// A writer on a new temporary file, and the I/O it writes through.
fn writer(block: usize) -> (DirectWriter, Arc<RecordingIo>) {
    let align = DioAlign { mem: MEM, block };
    let io = RecordingIo::new(align);
    let writer = empty_writer(align, Arc::clone(&io) as Arc<dyn LogIo>);
    (writer, io)
}

#[test]
fn appends_of_assorted_sizes_write_aligned_blocks_from_the_tail_start() {
    /// A block size, then the size of each append in turn.
    struct Case {
        block: usize,
        sizes: &'static [usize],
    }
    let cases = [
        Case {
            block: 512,
            sizes: &[1, 10, 100],
        },
        Case {
            block: 512,
            sizes: &[511, 1, 1],
        },
        Case {
            block: 512,
            sizes: &[512, 512, 1024],
        },
        Case {
            block: 512,
            sizes: &[300, 300, 300, 300],
        },
        Case {
            block: 512,
            sizes: &[1500, 7, 2048, 513],
        },
        Case {
            block: 4096,
            sizes: &[1, 4095, 4097, 100],
        },
        Case {
            block: 4096,
            sizes: &[10_000, 3, 8192],
        },
    ];
    for Case { block, sizes } in cases {
        let (mut writer, io) = writer(block);
        check!(writer.block() == block);
        let mut logical = Vec::new();
        for (seed, &size) in sizes.iter().enumerate() {
            let bytes = pattern(u8::try_from(seed).unwrap(), size);
            let tail_start = writer.end() - writer.end() % to_u64(block);
            writer.append(&[&bytes]).unwrap();
            writer.drain().unwrap();
            logical.extend_from_slice(&bytes);

            // The append writes the whole blocks it completes; the drain
            // then writes the partial last block alone.
            let new_end = to_u64(logical.len());
            let whole_end = new_end - new_end % to_u64(block);
            let mut expected_calls = Vec::new();
            if whole_end > tail_start {
                expected_calls.push(WriteCall {
                    offset: tail_start,
                    len: usize::try_from(whole_end - tail_start).unwrap(),
                });
            }
            if new_end > whole_end {
                expected_calls.push(WriteCall {
                    offset: whole_end,
                    len: block,
                });
            }
            check!(
                io.take_calls() == expected_calls,
                "block {block}, sizes {sizes:?}"
            );
            check!(writer.end() == new_end);
            check!(writer.written() == new_end);
            check!(writer.tail == logical[logical.len() - logical.len() % block..]);
            check!(
                file_contents(writer.file()) == padded(&logical, block),
                "block {block}, sizes {sizes:?}"
            );
        }
    }
}

#[test]
fn several_parts_append_as_their_concatenation() {
    let (mut writer, _io) = writer(512);
    let (a, b, c) = (pattern(1, 100), pattern(2, 0), pattern(3, 700));
    writer.append(&[&a, &b, &c]).unwrap();
    writer.drain().unwrap();
    let logical = [a, c].concat();
    check!(writer.end() == 800);
    check!(file_contents(writer.file()) == padded(&logical, 512));
}

#[test]
fn an_empty_append_writes_nothing() {
    let (mut writer, io) = writer(512);
    check!(writer.append(&[&[], &[]]).is_ok());
    writer.drain().unwrap();
    check!(io.take_calls().is_empty());
    check!(writer.end() == 0);
}

/// An append returns once its write is queued, so it does not wait for
/// the disk; an append that would overfill the queue waits for the
/// writes before it.
#[test]
fn an_append_waits_for_the_disk_only_when_the_queue_is_full() {
    let block = 512;
    let (mut writer, io) = writer(block);
    writer.queue_limit = block;
    io.hold(true);
    check!(writer.can_queue(block));
    check!(!writer.can_queue(2 * block));

    let first = pattern(1, 600);
    writer.append(&[&first]).unwrap();
    check!(writer.end() == 600);
    check!(writer.written() == 0);
    check!(!writer.can_queue(424));
    check!(!writer.can_queue(usize::MAX));

    let second = pattern(2, 600);
    let appended = AtomicBool::new(false);
    let writer = thread::scope(|scope| {
        let (second, appended) = (&second, &appended);
        let waiting = scope.spawn(move || {
            writer.append(&[second]).unwrap();
            appended.store(true, Ordering::SeqCst);
            writer
        });
        thread::sleep(Duration::from_millis(50));
        check!(!appended.load(Ordering::SeqCst));
        io.hold(false);
        waiting.join().unwrap()
    });
    writer.drain().unwrap();
    check!(writer.can_queue(block));
    check!(writer.written() == 1200);
    check!(file_contents(writer.file()) == padded(&[first, second].concat(), block));
}

#[test]
fn inline_queue_admission_refuses_contention_and_write_failure() {
    let (writer, _io) = writer(512);
    let mut queue = writer.shared.queue.lock().unwrap();
    check!(!writer.can_queue(1));
    queue.failure = Some((ErrorKind::StorageFull, "disk full".into()));
    drop(queue);
    check!(!writer.can_queue(1));
}

#[test]
fn an_oversized_queued_write_blocks_even_a_partial_block_append() {
    let (mut writer, io) = writer(512);
    writer.queue_limit = 512;
    io.hold(true);
    writer.append(&[&[7; 1024]]).unwrap();
    check!(!writer.can_queue(1));
    io.hold(false);
    writer.drain().unwrap();
    check!(writer.can_queue(1));
}

/// Writes finish out of order, and the written mark moves only over
/// bytes with no gap below them.
#[test]
fn written_advances_only_over_a_gap_free_prefix() {
    let mut queue = Queue {
        jobs: VecDeque::new(),
        in_flight: 0,
        queued_end: 4096,
        end: 4096,
        tail: Vec::new(),
        tail_writing: false,
        written: 0,
        finished: BTreeMap::new(),
        failure: None,
        ready: false,
        spare: Vec::new(),
        ring: false,
    };
    queue.finish(2048, 3072);
    queue.finish(1024, 2048);
    check!(queue.written == 0);
    // Writes that finished behind the gap stay in memory, so they still
    // count against the queue limit.
    check!(queue.unwritten() == 4096);
    queue.finish(0, 1024);
    check!(queue.written == 3072);
    check!(queue.unwritten() == 1024);
    queue.finish(3072, 4096);
    check!(queue.written == 4096);
    check!(queue.finished.is_empty());
}

/// The spare buffers a segment keeps hold at most `QUEUE_LIMIT` bytes
/// between them, whatever their count, so a segment that writes 1 MiB
/// batches does not keep sixteen of them.
#[test]
fn spare_buffers_hold_at_most_the_queue_limit() {
    let mut queue = Queue {
        jobs: VecDeque::new(),
        in_flight: 0,
        queued_end: 0,
        end: 0,
        tail: Vec::new(),
        tail_writing: false,
        written: 0,
        finished: BTreeMap::new(),
        failure: None,
        ready: false,
        spare: Vec::new(),
        ring: true,
    };
    let one_mib = |fill: usize| {
        let mut buf = AlignedBuf::new(4096);
        buf.resize(fill);
        buf
    };
    for _ in 0..RING_WRITES_IN_FLIGHT {
        queue.give_back(one_mib(1 << 20));
    }
    let held: usize = queue.spare.iter().map(AlignedBuf::capacity).sum();
    check!(held <= QUEUE_LIMIT);
    // Each buffer is a little over 1 MiB, its alignment slack included.
    check!((7..=8).contains(&queue.spare.len()));
    // Small buffers still fill the count up to the in-flight bound.
    let mut small = Queue {
        spare: Vec::new(),
        ..queue
    };
    for _ in 0..RING_WRITES_IN_FLIGHT + 4 {
        small.give_back(one_mib(4096));
    }
    check!(small.spare.len() == RING_WRITES_IN_FLIGHT);
}

/// One segment's whole-block writes run side by side, up to
/// `WRITES_IN_FLIGHT` of them, and the file still ends up as written.
#[test]
fn several_writes_of_one_segment_run_at_once() {
    #[derive(Debug)]
    struct Overlapping {
        running: AtomicUsize,
        most: AtomicUsize,
    }
    impl LogIo for Overlapping {
        fn write_direct(&self, file: &File, buf: &[u8], offset: u64) -> io::Result<usize> {
            use std::sync::atomic::Ordering::SeqCst;
            let now = self.running.fetch_add(1, SeqCst) + 1;
            self.most.fetch_max(now, SeqCst);
            thread::sleep(Duration::from_millis(20));
            self.running.fetch_sub(1, SeqCst);
            file.write_at(buf, offset)
        }
    }
    let align = DioAlign {
        mem: MEM,
        block: 512,
    };
    let io = Arc::new(Overlapping {
        running: 0.into(),
        most: 0.into(),
    });
    let mut writer = empty_writer(align, Arc::clone(&io) as Arc<dyn LogIo>);
    let mut logical = Vec::new();
    for seed in 0..8 {
        let bytes = pattern(seed, 1024);
        writer.append(&[&bytes]).unwrap();
        logical.extend_from_slice(&bytes);
    }
    writer.drain().unwrap();
    check!(io.most.load(Ordering::SeqCst) > 1);
    check!(io.most.load(Ordering::SeqCst) <= WRITES_IN_FLIGHT);
    check!(writer.written() == 8192);
    check!(file_contents(writer.file()) == logical);
}

/// A writer whose I/O changes writes the next appends through the new
/// one: after a switch from a test's I/O to the real file I/O, the test's
/// sees no more writes, and the file holds every append either way.
#[test]
fn a_new_io_makes_the_writes_after_it() {
    let (mut writer, io) = writer(512);
    let first = pattern(1, 700);
    writer.append(&[&first]).unwrap();
    writer.drain().unwrap();
    check!(!io.take_calls().is_empty());

    writer.set_io(Arc::new(FileIo));
    let mut logical = first;
    for seed in 2..40 {
        let bytes = pattern(seed, 300 * usize::from(seed));
        writer.append(&[&bytes]).unwrap();
        logical.extend_from_slice(&bytes);
    }
    writer.drain().unwrap();

    check!(io.take_calls().is_empty());
    check!(writer.written() == to_u64(logical.len()));
    check!(file_contents(writer.file()) == padded(&logical, 512));
}

/// The real file I/O hands its writes to the `io_uring` submitter where the
/// kernel offers one, and to the write threads where it does not. Either
/// way, appends of assorted sizes with the queue's worth of writes in
/// flight through a real `O_DIRECT` handle leave exactly the bytes they
/// appended, zero-padded to the block. A filesystem without `O_DIRECT`,
/// such as tmpfs, has no handle to write through and is not tested.
#[test]
fn the_real_io_puts_every_append_on_a_direct_handle() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("00000000000000000000.log");
    File::create(&path).unwrap();
    let io: Arc<dyn LogIo> = Arc::new(FileIo);
    let Ok(direct) = io.open_direct(&path) else {
        return;
    };
    let align = DioAlign {
        mem: direct.mem_align,
        block: direct.block,
    };
    let mut writer = DirectWriter::new(direct.file, align, 0, Vec::new(), io).unwrap();
    let mut logical = Vec::new();
    for seed in 0..=u8::MAX {
        let bytes = pattern(seed, 1 + usize::from(seed) * 997 % 70_000);
        writer.append(&[&bytes]).unwrap();
        logical.extend_from_slice(&bytes);
    }
    writer.drain().unwrap();
    check!(writer.written() == to_u64(logical.len()));
    check!(fs::read(&path).unwrap() == padded(&logical, align.block));
}

/// The bytes of a failed write were already acknowledged, so the writer
/// fails every later append and drain, and a cut leaves the file at the
/// end of the writes that did finish.
#[test]
fn a_failed_write_fails_the_writer_from_then_on() {
    let block = 512;
    let (mut writer, io) = writer(block);
    let first = pattern(1, 700);
    writer.append(&[&first]).unwrap();
    writer.drain().unwrap();

    io.fail.store(true, Ordering::SeqCst);
    writer.append(&[&pattern(2, 3000)]).unwrap();
    check!(writer.drain().unwrap_err().to_string() == "disk on fire");
    check!(writer.written() == 700);

    let end = writer.end();
    let tail = writer.tail.clone();
    check!(writer.append(&[&pattern(3, 10)]).unwrap_err().to_string() == "disk on fire");
    check!(writer.end() == end);
    check!(writer.tail == tail);

    writer.cut_padding().unwrap();
    check!(file_contents(writer.file()) == first);
}

/// A write that panics fails the writer as an error does, and the write
/// threads go on serving other segments.
#[test]
fn a_panicking_write_fails_the_writer() {
    let (mut writer, _io) = writer(512);
    // A buffer that is not a block multiple long trips the recording
    // I/O's alignment check.
    writer.align.block = 128;
    writer.append(&[&pattern(1, 300)]).unwrap();
    check!(writer.drain().unwrap_err().to_string() == "O_DIRECT write panicked");

    let (mut other, _io) = self::writer(512);
    other.append(&[&pattern(2, 300)]).unwrap();
    check!(other.drain().is_ok());
}

#[test]
fn an_interrupted_write_is_retried() {
    let file = tempfile::tempfile().unwrap();
    let interrupted = Cell::new(false);
    let bytes = padded(&pattern(1, 600), 512);
    write_aligned(&file, &bytes, 0, 512, |file, buf, offset| {
        if interrupted.replace(true) {
            file.write_at(buf, offset)
        } else {
            Err(Error::from(ErrorKind::Interrupted))
        }
    })
    .unwrap();
    check!(file_contents(&file) == bytes);
}

#[test]
fn a_write_of_zero_bytes_fails_with_write_zero() {
    let file = tempfile::tempfile().unwrap();
    let result = write_aligned(&file, &[0; 512], 0, 512, |_, _, _| Ok(0));
    check!(result.unwrap_err().kind() == ErrorKind::WriteZero);
}

/// A short write that stops inside a block goes on from the start of that
/// block, so every call stays aligned.
#[test]
fn a_short_unaligned_write_resumes_from_an_aligned_position() {
    let file = tempfile::tempfile().unwrap();
    let bytes = padded(&pattern(1, 1500), 512);
    let calls = RefCell::new(Vec::new());
    let first = Cell::new(true);
    write_aligned(&file, &bytes, 0, 512, |file, buf, offset| {
        calls.borrow_mut().push(WriteCall {
            offset,
            len: buf.len(),
        });
        let n = file.write_at(buf, offset)?;
        Ok(if first.replace(false) { n.min(700) } else { n })
    })
    .unwrap();
    check!(
        *calls.borrow()
            == vec![
                WriteCall {
                    offset: 0,
                    len: 1536
                },
                WriteCall {
                    offset: 512,
                    len: 1024
                }
            ]
    );
    check!(file_contents(&file) == bytes);
}

#[test]
fn a_short_write_with_no_aligned_progress_fails_with_write_zero() {
    let file = tempfile::tempfile().unwrap();
    let result = write_aligned(&file, &[0; 1024], 0, 512, |_, _, _| Ok(100));
    check!(result.unwrap_err().kind() == ErrorKind::WriteZero);
}

#[test]
fn reset_after_a_truncate_continues_from_the_new_end() {
    let block = 512;
    let (mut writer, io) = writer(block);
    let first = pattern(1, 1300);
    writer.append(&[&first]).unwrap();
    writer.drain().unwrap();
    io.take_calls();

    writer.file().set_len(900).unwrap();
    writer.reset(900, first[512..900].to_vec()).unwrap();
    check!(writer.end() == 900);
    check!(writer.written() == 900);

    let next = pattern(2, 200);
    writer.append(&[&next]).unwrap();
    writer.drain().unwrap();
    // The append completes the block the truncate ended in, and the
    // drain writes the new partial last block.
    check!(
        io.take_calls()
            == vec![
                WriteCall {
                    offset: 512,
                    len: 512
                },
                WriteCall {
                    offset: 1024,
                    len: 512
                }
            ]
    );
    check!(writer.end() == 1100);
    check!(file_contents(writer.file()) == padded(&[&first[..900], &next[..]].concat(), block));
}

#[test]
fn new_and_reset_reject_a_tail_of_the_wrong_length() {
    let align = DioAlign {
        mem: MEM,
        block: 512,
    };
    // Each case: the end, the tail length, and whether they agree.
    let cases = [
        (0, 0, true),
        (0, 1, false),
        (512, 0, true),
        (512, 512, false),
        (700, 188, true),
        (700, 187, false),
        (700, 700, false),
    ];
    for (end, tail_len, valid) in cases {
        let created = DirectWriter::new(
            tempfile::tempfile().unwrap(),
            align,
            end,
            vec![0; tail_len],
            RecordingIo::new(align),
        );
        check!(created.is_ok() == valid, "new: end {end}, tail {tail_len}");
        if let Err(error) = created {
            check!(error.kind() == ErrorKind::InvalidInput);
        }

        let (mut writer, _io) = writer(512);
        let reset = writer.reset(end, vec![0; tail_len]);
        check!(reset.is_ok() == valid, "reset: end {end}, tail {tail_len}");
        if let Err(error) = reset {
            check!(error.kind() == ErrorKind::InvalidInput);
            check!(writer.end() == 0);
        }
    }
}

#[test]
fn new_rejects_an_alignment_that_is_not_a_power_of_two() {
    let cases = [
        DioAlign { mem: 0, block: 512 },
        DioAlign { mem: 512, block: 0 },
        DioAlign {
            mem: 512,
            block: 1000,
        },
        DioAlign { mem: 3, block: 512 },
    ];
    for align in cases {
        let created = DirectWriter::new(
            tempfile::tempfile().unwrap(),
            align,
            0,
            Vec::new(),
            RecordingIo::new(DioAlign { mem: 1, block: 1 }),
        );
        check!(
            created.err().map(|error| error.kind()) == Some(ErrorKind::InvalidInput),
            "{align:?}"
        );
    }
}
