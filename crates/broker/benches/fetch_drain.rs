//! `CodSpeed` microbenchmarks for the broker's zero-copy fetch drain.
//!
//! A fetch response's records region reaches the socket one of two ways. The
//! vectored path (Increment C) `pread`s the region out of the segment file
//! into a buffer and writes the buffer; the sendfile path (Increments D + E)
//! hands the region's file descriptor to the kernel, which moves the pages to
//! the NIC without a userspace copy. Both produce the same wire bytes, so
//! nothing but a measurement says which is faster — and `sendfile(2)` is not
//! free: below some response size its syscall overhead costs more than the
//! copy it avoids.
//!
//! `sendfile_min` is the broker's guess at where that crossover is, and every
//! fetch is routed by it. This suite measures it. Each size drains a whole
//! response — the frame length, a response envelope, and one records region —
//! over a real loopback TCP socket with a reader draining the other end, once
//! through each path.
//!
//! Both cases pay their real per-response cost inside the timed region: the
//! vectored case runs `resolve_records_inline`, which is the `pread` the
//! broker's read path does for a run below the threshold, and the sendfile
//! case runs `resolve_records_sendfile`, which only clones the region
//! descriptor. The segment file is freshly written and so is warm in the page
//! cache, which is the state a tail read finds it in.
//!
//! What the sweep leaves out is the read planning ahead of the drain: the
//! batch-header walk (`Log::read_raw_desc`) that builds the file regions. That
//! omission does not tilt the table toward sendfile, because the walk is not
//! the sendfile path's alone. `handlers::fetch::read` runs it on every
//! sendfile-capable connection *before* it consults `sendfile_min`, so a
//! response the threshold rejects pays the walk and then pays `read_raw` on
//! top of it. Excluding the walk from both columns therefore understates the
//! vectored column's real cost, not sendfile's.
//!
//! `sendfile_min` still sits at the smallest swept size rather than below it:
//! the sweep says nothing about a response under 4 KiB, and a threshold is
//! only as good as the measurement under it.
//!
//! `bench_crossover` prints ns per response for both paths at every size, the
//! sendfile-over-vectored ratio, and the smallest swept size at which sendfile
//! wins — the number `sendfile_min`'s default has to match.

use {
    criterion::{Criterion, abs_file, criterion_main},
    std::{env, error::Error, process},
};

/// Any failure while building or measuring a case; it stops the run.
type BenchError = Box<dyn Error + Send + Sync>;

/// The sweep, on the platforms that have a file-to-socket `sendfile(2)`:
/// Linux, the Apple targets, and FreeBSD/DragonFly.
#[cfg(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "ios",
    target_os = "tvos",
    target_os = "watchos",
    target_os = "freebsd",
    target_os = "dragonfly",
))]
mod sweep {
    use {
        super::BenchError,
        assert2::assert,
        bytes::{BufMut, Bytes, BytesMut},
        criterion::{Criterion, Throughput},
        krabka_broker::{
            fetch_drain::{
                WriteOp, resolve_records_inline, resolve_records_sendfile, write_fetch_plan,
            },
            metrics::{BrokerMetrics, FetchDrainPath, FetchDrainPathLabel},
        },
        krabka_protocol::records::{FileRegion, RecordsPayload},
        std::{
            io::{self, Write as _},
            sync::Arc,
            time::{Duration, Instant},
        },
        tokio::{
            io::AsyncReadExt as _,
            net::{TcpListener, TcpStream},
            runtime::Runtime,
        },
    };

    /// Every response size the sweep covers, and the label each is reported under.
    const SIZES: [ResponseSize; 5] = [
        ResponseSize::new("4KiB", 4 * 1024),
        ResponseSize::new("16KiB", 16 * 1024),
        ResponseSize::new("32KiB", 32 * 1024),
        ResponseSize::new("64KiB", 64 * 1024),
        ResponseSize::new("256KiB", 256 * 1024),
    ];

    /// One response size the sweep covers, with the label it is reported under.
    struct ResponseSize {
        label: &'static str,
        records_bytes: usize,
    }

    impl ResponseSize {
        const fn new(label: &'static str, records_bytes: usize) -> Self {
            Self {
                label,
                records_bytes,
            }
        }
    }

    /// Untimed drains run before the measured ones, over the same socket.
    ///
    /// The first write on a fresh connection pays for the socket's send-buffer
    /// growth and for the reader task's first scheduling. Criterion amortizes
    /// that over a large `iters`, but [`bench_crossover`]'s short runs do not.
    const WARMUP: u64 = 8;

    /// Response bytes the writer emits ahead of the records: the correlation
    /// header and the partition metadata.
    ///
    /// The exact size does not matter to the comparison — it is identical on
    /// both paths — but its presence does, because a real drain always writes
    /// an inline op before it reaches the records.
    const ENVELOPE: usize = 64;

    /// Send and receive buffers for both ends of the loopback socket.
    ///
    /// It is the broker's own `socket_send_buffer` default, and it is larger
    /// than the largest response in the sweep, so no case is measured against
    /// a different amount of flow-control back-pressure than another.
    const SOCKET_BUFFER: usize = 1024 * 1024;

    /// The two ways a records region reaches the socket.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum Case {
        /// `pread` the region into a buffer, then write the buffer.
        Vectored,
        /// Hand the region's descriptor to the kernel.
        Sendfile,
    }

    impl Case {
        /// The label its drain must be counted under. A case that took the
        /// other path would compare two measurements of the same code.
        fn expected(self) -> FetchDrainPath {
            match self {
                Self::Vectored => FetchDrainPath::Vectored,
                Self::Sendfile => FetchDrainPath::Sendfile,
            }
        }

        fn name(self) -> &'static str {
            match self {
                Self::Vectored => "vectored",
                Self::Sendfile => "sendfile",
            }
        }
    }

    /// One response's worth of on-disk records, plus the envelope bytes that
    /// precede them on the wire.
    struct Response {
        /// Kept so the file outlives the regions that point into it.
        _file: tempfile::NamedTempFile,
        payload: RecordsPayload,
        /// The frame length and a response header, as the writer emits them.
        header: Bytes,
        records_bytes: usize,
    }

    impl Response {
        /// A response whose records region is `records_bytes` of segment file.
        fn new(records_bytes: usize) -> Result<Self, BenchError> {
            // A repeating non-uniform pattern rather than zeros: an all-zero
            // file is not what a segment looks like, and a filesystem is free
            // to store it as a hole.
            let records = (0..records_bytes)
                .map(|i| u8::try_from(i % 251))
                .collect::<Result<Vec<u8>, _>>()?;
            let mut file = tempfile::NamedTempFile::new()?;
            file.write_all(&records)?;
            file.flush()?;
            let handle = Arc::new(file.reopen()?);

            let mut header = BytesMut::with_capacity(4 + ENVELOPE);
            header.put_u32(u32::try_from(ENVELOPE + records_bytes)?);
            header.put_bytes(0, ENVELOPE);

            Ok(Self {
                _file: file,
                payload: RecordsPayload::FileRegions(vec![FileRegion {
                    file: handle,
                    offset: 0,
                    len: records_bytes,
                }]),
                header: header.freeze(),
                records_bytes,
            })
        }

        /// The write plan for one drain of this response, in `case`.
        ///
        /// Building it is inside the timed region because that is where the
        /// paths differ: the vectored resolver reads every records byte into a
        /// fresh buffer, and the sendfile resolver clones a descriptor.
        fn plan(&self, case: Case) -> Result<Vec<WriteOp>, BenchError> {
            let mut ops = vec![WriteOp::Inline(self.header.clone())];
            let records = match case {
                Case::Vectored => resolve_records_inline(&self.payload),
                Case::Sendfile => resolve_records_sendfile(&self.payload),
            }?;
            ops.extend(records);
            Ok(ops)
        }
    }

    /// Set both buffers on one end of the loopback pair, so a large response
    /// does not measure the reader's scheduling instead of the write.
    fn size_buffers(stream: &TcpStream) {
        let socket = socket2::SockRef::from(stream);
        let _ = socket.set_send_buffer_size(SOCKET_BUFFER);
        let _ = socket.set_recv_buffer_size(SOCKET_BUFFER);
    }

    /// The count `path` carries on the drain counter.
    fn drained(metrics: &BrokerMetrics, path: FetchDrainPath) -> u64 {
        metrics
            .fetch_response_drain
            .get_or_create(&FetchDrainPathLabel { path })
            .get()
    }

    /// Drain `response` `iters` times in `case`, returning the time the drains
    /// took.
    ///
    /// The socket pair, the reader task and the runtime are built once per
    /// call and are outside the timer. The path assertion afterwards is what
    /// keeps the comparison meaningful: were a change to route the sendfile
    /// case onto the copy path, the two columns would converge and the table
    /// would report `sendfile(2)` as costing nothing. This fails the run
    /// instead.
    fn timed_drains(response: &Response, case: Case, iters: u64) -> Result<Duration, BenchError> {
        let runtime = Runtime::new()?;
        runtime.block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await?;
            let addr = listener.local_addr()?;
            let reader = tokio::spawn(async move {
                let mut client = TcpStream::connect(addr).await?;
                size_buffers(&client);
                let mut sink = vec![0u8; SOCKET_BUFFER];
                while client.read(&mut sink).await.unwrap_or(0) > 0 {}
                Ok::<(), io::Error>(())
            });
            let (mut server, _) = listener.accept().await?;
            size_buffers(&server);
            let metrics = BrokerMetrics::new();

            for _ in 0..WARMUP {
                write_fetch_plan(&mut server, response.plan(case)?, &metrics).await?;
            }
            let start = Instant::now();
            for _ in 0..iters {
                write_fetch_plan(&mut server, response.plan(case)?, &metrics).await?;
            }
            let elapsed = start.elapsed();

            drop(server); // EOF for the reader
            reader.await??;

            let counts = FetchDrainPath::ALL.map(|p| drained(&metrics, p));
            let expected = FetchDrainPath::ALL.map(|p| {
                if p == case.expected() {
                    iters + WARMUP
                } else {
                    0
                }
            });
            assert!(
                counts == expected,
                "the {} case must drain every response on its own path",
                case.name()
            );
            Ok(elapsed)
        })
    }

    /// The first error a timed closure met. Criterion's `iter_custom` closure
    /// must return a [`Duration`], so the error waits here until criterion
    /// returns.
    #[derive(Default)]
    struct FirstError(Option<BenchError>);

    impl FirstError {
        /// The measured time, or zero after keeping the error.
        fn time(&mut self, timed: Result<Duration, BenchError>) -> Duration {
            timed.unwrap_or_else(|error| {
                self.0.get_or_insert(error);
                Duration::ZERO
            })
        }

        /// The kept error, if any.
        fn into_result(self) -> Result<(), BenchError> { self.0.map_or(Ok(()), Err) }
    }

    /// Nanoseconds per response for `case`: the fastest of `runs` measurement
    /// runs of `samples` drains each.
    fn fastest_nanos(
        response: &Response,
        case: Case,
        samples: u32,
        runs: u32,
    ) -> Result<f64, BenchError> {
        let mut fastest = f64::INFINITY;
        for _ in 0..runs {
            let elapsed = timed_drains(response, case, u64::from(samples))?;
            fastest = fastest.min(elapsed.as_secs_f64() * 1.0e9 / f64::from(samples));
        }
        Ok(fastest)
    }

    /// The criterion sweep: every size, on both paths.
    pub fn bench_fetch_drain(c: &mut Criterion) -> Result<(), BenchError> {
        let mut failure = FirstError::default();
        {
            let mut group = c.benchmark_group("broker/fetch_drain");

            for ResponseSize {
                label,
                records_bytes,
            } in SIZES
            {
                let response = Response::new(records_bytes)?;
                group.throughput(Throughput::Bytes(response.records_bytes as u64));
                for case in [Case::Vectored, Case::Sendfile] {
                    group.bench_function(format!("{label}/{}", case.name()), |b| {
                        b.iter_custom(|iters| failure.time(timed_drains(&response, case, iters)));
                    });
                }
            }

            group.finish();
        }
        failure.into_result()
    }

    /// Print both paths' cost at every size, and the crossover between them.
    ///
    /// Criterion compares a benchmark against its own previous run, not
    /// against a sibling, so the number this suite exists for — the response
    /// size at which `sendfile(2)` starts beating the copy, which is what
    /// `sendfile_min` has to be set to — has to be computed here.
    pub fn bench_crossover(_c: &mut Criterion) -> Result<(), BenchError> {
        /// Drains per measurement run.
        const SAMPLES: u32 = 200;
        /// Measurement runs per case, of which the fastest is reported.
        ///
        /// A single run is noise-dominated on a machine that is doing anything
        /// else. Interference only ever adds time, so the minimum over a few
        /// runs is the estimator it cannot pull the wrong way.
        const RUNS: u32 = 5;

        println!(
            "\nbroker/fetch_drain — nanoseconds per response (fastest of {RUNS} runs), and where sendfile starts winning"
        );
        println!(
            "{:<10} {:>12} {:>12} {:>20}",
            "size", "vectored", "sendfile", "sendfile/vectored"
        );
        let mut crossover: Option<&str> = None;
        for ResponseSize {
            label,
            records_bytes,
        } in SIZES
        {
            let response = Response::new(records_bytes)?;
            let vectored = fastest_nanos(&response, Case::Vectored, SAMPLES, RUNS)?;
            let sendfile = fastest_nanos(&response, Case::Sendfile, SAMPLES, RUNS)?;
            if sendfile < vectored && crossover.is_none() {
                crossover = Some(label);
            }
            println!(
                "{label:<10} {vectored:>12.0} {sendfile:>12.0} {:>19.2}x",
                sendfile / vectored
            );
        }
        match crossover {
            Some(size) => println!(
                "\nsendfile first wins at {size}: that is the response size sendfile_min should carry."
            ),
            None => println!(
                "\nsendfile won at no swept size: sendfile_min should sit above the largest of them."
            ),
        }
        println!();
        Ok(())
    }
}

/// The sweep has nothing to measure where the platform has no file-to-socket
/// `sendfile(2)`: every fetch drains through the vectored path, and
/// `sendfile_min` routes nothing.
#[cfg(not(any(
    target_os = "linux",
    target_os = "macos",
    target_os = "ios",
    target_os = "tvos",
    target_os = "watchos",
    target_os = "freebsd",
    target_os = "dragonfly",
)))]
mod sweep {
    use {super::BenchError, criterion::Criterion};

    pub fn bench_fetch_drain(_c: &mut Criterion) -> Result<(), BenchError> {
        println!(
            "broker/fetch_drain: this target has no file→socket sendfile, so every fetch drains \
             through the vectored path and there is no crossover to sweep"
        );
        Ok(())
    }

    pub fn bench_crossover(_c: &mut Criterion) -> Result<(), BenchError> { Ok(()) }
}

/// A benchmark target, under the name `criterion_group!` would give it.
type Target = (&'static str, fn(&mut Criterion) -> Result<(), BenchError>);

/// Every benchmark target, in run order.
const TARGETS: [Target; 2] = [
    ("sweep::bench_fetch_drain", sweep::bench_fetch_drain),
    ("sweep::bench_crossover", sweep::bench_crossover),
];

/// Runs every target in [`TARGETS`], stopping at the first one that fails.
fn run_targets(criterion: &mut Criterion) -> Result<(), (&'static str, BenchError)> {
    for (name, target) in TARGETS {
        if env::var("CODSPEED_ENV").is_ok() {
            criterion.set_current_file(abs_file!());
            criterion.set_macro_group(format!("benches::{name}"));
        }
        target(criterion).map_err(|error| (name, error))?;
    }
    Ok(())
}

/// The benchmark group, written out by hand where `criterion_group!` would
/// generate it, so that a failing target reports its error and stops the run.
fn benches() {
    let result = run_targets(&mut Criterion::default().configure_from_args());
    if let Err((name, error)) = result {
        eprintln!("{name}: {error}");
        process::exit(1);
    }
}

criterion_main!(benches);
