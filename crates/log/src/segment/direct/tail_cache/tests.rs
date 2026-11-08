//! Unit tests for the tail cache and its window: reads, eviction, truncation,
//! and the slabs it takes from and gives back to the arena.

use {super::*, crate::segment::direct::test_support::pattern, assert2::check, std::ops::Range};

/// A [`TailCache`] and the bytes pushed into it, in order.
struct ThreeChunks {
    cache: TailCache,
    all: Vec<u8>,
}

/// A window that starts at 1000 and holds chunks of 10, 20 and 30 bytes,
/// all of them on the disk.
fn three_chunks(cap: usize) -> ThreeChunks {
    let mut cache = TailCache::new(cap, 1000);
    cache.set_written(1060);
    let mut all = Vec::new();
    for (seed, size) in [(1, 10), (2, 20), (3, 30)] {
        let bytes = pattern(seed, size);
        all.extend_from_slice(&bytes);
        cache.push(&[&bytes]);
    }
    ThreeChunks { cache, all }
}

fn read(cache: &TailCache, pos: u64, len: usize) -> Option<Vec<u8>> {
    let mut out = vec![0xAA; len];
    if cache.window().read(pos, &mut out) {
        Some(out)
    } else {
        check!(
            out == vec![0xAA; len],
            "a failed read leaves the buffer alone"
        );
        None
    }
}

#[test]
fn tail_cache_reads_across_chunk_boundaries() {
    /// A position, a length, and the expected window slice.
    struct Case {
        pos: u64,
        len: usize,
        range: Option<Range<usize>>,
    }
    let ThreeChunks { cache, all } = three_chunks(1 << 20);
    check!((cache.start(), cache.end()) == (1000, 1060));
    let cases = [
        Case {
            pos: 1000,
            len: 60,
            range: Some(0..60),
        },
        Case {
            pos: 1000,
            len: 10,
            range: Some(0..10),
        },
        Case {
            pos: 1005,
            len: 10,
            range: Some(5..15),
        },
        Case {
            pos: 1009,
            len: 30,
            range: Some(9..39),
        },
        Case {
            pos: 1030,
            len: 30,
            range: Some(30..60),
        },
        Case {
            pos: 1059,
            len: 1,
            range: Some(59..60),
        },
        Case {
            pos: 1060,
            len: 0,
            range: Some(60..60),
        },
        Case {
            pos: 1000,
            len: 0,
            range: Some(0..0),
        },
        Case {
            pos: 999,
            len: 1,
            range: None,
        },
        Case {
            pos: 999,
            len: 0,
            range: None,
        },
        Case {
            pos: 1050,
            len: 11,
            range: None,
        },
        Case {
            pos: 1061,
            len: 0,
            range: None,
        },
        Case {
            pos: 2000,
            len: 5,
            range: None,
        },
        Case {
            pos: u64::MAX,
            len: 1,
            range: None,
        },
    ];
    for Case { pos, len, range } in cases {
        check!(
            read(&cache, pos, len) == range.map(|range| all[range].to_vec()),
            "pos {pos}, len {len}"
        );
    }
}

#[test]
fn tail_cache_evicts_the_oldest_chunks_past_the_cap() {
    // Each case: the cap, and the start of the window after the three
    // pushes.
    let cases = [
        (1000, 1000),
        (60, 1000),
        (59, 1010),
        (50, 1010),
        (30, 1030),
        (29, 1030),
        (0, 1030),
    ];
    for (cap, start) in cases {
        let ThreeChunks { cache, all } = three_chunks(cap);
        check!((cache.start(), cache.end()) == (start, 1060), "cap {cap}");
        check!(
            read(&cache, start, 1060 - usize::try_from(start).unwrap())
                == Some(all[usize::try_from(start - 1000).unwrap()..].to_vec())
        );
        check!(read(&cache, start - 1, 1).is_none());
    }
}

/// One chunk comes back as a slice of the cached bytes, with no copy;
/// several are joined up to `max`, and the first comes whatever its size.
#[test]
fn tail_cache_bytes_from_slices_one_chunk_and_joins_several() {
    let ThreeChunks { cache, all } = three_chunks(1000);
    let one = cache.window().bytes_from(1010, 5).unwrap();
    check!(one[..] == all[10..30]);
    check!(one.as_ptr() == cache.window().chunks[1].as_ptr());
    check!(cache.window().bytes_from(1015, 0).unwrap()[..] == all[15..30]);
    check!(cache.window().bytes_from(1000, 30).unwrap()[..] == all[..30]);
    check!(cache.window().bytes_from(1000, 60).unwrap()[..] == all[..]);
    check!(cache.window().bytes_from(1000, 59).unwrap()[..] == all[..30]);
    check!(cache.window().bytes_from(999, 10).is_none());
    check!(cache.window().bytes_from(1060, 10).is_none());
}

/// A chunk that is not on the disk yet stays, whatever the cap, so a read
/// below the window can always go to the file. Once it is written, the
/// next push evicts it.
#[test]
fn tail_cache_keeps_the_bytes_that_are_not_on_the_disk() {
    let mut cache = TailCache::new(0, 1000);
    for (seed, size) in [(1, 10), (2, 20), (3, 30)] {
        cache.push(&[&pattern(seed, size)]);
    }
    check!((cache.start(), cache.end()) == (1000, 1060));

    cache.set_written(1015);
    cache.push(&[&pattern(4, 5)]);
    check!(cache.start() == 1010);

    cache.set_written(1065);
    cache.push(&[&pattern(5, 5)]);
    check!((cache.start(), cache.end()) == (1065, 1070));

    cache.truncate(1062);
    check!((cache.start(), cache.end()) == (1062, 1062));
    cache.push(&[&pattern(6, 5)]);
    cache.push(&[&pattern(7, 5)]);
    check!(cache.start() == 1062);
}

/// A coalesced write leaves the same window as pushing its batches one
/// by one, and no batch left in the window shares the group's allocation,
/// so the window still holds at most `cap` bytes (or the newest batch).
#[test]
fn tail_cache_group_push_matches_single_pushes_and_detaches_survivors() {
    for cap in [1000, 60, 59, 50, 30, 29, 0] {
        let ThreeChunks { cache: single, all } = three_chunks(cap);
        let group = Bytes::from(all.clone());
        let mut grouped = TailCache::new(cap, 1000);
        grouped.set_written(1060);
        grouped.push_group(&group, &[10, 30, 60]);
        check!(
            (grouped.start(), grouped.end()) == (single.start(), single.end()),
            "cap {cap}"
        );
        let start = single.start();
        let len = 1060 - usize::try_from(start).unwrap();
        check!(
            read(&grouped, start, len) == read(&single, start, len),
            "cap {cap}"
        );
        let group_range = group.as_ptr().addr()..group.as_ptr().addr() + group.len();
        for chunk in &grouped.window().chunks {
            check!(
                !group_range.contains(&chunk.as_ptr().addr()),
                "cap {cap}: a retained batch still points into the group"
            );
        }
    }
}

#[test]
fn tail_cache_ignores_an_empty_push() {
    let mut cache = TailCache::new(0, 5);
    cache.push(&[b"abc"]);
    cache.push(&[&[]]);
    check!((cache.start(), cache.end()) == (5, 8));
    check!(read(&cache, 5, 3) == Some(b"abc".to_vec()));
}

#[test]
fn tail_cache_truncate_forgets_bytes_past_the_new_end() {
    // Each case: the truncate point, then the window it leaves.
    let cases = [
        (2000, 1000, 1060),
        (1060, 1000, 1060),
        (1045, 1000, 1045),
        (1030, 1000, 1030),
        (1010, 1000, 1010),
        (1003, 1000, 1003),
        (1000, 1000, 1000),
        (900, 900, 900),
    ];
    for (end, start, new_end) in cases {
        let ThreeChunks { mut cache, all } = three_chunks(1 << 20);
        cache.truncate(end);
        check!(
            (cache.start(), cache.end()) == (start, new_end),
            "truncate to {end}"
        );
        let kept = usize::try_from(new_end - start).unwrap();
        let from = usize::try_from(start.saturating_sub(1000)).unwrap();
        check!(read(&cache, start, kept) == Some(all[from..from + kept].to_vec()));
        check!(read(&cache, new_end, 1).is_none());

        // The window grows again from the new end.
        cache.push(&[b"xyz"]);
        check!(cache.end() == new_end + 3);
        check!(read(&cache, new_end, 3) == Some(b"xyz".to_vec()));
    }
}

/// Push `count` written batches of `size` bytes into `cache`.
fn fill(cache: &mut TailCache, count: usize, size: usize) {
    for seed in 0..count {
        let batch = pattern(u8::try_from(seed % 251).unwrap(), size);
        cache.push(&[&batch]);
        cache.set_written(cache.end());
    }
}

/// With no budget a window holds its floor; under a budget it grows to
/// its maximum; at the budget it keeps the size it has.
#[test]
fn tail_cache_grows_past_its_floor_only_under_a_budget() {
    let batch = 1000;
    let unbudgeted = Arc::new(TailArena::new(None));
    let mut fixed = TailCache::in_arena(10 * batch, 100 * batch, 0, Arc::clone(&unbudgeted));
    fill(&mut fixed, 300, batch);
    check!(fixed.end() - fixed.start() == to_u64(10 * batch));

    let roomy = Arc::new(TailArena::new(Some(1 << 30)));
    let mut grown = TailCache::in_arena(10 * batch, 100 * batch, 0, Arc::clone(&roomy));
    fill(&mut grown, 300, batch);
    check!(grown.end() - grown.start() == to_u64(100 * batch));

    // A budget the caches already exceed: a new window stops at what it
    // holds once full, and stays at its floor.
    let tight = Arc::new(TailArena::new(Some(to_u64(FIRST_SLAB))));
    let mut held = TailCache::in_arena(10 * batch, 100 * batch, 0, Arc::clone(&tight));
    fill(&mut held, 300, batch);
    check!(held.end() - held.start() == to_u64(10 * batch));
}

/// Slabs the window no longer reaches go back to the arena, and dropping
/// the cache gives back the rest, so the arena counts what the windows
/// hold and nothing more.
#[test]
fn tail_cache_gives_its_slabs_back() {
    let arena = Arc::new(TailArena::new(None));
    let mut cache = TailCache::in_arena(64 * 1024, 64 * 1024, 0, Arc::clone(&arena));
    fill(&mut cache, 2000, 10 * 1024);
    check!(cache.end() == to_u64(2000 * 10 * 1024));
    check!(to_u64(cache.slab_bytes()) == arena.held());
    check!(
        cache.slab_bytes() <= 2 * SLAB + 64 * 1024,
        "a 64 KiB window keeps no more than the slabs it reaches: {}",
        cache.slab_bytes()
    );
    drop(cache);
    check!(arena.held() == 0);
}

/// A read view clones only the chunks from its position on.
#[test]
fn tail_window_suffix_keeps_the_chunks_from_a_position_on() {
    let ThreeChunks { cache, all } = three_chunks(1 << 20);
    let window = cache.window();
    for (pos, start, chunks) in [
        (0, 1000, 3),
        (1000, 1000, 3),
        (1009, 1000, 3),
        (1010, 1010, 2),
        (1045, 1030, 1),
        (1060, 1060, 0),
        (5000, 1060, 0),
    ] {
        let suffix = window.suffix(pos);
        check!(
            (suffix.start(), suffix.end(), suffix.chunks.len()) == (start, 1060, chunks),
            "suffix from {pos}"
        );
        let from = usize::try_from(start - 1000).unwrap();
        check!(
            suffix
                .bytes_from(start, usize::MAX)
                .map(|bytes| bytes.to_vec())
                == (start < 1060).then(|| all[from..].to_vec())
        );
    }
}

/// A segment's cache takes the slabs the cache of the segment before it
/// gave back when it rolled, up to the budget.
#[test]
fn a_new_segment_cache_takes_the_rolled_caches_slabs() {
    let arena = Arc::new(TailArena::new(Some(64 << 20)));
    for _segment in 0..4 {
        let mut cache = TailCache::in_arena(32 << 20, 32 << 20, 0, Arc::clone(&arena));
        fill(&mut cache, 40, 1 << 20);
    }
    check!(
        arena.fresh() <= 9,
        "{} slabs made new for four 40 MiB segments with 32 MiB windows",
        arena.fresh()
    );
    check!(arena.held() == 0);
}

/// A window that turns over many times reuses the same few slabs.
#[test]
fn tail_cache_reuses_its_slabs() {
    let arena = Arc::new(TailArena::new(None));
    let mut cache = TailCache::in_arena(16 << 20, 16 << 20, 0, Arc::clone(&arena));
    fill(&mut cache, 1000, 1 << 20);
    check!(
        arena.fresh() <= 16,
        "{} slabs made new for 1000 MiB through a 16 MiB window ({} pooled)",
        arena.fresh(),
        arena.pooled()
    );
}

/// A truncation gives back the slabs it leaves wholly past the cut, so a
/// window cut back to its first batches does not keep its old size
/// counted against the budget.
#[test]
fn tail_cache_truncate_gives_back_the_slabs_past_the_cut() {
    let arena = Arc::new(TailArena::new(Some(1 << 30)));
    let mut cache = TailCache::in_arena(1 << 20, 64 << 20, 0, Arc::clone(&arena));
    fill(&mut cache, 48, 1 << 20);
    check!(cache.end() - cache.start() == 48 << 20);
    let grown = arena.held();
    check!(grown >= 48 << 20);
    cache.truncate(2 << 20);
    check!((cache.start(), cache.end()) == (0, 2 << 20));
    check!(to_u64(cache.slab_bytes()) == arena.held());
    check!(
        arena.held() <= to_u64(2 * SLAB),
        "{} bytes held after cutting {grown} back to 2 MiB",
        arena.held()
    );
    cache.push(&[b"after the cut"]);
    check!(cache.end() == (2 << 20) + 13);
    check!(read(&cache, 2 << 20, 13) == Some(b"after the cut".to_vec()));
}

/// A window at the budget takes a slab for each push before it gives an
/// old one back, and still reuses the slabs it gives back.
#[test]
fn a_window_at_the_budget_reuses_its_slabs() {
    let arena = Arc::new(TailArena::new(Some(32 << 20)));
    let mut cache = TailCache::in_arena(1 << 20, 1 << 30, 0, Arc::clone(&arena));
    fill(&mut cache, 400, 1 << 20);
    check!(
        arena.fresh() <= 12,
        "{} slabs made new for 400 MiB through a window held at a 32 MiB budget",
        arena.fresh()
    );
}
