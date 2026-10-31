//! The default `log_page_cache_budget`: half of the memory the broker may use.
//!
//! The budget keeps the page cache the broker's partitions hold below the
//! point where the kernel would reclaim it inside an append; see
//! `krabka_log::LogConfig::page_cache_budget`. That point is the memory limit
//! of the broker's cgroup when it has one, and the machine's memory
//! otherwise. Half of it leaves room for the broker's own memory, for its
//! sockets' buffers, which cgroup v2 charges to it as well, and for the dirty
//! pages of every partition's current write-back window.

use {
    krabka_units::prelude::{ByteSize, ByteSizeExt as _},
    std::fs,
};

/// Half of the memory the broker may use.
///
/// That is the memory limit of the cgroup this process is in, or the
/// machine's memory when the cgroup has no limit. `None` when neither can be
/// read: a kernel without cgroup v2 and without `/proc/meminfo`, or an
/// operating system other than Linux.
#[must_use]
pub fn host_default() -> Option<ByteSize> {
    memory_limit().map(|limit| ByteSize::from_bytes(limit / 2))
}

/// The default `log_tail_cache_budget`: an eighth of the memory the broker
/// may use.
///
/// That holds one busy partition's full `log_tail_cache_max` window in a
/// 10 GiB container. A quarter was too much beside what the broker already
/// holds: 100 partitions with `preallocate=true` taking 1 KiB records peaked
/// at 5.2 to 5.4 GiB resident with a quarter, 3.0 to 4.0 GiB with an
/// eighth, against 2.4 to 2.7 GiB with fixed 8 MiB windows. `None` when the
/// memory cannot be read, as for [`host_default`]; the tail caches then hold
/// their floor.
#[must_use]
pub fn tail_cache_default() -> Option<ByteSize> {
    memory_limit().map(|limit| ByteSize::from_bytes(limit / 8))
}

/// The memory limit of the cgroup this process is in, or the machine's
/// memory when the cgroup has no limit.
fn memory_limit() -> Option<u64> { cgroup_memory_limit().or_else(machine_memory) }

/// The `memory.max` of the cgroup this process is in, when cgroup v2 limits
/// it.
fn cgroup_memory_limit() -> Option<u64> {
    let cgroup = fs::read_to_string("/proc/self/cgroup").ok()?;
    let path = cgroup_v2_path(&cgroup)?;
    let max = fs::read_to_string(format!("/sys/fs/cgroup{path}/memory.max")).ok()?;
    memory_max(&max)
}

fn machine_memory() -> Option<u64> {
    let meminfo = fs::read_to_string("/proc/meminfo").ok()?;
    mem_total(&meminfo)
}

/// The cgroup v2 path in a `/proc/self/cgroup` body: the `0::` line's.
fn cgroup_v2_path(cgroup: &str) -> Option<&str> {
    cgroup
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .map(str::trim)
}

/// The bytes a `memory.max` body limits the cgroup to; `None` for `max`.
fn memory_max(body: &str) -> Option<u64> { body.trim().parse().ok() }

/// `MemTotal` in bytes, from a `/proc/meminfo` body.
fn mem_total(meminfo: &str) -> Option<u64> {
    let line = meminfo.lines().find(|line| line.starts_with("MemTotal:"))?;
    let mut fields = line.split_whitespace().skip(1);
    let kib: u64 = fields.next()?.parse().ok()?;
    if fields.next()? != "kB" {
        return None;
    }
    kib.checked_mul(1024)
}

#[cfg(test)]
mod tests {
    use super::{cgroup_v2_path, mem_total, memory_max};

    /// The v2 line is found among v1 controller lines, and its path is
    /// trimmed of the newline.
    #[yare::parameterized(
        plain = { "0::/system.slice/docker-abc.scope\n", Some("/system.slice/docker-abc.scope") },
        hybrid = { "12:memory:/docker/abc\n0::/docker/abc\n", Some("/docker/abc") },
        root = { "0::/\n", Some("/") },
        v1_only = { "12:memory:/docker/abc\n", None },
    )]
    fn the_cgroup_v2_path_is_the_zero_line(body: &str, expected: Option<&str>) {
        assert2::assert!(cgroup_v2_path(body) == expected);
    }

    /// A number is a limit; `max` is none.
    #[yare::parameterized(
        ten_gibibytes = { "10737418240\n", Some(10_737_418_240) },
        unlimited = { "max\n", None },
        empty = { "", None },
    )]
    fn memory_max_is_a_limit_or_none(body: &str, expected: Option<u64>) {
        assert2::assert!(memory_max(body) == expected);
    }

    /// `MemTotal` is read in kibibytes and nothing else is accepted.
    #[yare::parameterized(
        kibibytes = { "MemTotal:       16384 kB\nMemFree: 1 kB\n", Some(16_384 * 1024) },
        missing = { "MemFree: 1 kB\n", None },
        other_unit = { "MemTotal: 16 MB\n", None },
    )]
    fn mem_total_is_read_in_kibibytes(body: &str, expected: Option<u64>) {
        assert2::assert!(mem_total(body) == expected);
    }
}
