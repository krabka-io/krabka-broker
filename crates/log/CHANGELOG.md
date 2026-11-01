# Changelog

Krabka versions and tags the whole workspace as one unit, so this crate has no
release of its own. The [root changelog](../../CHANGELOG.md) records every
release that contains `krabka-log`.

## [Unreleased]

### Added

- `WriteBack` and `set_write_back` let an embedder install, once per process and before any log opens, how a log starts the write-out of its active segment's dirty pages and how it drops a written range from the page cache. krabka-broker installs one that uses `sync_file_range(2)`.

### Changed

- On Linux, the default write-back is the file's `sync_data`, as on the other non-WASI targets, and the default page drop is `posix_fadvise(POSIX_FADV_DONTNEED)` alone, without a wait for the range's write-out. krabka-log called `sync_file_range` from a rustix fork that the workspace applies through `[patch.crates-io]`, which `cargo publish` ignores, and the rustix on crates.io has no `sync_file_range`, so every 1.1.x publish of krabka-log failed to build.

The history of this crate before krabka-broker became its own repository is in
[robot-head/crabka](https://github.com/robot-head/crabka).
