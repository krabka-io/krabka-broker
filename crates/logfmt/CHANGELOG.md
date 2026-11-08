# Changelog

Krabka versions and tags the whole workspace as one unit, so this crate has no
release of its own. The [root changelog](../../CHANGELOG.md) records every
release that contains `krabka-logfmt`.

## [Unreleased]

### Added

- Trace correlation: with a Google Cloud project configured, an entry written
  inside an OpenTelemetry span carries `logging.googleapis.com/trace`,
  `logging.googleapis.com/spanId` and `logging.googleapis.com/trace_sampled`.
- An entry carries the fields of the span its event runs in.

### Changed

- `layer` is a builder: `layer().filter(..).writer(..).call()`, with an
  optional `gcp_project_id`.
- An event field named `timestamp`, `severity`, `target` or `fields`, or like a
  Cloud Logging special key, is kept under the `fields` object. Before, the
  entry-key ones were dropped and the special keys reached the entry.
- An integer whose magnitude exceeds 2^53 is written as a JSON string. Before,
  a `u64` was a number that Cloud Logging rounded, and an `i128` or `u128` was
  always a string.

The history of this crate before krabka-broker became its own repository is in
[robot-head/crabka](https://github.com/robot-head/crabka).
