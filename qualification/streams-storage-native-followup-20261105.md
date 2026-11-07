# Native topology storage follow-up (2026-11-05)

Base: `86d5ebfd2ca6537c70af0cd94ec5e861bb43eebe` (released-default graduation). This follow-up adds native
observability and bounded lifecycle regressions, not a new released-broker RF3
interoperability receipt or a KIP support-status graduation.

## Implemented and executed

Production native `set`, `get` and `delete` callbacks record completed operations
and duration in seconds, labelled only by operation and success/failure. There
are six possible combinations; group IDs, topology epochs, class names and backend
errors never become metric labels. Successful missing reads count as success.
Cancelled/panicking callbacks are not completed observations. Duration spans
awaiting the callback future; synchronous future construction, metadata appends
and the built-in memory implementation are excluded. Counters reset with the broker
process; no Kafka JMX-name/aggregation equivalence is claimed.

Regressions in
`crates/broker/src/coordinator/unified/streams/actor/description/tests.rs`:

- `native_storage_metrics_cover_callbacks_and_exclude_rejected_pushes`: actual
  actor callbacks cover all six outcomes; the production registry scrape has
  exact counter values and histogram observation counts. An invalid-member push
  adds no backend operation.
- `active_member_handoff_drains_admitted_backend_write`: block native `set`,
  queue shutdown, release the callback, verify successful durable epoch, replay
  the active-member seed on a replacement coordinator and resume its heartbeat
  without unnecessary description solicitation.
- `moved_active_group_cleanup_requires_ownership_then_membership_and_retries`:
  moved live membership protects data, non-owner sweep cannot delete, and an
  eligible owner retries a failed delete successfully.
- `moved_group_periodic_runner_retries_on_next_configured_tick`: actual
  `retention::run` with ownership from real controller metadata and virtual Tokio
  time retries failed native deletion at the next configured ten-second tick;
  the runner responds to cancellation. Backend barriers observe each attempt.

Verification:

- `CARGO_PROFILE_TEST_DEBUG=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test -p krabka-broker --lib streams`: **418 passed**, 0 failed.
- Metrics-contract test executable: **5 passed**, including exact regenerated
  snapshot equality. `cargo test -p krabka-broker --test metrics_contract` built
  the executable; its regeneration run requires an **absolute**
  `KRABKA_METRICS_BODY_OUT` path because Cargo runs tests from the broker crate.
  Direct execution also sets `CARGO_MANIFEST_DIR` to the broker crate path.
- `aspect check-metrics-contract`: passed, 177 references and 41 runbooks.
- `CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo clippy -p krabka-broker --lib --tests -- -D warnings`: passed.
- `aspect check-cpd`: passed, zero clones.
- Changed-file nightly Rustfmt and `git diff --check`: passed.

Local debug/incremental settings are execution choices, not new repository gates.

## Retained limits

The handoff is graceful and drains the old actor before replay. It does not
model simultaneously active owners, arbitrary delayed writes, RF3 partitions or
a released-broker active-member/concurrent-write wire differential. The periodic
runner is a bounded tick/retry test, not distributed scheduling equivalence or
unbounded fairness. Native backends still require host fencing and bounded,
cancellation-safe IO. No filesystem fsync/power-loss guarantee or JVM plugin
loading is added. See `docs/streams-topology-description-storage.md` for metric
semantics and the native extension boundary.

## Source identities

SHA-256 of the instrumented source and regression fixtures:

- `crates/broker/src/coordinator/unified/streams/actor.rs`: `dea68f4b6359a1de5579193a0a528ace9e37a58aecaf233da90e07eebe8324d2`
- `crates/broker/src/coordinator/unified/streams/actor/description.rs`: `b121a18b3f5623be8e6f1006cbdfaea1fa7db1c52073963693297e0dd446787f`
- `crates/broker/src/coordinator/unified/streams/actor/description/tests.rs`: `bee32c38acf820f2ef3cf5ed5a7f497260efd26bae2f27a6cd37bd2f2513aea3`
- `crates/broker/src/metrics.rs`: `57db819a731bbdbf5e7d902d2ceae839cd401bb8c78db473513e84f195fab8ee`
- `crates/broker/src/metrics/streams_storage.rs`: `b68e26f9eb7981b5b7ea1246efe60e3c03ee91c18643fa9e9882666737f7ba82`
- `crates/broker/tests/metrics_contract.rs`: `10f21e90214c3acc2b40e21dc5be03fb7a1d2ec39d33d967cfbc24f966dbefd6`
- `docs/operations/metrics-body.txt`: `d04246f926990e71b84b5c4e7a2d891bdbdeae62e00285573173f8ee499b76a5`
