# Maintenance advisory remediation (2026-11-05)

PR #1997 was blocked by RUSTSEC-2026-0440 and RUSTSEC-2026-0441. Remove the dependencies rather than ignoring advisories. Exact upstream patches are pinned until releases are available; the Apple DNS-SD patch is carried locally with source provenance and the original MIT license.

- Testcontainers: pin upstream PR https://github.com/testcontainers/testcontainers-rs/pull/1006 at dba4383d9bdbaed68d499f6e3ba933a289ba0a4a, replacing astral-tokio-tar with maintained tar 0.4.46. Synchronous tar work stays on blocking threads. Custom modes, nested paths, byte payloads and streaming copies are exercised by upstream tests.
- Loom: pin upstream acaa24a94cdd7c182e87d1ca05ffcab830cceb60, replacing pin-utils with standard pinning.
- async-dnssd: patch 0.5.1 locally to use checked pin-project projections; see vendor/async-dnssd/KRABKA-PATCH.md.

## Verified

- `cargo deny check`: advisories, bans, licenses and sources pass. Existing advisory ignores are unchanged; only the two exact upstream source repositories are added to the source allowlist.
- All-target lockfile no longer contains astral-tokio-tar or pin-utils.
- `cargo check --locked -p krabka-log --tests`: passes with the patched Testcontainers consumer.
- `cargo check --manifest-path vendor/async-dnssd/Cargo.toml --target aarch64-apple-darwin`: passes, with an existing deprecated libc alias warning.
- Exact pinned Testcontainers source copied to a private directory: `cargo test -p testcontainers --lib core::copy` passes 7/7; `cargo test -p testcontainers --test async_runner copy` passes 6/6 against Docker, covering permissions, payloads, copies to files/vectors and directory rejection.
- Exact pinned Loom source: `cargo test --features futures` passes all executed tests, including both future tests without exploration bounds. Additional `LOOM_MAX_PREEMPTIONS=2 ... --test futures` passes 2/2.

## Limits

The full upstream Testcontainers library suite is 71 passed / 2 failed. Image-platform pull fails JSON decoding with missing `Type`; the released 0.28.0 baseline reproduces that exact error. The host-network readiness test also fails on the candidate; its released-source baseline fails earlier because the registry package omits test assets, so equivalence is not established. This is not an all-green upstream suite. No changed copy test failed.

Apple verification is cross-compilation, not native Bonjour runtime discovery. The Linux host lacks the Avahi DNS-SD development library. This patch fixes this workspace graph, not independently published sibling crates or already released binaries. No production deployment, release publication or merge is included. Replace pins/local patch only after equivalent maintained releases pass these checks.

Raw local verification logs: `/home/matt/.openclaw/workspace/coordinator/.private/security-migration/` (deny-all.log, dnssd-check-final.log, archive-tests.log, container-copy.log, consumer-check.log, loom-tests.log, testcontainers-tests.log, baseline-platform.log, baseline-network.log).
