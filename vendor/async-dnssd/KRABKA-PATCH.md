# async-dnssd maintenance patch

Source: crates.io async-dnssd 0.5.1, upstream commit `42ce6ab34b7aef31f9201549337120b953ebb9ba`, registry SHA-256 `3d49ffe175ab45bbfd74b548313d9d7cdfff27161a94b007b52eeeb5f9aaa15e`.

Keep the upstream MIT license. The only functional changes replace pin-utils with pin-project 1.1: annotate the seven structurally pinned structs and fields, and retain their private accessor methods using generated checked projections. RegisterRecord.record remains unpinned. No new handwritten unsafe projections, DNS protocol changes, or build-script bypasses are introduced. Upstream formatting is retained where possible. Registry packaging markers, CI configuration and devbox files are omitted.

This local patch is needed because Apple DNS discovery enters through krabka-sspi and still depends on the unmaintained pin-utils crate (RUSTSEC-2026-0441). Remove the patch when upstream publishes an equivalent migration, then rerun all-target dependency/advisory and Apple compilation checks. This workspace patch does not fix independently published sibling crates; downstream source builds outside this workspace need their own remediation.

Verification: `cargo check --manifest-path vendor/async-dnssd/Cargo.toml --target aarch64-apple-darwin` succeeds. This is cross-compilation, not a native Apple DNS discovery runtime test. Linux runtime testing requires the Avahi DNS-SD compatibility library, which this test host does not have.
