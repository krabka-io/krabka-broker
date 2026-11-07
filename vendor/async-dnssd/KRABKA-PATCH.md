# async-dnssd maintenance patch

Source: crates.io async-dnssd 0.5.1, upstream commit `42ce6ab34b7aef31f9201549337120b953ebb9ba`, registry SHA-256 `3d49ffe175ab45bbfd74b548313d9d7cdfff27161a94b007b52eeeb5f9aaa15e`.

Keep the upstream MIT license. The pinning migration replaces pin-utils with pin-project 1.1: annotate the seven structurally pinned structs and fields and use generated checked projections. Four identical public stream wrappers are emitted by one private macro; other private accessor methods are retained. RegisterRecord.record remains unpinned. No new handwritten unsafe projections, DNS protocol changes, or build-script bypasses are introduced. Upstream formatting is retained where possible. Registry packaging markers, CI configuration and devbox files are omitted.

This local patch is needed because Apple DNS discovery enters through krabka-sspi and still depends on the unmaintained pin-utils crate (RUSTSEC-2026-0441). Remove the patch when upstream publishes an equivalent migration, then rerun all-target dependency/advisory and Apple compilation checks. This workspace patch does not fix independently published sibling crates; downstream source builds outside this workspace need their own remediation.

Verification: `cargo check --manifest-path vendor/async-dnssd/Cargo.toml --target aarch64-apple-darwin` succeeds. This is cross-compilation, not a native Apple DNS discovery runtime test. Linux runtime testing requires the Avahi DNS-SD compatibility library, which this test host does not have.

## Duplicate-source refactor (2026-11-07)

The repository's unchanged zero-clone gate also applies to this vendored source.
Shared helpers now own callback error decoding and boxed-context addressing,
callback dispatch, checked stream forwarding, DNS record length validation and
locked record creation, address result assembly, and length-prefixed TXT entry
advancement. One-shot sender consumption still precedes decoding; repeated
stream callbacks retain their sender. Service locks still cover the complete
FFI record creation and are released before constructing the owning record.
Public types, callback signatures, wire data and malformed-TXT behavior are
unchanged. No detector exclusions or suppressions were added.

Verification includes the full `aspect check-cpd` source inventory and Apple
cross-compilation. A private Linux harness compiles the exact callback and TXT
source modules and passes the upstream TXT mutation test plus callback success,
ordered stream/error delivery, decoder short-circuiting, and post-completion
polling checks. Its service is a test double; it does not exercise Bonjour/Avahi
or prove native FFI runtime behavior.
