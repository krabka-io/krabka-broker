# rstest-bdd 0.6.0 fixture guard lifetimes

This dependency branch carries upstream rstest-bdd source at
`72fb22635670e456545ca368805ba4c1c9d7bd69` (the published 0.6.0 revision).
The upstream ISC license and attribution are retained. Upstream GitHub workflows
are omitted from this source distribution.

The proc-macro patch releases each generated fixture borrow guard immediately
after the synchronous or asynchronous step invocation, before converting its
outcome into reporting or error values. Fixtures themselves remain alive for
the scenario. This shortens the borrow to its actual operation and passes
Clippy's significant-drop-tightening check without suppressing the lint.

The proc-macro package uses the published 0.6.0 harness, patterns and policy
support crates, so consuming this Git patch does not introduce duplicate Git
copies of runtime support crates.

Data-table metadata uses explicit array-to-slice coercions instead of range
indexing. This permits structured tables (including Background tables) inside
Scenario Outlines on stable Rust, without requiring const Index support.

Validation: all 504 upstream proc-macro unit tests pass, including the updated
wrapper snapshot. The broker's rewritten transactions and topic-freeze suites
pass Clippy with all warnings denied against this patch.
