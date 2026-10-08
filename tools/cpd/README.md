# Rust duplicate-code gate

`aspect check-cpd --base origin/main` runs native PMD 7.28.0 Rust CPD with
`minimumTokens = 40`. CI supplies the immutable PR target SHA or the previous
main SHA and requires the `cpd` job through the merge gate. Manual workflow
runs compare with `HEAD^`. Unresolvable bases fail.

The Java helper uses PMD's token APIs, which AXL cannot call directly. AXL owns
source snapshots, verified downloads, compilation, regression tests, and execution.
All tracked and nonignored untracked Rust files are included, including hidden
directories. The comparison uses exact lexer token images, independently of file
names, line numbers, comments, and whitespace. A removed repeat cannot compensate
for a different new repeat. Extra copies of an existing repeat fail too.

PMD's XML prunes some overlapping or nested matches. The regression check
therefore also indexes every exact 40-token window, extends matching occurrence
pairs, and counts every repeated prefix with at least 40 tokens in both source
trees. Counts use nonoverlapping copies within each file and reset at file
boundaries. This catches longer new repeats even when their 40-token windows
already existed, and copies hidden by PMD's report pruning. Work and memory
budgets fail explicitly if the exact comparison cannot complete; they never
truncate or accept a partial scan.

The allowance is computed from the target revision’s source. Each target update
therefore establishes the allowance for the next change.

## Lexer correction

The pinned PMD Rust grammar permits backslashes as unescaped string content.
That can consume a closing quote and cause valid files to be skipped. The task
changes only the two string/byte-string character classes to exclude backslashes,
generates the replacement Rust lexer with ANTLR 4.13.2, and puts it first on the
classpath. Matching, token filtering, and reporting remain PMD's implementation.
SHA-256 checks pin the distribution, ANTLR jar, and original grammar.

Lexical errors, missing sources, Rust symlinks, and comment directives that
suppress CPD are failures. Reports and regression diagnostics are retained in
`.cpd/` locally or the `cpd` CI artifact. The Java fixture suite runs before each
comparison; orchestration tests run with `aspect axl-tests --suite check-cpd`.

Local runs require Git, JDK 17+, curl, unzip, and tar. Optional `--pmd-archive`,
`--antlr-jar`, and `--rust-grammar` reuse local downloads only after checking the
same pinned digests. `--heap` defaults to `5g`; the dedicated CI job avoids
competing with Rust builds for memory.
