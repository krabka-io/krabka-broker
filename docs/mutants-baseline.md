# Mutation Sweep Baseline

The mutation sweeps are a
[nightly gate](../.github/workflows/mutants.yml), not an on-demand check. The
`mutants` workflow runs every Sunday at 07:00 UTC over the crates listed below,
splitting each crate's sweep across the shard width its `BUILD.bazel` declares.
A shard fails when a mutant it built and ran survived every test, because
[`.cargo/mutants.toml`](../.cargo/mutants.toml) admits no survivor baseline:
the excluded and equivalent mutants are named there individually, with a line
saying why, and everything else has to die.

The table records, per crate, the last scheduled run in which every shard of
that crate passed. It is the answer to "is this crate's sweep currently green,
and when was it last known to be". A crate whose row is old is a crate whose
mutation coverage nobody has confirmed since that date.

<!-- BEGIN last-green -->
| Crate | Last green sweep | Run |
| --- | --- | --- |
| `audit` | never | -- |
| `authz` | never | -- |
| `broker` | never | -- |
| `kraft-core` | never | -- |
| `log` | never | -- |
| `raft` | never | -- |
| `throttle` | never | -- |
| `verified` | never | -- |
<!-- END last-green -->

## How the table is maintained

The workflow's `report` job rewrites the rows between the two comment markers
above and commits the result to the default branch. Only the crates that were
green in that run get a new date; every other row is carried across unchanged,
so the file says when each crate last passed rather than only what the last run
did. The crate list follows the workflow's schedule matrix, so adding a crate
there adds a row here on the next run.

The commit is best effort. The run's `GITHUB_TOKEN` can push only where branch
protection allows it, and a rejected push does not fail the sweep -- a green
sweep that could not write a file is still a green sweep. The job therefore
also writes the same table to its step summary on every scheduled run, and that
summary is the record whenever the commit did not land. If the dates below are
stale while the workflow is green, the push is what is failing: the run's
summary carries a `could not push` warning, and the table in it is current.

## Reading a red sweep

Every shard uploads its log as `mutants-<crate>-<shard>`, holding `sweep.log`
-- the whole shard, including the `N mutants: C caught, M missed, U unviable`
line -- and `missed.txt`, the `MISSED` lines alone. The artifact is what makes
a survivor readable without rerunning a sweep that costs an hour of a runner.
A survivor is either a test gap to close or an equivalent mutant, and an
equivalent mutant goes in `.cargo/mutants.toml` with a line saying why it
cannot be killed.

A red scheduled run comments on the open `nightly-red` issue, or opens one, the
same way `ci.yml` reports its own scheduled lanes; the next all-green scheduled
run closes it. [`CONTRIBUTING.md`](../CONTRIBUTING.md) describes running one
crate's sweep locally.
