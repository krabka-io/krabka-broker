# Releasing

Krabka releases the whole workspace as one unit. A release is an annotated git
tag `vX.Y.Z` on `main`, and the tag is the name a person quotes in an incident,
in a bug report, and in a rollback. The [changelog](../CHANGELOG.md) records
what each tag contains.

The push of the tag starts
[`release.yml`](../.github/workflows/release.yml). That workflow first verifies
that the tag is releasable at all (step 4 below), then signs the image
`ci.yml`'s `delivery` job already built for that commit, attests the SBOM and
the provenance, moves the `vX.Y.Z` image tag -- and `latest`, when the tag is
the newest one -- to the signed digest, and creates the GitHub release. Nothing
else creates a release, and no other branch does.

Only a maintainer with push access to the repository can do steps 3 and 4.

## 1. Prepare the version

Set the new version in these places, then run `cargo generate-lockfile` to
update `Cargo.lock`:

- `[workspace.package] version` in the root `Cargo.toml`.
- The first-party `version` requirements in `[workspace.dependencies]`, and the
  same requirements in each member `Cargo.toml`. A path dependency also carries
  a version, and Cargo refuses a publish when it is stale.
- `version` in `MODULE.bazel` and `WORKSPACE_VERSION` in `bazel/defs.bzl`.
- Each `#![doc(html_root_url = "https://docs.rs/krabka-<name>/<version>")]`.

`aspect check-version-pins` reads every one of those places and fails on any
disagreement, so run it once the bump is made rather than grepping for the
version you replaced. It runs in the `checks` job of
[`ci.yml`](../.github/workflows/ci.yml) on every push, and again in the release
workflow with the tag as the version each pin must name.

```sh
aspect check-version-pins
aspect check-version-pins --expected v0.5.2  # what the release job runs
```

`WORKSPACE_VERSION` is the entry worth being careful about. `bazel/defs.bzl`
stamps it into the `purl` of every crate, so `aspect sbom` writes a stale one
into every component of the bill of materials the release then signs and
attests.

## 2. Prepare the changelog

Move the `[Unreleased]` entries of [`CHANGELOG.md`](../CHANGELOG.md) under a new
`## [X.Y.Z] - YYYY-MM-DD` heading, and leave `[Unreleased]` empty above it.
Write what changed for a reader who runs the broker, not a list of commit
subjects.

Then fix the link definitions at the end of the file. Add one for the new tag,
and move the `[Unreleased]` comparison onto that tag as well: it names the
previous one, so leaving it alone would keep listing everything this release
just shipped as unreleased.

```md
[Unreleased]: https://github.com/krabka-io/krabka-broker/compare/vX.Y.Z...HEAD
[X.Y.Z]: https://github.com/krabka-io/krabka-broker/releases/tag/vX.Y.Z
```

A changelog entry that quotes a performance number takes it from the latest
scheduled run of the `bench` job in
[`ci.yml`](../.github/workflows/ci.yml) -- the nightly criterion lane -- and not
from the tables in the source comments. Those tables record why a decision was
made and are not re-measured when the code around them changes, so a release
that repeats one is republishing an undated figure. The job summary of that run
holds the `bench_ratio` tables, and its `criterion-baseline` artifact holds the
samples; if the newest scheduled run is red, say no number rather than reaching
for an older one. The same applies to the `sendfile_min` default and the two
`PERF -- measured; decision: KEEP` sites: check the run before repeating them in
release notes.

Open a pull request with the version bump and the changelog entry together, and
merge it. The tag names that merge commit.

## 3. Tag the release

Take the merge commit of that pull request, and tag it. Use an annotated tag,
`git tag -a`. An annotated tag is an object of its own: it records who cut the
release and when, and a person can read that with `git show v0.5.2`. Push the
tag to `origin`, because `release.yml` calls `gh release create --verify-tag`,
which fails when the remote does not hold the tag.

```sh
git checkout main
git pull --ff-only
git tag -a v0.5.2 -m "krabka-broker v0.5.2"
git push origin v0.5.2
```

Never move or delete a tag that is pushed. The signed image digest and the
attestations point at it. Release a new patch version instead.

## 4. Check the result

Watch the `release` workflow. Its `verify` job runs before anything is built or
signed, and it refuses the release unless all three of these hold:

- The tagged commit is an ancestor of `origin/main`. A tag pushed from a
  feature branch is not a release.
- `aspect check-version-pins --expected <version>` passes, so the tree the tag
  names says the version the tag does.
- `ci.yml` has a `push` run for that commit whose conclusion is `success`.
  `gate` is the single required context on `main` and depends on every job that
  gates a merge, so a green run of it is what says the commit was tested. A
  `pull_request` run does not count: it tested the merge of the pull request,
  not this commit.

A cosign signature says who built an image, never that the image was tested.
Those three checks are what stands behind it.

The `release` job then signs the image `delivery` already pushed for that
commit -- `ghcr.io/krabka-io/krabka-broker:<commit sha>` -- rather than
building it again, so the digest that is signed is the digest that was tested.
It falls back to a rebuild only when that tag is absent, and the provenance
predicate records which of the two happened in `internalParameters.imageSource`.

It fails the release rather than publishing an unsigned or unverified image,
because it runs `cosign verify` and `cosign verify-attestation` against the
digest it signed.

`latest` moves onto the release only when the tag is the newest `v*` tag in the
repository, by `sort -V`. A patch cut after a later minor -- a `v0.5.4` tagged
after `v0.6.0` -- gets its own `vX.Y.Z` tag and leaves `latest` where it is.

When the workflow is green, confirm the release yourself:

```sh
cosign verify \
  --certificate-identity https://github.com/krabka-io/krabka-broker/.github/workflows/release.yml@refs/tags/v0.5.2 \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  ghcr.io/krabka-io/krabka-broker:v0.5.2
```

The GitHub release carries `sbom.cdx.json` for the same build.

## Crates.io

This repository publishes nothing to crates.io. The `krabka-*` names come from
[robot-head/crabka](https://github.com/robot-head/crabka), and consumers of this
repository pin it by git revision.
