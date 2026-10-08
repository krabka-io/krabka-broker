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

The broker release and the tested ecosystem set are separate claims. Before a
release is advertised as an ecosystem-qualified stack, the manually dispatched
`ecosystem qualification` workflow must pass on the candidate recorded in
[`qualification/ecosystem-eight-gate-baseline.json`](../qualification/ecosystem-eight-gate-baseline.json). See
[Ecosystem qualification](qualification.md). A broker release may exist without
that result; it must not be described as the tested operator/CLI/o11y stack.

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
Delivery hashes the built image manifest, checks the registry against it, and
records that digest in the `delivery-image` Actions artifact. Release
fetches that artifact from the successful `main` push run, requires the commit
tag to resolve to that exact digest, and resolves the immutable reference before
signing. Missing or expired evidence, an unavailable image, or a substituted
commit tag stops the release before signing, attestations or release tags change.
There is no rebuild fallback. The provenance records the CI run and immutable
image reference in `internalParameters.imageSource`.

The artifact is retained for 90 days. If it is unavailable, rerun main CI for
the release commit and require that complete run to succeed before rerunning the
release workflow. A locally rebuilt image or a manually replaced commit tag
cannot stand in for delivery evidence.
Commits from before delivery recorded digest artifacts require a new release
commit that includes this workflow; their old CI run cannot provide the evidence.

`aspect axl-tests --suite check-delivery-image` checks matching, missing,
substituted and malformed digests. To repeat the registry check without
publishing, download the successful run's `delivery-image` artifact and run:

```sh
aspect check-delivery-image --expected delivery-image.digest \
  --image ghcr.io/krabka-io/krabka-broker:<commit-sha>
```

The command prints only the approved digest on success. Repeating it with a
missing image reference or another image's digest must fail.

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

Also compare the release tag's digest against the delivery artifact:

```sh
aspect check-delivery-image --expected delivery-image.digest \
  --image ghcr.io/krabka-io/krabka-broker:v0.5.2
```

The GitHub release carries `sbom.cdx.json` for the same build.

## Crates.io

The same tag push also starts
[`publish.yml`](../.github/workflows/publish.yml), which publishes the
storage-layer library crates to crates.io. The two workflows are independent:
neither waits on the other, they hold separate concurrency groups, and
`publish.yml` touches neither the image nor the GitHub release. Each one checks
on its own that the tag is releasable, with the same three checks as step 4, so
a tag that `release.yml` refuses publishes nothing either.

### The published crates

| Crate | Depends on (workspace) | Why it is published |
| :--- | :--- | :--- |
| `krabka-macros` | | `krabka-log` derives with it |
| `krabka-verified` | | Library for other repositories |
| `krabka-log` | `krabka-macros`, `krabka-verified` | Library for other repositories |

Every other member sets `publish = false`, the broker among them. A new member
crate is published unless its manifest sets `publish = false`, so a crate that
is not a library for other repositories sets it.

`krabka-log` also depends on `krabka-ids`, `krabka-protocol`,
`krabka-compression` and `krabka-units`, and `krabka-verified` on `krabka-ids`.
Those come from krabka-protocol. `cargo publish` ignores `[patch.crates-io]`,
so each requirement must name a release that crates.io has, and krabka-protocol
releases before this repository does.

Each published crate sets `repository`, `description`, `readme` and an
`include` list. The list ships the library source and the README, and leaves
out tests, benches and fixtures. Cargo prints an "ignoring test" warning for
each `[[test]]` and `[[bench]]` that the package leaves out. The warnings are
expected.

`krabka-verified` holds the Creusot-proved kernels. Its contracts are
`creusot-std` attributes that a normal build erases, so the published crate
builds with stable `rustc` and needs no proof toolchain. Its `include` list
ships `src/`, `build.rs` (which registers `cfg(creusot)`) and the README. The
Why3 session artifacts under `verif/` live outside the crate and are not in the
package. The `proofs` job in `ci.yml` is what checks them.

Every `krabka-*` normal dependency of a published crate carries a `version` as
well as its `path`. A path or git dependency that is not on crates.io may
appear only as a dev-dependency, and without a `version`, so that cargo drops
it from the published manifest.

### Before you tag

Step 1's version bump covers the published crates: they inherit
`[workspace.package] version`, and `aspect check-version-pins` reads the
`version` of each path dependency.

Before you merge the release pull request, run a dry run from its branch: start
`publish.yml` from the Actions tab with `dry_run` on. It packages each crate
and builds it from the packaged sources, as crates.io users get them. Locally:

```sh
cargo publish --dry-run --locked -p krabka-macros -p krabka-verified -p krabka-log
```

### What the workflow does

The `plan` job holds no credential. It:

1. stops unless the tagged commit is an ancestor of `origin/main`.
2. stops unless a `push` run of `ci.yml` passed on that commit.
3. stops unless `aspect check-version-pins --expected <tag>` passes.
4. asks the crates.io API which crate versions exist, and keeps the others.
5. runs `cargo publish --dry-run` over the crates that it kept. Cargo resolves
   a pending sibling from the packages it has just made, and every other
   dependency from crates.io.

The `publish` job runs in the `crates-io` environment. It uploads the pending
crates one at a time, in dependency order. Cargo waits until each crate is in
the index before it uploads the next one.

A rerun is safe. The `plan` job skips each version that crates.io already
has, so a rerun after a failure uploads only the rest.

A manual run takes two inputs:

- `dry_run`, on by default. Off, the run uploads, and it must start from a
  `v*` tag.
- `crates`, a space-separated list of crate names. A run with a list publishes
  only those crates. Use it when one crate cannot publish and the others must
  not wait for it.

### Credentials: bootstrap, then trusted publishing

The `publish` job authenticates with one of two credentials:

- **A token.** When the `CARGO_REGISTRY_TOKEN` secret of the `crates-io`
  environment is set, the job uses it.
- **Trusted publishing.** When that secret is not set, the job runs
  [`rust-lang/crates-io-auth-action`](https://github.com/rust-lang/crates-io-auth-action).
  The action exchanges the job's GitHub OIDC token for a crates.io token. That
  token expires after 30 minutes, and the action revokes it when the job ends.
  No long-lived secret exists.

crates.io allows trusted publishing only for a crate that already exists. So
the first release of each crate name needs the token, and every later release
uses trusted publishing.

#### Once: the GitHub environment

In the repository settings, open **Environments** and create `crates-io`. Under
**Deployment branches and tags**, allow only tags that match `v*`. Add required
reviewers if a person should approve each publish.

#### First publish of a crate name

1. Sign in to crates.io with the account that will own the crates. Under
   **Account Settings**, open **API Tokens** and create a token with the
   `publish-new` and `publish-update` scopes. Limit it to the crate names
   `krabka-macros`, `krabka-verified` and `krabka-log`, or the pattern
   `krabka-*`, and give it a short expiry.
2. Add the token to the `crates-io` environment as the secret
   `CARGO_REGISTRY_TOKEN`.
3. Push the release tag, or start `publish.yml` on it with `dry_run` off.

crates.io limits new crate names to a burst of five, then one every ten
minutes, so the three names go up in one burst. On a `429` answer the job waits
ten minutes and tries again.

#### Then: trusted publishing for each crate

For each published crate:

1. On crates.io, open the crate, then **Settings**, then **Trusted
   Publishing**.
2. Add a GitHub publisher with these values:
   - Repository owner: `krabka-io`
   - Repository name: `krabka-broker`
   - Workflow filename: `publish.yml`
   - Environment: `crates-io`

When all three crates have a publisher, delete the `CARGO_REGISTRY_TOKEN`
secret, and revoke the token on crates.io. The next release uses trusted
publishing. Its log says "publishing through trusted publishing".

A crate that joins the published set later needs the token once, for its
first release. Add the secret again for that release, configure the new
crate's publisher, and delete the secret again.
