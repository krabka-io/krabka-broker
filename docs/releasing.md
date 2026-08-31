# Releasing

Krabka releases the whole workspace as one unit. A release is an annotated git
tag `vX.Y.Z` on `main`, and the tag is the name a person quotes in an incident,
in a bug report, and in a rollback. The [changelog](../CHANGELOG.md) records
what each tag contains.

The push of the tag starts
[`release.yml`](../.github/workflows/release.yml). That workflow builds the
image from the Bazel graph, signs it with cosign, attests the SBOM and the
provenance, moves the `vX.Y.Z` and `latest` image tags to the signed digest, and
creates the GitHub release. Nothing else creates a release, and no other branch
does.

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

`grep -rn '<old version>'` over those files finds anything the list misses.

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

Watch the `release` workflow. It fails the release rather than publishing an
unsigned or unverified image, because it runs `cosign verify` and
`cosign verify-attestation` against the digest it signed.

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
