#!/usr/bin/env bash
#
# Print the tag under which this checkout's proof image is published.
#
# The tag digests everything that decides what the image contains, so a tree
# that has not touched the image resolves to a tag the registry already holds
# and CI pulls it instead of spending eleven minutes installing Creusot, OCaml,
# Why3 and the provers again.
#
# Those inputs are this directory -- the apko lock, the installer, the layer
# build, the proof entrypoint and the package BUILD -- the pinned Creusot
# release, and the MODULE.bazel lines naming apko, rules_apko, rules_img and
# the lock this package translates. Everything else in the workspace is bind
# mounted when the image runs and is not baked into it.
#
# Over-approximating is free: an unrelated edit to one of those MODULE.bazel
# lines costs a single rebuild. Under-approximating is not, because it would
# run proofs against an image that no longer matches the tree, so the scrape
# and the digest both fail loudly rather than quietly hashing less.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
readonly root
cd "${root}"

pins="$(grep -E 'rules_img|rules_apko|apko_version|creusot' MODULE.bazel | grep -vE '^[[:space:]]*#')"
readonly pins
for required in rules_img rules_apko apko_version creusot_base_lock; do
  if ! grep -q "${required}" <<<"${pins}"; then
    echo "MODULE.bazel no longer names ${required} where this script looks" >&2
    exit 1
  fi
done

# Not `readonly digest=$(...)`: the exit status of an assignment that declares
# is the declaration's, which would hide a file this script can no longer read
# and hash the rest into a tag that claims to cover it.
digest="$(
  {
    printf '%s\n' "${pins}"
    sha256sum \
      .creusot-version \
      tools/creusot/BUILD.bazel \
      tools/creusot/build-layer.sh \
      tools/creusot/image.apko.lock.json \
      tools/creusot/image.apko.yaml \
      tools/creusot/install-creusot.sh \
      tools/creusot/prove.sh
  } | sha256sum | cut -c1-16
)"
readonly digest

printf '%s-%s\n' "$(cat .creusot-version)" "${digest}"
