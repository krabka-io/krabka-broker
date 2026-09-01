#!/usr/bin/env bash
#
# Print the tag under which this checkout's proof image is published.
#
# The tag digests everything that decides what the image contains, so a tree
# that has not touched the image resolves to a tag the registry already holds
# and CI pulls it instead of spending eleven minutes installing Creusot, OCaml,
# Why3 and the provers again.
#
# Those inputs are every file in this directory, the pinned Creusot release,
# the apko lock the `creusot_base_lock` mapping names, and the MODULE.bazel
# lines naming apko, rules_apko, rules_img and that mapping. Everything else in
# the workspace is bind mounted when the image runs and is not baked into it.
#
# Nothing here names a hashed file that a later edit could repoint elsewhere:
# the directory is globbed rather than listed, and the lock is read out of the
# mapping that selects it rather than assumed. Hashing a path the build no
# longer uses is how a digest silently stops covering an input.
#
# Over-approximating is free: an unrelated edit to one of those MODULE.bazel
# lines, or to this script, costs a single rebuild. Under-approximating is not,
# because it would run proofs against an image that no longer matches the tree,
# so every step below fails loudly rather than quietly hashing less.
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

# The lock the base image is actually built from, read out of the mapping that
# selects it. A repoint moves the hash to the new file rather than leaving it
# on a file the build stopped reading.
lock_label="$(
  awk '
    /^apk\.translate_lock\($/ { block = ""; in_block = 1; next }
    in_block && /^\)$/ { if (block ~ /name = "creusot_base_lock"/) print block; in_block = 0; next }
    in_block { block = block $0 "\n" }
  ' MODULE.bazel | sed -n 's/^[[:space:]]*lock = "\([^"]*\)".*/\1/p'
)"
readonly lock_label
if [[ ! "${lock_label}" =~ ^//.+:.+$ ]]; then
  echo "cannot read creusot_base_lock's lock label out of MODULE.bazel" >&2
  exit 1
fi
lock_path="${lock_label#//}"
lock_path="${lock_path/://}"
lock_path="${lock_path//\/\//\/}"
readonly lock_path
if [[ ! -f "${lock_path}" ]]; then
  echo "creusot_base_lock names ${lock_label}, which is not a file here" >&2
  exit 1
fi

mapfile -t inputs < <(
  {
    printf '%s\n' .creusot-version "${lock_path}"
    find tools/creusot -type f
  } | LC_ALL=C sort -u
)
readonly inputs
# A `find` that returned nothing would hash an empty set into a stable tag.
if ! printf '%s\n' "${inputs[@]}" | grep -qx 'tools/creusot/BUILD.bazel'; then
  echo "the input sweep did not reach tools/creusot" >&2
  exit 1
fi

# Not `readonly digest=$(...)`: the exit status of an assignment that declares
# is the declaration's, which would hide a file this script can no longer read
# and hash the rest into a tag that claims to cover it.
digest="$(
  {
    printf '%s\n' "${pins}"
    sha256sum "${inputs[@]}"
  } | sha256sum | cut -c1-16
)"
readonly digest

printf '%s-%s\n' "$(cat .creusot-version)" "${digest}"
