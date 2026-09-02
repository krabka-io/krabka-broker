#!/usr/bin/env bash
# Checks that the container image carries every binary an operator needs.
#
# The image's base has no shell, so a tool that is not in a layer cannot be run
# in a container at all -- including `krabka-format`, which has to run against
# the log directory before the broker will boot. This asserts three things about
# the built artifacts, not about //packaging/BUILD.bazel's text:
#
#   * the layers put exactly the expected binaries under /usr/bin,
#   * each of them runs and answers `--help`, which is what the by-hand check
#     `docker run --entrypoint /usr/bin/<tool> <image> --help` establishes,
#   * the image references those layers, so a layer that stopped being listed in
#     `layers` fails here rather than in a registry.
#
# Argument 1 is the image manifest JSON; the rest are the layer tarballs, all
# passed as runfiles by //packaging:image_binaries_test.
set -euo pipefail

# The contract. A binary dropped from a layer, or renamed out of the spelling
# the `krabka` operator CLI resolves on PATH, fails against this list.
expected=(
    krabka-audit
    krabka-barrier
    krabka-broker
    krabka-format
    krabka-guard
    krabka-restore
    krabka-worm-verify
)

manifest="$1"
shift
layers=("$@")

work="${TEST_TMPDIR:-$(mktemp -d)}"
rootfs="${work}/rootfs"
mkdir -p "${rootfs}"

fail() {
    echo "image_binaries_test: $*" >&2
    exit 1
}

sha256() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | cut -d' ' -f1
    else
        shasum -a 256 "$1" | cut -d' ' -f1
    fi
}

[[ -f "${manifest}" ]] || fail "image manifest ${manifest} is not a file"

for layer in "${layers[@]}"; do
    [[ -f "${layer}" ]] || fail "layer ${layer} is not a file"
    tar -xf "${layer}" -C "${rootfs}"

    # A layer the image no longer lists is a layer that does not ship. rules_img
    # stores a layer as the gzipped tarball the rule emits, so the blob digest
    # the manifest carries is that file's own.
    digest="$(sha256 "${layer}")"
    if ! grep -qF "sha256:${digest}" "${manifest}"; then
        fail "the image does not reference layer ${layer} (sha256:${digest})"
    fi
done

# `find` rather than the tar listing: this is the tree a container would see,
# with every layer applied.
shipped="$(find "${rootfs}/usr/bin" -type f -exec basename {} \; | LC_ALL=C sort | tr '\n' ' ')"
want="$(printf '%s\n' "${expected[@]}" | LC_ALL=C sort | tr '\n' ' ')"
if [[ "${shipped}" != "${want}" ]]; then
    fail "/usr/bin holds [${shipped}], expected [${want}]"
fi

for binary in "${expected[@]}"; do
    path="${rootfs}/usr/bin/${binary}"
    [[ -x "${path}" ]] || fail "${binary} is not executable in the image"
    status=0
    "${path}" --help >/dev/null 2>&1 || status=$?
    [[ "${status}" -eq 0 ]] || fail "${binary} --help exited ${status}"
done

echo "image_binaries_test: ${#expected[@]} binaries under /usr/bin, each answering --help"
