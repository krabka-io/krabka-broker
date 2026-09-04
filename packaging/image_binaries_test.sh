#!/usr/bin/env bash
# Checks that the container image carries every binary an operator needs.
#
# The image's base has no shell, so a tool that is not in a layer cannot be run
# in a container at all -- including `krabka-format`, which has to run against
# the log directory before the broker will boot. This asserts four things about
# the built artifacts, not about //packaging/BUILD.bazel's text:
#
#   * the layers put exactly the expected binaries under /usr/bin,
#   * each of them runs and answers `--help`, which is what the by-hand check
#     `docker run --entrypoint /usr/bin/<tool> <image> --help` establishes,
#   * the image references those layers, so a layer that stopped being listed in
#     `layers` fails here rather than in a registry,
#   * every binary is an ELF for the architecture the manifest declares. The
#     layers carry whatever the host toolchain built and the manifest's
#     architecture is a constant, so on a non-amd64 host the two disagree and
#     the image cannot exec at all. `//packaging:image` is restricted to x86_64
#     for that reason; this is the assertion behind the restriction, and it
#     needs no daemon to make it.
#
# Argument 1 is the architecture the image manifest declares (IMAGE_ARCH in
# //packaging/BUILD.bazel); argument 2 is the image manifest JSON; the rest are
# the layer tarballs, all passed as runfiles by //packaging:image_binaries_test.
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

arch="$1"
manifest="$2"
shift 2
layers=("$@")

# `e_machine`, the two bytes at offset 0x12 of an ELF header, little-endian for
# the LSB ELFs everything here builds. `readelf` is not in the Bazel test
# sandbox; `od` is. EM_X86_64 is 62 (0x003e), EM_AARCH64 is 183 (0x00b7).
want_machine=""
case "${arch}" in
    amd64) want_machine="3e00" ;;
    arm64) want_machine="b700" ;;
    *) ;;
esac

work="${TEST_TMPDIR:-$(mktemp -d)}"
rootfs="${work}/rootfs"
mkdir -p "${rootfs}"

fail() {
    echo "image_binaries_test: $*" >&2
    exit 1
}

# The name of whatever `e_machine` value a file actually carries, so a mismatch
# reports an architecture rather than a hex pair.
machine_name() {
    case "$1" in
        3e00) echo "amd64" ;;
        b700) echo "arm64" ;;
        0300) echo "i386" ;;
        2800) echo "arm" ;;
        f300) echo "riscv64" ;;
        "") echo "not an ELF file" ;;
        *) echo "unknown e_machine 0x$1" ;;
    esac
}

# The two bytes at 0x12, hex, empty for a file with no ELF magic.
elf_machine() {
    if [[ "$(od -An -tx1 -N 4 "$1" | tr -d ' \n')" != "7f454c46" ]]; then
        return 0
    fi
    od -An -tx1 -j 18 -N 2 "$1" | tr -d ' \n'
}

[[ -n "${want_machine}" ]] || fail "no ELF e_machine known for architecture ${arch}"

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
    found_machine="$(elf_machine "${path}")"
    if [[ "${found_machine}" != "${want_machine}" ]]; then
        fail "/usr/bin/${binary} is $(machine_name "${found_machine}"), but the image manifest declares ${arch}"
    fi
    status=0
    "${path}" --help >/dev/null 2>&1 || status=$?
    [[ "${status}" -eq 0 ]] || fail "${binary} --help exited ${status}"
done

echo "image_binaries_test: ${#expected[@]} ${arch} binaries under /usr/bin, each answering --help"
