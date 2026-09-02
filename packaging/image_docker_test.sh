#!/usr/bin/env bash
# The same check as //packaging:image_binaries_test, but through a real daemon:
# load the image and run every binary the layers carry, inside a container.
#
# `image_binaries_test` reads the layers the build produced, which is the check
# worth running on every commit. This one runs the tools under the base image's
# own loader and libc, so it catches what that cannot -- a binary linked against
# something the apko base does not carry. It needs a Docker daemon and materializes
# a several-hundred-megabyte tarball, so it is tagged `docker` and left out of
# the default run:
#
#     bazel test --config=docker //packaging:image_docker_test
#
# Argument 1 is the `image_load` runner, argument 2 the tag it loads under, and
# the rest are the layer tarballs, all passed by //packaging:image_docker_test.
set -euo pipefail

loader="$1"
tag="$2"
shift 2

if ! docker info >/dev/null 2>&1; then
    echo "image_docker_test: no reachable Docker daemon." >&2
    echo "  Start Docker, or run //packaging:image_binaries_test, which asserts" >&2
    echo "  the same layer contents without one." >&2
    exit 1
fi

# Whatever the layers carry, rather than a list kept in step by hand: a binary
# added to a layer is a binary this runs.
binaries=()
for layer in "$@"; do
    while IFS= read -r entry; do
        binaries+=("${entry#usr/bin/}")
    done < <(tar -tf "${layer}" | grep '^usr/bin/.' | LC_ALL=C sort)
done

if [[ "${#binaries[@]}" -eq 0 ]]; then
    echo "image_docker_test: the layers carry nothing under /usr/bin" >&2
    exit 1
fi

# The loader resolves its own runfiles, which are merged into this test's.
"${loader}"

for binary in "${binaries[@]}"; do
    if ! docker run --rm --entrypoint "/usr/bin/${binary}" "${tag}" --help >/dev/null; then
        echo "image_docker_test: ${binary} --help failed in ${tag}" >&2
        exit 1
    fi
done

# The image's own entrypoint, with no `--entrypoint` override: what a bare
# `docker run` starts.
if ! docker run --rm "${tag}" --help >/dev/null; then
    echo "image_docker_test: the image's entrypoint did not answer --help" >&2
    exit 1
fi

echo "image_docker_test: ${#binaries[@]} binaries ran in ${tag}: ${binaries[*]}"
