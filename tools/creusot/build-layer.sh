#!/usr/bin/env bash
set -euo pipefail

builder_tar="$1"
output_tar="$2"
builder_image="docker.io/krabka-io/creusot-builder:0.13.0"

# The container writes through a bind mount, so the directory has to exist on
# the host first. Docker would otherwise create it itself, as root, and a
# `realpath` of a path that does not exist fails outright under `set -e`.
mkdir -p "$(dirname "${output_tar}")"
output_dir="$(realpath "$(dirname "${output_tar}")")"
output_name="$(basename "${output_tar}")"

docker load --input "${builder_tar}"
docker run --rm \
  --mount "type=bind,source=${output_dir},target=/out" \
  "${builder_image}" "/out/${output_name}"

# Bazel reports a genrule whose command exits 0 without writing its declared
# output as "declared output ... was not created by genrule", which names
# neither the container nor what it did. Fail here instead, while the mount is
# still describable.
if [ ! -s "${output_tar}" ]; then
  echo "build-layer.sh: ${builder_image} exited 0 but wrote no ${output_name}" >&2
  echo "build-layer.sh: expected it at ${output_tar}" >&2
  echo "build-layer.sh: bind mount source was ${output_dir}, contents:" >&2
  ls -la "${output_dir}" >&2 || true
  exit 1
fi
