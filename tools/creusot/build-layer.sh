#!/usr/bin/env bash
set -euo pipefail

builder_tar="$1"
output_tar="$2"
builder_image="docker.io/krabka-io/creusot-builder:0.13.0"
output_dir="$(realpath "$(dirname "${output_tar}")")"
output_name="$(basename "${output_tar}")"

docker load --input "${builder_tar}"
docker run --rm \
  --mount "type=bind,source=${output_dir},target=/out" \
  "${builder_image}" "/out/${output_name}"
