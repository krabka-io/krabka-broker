#!/usr/bin/env bash
set -euo pipefail

builder_tar="$1"
output_tar="$2"
builder_image="docker.io/krabka-io/creusot-builder:0.13.0"
output_name="$(basename "${output_tar}")"

docker load --input "${builder_tar}"
container_id="$(docker create "${builder_image}" "/tmp/${output_name}")"
trap 'docker rm --force "${container_id}" >/dev/null 2>&1 || true' EXIT
docker start --attach "${container_id}"
docker cp "${container_id}:/tmp/${output_name}" "${output_tar}"
test -s "${output_tar}"
