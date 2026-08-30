#!/usr/bin/env bash
set -euo pipefail

output="${1:-sbom.cdx.json}"
work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# Resolve package URLs from the configured image graph. rules_rs supplies these
# for third-party crates; //bazel:defs.bzl supplies them for workspace crates.
bazel cquery 'kind("_package_metadata rule", deps(//packaging:image))' \
  --output=build 2>/dev/null |
  sed -n 's/^  purl = "\([^"]*\)",$/\1/p' | sort -u >"${work}/purls"
sed -n 's|^pkg:cargo/\([^?]*\).*|\1|p' "${work}/purls" |
  sort -u >"${work}/bazel"
cargo deny \
  --manifest-path crates/broker/Cargo.toml \
  --exclude-dev \
  list --format tsv |
  tail -n +2 | cut -f1 | sort -u >"${work}/deny"

# cargo-deny's unfiltered graph is a superset: it also contains build-only and
# non-Linux dependencies. Every Cargo component Bazel will ship must occur in
# it with the exact version.
comm -12 "${work}/bazel" "${work}/deny" >"${work}/deny-image"
diff -u "${work}/bazel" "${work}/deny-image"
test -s "${work}/bazel"

jq -Rn '
  [inputs
   | select(startswith("pkg:cargo/") or startswith("pkg:docker/"))
   | . as $purl
   | capture("^pkg:(?<kind>[^/]+)/(?<name>[^@]+)@(?<version>[^?]+)")
   | {type: (if .kind == "docker" then "container" else "library" end),
      name, version, "bom-ref": $purl, purl: $purl}]
' <"${work}/purls" >"${work}/components"

jq -n \
  --arg serial "urn:uuid:$(cat /proc/sys/kernel/random/uuid)" \
  --arg version "${GITHUB_REF_NAME:-${GITHUB_SHA:-local}}" \
  --slurpfile components "${work}/components" '
  {
    bomFormat: "CycloneDX",
    specVersion: "1.6",
    serialNumber: $serial,
    version: 1,
    metadata: {
      component: {
        type: "container",
        name: "ghcr.io/krabka-io/krabka-broker",
        version: $version
      }
    },
    components: $components[0]
  }
' >"${output}"

# Keep the signed Cargo component list mechanically identical to the validated
# Bazel image closure. Non-Cargo components, such as the base image, stay in the
# SBOM but are outside cargo-deny's scope.
jq -r '.components[] | select(.purl | startswith("pkg:cargo/")) |
  "\(.name)@\(.version)"' "${output}" |
  sort -u >"${work}/sbom"
diff -u "${work}/bazel" "${work}/sbom"
