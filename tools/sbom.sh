#!/usr/bin/env bash
set -euo pipefail

output="${1:-sbom.cdx.json}"
work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# Resolve the configured image closure, then use cargo-deny's package metadata
# for the Cargo packages that actually occur in that Bazel graph.
bazel cquery 'kind(alias, deps(//packaging:image))' --output=label 2>/dev/null |
  sed -E 's|^@crates//:||; s/ \([0-9a-f]+\)$//' |
  awk '/-[0-9]+\.[0-9]+/' | sort -u >"${work}/bazel"
cargo deny \
  --manifest-path crates/broker/Cargo.toml \
  --target x86_64-unknown-linux-gnu \
  --exclude-dev \
  list --format tsv |
  tail -n +2 | cut -f1 | sort -u >"${work}/deny"
sed 's/@/-/' "${work}/deny" | sort -u >"${work}/deny-bazel"
comm -12 "${work}/bazel" "${work}/deny-bazel" >"${work}/image-bazel"
while IFS= read -r package; do
  alias="${package/@/-}"
  grep -Fqx "${alias}" "${work}/image-bazel" && printf '%s\n' "${package}"
done <"${work}/deny" >"${work}/image"
test -s "${work}/image"

jq -Rn '
  [inputs
   | select(length > 0)
   | capture("^(?<name>.+)@(?<version>[0-9].*)$")
   | {type: "library", name, version,
      "bom-ref": ("pkg:cargo/" + .name + "@" + .version),
      purl: ("pkg:cargo/" + .name + "@" + .version)}]
' <"${work}/image" >"${work}/components"

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

# Keep the signed component list mechanically identical to cargo-deny's view
# of the configured Bazel image closure.
jq -r '.components[] | "\(.name)@\(.version)"' "${output}" |
  sort -u >"${work}/sbom"
diff -u "${work}/image" "${work}/sbom"
