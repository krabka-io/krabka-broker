#!/usr/bin/env bash
set -euo pipefail

metadata="$(mktemp)"
trap 'rm -f "${metadata}"' EXIT
cargo metadata --locked --format-version 1 >"${metadata}"

python3 - "${metadata}" <<'PY'
import json
import sys

with open(sys.argv[1], encoding="utf-8") as stream:
    metadata = json.load(stream)

packages = {package["id"]: package for package in metadata["packages"]}
nodes = {node["id"]: node for node in metadata["resolve"]["nodes"]}
expected = {
    "rsa": "0.10.0-rc.18",
    "krabka-sspi": "0.23.0",
    "picky": "7.0.0-rc.26",
}
found = {}
for package_id, package in packages.items():
    if package["name"] in expected:
        if package["name"] in found:
            raise SystemExit(f"multiple {package['name']} packages require a reachability review")
        if package["version"] != expected[package["name"]]:
            raise SystemExit(
                f"{package['name']} changed from reviewed {expected[package['name']]} "
                f"to {package['version']}; review RUSTSEC-2023-0071 reachability"
            )
        found[package["name"]] = package_id

missing = expected.keys() - found.keys()
if missing:
    raise SystemExit(
        "review and remove the RUSTSEC-2023-0071 exception; absent packages: "
        + ", ".join(sorted(missing))
    )

reverse = {package_id: set() for package_id in packages}
for parent_id, node in nodes.items():
    for dependency in node["deps"]:
        if any(kind["kind"] in (None, "normal") for kind in dependency["dep_kinds"]):
            reverse[dependency["pkg"]].add(packages[parent_id]["name"])

rsa_parents = reverse[found["rsa"]]
if rsa_parents != {"krabka-sspi", "picky"}:
    raise SystemExit(f"rsa parent set changed: {sorted(rsa_parents)}")
if reverse[found["picky"]] != {"krabka-sspi"}:
    raise SystemExit(f"picky parent set changed: {sorted(reverse[found['picky']])}")
if reverse[found["krabka-sspi"]] != {"krabka-security"}:
    raise SystemExit(f"krabka-sspi parent set changed: {sorted(reverse[found['krabka-sspi']])}")
PY
