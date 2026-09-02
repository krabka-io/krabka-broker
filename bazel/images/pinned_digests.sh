#!/usr/bin/env bash
# Print the container-image digests //MODULE.bazel pins, one `NAME=VALUE` line
# per image, in the form rules_rust's `rustc_env_files` reads.
#
# The pins live in MODULE.bazel because that is where `pull` needs them, and a
# MODULE.bazel cannot `load()`, so nothing else can share the literal. Anything
# that must agree with a pin therefore has to be handed the pin by the build.
# `crates/broker/build.rs` does the same job for Cargo; see the header there.
set -euo pipefail

readonly module_file="${1:?usage: pinned_digests.sh MODULE.bazel}"

sed -nE 's/^ *\("([a-z0-9_]+)", "[^"]*", "[^"]*", "[^"]*", "(sha256:[0-9a-f]{64})"\),?$/\1 \2/p' \
	"${module_file}" |
	awk '
		{ printf "KRABKA_PINNED_IMAGE_%s=%s\n", toupper($1), $2; found++ }
		END {
			if (!found) {
				print "no image pins found in '"${module_file}"'" > "/dev/stderr"
				exit 1
			}
		}
	'
