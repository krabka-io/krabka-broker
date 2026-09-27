#!/bin/bash
set -euo pipefail

packages=(krabka-verified)

# Creusot builds with the nightly its release pins, which trails the stable
# `rust-version` the sibling crates declare: Creusot 0.13.0's nightly-2026-06-22
# reports 1.98.0-nightly against their 1.98.1, and Cargo refuses the build
# outright. The nightly carries every feature those crates use, so skip the
# MSRV check rather than hold the workspace back to Creusot's compiler.
for package in "${packages[@]}"; do
  cargo creusot --package "${package}" -- --ignore-rust-version
done
