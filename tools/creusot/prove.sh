#!/bin/bash
set -euo pipefail

packages=(krabka-verified)

for package in "${packages[@]}"; do
  cargo creusot --package "${package}"
done
