#!/bin/bash
set -euo pipefail

python3 tools/test_creusot_packages.py
packages=(krabka-verified)
python3 tools/creusot_packages.py "${packages[@]}"

for package in "${packages[@]}"; do
  cargo creusot --package "${package}"
done
