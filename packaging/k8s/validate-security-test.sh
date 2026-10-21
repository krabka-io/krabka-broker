#!/usr/bin/env bash
set -euo pipefail

root="${TEST_SRCDIR}/${TEST_WORKSPACE}/packaging/k8s"
python3 "${root}/validate-security.py" "${root}"
