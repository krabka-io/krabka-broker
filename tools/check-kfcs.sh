#!/usr/bin/env bash
# Check every KFC under docs/KFCs against the template its README mandates.
#
# The check asserts two things for each `KFC-*.md`:
#
# 1. Its level-two headings are exactly the seven that the README lists under
#    "Required Sections", in the README's order.
# 2. The status value its Status section states matches the status the README
#    index row carries for that file.
#
# It also asserts that every KFC file has an index row and that every index
# row names a file that exists.
#
# Usage: check-kfcs.sh [KFC_DIR]
#
# KFC_DIR defaults to docs/KFCs next to this script's repository root. The
# self-test passes a temporary copy.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
kfc_dir="${1:-${script_dir}/../docs/KFCs}"
readme="${kfc_dir}/README.md"

if [[ ! -f "${readme}" ]]; then
  echo "check-kfcs: ${readme} not found" >&2
  exit 2
fi

# The seven headings, in the order the README's "Required Sections" table
# lists them. Read from the README so the two cannot drift apart.
mapfile -t expected < <(
  awk '
    /^## Required Sections/ { in_section = 1; next }
    in_section && /^## /    { exit }
    in_section && /^\| / {
      line = $0
      sub(/^\| */, "", line)
      sub(/ *\|.*$/, "", line)
      if (line != "Section" && line !~ /^:?-+:?$/) print line
    }
  ' "${readme}"
)

if [[ "${#expected[@]}" -ne 7 ]]; then
  echo "check-kfcs: expected seven required sections in ${readme}, found ${#expected[@]}" >&2
  exit 2
fi

# Index rows: "<file>\t<status>", one per row of the README index table.
declare -A index_status
while IFS=$'\t' read -r file status; do
  [[ -n "${file}" ]] || continue
  index_status["${file}"]="${status}"
done < <(
  awk '
    /^## Index/        { in_index = 1; next }
    in_index && /^## / { exit }
    in_index && /^\| \[KFC-[0-9]+\]\(/ {
      match($0, /\(([^)]+)\)/)
      file = substr($0, RSTART + 1, RLENGTH - 2)
      n = split($0, cells, "|")
      status = cells[n - 1]
      gsub(/^ +| +$/, "", status)
      printf "%s\t%s\n", file, status
    }
  ' "${readme}"
)

failures=0
fail() {
  echo "check-kfcs: $*" >&2
  failures=$((failures + 1))
}

shopt -s nullglob
kfcs=("${kfc_dir}"/KFC-*.md)
shopt -u nullglob

if [[ "${#kfcs[@]}" -eq 0 ]]; then
  fail "no KFC-*.md files under ${kfc_dir}"
fi

for path in "${kfcs[@]}"; do
  name="$(basename "${path}")"

  # Level-two headings, in file order. Fenced code is skipped so a `## ` line
  # inside an example cannot count as a heading.
  mapfile -t actual < <(
    awk '
      /^```/ { in_fence = !in_fence; next }
      !in_fence && /^## / { sub(/^## /, ""); print }
    ' "${path}"
  )

  if [[ "${actual[*]}" != "${expected[*]}" ]]; then
    fail "${name}: level-two headings are [$(IFS='|'; echo "${actual[*]}")], the README requires [$(IFS='|'; echo "${expected[*]}")] in that order"
  fi

  # The Status section opens with the value in bold, "**Adopted.**".
  status="$(
    awk '
      /^## Status/ { in_status = 1; next }
      in_status && /^## / { exit }
      in_status && match($0, /^\*\*[^*]+\*\*/) {
        value = substr($0, RSTART + 2, RLENGTH - 4)
        sub(/\.$/, "", value)
        print value
        exit
      }
    ' "${path}"
  )"

  if [[ -z "${status}" ]]; then
    fail "${name}: the Status section does not open with a bold status value such as **Adopted.**"
  elif [[ -z "${index_status[${name}]+set}" ]]; then
    fail "${name}: no row in the README index names it"
  elif [[ "${status}" != "${index_status[${name}]}" ]]; then
    fail "${name}: Status section says '${status}', the README index says '${index_status[${name}]}'"
  fi
done

for file in "${!index_status[@]}"; do
  if [[ ! -f "${kfc_dir}/${file}" ]]; then
    fail "README index names ${file}, which does not exist"
  fi
done

if [[ "${failures}" -ne 0 ]]; then
  echo "check-kfcs: ${failures} failure(s)" >&2
  exit 1
fi

echo "check-kfcs: ${#kfcs[@]} KFCs match the README template"
