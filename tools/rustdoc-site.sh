#!/usr/bin/env bash
# Assemble the rendered rustdoc set into one static site with a landing page.
#
# `bazel build $(bazel query 'kind("rust_doc rule", //crates/...)')` writes one
# `<lib>_doc.rustdoc` directory per crate under `bazel-bin/crates/<pkg>/`. This
# copies each of them to `<out>/<cargo-name>/`, adds a redirect from that
# directory to the crate's own `index.html`, and writes `<out>/index.html` that
# lists every crate with its Cargo description. The docs-pages workflow uploads
# `<out>` to GitHub Pages.
set -euo pipefail

out="${1:?usage: rustdoc-site.sh OUT_DIR}"
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

rm -rf "${out}"
mkdir -p "${out}"

# One `name`/`description` pair per crate, read from the manifest rather than
# from the rustdoc output so the page names the crate the way Cargo does.
manifest_field() {
  local manifest="$1" field="$2"
  sed -n "s/^${field} = \"\(.*\)\"\$/\1/p" "${manifest}" | head -n 1
}

rows=""
for manifest in "${root}"/crates/*/Cargo.toml; do
  pkg_dir="$(dirname "${manifest}")"
  pkg="$(basename "${pkg_dir}")"
  name="$(manifest_field "${manifest}" name)"
  description="$(manifest_field "${manifest}" description)"
  snake="${name//-/_}"

  rustdoc="$(find "${root}/bazel-bin/crates/${pkg}" -maxdepth 1 -name '*_doc.rustdoc' -print -quit 2>/dev/null || true)"
  if [ -z "${rustdoc}" ]; then
    echo "no rustdoc output for ${name} under bazel-bin/crates/${pkg}" >&2
    exit 1
  fi
  if [ ! -f "${rustdoc}/${snake}/index.html" ]; then
    echo "${rustdoc} has no ${snake}/index.html" >&2
    exit 1
  fi

  # `-L` follows Bazel's output symlinks; the copy is writable so a later step
  # can add the redirect beside read-only outputs.
  cp -rL "${rustdoc}" "${out}/${name}"
  chmod -R u+w "${out}/${name}"
  cat >"${out}/${name}/index.html" <<EOF
<!doctype html>
<meta charset="utf-8">
<meta http-equiv="refresh" content="0; url=${snake}/index.html">
<title>${name}</title>
<a href="${snake}/index.html">${name}</a>
EOF

  rows="${rows}<tr><td><a href=\"${name}/${snake}/index.html\"><code>${name}</code></a></td><td>${description}</td></tr>
"
done

cat >"${out}/index.html" <<EOF
<!doctype html>
<html lang="en">
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>krabka-broker API documentation</title>
<style>
  body { font: 16px/1.5 system-ui, sans-serif; margin: 2rem auto; max-width: 60rem; padding: 0 1rem; }
  table { border-collapse: collapse; width: 100%; }
  th, td { border-bottom: 1px solid #ccc; padding: 0.5rem; text-align: left; vertical-align: top; }
</style>
<h1>krabka-broker API documentation</h1>
<p>Rustdoc for every crate in
<a href="https://github.com/krabka-io/krabka-broker">krabka-io/krabka-broker</a>,
rendered from the <code>main</code> branch.</p>
<table>
<thead><tr><th>Crate</th><th>What it is</th></tr></thead>
<tbody>
${rows}</tbody>
</table>
</html>
EOF

# `.nojekyll` keeps Pages from dropping rustdoc's `static.files` and other
# underscore-prefixed paths.
touch "${out}/.nojekyll"
echo "wrote ${out}"
