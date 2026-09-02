#!/bin/bash
set -euo pipefail

readonly creusot_release="$(cat /opt/creusot-version)"
readonly creusot_commit="318615be3b8bbc60d1f6d52469ba5c0bdebed4f1"
readonly creusot_source="/opt/creusot"
readonly output_tar="$1"

if [[ "${creusot_release}" != "v0.13.0" ]]; then
  echo "update the immutable Creusot commit for ${creusot_release}" >&2
  exit 1
fi

git init --quiet "${creusot_source}"
git -C "${creusot_source}" fetch --quiet --depth 1 \
  https://github.com/creusot-rs/creusot "${creusot_commit}"
git -C "${creusot_source}" checkout --quiet --detach FETCH_HEAD

readonly rust_channel="$(sed -n 's/^channel = "\(.*\)"/\1/p' "${creusot_source}/rust-toolchain")"
rustup set profile minimal
rustup toolchain install "${rust_channel}" \
  --component rustfmt \
  --component rustc-dev \
  --component llvm-tools
readonly rustc_path="$(rustup which --toolchain "${rust_channel}" rustc)"
export PATH="${rustc_path%/*}:${PATH}"

opam init --disable-sandboxing --bare -y --no-setup
opam --cli=2.1 var --global in-creusot-ci=true

export OPAMYES=1
export OPAMCONFIRMLEVEL=unsafe-yes

cd "${creusot_source}"
cargo run --locked --bin creusot-install -- \
  prelude why3 provers why3-conf cargo-creusot creusot-rustc \
  cargo-creusot-config
for rust_tool in cargo rustc rustdoc rustfmt; do
  ln -sf /usr/bin/rustup "/root/.cargo/bin/${rust_tool}"
done
opam clean --all-switches --download-cache --switch-cleanup -y

install_paths=(
  root/.cargo/bin
  root/.local/share/creusot
  root/.rustup
)
if [[ -d /root/.config/creusot ]]; then
  install_paths+=(root/.config/creusot)
fi

tar -cf "${output_tar}" -C / "${install_paths[@]}"
