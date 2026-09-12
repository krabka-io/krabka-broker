#!/usr/bin/env bash
# Run an Ansible playbook for one stack: kafka, krabka, or client.
# Usage:
#   deploy-stack.sh kafka                  # install Kafka brokers
#   deploy-stack.sh krabka                 # install Krabka broker
#   deploy-stack.sh client                 # install OMB workers + render driver YAMLs
#   deploy-stack.sh all                    # all of the above
#   deploy-stack.sh kafka -e force_format=true   # extra ansible-playbook args pass through

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

require ansible-playbook

stack="${1:-}"; shift || true
[[ -n "$stack" ]] || die "usage: $0 {kafka|krabka|client|all} [-- extra ansible-playbook args]"

run_play() {
  local play="$1"
  log "→ ansible-playbook $play"
  ( cd "$ANSIBLE_DIR" && ansible-playbook "$play" "$@" )
}

case "$stack" in
  kafka)  run_play deploy-kafka.yaml  "$@" ;;
  krabka) run_play deploy-krabka.yaml "$@" ;;
  client)
    # Client install needs the OMB checkout present for the tarball.
    if [[ ! -d "$OMB_CHECKOUT" ]]; then
      die "OMB checkout missing — run scripts/fetch-omb.sh first."
    fi
    run_play deploy-client.yaml "$@"
    ;;
  all)
    run_play deploy-kafka.yaml  "$@"
    run_play deploy-krabka.yaml "$@"
    if [[ ! -d "$OMB_CHECKOUT" ]]; then
      die "OMB checkout missing — run scripts/fetch-omb.sh first."
    fi
    run_play deploy-client.yaml "$@"
    ;;
  *)
    die "unknown stack: $stack (expected kafka|krabka|client|all)"
    ;;
esac
