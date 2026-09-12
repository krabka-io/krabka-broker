#!/usr/bin/env bash
# Pull all results from /opt/benchmark/results/ on every client VM
# into bench/omb/results/<stack>/. Useful when you've left runs going
# detached or want to recover after a network blip.

set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

# `rsync` is the command this script actually runs. Requiring `scp` alone let a
# machine without rsync reach the transfer loop and report success.
require terraform jq rsync ssh

tf_outs="$(terraform -chdir="$TF_DIR" output -json)"
ssh_user="$(jq -r '.ssh_user.value' <<<"$tf_outs")"

mkdir -p "$RESULTS_DIR"
ssh_opts=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null)

# Try every client, then fail if any transfer failed. A partial recovery is
# worth keeping, but reporting success over a failed one is what made a run with
# no result files look like a run that worked.
failed=()
while read -r ip; do
  log "Pulling results from $ip"
  rsync_dir="$RESULTS_DIR/_raw/${ip}"
  mkdir -p "$rsync_dir"
  if ! rsync -az -e "ssh ${ssh_opts[*]}" "${ssh_user}@${ip}:/opt/benchmark/results/" "$rsync_dir/"; then
    log "ERROR: transfer from $ip failed"
    failed+=("$ip")
  fi
done < <(jq -r '.clients.value[].public_ip' <<<"$tf_outs")

if (( ${#failed[@]} > 0 )); then
  die "result transfer failed for: ${failed[*]}"
fi

log "Raw results under $RESULTS_DIR/_raw/. The per-stack copies created by run-workload.sh are unaffected."
