#!/usr/bin/env bash
# Install the Krabka operator and its CRDs.
# Idempotent - safe to re-run.
#
# The operator, its CRD manifests and its Helm chart live in
# `krabka-io/krabka-operator`, not in this repository. Check that repository
# out beside this one, or set KRABKA_OPERATOR_REPO to where it is.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

: "${KRABKA_OPERATOR_REPO:=$REPO_ROOT/../krabka-operator}"
: "${KRABKA_OPERATOR_IMAGE_REPO:=ghcr.io/krabka-io/krabka-operator}"
: "${KRABKA_OPERATOR_IMAGE_TAG:=0.1.1}"
: "${KRABKA_BROKER_IMAGE_REPO:=ghcr.io/krabka-io/krabka-broker}"
: "${KRABKA_BROKER_IMAGE_TAG:=0.1.1}"
: "${KRABKA_IMAGE_PULL_POLICY:=IfNotPresent}"

if [[ ! -d "$KRABKA_OPERATOR_REPO" ]]; then
  log "no operator checkout at $KRABKA_OPERATOR_REPO"
  log "clone https://github.com/krabka-io/krabka-operator or set KRABKA_OPERATOR_REPO"
  exit 1
fi

log "installing Krabka operator (image=$KRABKA_OPERATOR_IMAGE_REPO:$KRABKA_OPERATOR_IMAGE_TAG)"

kubectl apply -f "$KRABKA_OPERATOR_REPO/deploy/crds/krabka.io_kafkas.yaml"
kubectl apply -f "$KRABKA_OPERATOR_REPO/deploy/crds/krabka.io_kafkanodepools.yaml"
kubectl apply -f "$KRABKA_OPERATOR_REPO/deploy/crds/krabka.io_kafkatopics.yaml"
kubectl apply -f "$KRABKA_OPERATOR_REPO/deploy/crds/krabka.io_kafkausers.yaml"

kubectl create namespace krabka-operator --dry-run=client -o yaml | kubectl apply -f -

helm upgrade --install operator "$KRABKA_OPERATOR_REPO/charts/krabka-operator" \
  --namespace krabka-operator \
  --set "image.repository=$KRABKA_OPERATOR_IMAGE_REPO" \
  --set "image.tag=$KRABKA_OPERATOR_IMAGE_TAG" \
  --set "image.pullPolicy=$KRABKA_IMAGE_PULL_POLICY" \
  --set "brokerImage.repository=$KRABKA_BROKER_IMAGE_REPO" \
  --set "brokerImage.tag=$KRABKA_BROKER_IMAGE_TAG" \
  --set "brokerImage.pullPolicy=$KRABKA_IMAGE_PULL_POLICY"

log "waiting for krabka-operator rollout"
kubectl rollout status -n krabka-operator deploy/operator-krabka-operator --timeout=300s
log "Krabka operator ready"
