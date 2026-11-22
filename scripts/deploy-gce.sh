#!/usr/bin/env bash
# Deploy the 5-node Vault stack (docker-compose.yml + docker-compose.selfheal.yml)
# to a single Compute Engine VM. Re-running it ships the current working tree
# and rebuilds; node volumes on the VM are kept.
#
# Requires: gcloud logged in, and a project with billing enabled.
#   PROJECT=my-project ./scripts/deploy-gce.sh
#
# Chaos endpoints are enabled (as in docker-compose.yml) and the API has no
# auth, so by default only your current public IP may reach it. Pass
# SOURCE_RANGES=0.0.0.0/0 to open it to everyone.
set -euo pipefail

PROJECT=${PROJECT:-$(gcloud config get-value project 2>/dev/null)}
ZONE=${ZONE:-asia-south1-a}
VM=${VM:-vaultify}
MACHINE=${MACHINE:-e2-small}
PORT=${PORT:-8080}
SOURCE_RANGES=${SOURCE_RANGES:-$(curl -fsS https://api.ipify.org)/32}

cd "$(dirname "$0")/.."
g() { gcloud --project "$PROJECT" "$@"; }
ssh_vm() { g compute ssh "$VM" --zone "$ZONE" --quiet --strict-host-key-checking=no --command "$1"; }

if [ "$(g billing projects describe "$PROJECT" --format='value(billingEnabled)' 2>/dev/null)" != "True" ]; then
  echo "deploy-gce: billing is not enabled on project $PROJECT" >&2
  echo "  enable it at https://console.cloud.google.com/billing/linkedaccount?project=$PROJECT" >&2
  exit 1
fi

g services enable compute.googleapis.com

if ! g compute instances describe "$VM" --zone "$ZONE" >/dev/null 2>&1; then
  g compute instances create "$VM" --zone "$ZONE" --machine-type "$MACHINE" \
    --image-family debian-12 --image-project debian-cloud --boot-disk-size 30GB \
    --tags vaultify \
    --metadata startup-script='#!/bin/bash
command -v docker >/dev/null || curl -fsSL https://get.docker.com | sh'
fi

if ! g compute firewall-rules describe vaultify-api >/dev/null 2>&1; then
  g compute firewall-rules create vaultify-api --allow "tcp:$PORT" \
    --target-tags vaultify --source-ranges "$SOURCE_RANGES"
fi

echo "waiting for docker on $VM…"
until ssh_vm 'sudo docker compose version' >/dev/null 2>&1; do sleep 10; done

COPYFILE_DISABLE=1 tar --exclude ./bin --exclude ./data --exclude ./.git -czf - . |
  ssh_vm 'rm -rf ~/vaultify && mkdir -p ~/vaultify && tar --warning=no-unknown-keyword -xzf - -C ~/vaultify'

ssh_vm "cd ~/vaultify && sudo VAULT_API_PORT=$PORT docker compose -p vaultify \
  -f docker-compose.yml -f docker-compose.selfheal.yml up --build -d"

IP=$(g compute instances describe "$VM" --zone "$ZONE" \
  --format='get(networkInterfaces[0].accessConfigs[0].natIP)')
echo "waiting for http://$IP:$PORT/health…"
until curl -fsS "http://$IP:$PORT/health" >/dev/null 2>&1; do sleep 3; done
echo "Vault is up: http://$IP:$PORT/"
