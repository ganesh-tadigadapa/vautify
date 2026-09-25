#!/usr/bin/env bash
# Smoke test for the Docker Compose deployment. Requires a running stack:
#   docker compose up --build -d
# Exercises: replication onto three container volumes, silent corruption of
# one replica inside a live container, and a stopped node container.
set -euo pipefail

API=${API:-http://localhost:8080}
OBJ=smoke-$(date +%s).bin
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"; docker compose start vault-node-3 >/dev/null 2>&1 || true' EXIT

pass() { printf '  \033[32m✓\033[0m %s\n' "$*"; }
fail() { printf '  \033[31m✗\033[0m %s\n' "$*"; exit 1; }
sha()  { shasum -a 256 "$1" | cut -d' ' -f1; }

echo "Docker smoke test against $API"
head -c 262144 /dev/urandom > "$TMP/in.bin"
WANT=$(sha "$TMP/in.bin")

curl -fsS -X PUT --data-binary @"$TMP/in.bin" "$API/objects/$OBJ" > "$TMP/put.json"
grep -q '"achieved": 3' "$TMP/put.json" && pass "PUT replicated 3/3" || fail "PUT: $(cat "$TMP/put.json")"

for n in 1 2 3; do
  path=/data/objects/$OBJ/00000000000000000001.data
  got=$(docker compose exec -T vault-node-$n sha256sum "$path" | cut -d' ' -f1)
  [ "$got" = "$WANT" ] && pass "vault-node-$n volume holds the replica ($path)" || fail "vault-node-$n: $got"
done

# Silent corruption inside a running container: flip the first 16 bytes.
docker compose exec -T vault-node-2 sh -c \
  "p=/data/objects/$OBJ/00000000000000000001.data; head -c 16 /dev/zero | dd of=\$p bs=1 seek=0 conv=notrunc 2>/dev/null"
curl -fsS "$API/nodes" > "$TMP/nodes.json"
grep -A2 '"id": "node-2"' "$TMP/nodes.json" | grep -q HEALTHY && pass "node-2 corrupted but still HEALTHY" || fail "node-2 state"

curl -fsS -D "$TMP/h" -o "$TMP/out.bin" "$API/objects/$OBJ?prefer=node-2"
grep -qi 'x-vault-rejected-replicas: node-2=checksum_mismatch' "$TMP/h" && pass "node-2 rejected: checksum_mismatch" || fail "headers: $(cat "$TMP/h")"
[ "$(sha "$TMP/out.bin")" = "$WANT" ] && pass "served verified bytes from $(grep -i x-vault-served-by "$TMP/h" | tr -d '\r' | cut -d' ' -f2)" || fail "bytes differ"

docker compose logs --no-color vault-api > "$TMP/api.log" 2>&1
grep -q '"event":"CHECKSUM_MISMATCH".*"node_id":"node-2"' "$TMP/api.log" && pass "CHECKSUM_MISMATCH event in api logs" || fail "no event"

docker compose stop vault-node-3 >/dev/null
curl -fsS -D "$TMP/h" -o "$TMP/out.bin" "$API/objects/$OBJ"
[ "$(sha "$TMP/out.bin")" = "$WANT" ] && pass "vault-node-3 stopped: still served verified by $(grep -i x-vault-served-by "$TMP/h" | tr -d '\r' | cut -d' ' -f2)" || fail "read after stop"

sleep 2 # let the API's health prober observe the stopped container
code=$(curl -s -o /dev/null -w '%{http_code}' -X PUT --data-binary 'x' "$API/objects/$OBJ-rf3")
[ "$code" = 503 ] && pass "RF=3 write with 2 healthy nodes refused (HTTP 503)" || fail "got HTTP $code"

docker compose start vault-node-3 >/dev/null
for _ in $(seq 1 30); do
  curl -fsS "$API/nodes" > "$TMP/nodes.json" && grep -A2 '"id": "node-3"' "$TMP/nodes.json" | grep -q HEALTHY && break
  sleep 1
done
curl -fsS -D "$TMP/h" -o "$TMP/out.bin" "$API/objects/$OBJ?prefer=node-3"
grep -qi 'x-vault-served-by: node-3' "$TMP/h" && [ "$(sha "$TMP/out.bin")" = "$WANT" ] \
  && pass "restarted vault-node-3 serves its replica from its volume" || fail "node-3 after restart"
echo "all checks passed"
