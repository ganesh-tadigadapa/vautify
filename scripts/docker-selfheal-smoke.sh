#!/usr/bin/env bash
# Self-healing smoke test for the 5-node Docker deployment:
#   docker compose -f docker-compose.yml -f docker-compose.selfheal.yml up --build -d
# Kills a node container via the chaos endpoint, waits for automatic repair,
# then checks every replica inside the containers; then corrupts a replica on
# a live container, scrubs, and checks the verified repair.
set -euo pipefail

API=${API:-http://localhost:8080}
COMPOSE="docker compose -f docker-compose.yml -f docker-compose.selfheal.yml"
TMP=$(mktemp -d)
RUN=$(date +%s)
trap 'rm -rf "$TMP"; $COMPOSE start vault-node-2 >/dev/null 2>&1 || true' EXIT

pass() { printf '  \033[32m✓\033[0m %s\n' "$*"; }
fail() { printf '  \033[31m✗\033[0m %s\n' "$*"; exit 1; }
sha()  { shasum -a 256 "$1" | cut -d' ' -f1; }
json() { python3 -c "import json,sys; d=json.load(open('$1')); print($2)"; }

wait_healed() {
  for _ in $(seq 1 60); do
    curl -fsS "$API/cluster/status" > "$TMP/st.json"
    [ "$(json "$TMP/st.json" "d['objects']['healthy']==d['objects']['total'] and d['repair']['queue_depth']==0 and d['repair']['active']==0")" = True ] && return 0
    sleep 0.5
  done
  return 1
}

echo "Docker self-heal smoke test against $API"
for i in $(seq 1 12); do
  head -c 131072 /dev/urandom > "$TMP/o$i"
  curl -fsS -o /dev/null -X PUT --data-binary @"$TMP/o$i" "$API/objects/heal-$RUN-$i"
done
pass "uploaded 12 objects (RF=3) to 5 node containers"

curl -fsS -X POST "$API/chaos/kill-node/node-2" > "$TMP/kill.json"
[ "$(json "$TMP/kill.json" "d['confirmed_unavailable']")" = True ] && pass "chaos kill: node-2 confirmed unreachable" || fail "kill"
sleep 1
state=$($COMPOSE ps -a --format '{{.Service}} {{.State}}' | awk '$1=="vault-node-2"{print $2}')
[ "$state" = exited ] && pass "vault-node-2 container state: exited" || fail "container state $state"

wait_healed && pass "all objects HEALTHY again, repair queue empty" || fail "not healed"
curl -fsS "$API/repair/metrics" > "$TMP/m.json"
pass "repairs completed: $(json "$TMP/m.json" "d['repair']['repaired_total']"), bytes transferred: $(json "$TMP/m.json" "d['repair']['bytes_transferred_total']"), failed: $(json "$TMP/m.json" "d['repair']['failed_total']")"

checked=0
for i in $(seq 1 12); do
  id=heal-$RUN-$i
  curl -fsS "$API/objects/$id/metadata" > "$TMP/meta.json"
  want=$(json "$TMP/meta.json" "d['sha256']")
  [ "$want" = "$(sha "$TMP/o$i")" ] || fail "$id metadata sha differs from upload"
  for n in $(json "$TMP/meta.json" "' '.join(d['replicas'])"); do
    [ "$n" = node-2 ] && fail "$id still lists dead node-2"
    got=$($COMPOSE exec -T "vault-$n" sha256sum "/data/objects/$id/00000000000000000001.data" | cut -d' ' -f1)
    [ "$got" = "$want" ] || fail "$id on $n: $got"
    checked=$((checked+1))
  done
  curl -fsS -o "$TMP/r" "$API/objects/$id"
  [ "$(sha "$TMP/r")" = "$want" ] || fail "$id read"
done
pass "$checked replicas verified inside live containers; 12/12 reads byte-identical"

id=heal-$RUN-1
curl -fsS "$API/objects/$id/metadata" > "$TMP/meta.json"
victim=$(json "$TMP/meta.json" "d['replicas'][0]")
curl -fsS -X POST "$API/chaos/corrupt-replica/$id/$victim" > /dev/null
pass "silently corrupted $id on live $victim"
curl -fsS -X POST "$API/scrub/run" > "$TMP/scrub.json"
[ "$(json "$TMP/scrub.json" "d['corrupted']")" = 1 ] && pass "scrub found 1 corrupted replica" || fail "scrub: $(cat "$TMP/scrub.json")"
wait_healed && pass "object repaired and HEALTHY" || fail "not repaired"
curl -fsS "$API/objects/$id/health" > "$TMP/h.json"
src=$(json "$TMP/h.json" "d['last_repair']['result']['source']")
[ "$src" != "$victim" ] && pass "repair source $src (never the corrupted $victim)" || fail "used corrupted source"

# Phase 3: versions, CAS, tombstones, consistency.
id=heal-$RUN-2
v=$(curl -fsS "$API/objects/$id/metadata" | python3 -c "import json,sys; print(json.load(sys.stdin)['version'])")
code=$(curl -s -o /dev/null -w '%{http_code}' -X PUT --data-binary 'x' "$API/objects/$id?expected_version=$((v+7))")
[ "$code" = 409 ] && pass "CAS with wrong expected_version → 409 write_conflict" || fail "CAS: HTTP $code"
curl -fsS -o /dev/null -X PUT --data-binary 'second' "$API/objects/$id?expected_version=$v"
pass "CAS with expected_version=$v → new version $((v+1))"
curl -fsS -X DELETE "$API/objects/$id" > "$TMP/del.json"
tv=$(json "$TMP/del.json" "d['tombstone_version']")
[ "$tv" = $((v+2)) ] && pass "DELETE wrote tombstone v$tv" || fail "tombstone $tv"
code=$(curl -s -o /dev/null -w '%{http_code}' "$API/objects/$id")
[ "$code" = 404 ] && pass "deleted object reads 404" || fail "GET after delete: $code"
curl -fsS "$API/objects/$id/consistency" > "$TMP/c.json"
[ "$(json "$TMP/c.json" "d['state']")" = deleted ] && pass "consistency: state=deleted, current_version=$(json "$TMP/c.json" "d['current_version']")" || fail "consistency"
curl -fsS -o /dev/null -X PUT --data-binary 'reborn' "$API/objects/$id"
nv=$(curl -fsS "$API/objects/$id/metadata" | python3 -c "import json,sys; print(json.load(sys.stdin)['version'])")
[ "$nv" = $((tv+1)) ] && pass "PUT after delete → v$nv (monotonic past the tombstone)" || fail "version $nv"

$COMPOSE start vault-node-2 >/dev/null
echo "all checks passed"
