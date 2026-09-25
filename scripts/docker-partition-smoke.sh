#!/usr/bin/env bash
# Phase 4 smoke test for the 5-node Docker deployment, from fresh volumes:
#   docker compose -f docker-compose.yml -f docker-compose.selfheal.yml down -v
#   docker compose -f docker-compose.yml -f docker-compose.selfheal.yml up --build -d
# Partitions node containers from the coordinator (they keep running),
# checks write/read quorum, deletes and overwrites during the partition,
# heals, and verifies reconciliation + repair inside the containers; then
# checks the guarded replacement order and the storage metrics against the
# bytes actually stored in the containers.
set -euo pipefail

API=${API:-http://localhost:8080}
COMPOSE=${COMPOSE:-docker compose -f docker-compose.yml -f docker-compose.selfheal.yml}
TMP=$(mktemp -d)
RUN=$(date +%s)
NODES="node-1 node-2 node-3 node-4 node-5"
trap 'rm -rf "$TMP"; curl -s -o /dev/null -X POST "$API/chaos/heal" || true' EXIT

pass() { printf '  \033[32m✓\033[0m %s\n' "$*"; }
fail() { printf '  \033[31m✗\033[0m %s\n' "$*"; exit 1; }
sha()  { shasum -a 256 "$1" | cut -d' ' -f1; }
json() { python3 -c "import json,sys; d=json.load(open('$1')); print($2)"; }

node_state() { curl -fsS "$API/nodes" | python3 -c "import json,sys; print([n['state'] for n in json.load(sys.stdin)['nodes'] if n['id']=='$1'][0])"; }
wait_state() {
  for _ in $(seq 1 60); do [ "$(node_state "$1")" = "$2" ] && return 0; sleep 0.25; done
  fail "$1 never became $2"
}
wait_healed() {
  for _ in $(seq 1 120); do
    curl -fsS "$API/cluster/status" > "$TMP/st.json"
    [ "$(json "$TMP/st.json" "d['objects']['healthy']==d['objects']['total'] and d['repair']['queue_depth']==0 and d['repair']['active']==0")" = True ] && return 0
    sleep 0.5
  done
  return 1
}
# Bytes of every replica file inside the node containers.
disk_bytes() {
  local total=0 n
  for n in $NODES; do
    b=$($COMPOSE exec -T "vault-$n" sh -c 'find /data/objects -name "*.data" -exec stat -c %s {} + 2>/dev/null | awk "{s+=\$1} END {print s+0}"')
    total=$((total + b))
  done
  echo "$total"
}
http() { curl -s -o "$TMP/body" -w '%{http_code}' "$@"; }
code_of() { json "$TMP/body" "d['error']['code']"; }

echo "Docker Phase 4 smoke test against $API"
for i in $(seq 1 6); do
  head -c 65536 /dev/urandom > "$TMP/o$i"
  curl -fsS -o /dev/null -X PUT --data-binary @"$TMP/o$i" "$API/objects/p4-$RUN-$i"
done
pass "uploaded 6 × 64 KiB objects (RF=3) to 5 node containers"
curl -fsS "$API/storage/metrics" > "$TMP/m.json"
disk=$(disk_bytes)
[ "$(json "$TMP/m.json" "d['complete'] and d['physical_bytes']==$disk and d['storage_overhead']==3.0")" = True ] \
  && pass "storage metrics: logical $(json "$TMP/m.json" "d['logical_bytes']") B, physical $(json "$TMP/m.json" "d['physical_bytes']") B (= $disk B found inside the containers), overhead $(json "$TMP/m.json" "d['storage_overhead']")" \
  || fail "baseline metrics: $(cat "$TMP/m.json") disk=$disk"

# ── 1. partition one node: it keeps running, the coordinator cannot reach it
curl -fsS "$API/objects/p4-$RUN-1/metadata" > "$TMP/meta1.json"
A=$(json "$TMP/meta1.json" "d['replicas'][0]")
OVER=p4-$RUN-1
GONE=""
for i in $(seq 2 6); do
  curl -fsS "$API/objects/p4-$RUN-$i/metadata" > "$TMP/mi.json"
  if [ "$(json "$TMP/mi.json" "'$A' in d['replicas']")" = True ]; then GONE=p4-$RUN-$i; break; fi
done
[ -n "$GONE" ] || fail "no second object on $A"
curl -fsS -X POST "$API/chaos/partition/$A" > /dev/null
wait_state "$A" UNAVAILABLE
cstate=$($COMPOSE ps --format '{{.Service}} {{.State}}' | awk -v s="vault-$A" '$1==s{print $2}')
[ "$cstate" = running ] && pass "partitioned $A: coordinator sees UNAVAILABLE, container state is still '$cstate'" || fail "container $cstate"
$COMPOSE exec -T "vault-$A" wget -qO- http://127.0.0.1:9001/health | grep -q "\"node_id\": \"$A\"" \
  && pass "$A answers its own /health inside the container (partition ≠ node failure)" || fail "$A not alive"
curl -fsS "$API/cluster/diagnosis" > "$TMP/d.json"
[ "$(json "$TMP/d.json" "[n['diagnosis'] for n in d['nodes'] if n['node_id']=='$A'][0]")" = NETWORK_PARTITION ] \
  && pass "diagnosis: $A = NETWORK_PARTITION" || fail "diagnosis $(cat "$TMP/d.json")"

head -c 65536 /dev/urandom > "$TMP/v2"
curl -fsS -o /dev/null -X PUT --data-binary @"$TMP/v2" "$API/objects/$OVER"
curl -fsS -o /dev/null -X DELETE "$API/objects/$GONE"
pass "during the partition: overwrote $OVER (v2) and deleted $GONE; $A still holds their old bytes"

# ── 2. quorum: cut three more links, leaving 2 of 5 nodes, then 1 of 5
others=$(for n in $NODES; do [ "$n" != "$A" ] && echo "$n"; done)
B=$(echo "$others" | sed -n 1p); C=$(echo "$others" | sed -n 2p); D=$(echo "$others" | sed -n 3p)
for n in $B $C; do curl -fsS -o /dev/null -X POST "$API/chaos/partition/$n"; wait_state "$n" UNAVAILABLE; done
head -c 32768 /dev/urandom > "$TMP/q"
st=$(http -X PUT --data-binary @"$TMP/q" "$API/objects/quorum-$RUN?write_quorum=2")
[ "$st" = 201 ] && [ "$(json "$TMP/body" "d['replication']['achieved']==2 and d['replication']['status']=='degraded'")" = True ] \
  && pass "2 of 5 nodes reachable: PUT ?write_quorum=2 → 201, achieved 2/3, status degraded" || fail "W=2 put: $st $(cat "$TMP/body")"
st=$(http -X PUT --data-binary @"$TMP/q" "$API/objects/quorum2-$RUN")
[ "$st" = 503 ] && pass "same write with the default quorum (all) → 503 $(code_of) (P1-P3 behaviour kept)" || fail "W=all put: $st"
st=$(http "$API/objects/quorum-$RUN?read_quorum=2")
[ "$st" = 200 ] && [ "$(sha "$TMP/body")" = "$(sha "$TMP/q")" ] && pass "GET ?read_quorum=2 → 200, verified bytes" || fail "R=2 get: $st"
curl -fsS -o /dev/null -X POST "$API/chaos/partition/$D"; wait_state "$D" UNAVAILABLE
st=$(http -X PUT --data-binary @"$TMP/q" "$API/objects/quorum3-$RUN?write_quorum=2")
[ "$st" = 503 ] && [ "$(code_of)" = quorum_not_met ] && pass "1 of 5 reachable: PUT ?write_quorum=2 → 503 quorum_not_met" || fail "no-quorum put: $st $(cat "$TMP/body")"
st=$(http "$API/objects/quorum-$RUN?read_quorum=2")
[ "$st" = 503 ] && [ "$(code_of)" = quorum_not_met ] && pass "GET ?read_quorum=2 → 503 quorum_not_met (no bytes returned)" || fail "no-quorum get: $st"
st=$(http "$API/objects/quorum3-$RUN/metadata")
[ "$st" = 404 ] && pass "failed quorum write left no metadata" || fail "metadata after failed write: $st"
curl -fsS "$API/storage/metrics" > "$TMP/m.json"
[ "$(json "$TMP/m.json" "(not d['complete']) and d['physical_bytes'] is None and d['storage_overhead'] is None and len(d['unreachable_nodes'])==4")" = True ] \
  && pass "storage metrics with 4 nodes unreachable: complete=false, physical_bytes=null, overhead=null (not invented)" || fail "incomplete metrics: $(cat "$TMP/m.json")"

# ── 3. heal: reconcile on recovery, repair back to RF, consistent state
curl -fsS -o /dev/null -X POST "$API/chaos/heal"
for n in $NODES; do wait_state "$n" HEALTHY; done
pass "healed all links"
wait_healed && pass "all objects HEALTHY again, repair queue empty" || fail "not healed"
for _ in $(seq 1 60); do
  curl -fsS "$API/events?event=RECONCILE_COMPLETED" > "$TMP/ev.json"
  [ "$(json "$TMP/ev.json" "len({e['fields']['node_id'] for e in d['events'] if e['fields'].get('trigger')=='node_recovered'})")" -ge 4 ] && break
  sleep 0.5
done
pass "reconcile on recovery ran for: $(json "$TMP/ev.json" "sorted({e['fields']['node_id'] for e in d['events'] if e['fields'].get('trigger')=='node_recovered'})")"
for _ in $(seq 1 60); do
  held=$($COMPOSE exec -T "vault-$A" sh -c "ls /data/objects/$OVER /data/objects/$GONE 2>/dev/null | grep -c '^00000000000000000001.data' || true")
  [ "$held" = 0 ] && break
  sleep 0.5
done
[ "$held" = 0 ] && pass "$A: stale v1 of $OVER and deleted $GONE's bytes removed" || fail "$A still holds old bytes"
st=$(http "$API/objects/$GONE")
[ "$st" = 404 ] && pass "$GONE still 404 after healing (no resurrection)" || fail "$GONE resurrected: $st"
st=$(http "$API/objects/$OVER?read_quorum=3")
[ "$st" = 200 ] && [ "$(sha "$TMP/body")" = "$(sha "$TMP/v2")" ] && pass "$OVER read with read_quorum=3 → newest version v2" || fail "$OVER read $st"
for id in $OVER $GONE quorum-$RUN p4-$RUN-3 p4-$RUN-4 p4-$RUN-5 p4-$RUN-6; do
  curl -fsS "$API/objects/$id/consistency" > "$TMP/c.json"
  [ "$(json "$TMP/c.json" "d['consistent']")" = True ] || fail "$id not consistent: $(cat "$TMP/c.json")"
done
pass "consistency endpoint: every object consistent (live: exactly RF authoritative replicas; deleted: no bytes anywhere)"
for n in $NODES; do
  for f in $($COMPOSE exec -T "vault-$n" sh -c "ls /data/objects/$OVER 2>/dev/null || true"); do
    case "$f" in *.data) [ "$f" = 00000000000000000002.data ] || fail "$n holds $OVER/$f";; esac
  done
done
pass "inside every container, $OVER exists only as v2"

# ── 4. guarded replacement: corrupt a replica on a live node
id=p4-$RUN-3
curl -fsS "$API/objects/$id/metadata" > "$TMP/meta.json"
victim=$(json "$TMP/meta.json" "d['replicas'][1]")
want=$(json "$TMP/meta.json" "d['sha256']")
curl -fsS -o /dev/null -X POST "$API/chaos/corrupt-replica/$id/$victim"
curl -fsS -o /dev/null -X POST "$API/scrub/run"
wait_healed && pass "corrupted $id on live $victim → scrub → repaired" || fail "not repaired"
curl -fsS "$API/events?limit=5000" > "$TMP/ev.json"
python3 - "$TMP/ev.json" "$id" "$victim" <<'PY' || fail "replacement order"
import json, sys
evs, oid, victim = json.load(open(sys.argv[1]))['events'], sys.argv[2], sys.argv[3]
def seq(name, ok):
    s = [e['seq'] for e in evs if e['event'] == name and e['fields'].get('object_id') == oid and ok(e['fields'])]
    if not s: sys.exit(f"missing {name}")
    return s[-1]
written = seq('REPAIR_REPLICA_WRITTEN', lambda f: True)
verified = seq('REPAIR_CHECKSUM_VERIFIED', lambda f: f.get('version_verified') is True)
committed = seq('REPAIR_METADATA_COMMITTED', lambda f: True)
removed = seq('REPLICA_REMOVED', lambda f: f.get('node_id') == victim and f.get('class') == 'invalid_copy')
assert written < verified < committed < removed, (written, verified, committed, removed)
print(f"    event order: REPLICA_WRITTEN #{written} < CHECKSUM+VERSION_VERIFIED #{verified} < METADATA_COMMITTED #{committed} < REPLICA_REMOVED({victim}) #{removed}")
PY
pass "old replica removed only after the replacement was written, version+checksum verified and committed"
[ -z "$($COMPOSE exec -T "vault-$victim" sh -c "ls /data/objects/$id 2>/dev/null || true")" ] && pass "corrupted bytes gone from $victim" || fail "$victim still holds $id"
curl -fsS "$API/objects/$id/metadata" > "$TMP/meta.json"
for n in $(json "$TMP/meta.json" "' '.join(d['replicas'])"); do
  got=$($COMPOSE exec -T "vault-$n" sha256sum "/data/objects/$id/00000000000000000001.data" | cut -d' ' -f1)
  [ "$got" = "$want" ] || fail "$id on $n: $got"
done
pass "all 3 replicas of $id verified inside their containers"

# ── 5. storage metrics after everything
curl -fsS "$API/storage/metrics" > "$TMP/m.json"
disk=$(disk_bytes)
[ "$(json "$TMP/m.json" "d['complete'] and d['physical_bytes']==$disk and d['storage_overhead']==3.0 and d['breakdown_reachable']['stale']['replicas']==0 and d['breakdown_reachable']['deleted_object']['replicas']==0")" = True ] \
  || fail "final metrics: $(cat "$TMP/m.json") disk=$disk"
pass "final metrics: logical $(json "$TMP/m.json" "d['logical_bytes']") B, physical $(json "$TMP/m.json" "d['physical_bytes']") B = $disk B in containers, replicas $(json "$TMP/m.json" "d['physical_replicas']"), overhead $(json "$TMP/m.json" "d['storage_overhead']")"
pass "repair bytes written: $(json "$TMP/m.json" "d['repair']['bytes_written']") ($(json "$TMP/m.json" "d['repair']['replicas_created']") replicas); guarded removals: $(json "$TMP/m.json" "d['guarded_removals']['by_class']"); adopted: $(json "$TMP/m.json" "d['reconcile']['replicas_adopted']")"
echo "all checks passed"
