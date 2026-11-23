# Vautify

[![CI](https://github.com/ganesh-tadigadapa/vautify/actions/workflows/ci.yml/badge.svg)](https://github.com/ganesh-tadigadapa/vautify/actions/workflows/ci.yml)
[![Live demo](https://img.shields.io/badge/live%20demo-vault--store--865.netlify.app-2ea44f)](https://vault-store-865.netlify.app)
![Go](https://img.shields.io/badge/go-1.23%2B-00ADD8)

**▶ Live demo: <https://vault-store-865.netlify.app>**

A distributed object store in Go that keeps several copies of every file, checks every byte on read, and repairs itself when nodes crash, corrupt data or drop off the network.

I wanted to understand what actually goes into storage systems like S3 beyond "keep three copies". Replication is the easy part. The interesting problems are the ones where a node is up and answering but its data is wrong, or comes back after an outage holding old bytes, or a write half-lands during a network cut. So I built this to work through those cases one at a time, with real processes and real files on disk, and tests that kill and corrupt things on purpose.

It uses only the Go standard library. The binary and Go module are called `vault`.

There's no auth and no TLS. It's a learning project, not something to put real data in.

## Try the live demo

<https://vault-store-865.netlify.app> is a real 5-node cluster (replication factor 3, write quorum `all`, read quorum `1`, self-healing on), not a mock-up. Everything you do there happens to real processes and real files on disk. A two-minute tour:

1. **Overview**: see all five nodes, the object count and the cluster topology.
2. **Objects**: upload a file and see which three nodes got a copy.
3. **Failure Lab**: kill a node or corrupt one replica of your file.
4. **Self-Healing**: watch the object go DEGRADED and get repaired back to three verified copies.
5. **Events**: read what happened, step by step, in plain language.

It's a shared playground, so other people may be breaking nodes at the same time. If the site is down, my machine is probably off (see [Running it online](#running-it-online)).

## The core ideas

**Every read is checksummed.** Each object's SHA-256 and size are stored in metadata. On a read, the coordinator streams a replica, hashes it, and compares. If it doesn't match, that replica is rejected, the failure is recorded, and the next replica is tried. If no replica verifies, the client gets `503 no_verified_replica`. Corrupt bytes are never returned. Nodes also hash incoming bytes on write and refuse to commit them if they don't match what the coordinator sent.

A big thing I learned here is that "the node is alive" and "the node's data is correct" are separate questions. The health checker only knows whether a process answers. Integrity is decided per replica, by checksum.

**It repairs itself.** A background scrubber asks each node to re-hash its own files (so no object bytes cross the network) and compares the result against metadata. Objects are classified as HEALTHY, DEGRADED, CRITICAL (one verified copy left) or LOST. DEGRADED and CRITICAL objects go into a repair queue that handles CRITICAL first, then fewer verified copies, then oldest. A fixed number of workers copy from a verified source to a fresh node, have the target re-hash the copy from disk, and only then update metadata. Failed repairs retry with exponential backoff and get parked after a few attempts instead of looping forever.

**Versions only go forward.** Every PUT creates the next version number. A DELETE writes a durable tombstone at the next version before touching any node, so a node that missed the delete can never bring the object back. Nodes refuse to store a version older than the newest one they hold. PUT and DELETE also take `?expected_version=N` for compare-and-swap, which returns `409 write_conflict` on a mismatch.

**Quorums are configurable.** `--write-quorum` (default `all`) and `--read-quorum` (default `1`) take `all`, `majority`, `one` or a number, and can be overridden per request with `?write_quorum=` / `?read_quorum=`. A write is acknowledged once W replicas have stored and verified it. If fewer than W succeed, everything written is rolled back and the client gets `503 quorum_not_met`. A stale or unreachable replica never counts toward a quorum.

**Returning nodes get reconciled.** When a node that was unreachable comes back, the coordinator checks everything it holds against the latest metadata before releasing repairs: data for deleted objects is removed, old versions are purged, and a valid copy of the current version can be adopted back into the replica set without copying anything. All of this goes through one guarded removal path (`internal/coordinator/safe_remove.go`) that never deletes the last valid copy of anything.

## Quick start

You need Go 1.23 or newer. Docker is optional.

```sh
make demo              # corrupt a replica and watch reads reject it
make demo-selfheal     # 5 nodes, kill 2, watch CRITICAL objects get repaired first
make demo-corruption   # silent corruption on a live node, found by the scrubber and repaired
make demo-all          # all three
```

Each demo starts a real cluster in a temp directory, prints what happens step by step, and exits non-zero if the store misbehaves.

To run a cluster and poke at it yourself:

```sh
make cluster           # API on :8080, three nodes on :9001-9003 (Ctrl-C to stop)
```

Then in another terminal:

```sh
curl -X PUT --data-binary @photo.jpg localhost:8080/objects/photo.jpg
curl -o out.jpg -D - localhost:8080/objects/photo.jpg
curl localhost:8080/objects/photo.jpg/metadata
ls ./data/node-*/objects/photo.jpg/      # the actual bytes, in three node directories
```

The read response headers tell you which node served it and which replicas were rejected (`X-Vault-Served-By`, `X-Vault-Rejected-Replicas`). There's also a dashboard at <http://localhost:8080/>. It's one static page (`internal/api/ui/index.html`) embedded in the binary that calls the same JSON endpoints as `curl`. A sidebar switches between views, and each view has its own URL hash so you can bookmark it:

| View | Shows |
|---|---|
| `#overview` | node, object and data totals, live node states, cluster topology |
| `#objects` | uploads, the object list and where each replica lives |
| `#analytics` | storage analytics |
| `#events` | the event log, with a plain explanation of each event |
| `#lab` | Failure Lab: kill, corrupt, partition, add latency, scrub, heal |
| `#healing` | the self-healing lifecycle and the repair engine |
| `#quorum` | quorum and consistency checks |
| `#about` | what it is and how it's built |

`make cluster` turns on the chaos endpoints, so you can break things on purpose:

```sh
curl -X POST localhost:8080/chaos/corrupt-replica/photo.jpg/node-2   # flips 16 bytes on node-2's disk
curl -X POST localhost:8080/chaos/kill-node/node-3                   # really ends the process
curl -X POST "localhost:8080/chaos/partition/node-1?duration=10s"    # cuts the link for 10s
curl localhost:8080/cluster/status
```

With Docker:

```sh
make docker-up && make docker-smoke                     # 3 nodes
make docker-selfheal-up && make docker-selfheal-smoke   # 5 nodes, self-healing
make docker-partition-up && make docker-partition-smoke # partition, quorum, healing
```

The compose files are a demo setup with chaos endpoints turned on. The restart policies are set up so the cluster survives a reboot without undoing your experiments:

- `vault-api` uses `unless-stopped`, so it comes back after a crash, a reboot or a Docker restart.
- Nodes use `on-failure` with `VAULT_EXIT_NONZERO_ON_SIGNAL=true`. A node you killed from the Failure Lab exits 0 and stays down, even across a reboot (`docker compose start vault-node-N` brings it back). A real crash gets restarted. When Docker shuts down, nodes exit 143, so they start again with the daemon.
- Logs are rotated (5 × 20 MB per container), because the scrubber is chatty.

Background loops (repair workers, scrubber, self-heal controller, health probes) recover from panics: they log the stack trace and keep running. A repair that panics counts as a failed attempt.

## Architecture

```
            client / curl / dashboard
                       |
                     HTTP
                       |
   +-------------------------------------------+
   |            vault api (coordinator)        |
   |  metadata store   placement   health      |
   |  self-heal controller (scrub, repair)     |
   |  netchaos transport (fault injection)     |
   +-------------------------------------------+
          |              |              |
       node-1         node-2         node-3
        disk           disk           disk
```

- There's one coordinator (`vault api`) and N storage nodes (`vault node`). Each node is its own OS process with its own data directory.
- All traffic is coordinator to node. Nodes never talk to each other.
- Placement uses rendezvous hashing over the healthy nodes. If a target fails during a write, the next one in order is tried. A write either lands on all required nodes or gets rolled back.
- Nodes write to a temp file, fsync, then rename into `objects/<id>/<version>.data` with a JSON sidecar holding the checksum.
- Metadata is one JSON file per object, written with fsync and atomic rename.
- Every request from the coordinator to a node goes through the `netchaos` HTTP transport, which is where partitions and latency get injected when chaos is enabled. Nothing above it knows the fault is fake.

A write (`PUT /objects/{id}`) goes roughly like this:

1. Validate the ID (no path separators, no leading dot) and check there are enough healthy nodes before reading the body.
2. Stream the body to a spool file while hashing it.
3. Pick target nodes by rendezvous hashing and upload to all of them at once, each with the expected checksum.
4. If enough replicas were stored and verified, commit metadata with the new version. Otherwise delete whatever was written and return a 503. The previous version is left untouched.

A read (`GET /objects/{id}`) looks up the expected checksum, tries replicas one by one (healthy nodes first), and only sends bytes that hashed correctly. Writes to the same object are serialized with a per-object lock; different objects run in parallel.

Errors always come back in the same shape: `{"error": {"code": "...", "message": "...", "details": {...}}}`.

```
cmd/vault/            node | api | cluster | demo subcommands
internal/node/        on-disk replica store + node HTTP server
internal/coordinator/ write, read, delete, repair, scrub, CAS, quorum, reconcile
internal/metadata/    metadata model and FileStore
internal/placement/   rendezvous hashing
internal/durability/  replica/object states, blast radius
internal/repair/      priority queue, workers, retry/backoff
internal/selfheal/    reacts to failures, runs scrubs and sweeps
internal/netchaos/    partition/latency injection
internal/api/         client HTTP API and the dashboard
test/integration/     tests that spawn real api + node processes
```

## Tests

```sh
make test         # go vet + unit tests + multi-process integration tests
make test-race    # same thing with the race detector
```

Or individually:

```sh
go test ./internal/...               # unit tests
go test -count=1 ./test/integration  # builds the binary and launches real processes
```

The integration tests are the part I spent the most time on. They start actual node and API processes on localhost and check things like: a corrupted replica on a live node is never served, killing a node triggers repair back to full replication with the files verified on disk, a CRITICAL object is repaired before a DEGRADED one, a node that missed a delete can't resurrect the object even across restarts, a coordinator crash mid-repair commits nothing, and concurrent compare-and-swap clients never lose an update.

CI builds, vets and runs the full suite on every push to main and on pull requests.

## Running it online

The live demo runs on my own machine. I don't have a cloud VM for it:

```
browser -> Netlify (dashboard + proxy) -> Cloudflare quick tunnel -> docker compose (5 nodes + API)
```

Netlify serves the dashboard and proxies every API path to a `cloudflared` quick tunnel (`scripts/deploy-netlify.sh`). The catch is that a quick tunnel gets a new random URL every time it restarts, so after any reboot or Wi-Fi drop the site pointed at a dead address. `scripts/vaultctl.sh` fixes that. `vaultctl install` sets up launchd jobs that keep the tunnel running and check the public `/health` every two minutes. If something's broken, it starts Docker (and force-restarts it if it's frozen), starts the containers, gets a working tunnel and redeploys Netlify with the new URL. In testing, a killed tunnel was back in about a minute and a killed Docker in about 75 seconds.

```sh
vaultctl status    # Docker / API / tunnel / public site, each OK or DOWN
vaultctl fix       # repair whatever is broken
vaultctl publish   # deploy dashboard changes
vaultctl rebuild   # rebuild the backend containers
```

It still needs the machine to be on and online. `scripts/deploy-gce.sh` is there for moving the backend to a VM.

## Known limits

- Metadata lives in the single API process. It survives restarts but isn't replicated, and there's no consensus (no Raft). There's only ever one coordinator.
- Partitions are simulated in the coordinator's transport, not with real firewall rules.
- Tombstones are never garbage collected.
- No rebalancing, and placement doesn't know about racks or capacity.
- The scrubber trusts each node's re-hash of its own disk, so it catches bit rot but not a node that lies. Client reads still verify everything in the coordinator.
- Repair metrics and history are in memory and reset when the API restarts.
- No auth, no TLS, no multipart or range requests.
