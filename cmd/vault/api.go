package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"vault/internal/api"
	"vault/internal/coordinator"
	"vault/internal/events"
	"vault/internal/health"
	"vault/internal/httpx"
	"vault/internal/metadata"
	"vault/internal/netchaos"
	"vault/internal/nodeclient"
	"vault/internal/placement"
	"vault/internal/repair"
	"vault/internal/selfheal"
	"vault/internal/storage"
)

func runAPI(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("api", flag.ContinueOnError)
	listen := fs.String("listen", envOr("VAULT_LISTEN", "127.0.0.1:8080"), "listen address (env VAULT_LISTEN)")
	nodesFlag := fs.String("nodes", envOr("VAULT_NODES", ""), "comma-separated ID=URL list of storage nodes (env VAULT_NODES)")
	data := fs.String("data", envOr("VAULT_DATA", ""), "API data directory for metadata and spool files (env VAULT_DATA)")
	rf := fs.Int("replication-factor", 3, "default replication factor")
	maxSize := fs.Int64("max-object-size", 256<<20, "largest object accepted, in bytes")
	probe := fs.Duration("probe-interval", time.Second, "node health probe interval")
	probeTimeout := fs.Duration("probe-timeout", 800*time.Millisecond, "node health probe timeout")

	autoRepair := fs.Bool("auto-repair", envOr("VAULT_AUTO_REPAIR", "true") == "true", "automatically repair objects below policy (env VAULT_AUTO_REPAIR)")
	workers := fs.Int("repair-workers", envInt("VAULT_REPAIR_WORKERS", 4), "concurrent repair workers (env VAULT_REPAIR_WORKERS)")
	maxAttempts := fs.Int("repair-max-attempts", 5, "attempts before a repair is parked as failed")
	backoff := fs.Duration("repair-backoff", 500*time.Millisecond, "initial retry backoff (doubles per attempt)")
	maxBackoff := fs.Duration("repair-max-backoff", 30*time.Second, "maximum retry backoff")
	scrubInterval := fs.Duration("scrub-interval", envDuration("VAULT_SCRUB_INTERVAL", time.Minute), "background scrub interval, 0 disables (env VAULT_SCRUB_INTERVAL)")
	scrubConc := fs.Int("scrub-concurrency", envInt("VAULT_SCRUB_CONCURRENCY", 4), "concurrent replica verifications per scrub (env VAULT_SCRUB_CONCURRENCY)")
	sweepInterval := fs.Duration("sweep-interval", 5*time.Second, "periodic durability sweep interval")
	commitDelay := fs.Duration("test-repair-commit-delay", 0,
		"FAULT INJECTION for tests: pause each repair after its copies are verified, before the metadata commit")
	chaos := fs.Bool("enable-chaos", envOr("VAULT_ENABLE_CHAOS", "") == "true", "serve /chaos endpoints — demos and tests only (env VAULT_ENABLE_CHAOS=true)")
	reconcile := fs.Bool("reconcile-on-recovery", envOr("VAULT_RECONCILE_ON_RECOVERY", "true") == "true",
		"reconcile a node against metadata as soon as it is reachable again after an outage or partition (env VAULT_RECONCILE_ON_RECOVERY)")
	writeQuorum := fs.String("write-quorum", envOr("VAULT_WRITE_QUORUM", "all"),
		"replicas that must store and verify a write before it is acknowledged: all | majority | N (env VAULT_WRITE_QUORUM)")
	readQuorum := fs.String("read-quorum", envOr("VAULT_READ_QUORUM", "1"),
		"replicas that must hold the current version with the metadata checksum before a read returns: 1 | majority | all | N (env VAULT_READ_QUORUM)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *nodesFlag == "" || *data == "" {
		return errors.New("api: --nodes and --data are required")
	}

	wq, err := coordinator.ParseQuorum(*writeQuorum)
	if err != nil {
		return fmt.Errorf("api: --write-quorum: %w", err)
	}
	rq, err := coordinator.ParseQuorum(*readQuorum)
	if err != nil {
		return fmt.Errorf("api: --read-quorum: %w", err)
	}

	recorder := events.NewRecorder(5000)
	log := events.New(os.Stderr, "api").WithRecorder(recorder)
	base := nodeclient.NewTransport()
	// With chaos enabled, every coordinator→node request (probes, reads,
	// writes, verifications, repairs) goes through the network fault
	// injector, so injected partitions and latency hit the real code path.
	var network *netchaos.Injector
	if *chaos {
		ids, err := nodeIDs(*nodesFlag)
		if err != nil {
			return err
		}
		network = netchaos.New(log, ids)
	}
	nodes, err := parseNodes(*nodesFlag, func(id string) *http.Client {
		return &http.Client{Transport: network.Transport(id, base)}
	})
	if err != nil {
		return err
	}

	meta, err := metadata.OpenFileStore(filepath.Join(*data, "metadata"))
	if err != nil {
		return err
	}
	probers := make([]health.Prober, len(nodes))
	for i, n := range nodes {
		probers[i] = n
	}
	checker := health.New(probers, *probe, *probeTimeout, log)

	coord, err := coordinator.New(coordinator.Config{
		DefaultReplicationFactor: *rf,
		MaxObjectSize:            *maxSize,
		TempDir:                  filepath.Join(*data, "tmp"),
		RepairCommitDelay:        *commitDelay,
		WriteQuorum:              wq,
		ReadQuorum:               rq,
	}, nodes, meta, placement.Rendezvous{}, checker, log)
	if err != nil {
		return err
	}
	if err := clearSpool(filepath.Join(*data, "tmp")); err != nil {
		return err
	}

	repairs := repair.NewManager(coord, repair.Config{
		Workers: *workers, MaxAttempts: *maxAttempts, BaseBackoff: *backoff, MaxBackoff: *maxBackoff,
	}, log)
	ctrl := selfheal.New(selfheal.Config{
		AutoRepair: *autoRepair, ScrubInterval: *scrubInterval, ScrubConcurrency: *scrubConc, SweepInterval: *sweepInterval,
		ReconcileOnRecovery: *reconcile,
	}, coord, meta, checker, repairs, log)
	checker.Subscribe(ctrl.NodeStateChanged)
	coord.SetIntegrityFailureHook(ctrl.IntegrityFailureDetected)
	repairs.OnDone(ctrl.RepairFinished)

	// First probe synchronously so the API starts with a real view of the
	// cluster, then keep probing in the background.
	checker.ProbeAll(ctx)
	go checker.Run(ctx)
	go repairs.Run(ctx)
	go ctrl.Run(ctx)

	srv := api.New(coord, meta, log, api.Options{SelfHeal: ctrl, Events: recorder, EnableChaos: *chaos, Network: network})
	return httpx.Serve(ctx, *listen, srv.Handler(), httpx.DefaultServerConfig(), func(a net.Addr) {
		log.Info(ctx, "api started", "listen", a.String(), "nodes", len(nodes),
			"healthy", len(checker.Healthy()), "default_replication_factor", *rf,
			"auto_repair", *autoRepair, "repair_workers", repairs.Config().Workers,
			"scrub_interval", scrubInterval.String(), "chaos_enabled", *chaos,
			"write_quorum", wq.String(), "read_quorum", rq.String())
	})
}

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return def
}

func envDuration(key string, def time.Duration) time.Duration {
	if d, err := time.ParseDuration(os.Getenv(key)); err == nil {
		return d
	}
	return def
}

// nodeIDs returns the IDs in a --nodes spec, in order.
func nodeIDs(spec string) ([]string, error) {
	var ids []string
	for _, part := range strings.Split(spec, ",") {
		if part = strings.TrimSpace(part); part == "" {
			continue
		}
		id, _, ok := strings.Cut(part, "=")
		if !ok || id == "" {
			return nil, fmt.Errorf("api: bad --nodes entry %q, want ID=URL", part)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func parseNodes(spec string, client func(id string) *http.Client) ([]storage.StorageNode, error) {
	var nodes []storage.StorageNode
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, addr, ok := strings.Cut(part, "=")
		if !ok || id == "" || addr == "" {
			return nil, fmt.Errorf("api: bad --nodes entry %q, want ID=URL", part)
		}
		c, err := nodeclient.New(id, addr, client(id))
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, c)
	}
	if len(nodes) == 0 {
		return nil, errors.New("api: no nodes configured")
	}
	return nodes, nil
}

// clearSpool removes spool files left behind by a previous crash.
func clearSpool(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "vault-spool-") {
			if err := os.Remove(filepath.Join(dir, e.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}
