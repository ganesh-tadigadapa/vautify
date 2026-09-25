package main

import (
	"context"
	"errors"
	"flag"
	"net"
	"os"
	"time"

	"vault/internal/events"
	"vault/internal/httpx"
	"vault/internal/node"
)

func runNode(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("node", flag.ContinueOnError)
	id := fs.String("id", envOr("VAULT_NODE_ID", ""), "unique node ID (env VAULT_NODE_ID)")
	listen := fs.String("listen", envOr("VAULT_LISTEN", "127.0.0.1:9001"), "listen address (env VAULT_LISTEN)")
	data := fs.String("data", envOr("VAULT_DATA", ""), "data directory (env VAULT_DATA)")
	maxSize := fs.Int64("max-object-size", 256<<20, "largest replica accepted, in bytes")
	chaos := fs.Bool("enable-chaos", envOr("VAULT_ENABLE_CHAOS", "") == "true",
		"enable /admin chaos endpoints (terminate, corrupt) — for demos and tests only (env VAULT_ENABLE_CHAOS=true)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *id == "" || *data == "" {
		return errors.New("node: --id and --data are required")
	}

	log := events.New(os.Stderr, *id)
	store, err := node.OpenStore(*data, *id)
	if err != nil {
		return err
	}
	srv := node.NewServer(store, log, *maxSize)
	if *chaos {
		srv.EnableChaos(func() {
			// Give the HTTP response a moment to leave, then stop abruptly,
			// like a crash. Committed replicas are already fsynced.
			time.Sleep(50 * time.Millisecond)
			os.Exit(0)
		})
	}
	err = httpx.Serve(ctx, *listen, srv.Handler(), httpx.DefaultServerConfig(), func(a net.Addr) {
		c, _ := store.Capacity()
		log.Info(ctx, "node started", "node_id", *id, "listen", a.String(), "data_dir", *data,
			"replicas_on_disk", c.ReplicaCount, "used_bytes", c.UsedBytes, "chaos_enabled", *chaos)
	})
	log.Info(context.Background(), "node stopped", "node_id", *id)
	return err
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
