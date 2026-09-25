package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"vault/internal/cluster"
)

func runCluster(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("cluster", flag.ContinueOnError)
	data := fs.String("data", "./data", "base data directory")
	nodes := fs.Int("nodes", 3, "number of storage nodes")
	rf := fs.Int("replication-factor", 3, "default replication factor")
	apiPort := fs.Int("api-port", 8080, "API port")
	basePort := fs.Int("node-base-port", 9001, "first node port (node-i uses base+i-1)")
	quiet := fs.Bool("quiet", false, "do not echo process logs to the terminal")
	autoRepair := fs.Bool("auto-repair", true, "enable automatic repair")
	chaos := fs.Bool("enable-chaos", true, "enable /chaos endpoints (local development cluster)")
	workers := fs.Int("repair-workers", envInt("VAULT_REPAIR_WORKERS", 4), "repair workers (env VAULT_REPAIR_WORKERS)")
	scrub := fs.Duration("scrub-interval", time.Minute, "background scrub interval, 0 disables")
	if err := fs.Parse(args); err != nil {
		return err
	}
	bin, err := os.Executable()
	if err != nil {
		return err
	}
	dir, err := filepath.Abs(*data)
	if err != nil {
		return err
	}
	spec := cluster.Spec{
		Binary: bin, Dir: dir, Nodes: *nodes, ReplicationFactor: *rf,
		APIPort: *apiPort, NodeBasePort: *basePort,
		AutoRepair: *autoRepair, EnableChaos: *chaos, RepairWorkers: *workers, ScrubInterval: *scrub,
	}
	if !*quiet {
		spec.LogTee = os.Stderr
	}
	c, err := cluster.Start(ctx, spec)
	if err != nil {
		return err
	}
	defer c.Stop()

	fmt.Printf("\nVault local cluster is up (data in %s)\n", dir)
	fmt.Printf("  api      %s  pid %d\n", c.API.URL, c.API.PID())
	for _, n := range c.Nodes {
		fmt.Printf("  %-8s %s  pid %d  data %s\n", n.Name, n.URL, n.PID(), n.DataDir)
	}
	fmt.Printf("\nTry:\n  curl -X PUT --data-binary @README.md %s/objects/readme.md\n", c.API.URL)
	fmt.Printf("  curl -s %s/objects/readme.md/metadata\n", c.API.URL)
	fmt.Printf("  curl -s %s/cluster/status\n", c.API.URL)
	if *chaos {
		fmt.Printf("  curl -X POST %s/chaos/kill-node/node-2     # chaos endpoints are ENABLED\n", c.API.URL)
	}
	fmt.Println("\nPress Ctrl-C to stop.")

	// Keep running until interrupted. A node exiting on its own is reported
	// but does not bring the cluster down: that is exactly the failure mode
	// Vault must tolerate.
	reported := map[string]bool{}
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			fmt.Println("\nstopping cluster…")
			return nil
		case <-tick.C:
		}
		if !c.API.Running() {
			return fmt.Errorf("api process exited; see %s", c.API.LogPath)
		}
		for _, n := range c.Nodes {
			if !n.Running() && !reported[n.Name] {
				reported[n.Name] = true
				fmt.Fprintf(os.Stderr, "storage node %s exited; see %s\n", n.Name, n.LogPath)
			} else if n.Running() {
				reported[n.Name] = false
			}
		}
	}
}
