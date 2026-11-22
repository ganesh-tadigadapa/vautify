// Command vault runs Vault storage nodes, the API coordinator, a local
// multi-process cluster, and the demo.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
)

const usage = `vault — distributed object storage

Usage:
  vault node     --id ID --listen ADDR --data DIR         run one storage node
  vault api      --listen ADDR --nodes ID=URL,... --data DIR  run the API/coordinator
  vault cluster  [--data DIR] [--nodes N]                 run a local cluster (N nodes + api)
  vault demo     [integrity|self-heal|corruption-repair|all] [--keep]  run a demo

Run "vault <command> -h" for command flags.
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	// Remember which signal (if any) asked us to stop; see exitAfterSignal.
	sigSeen := make(chan os.Signal, 1)
	signal.Notify(sigSeen, os.Interrupt, syscall.SIGTERM)

	var err error
	args := os.Args[2:]
	switch os.Args[1] {
	case "node":
		err = runNode(ctx, args)
	case "api":
		err = runAPI(ctx, args)
	case "cluster":
		err = runCluster(ctx, args)
	case "demo":
		err = runDemo(ctx, args)
	case "-h", "--help", "help":
		fmt.Fprint(os.Stdout, usage)
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n%s", os.Args[1], usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "vault:", err)
		os.Exit(1)
	}
	exitAfterSignal(sigSeen)
}

// exitAfterSignal: with VAULT_EXIT_NONZERO_ON_SIGNAL=true, a clean shutdown
// caused by SIGTERM/SIGINT exits with the conventional 128+signal status
// instead of 0. Docker's on-failure restart policy restarts a container on
// daemon start (Mac reboot, Docker Desktop restart) only if it last exited
// non-zero, while a chaos-killed node still exits 0 and stays down. Off by
// default, so local clusters, tests and demos keep exiting 0.
func exitAfterSignal(sigSeen <-chan os.Signal) {
	if os.Getenv("VAULT_EXIT_NONZERO_ON_SIGNAL") != "true" {
		return
	}
	select {
	case sig := <-sigSeen:
		if s, ok := sig.(syscall.Signal); ok {
			os.Exit(128 + int(s))
		}
		os.Exit(1)
	default:
	}
}
