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
}
