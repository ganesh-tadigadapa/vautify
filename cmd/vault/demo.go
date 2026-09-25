package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"vault/internal/demo"
)

var scenarios = map[string]func(context.Context, io.Writer, demo.Options) error{
	"integrity":         demo.Run,
	"self-heal":         demo.RunSelfHeal,
	"corruption-repair": demo.RunCorruptionRepair,
}

func runDemo(ctx context.Context, args []string) error {
	scenario := "integrity"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		scenario, args = args[0], args[1:]
	}
	var run []string
	if scenario == "all" {
		run = []string{"integrity", "self-heal", "corruption-repair"}
	} else if _, ok := scenarios[scenario]; ok {
		run = []string{scenario}
	} else {
		return fmt.Errorf("unknown demo scenario %q (integrity | self-heal | corruption-repair | all)", scenario)
	}
	fs := flag.NewFlagSet("demo", flag.ContinueOnError)
	keep := fs.Bool("keep", false, "keep the demo's data directory and logs afterwards")
	dir := fs.String("dir", "", "working directory (default: a new temp directory)")
	size := fs.Int("size", 0, "payload size in bytes (default: per scenario)")
	noColor := fs.Bool("no-color", false, "disable ANSI colors")
	if err := fs.Parse(args); err != nil {
		return err
	}
	bin, err := os.Executable()
	if err != nil {
		return err
	}
	work := *dir
	if work == "" {
		if work, err = os.MkdirTemp("", "vault-demo-*"); err != nil {
			return err
		}
	} else if err := os.MkdirAll(work, 0o755); err != nil {
		return err
	}
	if !*keep && *dir == "" {
		defer os.RemoveAll(work)
	}

	for i, name := range run {
		if i > 0 {
			fmt.Println(strings.Repeat("─", 72))
		}
		dir := filepath.Join(work, name)
		if err = os.MkdirAll(dir, 0o755); err != nil {
			break
		}
		if err = scenarios[name](ctx, os.Stdout, demo.Options{
			Binary: bin, Dir: dir, Size: *size, Color: !*noColor && isTerminal(os.Stdout),
		}); err != nil {
			err = fmt.Errorf("demo %s: %w", name, err)
			break
		}
	}
	if *keep || err != nil {
		fmt.Printf("demo data and process logs: %s\n", work)
		if err != nil && *dir == "" && !*keep {
			fmt.Println("(deleted on exit; rerun with --keep to inspect)")
		}
	}
	return err
}

func isTerminal(f *os.File) bool {
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}
