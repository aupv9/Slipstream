// Command slipstream is the single binary that runs one CDC pipeline.
//
// Usage:
//
//	slipstream run     -config slipstream.yaml   # compete for the lease, then stream
//	slipstream migrate -config slipstream.yaml   # apply the control-plane schema
//	slipstream schema                            # print the control-plane DDL
//	slipstream version
//
// Run two or more `run` processes with the same config against the same
// control plane to get HA: only the lease holder attaches to the source.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/aupv9/slipstream/internal/config"
	"github.com/aupv9/slipstream/internal/controlplane"
	"github.com/aupv9/slipstream/internal/observability"
	"github.com/aupv9/slipstream/internal/pipeline"
)

// version is overridden at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "slipstream: %v\n", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		usage()
		return errors.New("a command is required")
	}

	cmd, rest := args[0], args[1:]
	switch cmd {
	case "run":
		return cmdRun(rest)
	case "migrate":
		return cmdMigrate(rest)
	case "schema":
		fmt.Print(controlplane.Schema)
		return nil
	case "version":
		fmt.Println(version)
		return nil
	case "-h", "--help", "help":
		usage()
		return nil
	default:
		usage()
		return fmt.Errorf("unknown command %q", cmd)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `slipstream — lean, accurate change data capture

Commands:
  run      -config FILE [-log-level LEVEL] [-log-format text|json]
  migrate  -config FILE
  schema
  version
`)
}

func cmdRun(args []string) error {
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	path := fs.String("config", "slipstream.yaml", "path to the pipeline config")
	level := fs.String("log-level", "info", "debug, info, warn or error")
	format := fs.String("log-format", "text", "text or json")
	if err := fs.Parse(args); err != nil {
		return err
	}

	log, err := newLogger(*level, *format)
	if err != nil {
		return err
	}

	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	store, err := controlplane.Open(ctx, cfg.ControlPlane.DSN)
	if err != nil {
		return err
	}
	defer store.Close()

	if cfg.ControlPlane.AutoMigrate {
		if err := store.Migrate(ctx, controlplane.Schema); err != nil {
			return err
		}
		log.Info("control-plane schema applied")
	}

	var (
		metrics *observability.Registry
		health  = &observability.Health{}
		obs     *observability.Server
	)
	if cfg.Observability.Addr != "" {
		metrics = observability.NewRegistry()
		observability.Register(metrics)
		obs = observability.NewServer(cfg.Observability.Addr, metrics, health, log)
		if err := obs.Start(); err != nil {
			return err
		}
		defer func() {
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = obs.Stop(shutdown)
			cancel()
		}()
	} else {
		log.Info("metrics and health endpoints are disabled; set observability.addr to enable them")
	}

	log.Info("starting",
		"version", version,
		"pipeline", cfg.Pipeline.ID,
		"instance", cfg.InstanceID,
		"source", cfg.Pipeline.Source.Type,
		"sinks", sinkNames(cfg),
		"lease_ttl", cfg.ControlPlane.LeaseTTL.D())

	err = pipeline.NewRunner(cfg, store, metrics, health, log).Run(ctx)
	log.Info("stopped")
	return err
}

func cmdMigrate(args []string) error {
	fs := flag.NewFlagSet("migrate", flag.ContinueOnError)
	path := fs.String("config", "slipstream.yaml", "path to the pipeline config")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}

	ctx := context.Background()
	store, err := controlplane.Open(ctx, cfg.ControlPlane.DSN)
	if err != nil {
		return err
	}
	defer store.Close()

	if err := store.Migrate(ctx, controlplane.Schema); err != nil {
		return err
	}
	fmt.Println("control-plane schema applied")
	return nil
}

func newLogger(level, format string) (*slog.Logger, error) {
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		return nil, fmt.Errorf("invalid log level %q", level)
	}
	opts := &slog.HandlerOptions{Level: lvl}

	switch format {
	case "json":
		return slog.New(slog.NewJSONHandler(os.Stderr, opts)), nil
	case "text":
		return slog.New(slog.NewTextHandler(os.Stderr, opts)), nil
	default:
		return nil, fmt.Errorf("invalid log format %q", format)
	}
}

func sinkNames(cfg *config.Config) string {
	names := make([]string, 0, len(cfg.Pipeline.Sinks))
	for _, s := range cfg.Pipeline.Sinks {
		names = append(names, s.Name+"("+s.Type+")")
	}
	return strings.Join(names, ",")
}
