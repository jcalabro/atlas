package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/jcalabro/atlas/internal/foundation"
	"github.com/jcalabro/atlas/internal/ingester"
	"github.com/jcalabro/atlas/internal/server"
	"github.com/urfave/cli/v3"
)

var fdbFlags = []cli.Flag{
	&cli.StringFlag{
		Name:  "fdb-cluster-file",
		Value: "foundation.cluster",
	},
	&cli.IntFlag{
		Name:  "fdb-api-version",
		Value: 730,
	},
}

func main() {
	cmd := &cli.Command{
		Name:  "atlas",
		Usage: "ATlas is the atmosphere database",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "log-lvl",
				Usage: "Minimum logging level (debug, info, warn, err)",
				Value: "info",
			},
			&cli.StringFlag{
				Name:  "log-fmt",
				Usage: "Log output format (default, json)",
				Value: "json",
			},
			&cli.BoolFlag{
				Name:  "log-src",
				Usage: "Whether or not to include source line numbers in log lines",
				Value: true,
			},
		},
		Before: func(ctx context.Context, c *cli.Command) (context.Context, error) {
			if err := setDefaultLogger(
				c.String("log-lvl"),
				c.String("log-fmt"),
				c.Bool("log-src"),
			); err != nil {
				return nil, fmt.Errorf("unable to set default logger: %w", err)
			}
			return ctx, nil
		},
		Commands: []*cli.Command{
			{
				Name:        "server",
				Description: "Runs the primary user-facing ConnectRPC server",
				Flags: append(fdbFlags,
					&cli.StringFlag{
						Name:  "addr",
						Usage: "Bind address of the primary HTTP server",
						Value: "0.0.0.0:2866",
					},
					&cli.StringFlag{
						Name:  "metrics-addr",
						Usage: "Bind address of the metrics/pprof HTTP server (empty string to disable)",
						Value: "0.0.0.0:6060",
					},
					&cli.DurationFlag{
						Name:  "read-timeout",
						Usage: "Primary HTTP server read timeout",
						Value: 5 * time.Second,
					},
					&cli.DurationFlag{
						Name:  "write-timeout",
						Usage: "Primary HTTP server write timeout",
						Value: 5 * time.Second,
					},
				),
				Action: func(ctx context.Context, c *cli.Command) error {
					return server.Run(ctx, &server.Args{
						Addr:         c.String("addr"),
						MetricsAddr:  c.String("metrics-addr"),
						ReadTimeout:  c.Duration("read-timeout"),
						WriteTimeout: c.Duration("write-timeout"),
						FDB: foundation.Config{
							ClusterFile: c.String("fdb-cluster-file"),
							APIVersion:  c.Int("fdb-api-version"),
						},
					})
				},
			},
			{
				Name:        "ingester",
				Description: "Runs the tap websocket ingester",
				Flags: append(fdbFlags,
					&cli.StringFlag{
						Name:  "tap-addr",
						Usage: "Websocket address of the tap ingestion server",
						Value: "ws://localhost:2480/channel",
					},
					&cli.StringFlag{
						Name:  "metrics-addr",
						Usage: "Bind address of the metrics/pprof HTTP server (empty string to disable)",
						Value: "0.0.0.0:6061",
					},
				),
				Action: func(ctx context.Context, c *cli.Command) error {
					return ingester.Run(ctx, &ingester.Args{
						TapAddr:     c.String("tap-addr"),
						MetricsAddr: c.String("metrics-addr"),
						FDB: foundation.Config{
							ClusterFile: c.String("fdb-cluster-file"),
							APIVersion:  c.Int("fdb-api-version"),
						},
					})
				},
			},
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		slog.Error("failed to run command", "err", err)
		os.Exit(1)
	}
}

func setDefaultLogger(llevel, lfmt string, addSource bool) error {
	opts := &slog.HandlerOptions{
		AddSource: addSource,
	}

	switch llevel {
	case "d", "dbg", "debug":
		opts.Level = slog.LevelDebug
	case "i", "info":
		opts.Level = slog.LevelInfo
	case "w", "warn", "warning":
		opts.Level = slog.LevelWarn
	case "e", "err", "error":
		opts.Level = slog.LevelError
	}

	var log *slog.Logger
	switch strings.ToLower(lfmt) {
	case "default":
		log = slog.New(slog.NewTextHandler(os.Stdout, opts))
	case "json":
		log = slog.New(slog.NewJSONHandler(os.Stdout, opts))
	default:
		return fmt.Errorf(`unsupported log format: %s (wanted "default" or "json")`, lfmt)
	}

	slog.SetDefault(slog.New(log.Handler()))
	return nil
}
