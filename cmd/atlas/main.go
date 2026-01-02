package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

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
			pdsCmd(),
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
