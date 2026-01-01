package main

import (
	"context"
	"time"

	"github.com/jcalabro/atlas/internal/foundation"
	"github.com/jcalabro/atlas/internal/pds"
	"github.com/urfave/cli/v3"
)

var (
	pdsCmd = &cli.Command{
		Name:        "pds",
		Description: "Run the PDS server",
		Flags: append(fdbFlags,
			&cli.StringFlag{
				Name:  "addr",
				Usage: "Bind address of the primary HTTP server",
				Value: "0.0.0.0:8080",
			},
			&cli.StringFlag{
				Name:  "metrics-addr",
				Usage: "Bind address of the metrics/pprof HTTP server (empty string to disable)",
				Value: "0.0.0.0:6060",
			},
			&cli.DurationFlag{
				Name:  "read-timeout",
				Usage: "Primary HTTP server read timeout",
				Value: time.Minute,
			},
			&cli.DurationFlag{
				Name:  "write-timeout",
				Usage: "Primary HTTP server write timeout",
				Value: time.Minute,
			},
			&cli.StringFlag{
				Name:  "plc",
				Usage: "URL of the PLC server to use",
				Value: "https://plc.directory",
			},
		),
		Action: func(ctx context.Context, c *cli.Command) error {
			return pds.Run(ctx, &pds.Args{
				Addr:         c.String("addr"),
				MetricsAddr:  c.String("metrics-addr"),
				ReadTimeout:  c.Duration("read-timeout"),
				WriteTimeout: c.Duration("write-timeout"),
				PLCAddr:      c.String("plc"),
				FDB: foundation.Config{
					ClusterFile: c.String("fdb-cluster-file"),
					APIVersion:  c.Int("fdb-api-version"),
				},
			})
		},
	}
)
