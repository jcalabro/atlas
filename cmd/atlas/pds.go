package main

import (
	"context"
	"time"

	"github.com/jcalabro/atlas/internal/env"
	"github.com/jcalabro/atlas/internal/foundation"
	"github.com/jcalabro/atlas/internal/pds"
	"github.com/urfave/cli/v3"
)

var (
	defaultSigningKeyPath = "./testdata/jwt-signing-key.pem"
	defaultServiceDID     = "did:web:dev.atlaspds.net"
	defaultHostname       = "dev.atlaspds.net"
	defaultUserDomains    = []string{".dev.atlaspds.net"}
)

func init() {
	if env.IsProd() {
		defaultSigningKeyPath = ""
		defaultServiceDID = ""
		defaultHostname = ""
		defaultUserDomains = []string{}
	}
}

func pdsCmd() *cli.Command {
	return &cli.Command{
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
			&cli.StringFlag{
				Name:  "jwt-signing-key",
				Usage: "Path to EC private key file for signing JWTs (PEM format)",
				Value: defaultSigningKeyPath,
			},
			&cli.StringFlag{
				Name:  "service-did",
				Usage: "DID of this PDS service (used as 'aud' claim in JWTs)",
				Value: defaultServiceDID,
			},
			&cli.StringFlag{
				Name:  "hostname",
				Usage: "Public hostname of this PDS server (used in well-known endpoints)",
				Value: defaultHostname,
			},
			&cli.StringSliceFlag{
				Name:  "user-domains",
				Usage: "List of domains on which users are allowed to signup",
				Value: defaultUserDomains,
			},
			&cli.StringFlag{
				Name:  "contact-email",
				Usage: "Contact email for the server admin",
			},
			&cli.StringFlag{
				Name:  "privacy-policy",
				Usage: "Link to the privacy policy document",
			},
			&cli.StringFlag{
				Name:  "terms-of-service",
				Usage: "Link to the terms of service document",
			},
		),
		Action: func(ctx context.Context, c *cli.Command) error {
			return pds.Run(ctx, &pds.Args{
				Addr:           c.String("addr"),
				MetricsAddr:    c.String("metrics-addr"),
				ReadTimeout:    c.Duration("read-timeout"),
				WriteTimeout:   c.Duration("write-timeout"),
				PLCURL:         c.String("plc"),
				JWTSigningKey:  c.String("jwt-signing-key"),
				ServiceDID:     c.String("service-did"),
				Hostname:       c.String("hostname"),
				UserDomains:    c.StringSlice("user-domains"),
				ContactEmail:   c.String("contact-email"),
				PrivacyPolicy:  c.String("privacy-policy"),
				TermsOfService: c.String("terms-of-service"),
				FDB: foundation.Config{
					ClusterFile: c.String("fdb-cluster-file"),
					APIVersion:  c.Int("fdb-api-version"),
				},
			})
		},
	}
}
