# CLAUDE.md

## Relevant Build Commands

```bash
just                 # Default: lint and test
just up              # Start FoundationDB cluster (Docker)
just down            # Stop FoundationDB cluster
just lint            # Run golangci-lint
just test            # Run tests with race detector
just t               # Run tests without race detector
just t ./internal/ingester/...  # Run tests for specific package
just run atlas server   # Run with race detector
just r atlas server     # Run without race detector
just build-protos    # Generate protobuf sources with buf
just fdbcli          # Connect to local FoundationDB CLI
```

## Architecture

Atlas is a Go backend for building AT Protocol AppViews such as Bluesky, using FoundationDB as its storage layer. Atlas is a database that allows you to build production-grade ATProto apps that are fast and scalable quickly, easily, and securely.

## Main Components

- **cmd/atlas/main.go**: CLI entry point using urfave/cli with two commands:
  - `server`: Primary ConnectRPC server to which clients will connect and send queries
  - `ingester`: Tap websocket consumer for atproto events that come off the PDS and firehose, as well as private data from the open source atproto compoenent "bsync" (bsync integration not yet implemented)
- **internal/foundation/**: FoundationDB client initialization and configuration
- **internal/ingester/**: Consumes TAP messages from ATProto, processes `IdentityEvent` and `RecordEvent` types
- **internal/server/**: HTTP server with ConnectRPC endpoints (WIP)
- **internal/metrics/**: Prometheus metrics and OpenTelemetry tracing setup
- **internal/env/**: Environment configuration and version handling

## Key Patterns

- **Observability**: All components use structured logging (slog), OpenTelemetry tracing, and Prometheus metrics
- **Signal handling**: Graceful shutdown on SIGINT/SIGTERM to allow in-progress queries to finish
- **Error handling**: Wrapped errors with context via `fmt.Errorf`. Errors must be handled properly and robustly.

## Style

- We write relatively basic, straightforward go. We don't try to get too fancy.
- We don't add too many comments, especially in the middle of functions. However, we always document user-facing APIs with godoc style comments (i.e. public types we expect others to import)

## Development Setup

Requires:
- Go 1.25
- FoundationDB 7.3.63 clients installed locally
- Docker for local FoundationDB cluster
  - Note that the docker-based foundation cluster only works on Linux. On macOS (the other supported development platform), developers must install and run FoundationDB manually.

## Linting

Uses golangci-lint v2 with:
- `paralleltest` enabled - tests must use `t.Parallel()`
- `gocritic` with all checks enabled
- Type assertion error checking required
- `interface{}` auto-rewritten to `any`
