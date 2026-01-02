# CLAUDE.md

@README.md

## Relevant Build Commands

```bash
$ just --list
Available recipes:
    build-protos       # Generates protobuf sources
    cover              # run `just test` first, then run this to view test coverage
    default            # Lints and runs all tests
    down               # Tears down the local development dependencies
    fdbcli             # Connects to the local foundationdb developement server
    install-tools      # Ensures that all tools required for local development are installed. Before running, ensure you have go installed as well as protoc
    lint *ARGS="./..." # Lints the code
    r *ARGS            # Builds and runs the Go executable
    run *ARGS          # Builds and runs the Go executable with the race detector enabled
    t *ARGS="./..."    # Runs the tests
    test *ARGS="./..." # Runs the tests with the race detector enabled
    up                 # Stands up local development dependencies in docker
```

Typically, to test, you should run `just`. This will run the linter and all tests with the race detector enabled.

To build and run a go executable, you should typically run `just run pds`.

All commands should use the `justfile`. It is quite rare to need to reach directly for the `go` compiler, or other tools. The `justfile` is designed to be comprehensive.

To update protobuf types, edit the `.proto` file, then run `just build-protos` to build the go source files.

## Main Components

- **cmd/atlas/main.go**: CLI entry point using urfave/cli with the following commands:
  - `pds`: Runs the PDS webserver
  - `--help`: Displays the help command and exits
- **internal/at/**: Functions and types that allow us to interface with ATProto
- **internal/env/**: Environment configuration and version handling
- **internal/foundation/**: FoundationDB client initialization and configuration
- **internal/metrics/**: Prometheus metrics and OpenTelemetry tracing setup
- **internal/pds/**: HTTP server that implements the ATProto PDS XRPC interface
- **internal/types/**: Protobuf generated types

## Key Patterns

- **Observability**: All components use structured logging (slog), OpenTelemetry tracing, and Prometheus metrics
- **Signal handling**: Graceful shutdown on SIGINT/SIGTERM to allow in-progress queries to finish
- **Error handling**: Wrapped errors with context via `fmt.Errorf`. Errors must be handled properly and robustly. Never use `panic`.
- **Testing**: The code that we write should be well tested
    - When writing a new feature, don't ask me if tests should be writte; just write them
    - The tests should be thorough enough that we have reasonably high confidence in them, but not so aggressive to be "overfit"
    - Tests should use `t.Parallel()` wherever possible for performance and to better attempt to detect race conditions
    - Declare a `ctx := t.Context()` at the top of each test function that needs a context object and use that throughout. Don't use `context.Background()` if you can avoid it.
    - Prefer `require` over `assert` from the testify library

## Style

- We write relatively basic, straightforward go. We try not to get fancy.
- We don't add too many comments, especially in the middle of functions. However, we always document user-facing APIs with godoc style comments (i.e. public types we expect others to import)
- Comments that occur in the middle of functions should be `// lower case` unless they `// Are mutliple sentences. Like this.`
- Always use `any` instead of `interface{}`. There is a linter rule to validate this.

## Development Setup

Requires:
- Go 1.25
- FoundationDB 7.3.63 clients installed locally
- Docker for local FoundationDB cluster
  - Note that the docker-based foundation cluster only works on Linux. On macOS (the other supported development platform), developers must install and run FoundationDB manually.
