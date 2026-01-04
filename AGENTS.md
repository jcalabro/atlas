# CLAUDE.md

@README.md

## Relevant Build Commands

```bash
$ just --list
Available recipes:
    build-protos       # Generates protobuf sources
    cover              # run `just test` first, then run this to view test coverage
    default            # Lints and runs all tests (race detector disabled)
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

Typically, to test, you should simply run `just`. This will run the linter and all tests with the race detector enabled.

To build and run a go executable, you should typically run `just run pds`. This runs the server with the race detector enabled.

All commands should use the `justfile`. It is quite rare to need to reach directly for the `go` compiler, or other tools. The `justfile` is designed to be comprehensive.

To update protobuf types, edit the `.proto` file, then run `just build-protos` to build the go source files.

## Main Components

- **cmd/atlas/main.go**: CLI entry point using urfave/cli with the following commands:
  - `pds`: Runs the PDS webserver
  - `--help`: Displays the help command and exits
- **internal/at/**: AT Protocol utilities, including AT URI parsing and construction
- **internal/env/**: Environment configuration and version handling
- **internal/foundation/**: FoundationDB client initialization and configuration. Think of this as an ORM for foundationdb.
- **internal/metrics/**: Prometheus metrics and OpenTelemetry tracing setup
- **internal/pds/**: HTTP server that implements the ATProto PDS XRPC interface. This is where the PDS business logic lives.
- **internal/plc/**: Implements a HTTP client for `plc.directory`, one of the main DID identity systems for atproto. Includes a mock client for tests.
- **internal/types/**: Protobuf generated types
- **internal/util**: Small utility helpers. We really try to avoid putting stuff in here as much as possible.

## System Design

Atlas PDS is designed as a robust, multi-tennant, high availability, high-scale PDS. It is written in go, uses foundationdb for storage, and is designed to be horizontally scalable (i.e. we will be running many PDS processes to form a single logical server).

We say it's multi-tennant because we are designing it to host multiple logical hostnames. Bluesky itself currently has about 100 real-world PDSes deployed, and we would like to re-home them all to a single Atlas PDS instance, but keep all the existing hostnames. This means that we frequently observe the HTTP `Host` header to inform query decisions. For instance, if a user is hosted on `a.atlaspds.net`, their data should never appear in queries to `b.atlaspds.net`. This will allow us to seamlessly migrate existing users from the existing PDS implementation to the new Atlas PDS.

We say it's high availability because the system is designed to be run with multiple pods. It's basically a stateless HTTP/XRPC/Websocket server, and we run many horizontally scalable instances of it. Because of this, it's important that we don't make any architecture decisions that limit our ability to horizontally scale, and all server instances must provide a consistent view of the data.

## Key Patterns

- **Observability**: All components use structured logging (slog), OpenTelemetry tracing, and Prometheus metrics
- **Signal handling**: Graceful shutdown on SIGINT/SIGTERM to allow in-progress queries to finish
- **Error handling**: Wrapped errors with context via `fmt.Errorf`. Errors must be handled properly and robustly. Never use `panic`.
- **Testing**: The code that we write should be well tested
    - When writing a new feature or major chunk of code, don't ask me if tests should be written; just write them
    - The tests should be thorough enough that we have reasonably high confidence in them, but not so aggressive to be "overfit"
    - Tests should use `t.Parallel()` wherever possible for performance and to better attempt to detect race conditions
    - Declare a `ctx := t.Context()` at the top of each test function that needs a context object and use that throughout. Don't use `context.Background()` if you can avoid it.
    - Prefer `require` over `assert` from the testify library
- **Database interactions**:
    - When we are using foundationdb, prefer writing maximally correct and resource effecient code over getting the most code reuse. It's better to do a single read transaction than have an N+1 query problem where we're calling some other helper function, which requires many transactions.

## Style

- We write relatively basic, straightforward go. We try not to get fancy.
- In general, we prefer not to add new library dependencies, though sometimes it's unavoidable. Please always check before attempting to add a library to the `go.mod` file. We are trying to keep our dependencies to a minimum.
- We don't add too many comments, especially in the middle of functions. However, we frequently document user-facing APIs with godoc style comments (i.e. public types we expect others to import) unless it's very obvious what the code is doing
- Comments that occur in the middle of functions should be `// lower case` unless they `// Are mutliple sentences. Like this.`
- Comments that are a single sentence should never end in a period
- Always use `any` instead of `interface{}`. There is a linter rule to validate this.
- Never use `*.bsky.social` domains/handles. Instead, use `*.dev.atlaspds.net`.
- Always use the new go for loop syntax `for i := range count`, or just `for range count`
