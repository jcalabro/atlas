# Atlas PDS

[![ci](https://github.com/jcalabro/atlas/actions/workflows/ci.yaml/badge.svg)](https://github.com/jcalabro/atlas/actions/workflows/ci.yaml)

A scalable, multi-tenant [Personal Data Server](https://atproto.com/guides/overview#personal-data-servers) (PDS) for the [AT Protocol](https://atproto.com/).

## Quick Start

### Prerequisites

- Go 1.25+
- Docker (for local development)
- [just](https://github.com/casey/just) command runner

Note that `just up` on macOS does not work because the FoundationDB docker images only run on x86_64 Linux. macOS users will need to run their own local development FDB cluster.

### Development Setup

```bash
# Run this once
just install-tools

# start local development dependencies
just up

# run the linter and all tests
just

# run the PDS server
just run pds

# list all commands
just --list
```

## Configuration

Atlas uses a TOML configuration file to define hosts and settings. See an example at [testdata/config.toml](https://github.com/jcalabro/atlas/blob/main/testdata/config.toml).
