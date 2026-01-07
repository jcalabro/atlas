set dotenv-required
set shell := ["bash", "-cu"]

# Lints and runs all tests with the race detector enabled
default: lint test

# Ensures that all tools required for local development are installed. Before running, ensure you have go installed as well as protoc
install-tools:
    go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.7.2

    go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.36.6
    go install github.com/bufbuild/buf/cmd/buf@v1.54.0
    go install connectrpc.com/connect/cmd/protoc-gen-connect-go@v1.18.1

# Stands up local development dependencies in docker
up:
    #!/usr/bin/env bash
    docker compose up -d

    if ! fdbcli -C foundation.cluster --exec status --timeout 1 ; then
        if ! fdbcli -C foundation.cluster --exec "configure new single ssd-redwood-1 ; status" --timeout 10 ; then
            echo "Unable to configure new FDB cluster."
            exit 1
        fi
    fi

    just init-garage

    echo "development environment is ready"

# Initializes the garage S3-compatible storage (called automatically by `up`)
init-garage:
    #!/usr/bin/env bash
    set -e

    # hardcoded dev credentials (never use in production)
    KEY_ID="GK000000000000000000000000"
    SECRET_KEY="0000000000000000000000000000000000000000000000000000000000000000"

    # wait for garage to be ready
    for i in {1..30}; do
        if docker exec atlas-garage /garage status &>/dev/null; then
            break
        fi
        sleep 0.5
    done

    # configure layout if needed
    if docker exec atlas-garage /garage layout show 2>&1 | grep -q "No nodes"; then
        NODE_ID=$(docker exec atlas-garage /garage status 2>/dev/null | grep -oE '[a-f0-9]{16}' | head -1)
        docker exec atlas-garage /garage layout assign -z dc1 -c 1G "$NODE_ID"
        docker exec atlas-garage /garage layout apply --version 1
    fi

    # import dev key and create bucket if needed
    if ! docker exec atlas-garage /garage key info atlas-dev &>/dev/null; then
        docker exec atlas-garage /garage key import -n atlas-dev --yes "$KEY_ID" "$SECRET_KEY"
    fi

    if ! docker exec atlas-garage /garage bucket info blobs &>/dev/null; then
        docker exec atlas-garage /garage bucket create blobs
        docker exec atlas-garage /garage bucket allow --read --write --owner blobs --key atlas-dev
    fi

    echo "garage S3 ready - endpoint: http://localhost:3900  bucket: blobs"
    echo "  access_key: $KEY_ID"
    echo "  secret_key: $SECRET_KEY"

# Tears down the local development dependencies
down:
    docker compose down --remove-orphans

# Lints the code
lint *ARGS="./...":
    golangci-lint run --timeout 1m {{ARGS}}

# Builds and runs the Go executable
r *ARGS:
    go run ./cmd/atlas {{ARGS}}

# Builds and runs the Go executable with the race detector enabled
run *ARGS:
    go run -race ./cmd/atlas {{ARGS}}

# Runs the tests
t *ARGS="./...":
    go test -count=1 -covermode=atomic -coverprofile=test-coverage.out {{ARGS}}

# Runs the tests with the race detector enabled
test *ARGS="./...":
    just t -race {{ARGS}}

# run `just test` first, then run this to view test coverage
cover:
    go tool cover -html coverage.out

# Connects to the local foundationdb developement server
fdbcli:
    fdbcli -C foundation.cluster

# Generates protobuf sources
build-protos:
    #!/usr/bin/env bash
    set +x

    pushd internal > /dev/null

    # generate, then clean up the protos for all connect services
    for PKG in types; do
        pushd $PKG > /dev/null

        buf lint
        buf generate

        if [[ -e "${PKG}connect/${PKG}.connect.go" ]]; then
            mv "${PKG}connect/${PKG}.connect.go" .
            rmdir "${PKG}connect"

            # remove unnecessary/broken import
            sed -i.bak "/^\t${PKG} \"/d" "${PKG}.connect.go"

            # remove qualified names from that removed import, but ignore ones
            # that are preceeded by a slash character (i.e. "/agent.Service/Ping")
            sed -i.bak "s/\([^/]\)${PKG}\./\1/g; s/^${PKG}\.//" "${PKG}.connect.go"

            # move the generated code to our top level package
            sed -i.bak "s/package ${PKG}connect/package ${PKG}/" "${PKG}.connect.go"

            # remove package qualification, since it's all in our top level package
            sed -i.bak 's/__\.//g' "${PKG}.connect.go"

            # clean up .bak file. This was required to make sed in-place flag work the same on mac and linux
            rm "${PKG}.connect.go.bak"

            # run go fmt
            go fmt "${PKG}.connect.go" >/dev/null
        fi

        popd > /dev/null
    done

    go fmt ./...
    popd > /dev/null
