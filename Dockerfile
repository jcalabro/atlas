FROM golang:1.25.5-bookworm AS builder

# Install FoundationDB client libraries and git
RUN apt-get update \
    && apt-get install -y --no-install-recommends git \
    && wget -O /tmp/fdb-clients.deb https://github.com/apple/foundationdb/releases/download/7.3.63/foundationdb-clients_7.3.63-1_amd64.deb \
    && dpkg -i /tmp/fdb-clients.deb \
    && rm /tmp/fdb-clients.deb \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Download dependencies first for better layer caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source (including .git for version) and build
COPY . .
RUN CGO_ENABLED=1 go build -ldflags="-X github.com/jcalabro/atlas/internal/env.Version=$(git rev-parse --short HEAD)" -o /atlas ./cmd/atlas

# Runtime image
FROM debian:bookworm-slim

# Install FoundationDB client libraries and CA certificates
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates wget \
    && wget -O /tmp/fdb-clients.deb https://github.com/apple/foundationdb/releases/download/7.3.63/foundationdb-clients_7.3.63-1_amd64.deb \
    && dpkg -i /tmp/fdb-clients.deb \
    && rm /tmp/fdb-clients.deb \
    && apt-get purge -y wget \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /atlas /usr/local/bin/atlas

ENTRYPOINT ["atlas"]
CMD ["pds"]
