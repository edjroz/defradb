# Multi-stage build for the crossdevice benchnode.
#
# NOTE: the node pulls in lens/host-go, whose Default() runtime is wasmtime — a
# CGO library (wasmtime-go bundles a static libwasmtime.a but still needs cgo +
# a C compiler, and links glibc dynamically). So this is a CGO_ENABLED=1 build on
# a glibc base; a static/musl (alpine, scratch) image will NOT work.
#
# The build context MUST be the repo root (the Go module needs the full tree):
#   docker build -f tests/bench/crossdevice/docker/Dockerfile -t benchnode:local .
# (compose-run.sh does this for you.)

# Pin the builder to bookworm so its glibc matches the bookworm-slim runtime
# (the default golang:1.25 tag is debian trixie, whose newer glibc the slim
# runtime lacks → "GLIBC_2.39 not found" at start).
FROM golang:1.25-bookworm AS build
WORKDIR /src
# Cache module downloads independently of source changes.
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# golang image ships gcc; CGO is required for the wasmtime lens runtime.
RUN CGO_ENABLED=1 go build -trimpath -o /benchnode ./tests/bench/crossdevice/cmd/benchnode

FROM debian:bookworm-slim
# wget for the compose healthcheck; ca-certificates is harmless and future-proof.
# glibc matches the bookworm builder, so the cgo-linked binary runs as-is.
RUN apt-get update \
    && apt-get install -y --no-install-recommends wget ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /benchnode /usr/local/bin/benchnode
# control API + libp2p (both bound to 0.0.0.0 inside the container)
EXPOSE 17700 9300
ENTRYPOINT ["benchnode"]
