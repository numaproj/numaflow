ARG BASE_IMAGE=scratch
ARG ARCH=$TARGETARCH
####################################################################################################
# base (host-built Go binary + CA/tz data)
####################################################################################################
FROM alpine:3.17 AS base
ARG ARCH
RUN apk update && apk upgrade && \
    apk add ca-certificates && \
    apk --no-cache add tzdata

COPY dist/numaflow-linux-${ARCH} /bin/numaflow
RUN chmod +x /bin/numaflow

####################################################################################################
# Rust binary
####################################################################################################
# Dependency reuse relies on BuildKit cache mounts below (not cargo-chef layer caching).
# ENABLE_MEMORY_PROFILING=true builds a dynamically linked glibc binary (required for LD_PRELOAD).
FROM rust:1.97.1-trixie AS rust-builder
ARG TARGETPLATFORM
ARG ENABLE_MEMORY_PROFILING=false
WORKDIR /numaflow
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler cmake clang \
    && rm -rf /var/lib/apt/lists/*

# Install the pinned toolchain so source-only changes do not re-download it.
COPY rust/rust-toolchain.toml ./rust-toolchain.toml
RUN rustup show

COPY ./rust/ .

ARG ARCH
ARG VERSION=latest
ENV VERSION=$VERSION
ARG BUILD_DATE
ENV BUILD_DATE=$BUILD_DATE
ARG GIT_COMMIT
ENV GIT_COMMIT=$GIT_COMMIT
ARG GIT_BRANCH
ENV GIT_BRANCH=$GIT_BRANCH
ARG GIT_TAG
ENV GIT_TAG=$GIT_TAG
ARG GIT_TREE_STATE
ENV GIT_TREE_STATE=$GIT_TREE_STATE
# Cargo profile: "release" (default, fat LTO) or "image-dev" (faster local builds).
ARG CARGO_PROFILE=release

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/numaflow/target \
    case ${TARGETPLATFORM} in \
        "linux/amd64") TARGET="x86_64-unknown-linux-gnu" ;; \
        "linux/arm64") TARGET="aarch64-unknown-linux-gnu" ;; \
    *) echo "Unsupported platform: ${TARGETPLATFORM}" && exit 1 ;; \
    esac && \
    if [ "${ENABLE_MEMORY_PROFILING}" = "true" ]; then \
        RUSTFLAGS_VAL=""; \
    else \
        RUSTFLAGS_VAL="-C target-feature=+crt-static"; \
    fi && \
    RUSTFLAGS="${RUSTFLAGS_VAL}" cargo build -p numaflow --profile ${CARGO_PROFILE} --target ${TARGET} && \
    case ${CARGO_PROFILE} in \
        "dev") OUT_DIR="debug" ;; \
        *) OUT_DIR="${CARGO_PROFILE}" ;; \
    esac && \
    cp -pv target/${TARGET}/${OUT_DIR}/numaflow /root/numaflow && \
    cp -pv target/${TARGET}/${OUT_DIR}/entrypoint /root/entrypoint

####################################################################################################
# bytehound (libbytehound.so) — built from source for amd64 and arm64
####################################################################################################
FROM rust:1.97.1-trixie AS bytehound-builder
ARG BYTEHOUND_REF=0.11.0
RUN apt-get update && apt-get install -y --no-install-recommends \
    git ca-certificates build-essential pkg-config \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
# Clone first so cargo picks up bytehound's rust-toolchain (pinned nightly).
RUN git clone --depth 1 --branch "${BYTEHOUND_REF}" https://github.com/koute/bytehound.git .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo build --release -p bytehound-preload \
    && cp -pv target/release/libbytehound.so /libbytehound.so

####################################################################################################
# numaflow (local / image-dev — Rust from rust-builder)
####################################################################################################
ARG BASE_IMAGE
FROM ${BASE_IMAGE} AS numaflow
ARG ARCH

COPY --from=base /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=base /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=base /bin/numaflow /bin/numaflow
COPY --from=rust-builder /root/numaflow /bin/numaflow-rs
COPY ui/build /ui/build

# TODO: remove this when we are ported everything to Rust
COPY --from=rust-builder /root/entrypoint /bin/entrypoint
ENTRYPOINT ["/bin/entrypoint"]

####################################################################################################
# numaflow-ci (GitHub Actions — host-built Rust binaries from dist/, skips rust-builder)
####################################################################################################
FROM base AS base-ci
ARG ARCH
COPY dist/numaflow-rs-linux-${ARCH} /bin/numaflow-rs
COPY dist/entrypoint-linux-${ARCH} /bin/entrypoint
RUN chmod +x /bin/numaflow-rs /bin/entrypoint

ARG BASE_IMAGE
FROM ${BASE_IMAGE} AS numaflow-ci
ARG ARCH

COPY --from=base-ci /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=base-ci /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=base-ci /bin/numaflow /bin/numaflow
COPY --from=base-ci /bin/numaflow-rs /bin/numaflow-rs
COPY ui/build /ui/build

# TODO: remove this when we are ported everything to Rust
COPY --from=base-ci /bin/entrypoint /bin/entrypoint
ENTRYPOINT ["/bin/entrypoint"]

####################################################################################################
# numaflow-memprofile (dynamic glibc + libbytehound.so for LD_PRELOAD profiling)
# Build with: --target numaflow-memprofile --build-arg ENABLE_MEMORY_PROFILING=true
####################################################################################################
FROM debian:trixie-slim AS numaflow-memprofile
ARG ARCH

COPY --from=base /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=base /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=base /bin/numaflow /bin/numaflow
COPY --from=rust-builder /root/numaflow /bin/numaflow-rs
COPY --from=rust-builder /root/entrypoint /bin/entrypoint
COPY --from=bytehound-builder /libbytehound.so /usr/share/libbytehound.so
COPY ui/build /ui/build

ENTRYPOINT ["/bin/entrypoint"]

####################################################################################################
# testbase
####################################################################################################
FROM alpine:3.17 AS testbase
RUN apk update && apk upgrade && \
    apk add ca-certificates && \
    apk --no-cache add tzdata

COPY dist/e2eapi /bin/e2eapi
RUN chmod +x /bin/e2eapi

####################################################################################################
# testapi
####################################################################################################
FROM scratch AS e2eapi
COPY --from=testbase /bin/e2eapi .
ENTRYPOINT ["/e2eapi"]
