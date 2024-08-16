ARG BASE_IMAGE=gcr.io/distroless/cc-debian12
ARG ARCH=$TARGETARCH
####################################################################################################
# base
####################################################################################################
FROM debian:bullseye AS base
ARG ARCH

COPY dist/numaflow-linux-${ARCH} /bin/numaflow

RUN chmod +x /bin/numaflow

####################################################################################################
# Rust binary
####################################################################################################
FROM lukemathwalker/cargo-chef:latest-rust-1.80 AS chef
ARG TARGETPLATFORM
RUN apt-get update && apt-get install -y protobuf-compiler clang curl
WORKDIR /numaflow

FROM chef AS planner
COPY ./rust/ .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
ARG TARGETPLATFORM
ARG ARCH
COPY --from=planner /numaflow/recipe.json recipe.json
# Build dependencies - this is the caching Docker layer!
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    case ${TARGETPLATFORM} in \
      "linux/amd64") TARGET="x86_64-unknown-linux-gnu" ;; \
      "linux/arm64") TARGET="aarch64-unknown-linux-gnu" ;; \
      *) echo "Unsupported platform: ${TARGETPLATFORM}" && exit 1 ;; \
    esac && \
    RUSTFLAGS='-C target-feature=+crt-static' cargo chef cook --release --target ${TARGET} --recipe-path recipe.json

# Build application
COPY ./rust/ .
# RUN --mount=type=cache,target=/usr/local/cargo/registry RUSTFLAGS='-C target-feature=+crt-static' cargo build --release --target aarch64-unknown-linux-gnu
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    case ${TARGETPLATFORM} in \
      "linux/amd64") TARGET="x86_64-unknown-linux-gnu" ;; \
      "linux/arm64") TARGET="aarch64-unknown-linux-gnu" ;; \
      *) echo "Unsupported platform: ${TARGETPLATFORM}" && exit 1 ;; \
    esac && \
    RUSTFLAGS='-C target-feature=+crt-static' cargo build --release --target ${TARGET} && \
    cp -pv target/${TARGET}/release/numaflow /root/numaflow-rs-linux-${ARCH}

####################################################################################################
# numaflow
####################################################################################################
ARG BASE_IMAGE
FROM debian:bookworm AS numaflow
ARG ARCH

COPY dist/numaflow-linux-${ARCH} /bin/numaflow
COPY dist/numaflow-rs-linux-${ARCH} /bin/numaflow-rs
COPY ui/build /ui/build

COPY ./rust/serving/config config

ENTRYPOINT [ "/bin/numaflow" ]

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