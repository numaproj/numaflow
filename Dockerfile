ARG BASE_IMAGE=gcr.io/distroless/cc-debian12
ARG ARCH=$TARGETARCH
####################################################################################################
# base
####################################################################################################
FROM debian:bullseye as base
ARG ARCH

COPY dist/numaflow-linux-${ARCH} /bin/numaflow

RUN chmod +x /bin/numaflow

####################################################################################################
# extension base
####################################################################################################
FROM rust:1.79-bookworm as extension-base

# For faster/easier installation of Rust binaries
RUN curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash

RUN apt-get update \
  && apt-get install -y protobuf-compiler

RUN cargo new /serve
WORKDIR /serve
COPY ./serving/Cargo.toml .

RUN cargo new extras/upstreams
COPY ./serving/extras/upstreams/Cargo.toml extras/upstreams/Cargo.toml

RUN cargo new servesink
COPY ./serving/servesink/Cargo.toml servesink/Cargo.toml

RUN cargo new backoff
COPY serving/backoff/Cargo.toml backoff/Cargo.toml

RUN cargo build --release

COPY ./serving/ /serve
# update timestamps to force a new build
RUN touch src/main.rs servesink/src/main.rs extras/upstreams/src/main.rs

RUN --mount=type=cache,target=/usr/local/cargo/registry cargo build --release

COPY --from=base /bin/numaflow /bin/numaflow

RUN chmod +x /serve/target/release/serve

####################################################################################################
# numaflow
####################################################################################################
ARG BASE_IMAGE
FROM ${BASE_IMAGE} as numaflow

COPY --from=extension-base /bin/numaflow /bin/numaflow
COPY ui/build /ui/build

COPY --from=extension-base /serve/target/release/serve /bin/serve
COPY ./serving/config config

ENTRYPOINT [ "/bin/numaflow" ]

####################################################################################################
# testbase
####################################################################################################
FROM alpine:3.17 as testbase
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