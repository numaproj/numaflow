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

RUN curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash

RUN cargo new serve
# Create a new empty shell project
WORKDIR /serve
RUN cargo new servesink
COPY ./serving/servesink/Cargo.toml ./servesink/

RUN cargo new extras/upstreams
COPY ./serving/extras/upstreams/Cargo.toml ./extras/upstreams/

RUN cargo new backoff
COPY ./serving/backoff/Cargo.toml ./backoff/Cargo.toml

# Copy all Cargo.toml and Cargo.lock files for caching dependencies
COPY ./serving/Cargo.toml ./serving/Cargo.lock ./

# Build only the dependencies to cache them
RUN cargo build --release

# Copy the actual source code files of the main project and the subprojects
COPY ./serving/src ./src
COPY ./serving/servesink/src ./servesink/src
COPY ./serving/extras/upstreams/src ./extras/upstreams/src
COPY ./serving/backoff/src ./backoff/src

# Build the real binaries
RUN touch src/main.rs servesink/main.rs extras/upstreams/main.rs && \
    cargo build --release

####################################################################################################
# numaflow
####################################################################################################
ARG BASE_IMAGE
FROM ${BASE_IMAGE} as numaflow

COPY --from=base /bin/numaflow /bin/numaflow
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