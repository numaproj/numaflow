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

RUN apt-get update
RUN apt-get install protobuf-compiler -y

RUN cargo new numaflow
# Create a new empty shell project
WORKDIR /numaflow

RUN cargo new servesink
COPY ./rust/servesink/Cargo.toml ./servesink/

RUN cargo new backoff
COPY ./rust/backoff/Cargo.toml ./backoff/

RUN cargo new numaflow-models
COPY ./rust/numaflow-models/Cargo.toml ./numaflow-models/

RUN cargo new monovertex
COPY ./rust/monovertex/Cargo.toml ./monovertex/

RUN cargo new serving
COPY ./rust/serving/Cargo.toml ./serving/Cargo.toml

# Copy all Cargo.toml and Cargo.lock files for caching dependencies
COPY ./rust/Cargo.toml ./rust/Cargo.lock ./

# Build to cache dependencies
RUN mkdir -p src/bin && echo "fn main() {}" > src/bin/main.rs && \
    cargo build --workspace --all --release

# Copy the actual source code files of the main project and the subprojects
COPY ./rust/src ./src
COPY ./rust/servesink/src ./servesink/src
COPY ./rust/backoff/src ./backoff/src
COPY ./rust/numaflow-models/src ./numaflow-models/src
COPY ./rust/serving/src ./serving/src
COPY ./rust/monovertex/src ./monovertex/src
COPY ./rust/monovertex/build.rs ./monovertex/build.rs
COPY ./rust/monovertex/proto ./monovertex/proto

# Build the real binaries
RUN touch src/bin/main.rs && \
    cargo build --workspace --all --release

####################################################################################################
# numaflow
####################################################################################################
ARG BASE_IMAGE
FROM debian:bookworm as numaflow

# Install necessary libraries
RUN apt-get update && apt-get install -y libssl3

COPY --from=base /bin/numaflow /bin/numaflow
COPY ui/build /ui/build

COPY --from=extension-base /numaflow/target/release/numaflow /bin/numaflow-rs
COPY ./rust/serving/config config

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