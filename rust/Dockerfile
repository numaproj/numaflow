# Use multi-stage builds to keep the final image small
# Use an official Rust image for the build stage
FROM rust:1.79-bookworm as builder

RUN curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash

RUN apt-get update
RUN apt-get install protobuf-compiler -y

RUN cargo new numaflow
# Create a new empty shell project
WORKDIR /numaflow

RUN cargo new servesink
COPY ./servesink/Cargo.toml ./servesink/Cargo.toml

RUN cargo new backoff
COPY ./backoff/Cargo.toml ./backoff/Cargo.toml

RUN cargo new numaflow-models
COPY ./numaflow-models/Cargo.toml ./numaflow-models/Cargo.toml

RUN cargo new monovertex
COPY monovertex/Cargo.toml ./monovertex/Cargo.toml

RUN cargo new serving
COPY ./serving/Cargo.toml ./serving/Cargo.toml

# Copy all Cargo.toml and Cargo.lock files for caching dependencies
COPY ./Cargo.toml ./Cargo.lock ./

# Build to cache dependencies
RUN mkdir -p src/bin && echo "fn main() {}" > src/bin/main.rs && \
    cargo build --workspace --all --release

# Copy the actual source code files of the main project and the subprojects
COPY ./src ./src
COPY ./servesink/src ./servesink/src
COPY ./backoff/src ./backoff/src
COPY ./numaflow-models/src ./numaflow-models/src
COPY ./serving/src ./serving/src
COPY monovertex/src ./monovertex/src
COPY monovertex/build.rs ./monovertex/build.rs
COPY monovertex/proto ./monovertex/proto

# Build the real binaries
RUN touch src/bin/main.rs && \
    cargo build --workspace --all --release

# Use a lightweight image for the runtime
FROM debian:bookworm as numaflow-ext

RUN apt-get update && apt-get install -y libssl3

COPY --from=builder /numaflow/target/release/ .
COPY serving/config config

ENTRYPOINT ["./numaflow"]