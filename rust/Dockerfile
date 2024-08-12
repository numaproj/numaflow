# Use multi-stage builds to keep the final image small
# Use an official Rust image for the build stage
FROM rust:1.79-bookworm as builder

RUN curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash

RUN apt-get update
RUN apt-get install protobuf-compiler -y

RUN cargo new serve
# Create a new empty shell project
WORKDIR /serve
RUN cargo new servesink
COPY ./servesink/Cargo.toml ./servesink/


RUN cargo new backoff
COPY ./backoff/Cargo.toml ./backoff/Cargo.toml

RUN cargo new numaflow-models
COPY ./numaflow-models/Cargo.toml ./numaflow-models/

RUN cargo new source-sink
COPY ./source-sink/Cargo.toml ./source-sink/Cargo.toml

# Copy all Cargo.toml and Cargo.lock files for caching dependencies
COPY ./Cargo.toml ./Cargo.lock ./

# Build only the dependencies to cache them
RUN cargo build --release

# Copy the actual source code files of the main project and the subprojects
COPY ./src ./src
COPY ./servesink/src ./servesink/src
COPY ./backoff/src ./backoff/src
COPY ./numaflow-models/src ./numaflow-models/src
COPY ./source-sink/src ./source-sink/src
COPY ./source-sink/build.rs ./source-sink/build.rs
COPY ./source-sink/proto ./source-sink/proto

# Build the real binaries
RUN touch src/main.rs servesink/src/main.rs numaflow-models/src/main.rs source-sink/src/main.rs && \
    cargo build --workspace --all --release

# Use a lightweight image for the runtime
FROM debian:bookworm as numaflow-ext

RUN apt-get update && apt-get install -y libssl3

COPY --from=builder /serve/target/release/ .
COPY serving/config config

ENTRYPOINT ["./serve"]