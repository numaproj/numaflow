# Use multi-stage builds to keep the final image small
# Use an official Rust image for the build stage
FROM rust:1.79-bookworm as builder

RUN curl -L --proto '=https' --tlsv1.2 -sSf https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash

RUN cargo new serve
# Create a new empty shell project
WORKDIR /serve
RUN cargo new servesink
COPY ./servesink/Cargo.toml ./servesink/

RUN cargo new extras/upstreams
COPY ./extras/upstreams/Cargo.toml ./extras/upstreams/

RUN cargo new backoff
COPY ./backoff/Cargo.toml ./backoff/Cargo.toml

# Copy all Cargo.toml and Cargo.lock files for caching dependencies
COPY ./Cargo.toml ./Cargo.lock ./

# Build only the dependencies to cache them
RUN cargo build --release

# Copy the actual source code files of the main project and the subprojects
COPY ./src ./src
COPY ./servesink/src ./servesink/src
COPY ./extras/upstreams/src ./extras/upstreams/src
COPY ./backoff/src ./backoff/src

# Build the real binaries
RUN touch src/main.rs servesink/main.rs extras/upstreams/main.rs && \
    cargo build --release

# Use a lightweight image for the runtime
FROM gcr.io/distroless/cc-debian12 as numaflow-ext

COPY --from=builder /serve/target/release/serve .
COPY ./config config


ENTRYPOINT ["./serve"]