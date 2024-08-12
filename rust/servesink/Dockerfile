FROM rust:1.79 as build

RUN apt-get update
RUN apt-get install protobuf-compiler -y

# create a new empty shell project
WORKDIR /servesink

# copy your source tree
COPY src ./src

COPY ./Cargo.toml ./Cargo.toml

# build for release
RUN cargo build --release

# our final base
FROM debian:bookworm

RUN apt-get update \
  && apt-get install -y openssl

# copy the build artifact from the build stage
COPY --from=build /servesink/target/release/servesink .

# set the startup command to run your binary
CMD ["./servesink"]
