FROM rust:1.76-bookworm AS build

RUN apt-get update
RUN apt-get install protobuf-compiler -y

WORKDIR /source-sink
COPY ./ ./

# build for release
RUN cargo build --release

# our final base
FROM debian:bookworm AS simple-source

# copy the build artifact from the build stage
COPY --from=build /source-sink/target/release/source-sink .

# set the startup command to run your binary
CMD ["./source-sink"]