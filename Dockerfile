ARG BASE_IMAGE=scratch
ARG ARCH=$TARGETARCH
####################################################################################################
# base
####################################################################################################
FROM alpine:3.17 AS base
ARG ARCH
RUN apk update && apk upgrade && \
    apk add ca-certificates && \
    apk --no-cache add tzdata

COPY dist/numaflow-linux-${ARCH} /bin/numaflow
COPY dist/numaflow-rs-linux-${ARCH} /bin/numaflow-rs

RUN chmod +x /bin/numaflow
RUN chmod +x /bin/numaflow-rs

####################################################################################################
# Rust binary
####################################################################################################
FROM lukemathwalker/cargo-chef:latest-rust-1.80 AS chef
ARG TARGETPLATFORM
WORKDIR /numaflow
RUN apt-get update && apt-get install -y protobuf-compiler


FROM chef AS planner
COPY ./rust/ .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS rust-builder
ARG TARGETPLATFORM
ARG ARCH
COPY --from=planner /numaflow/recipe.json recipe.json

# Build to cache dependencies
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    case ${TARGETPLATFORM} in \
        "linux/amd64") TARGET="x86_64-unknown-linux-gnu" ;; \
        "linux/arm64") TARGET="aarch64-unknown-linux-gnu" ;; \
    *) echo "Unsupported platform: ${TARGETPLATFORM}" && exit 1 ;; \
    esac && \
    RUSTFLAGS='-C target-feature=+crt-static' cargo chef cook --workspace --release --target ${TARGET} --recipe-path recipe.json

# Copy the actual source code files of the main project and the subprojects
COPY ./rust/ .

# Build the real binaries
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    case ${TARGETPLATFORM} in \
        "linux/amd64") TARGET="x86_64-unknown-linux-gnu" ;; \
        "linux/arm64") TARGET="aarch64-unknown-linux-gnu" ;; \
    *) echo "Unsupported platform: ${TARGETPLATFORM}" && exit 1 ;; \
    esac && \
    RUSTFLAGS='-C target-feature=+crt-static' cargo build --workspace --all --release --target ${TARGET} && \
    cp -pv target/${TARGET}/release/numaflow /root/numaflow

####################################################################################################
# numaflow
####################################################################################################
ARG BASE_IMAGE
FROM ${BASE_IMAGE} AS numaflow
ARG ARCH

COPY --from=base /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=base /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=base /bin/numaflow /bin/numaflow
COPY --from=base /bin/numaflow-rs /bin/numaflow-rs
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