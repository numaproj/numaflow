#!/bin/bash

set -xeuo pipefail

# Builds static rust binaries for both aarch64 and amd64
# Intended for building Linux binries from Mac OS host in a docker container.
# Usage: (from root directory of numaflow repo)
# docker run -v ./rust/:/app/ -w /app --rm ubuntu:24.04 bash build.sh

# Detect the host machine architecture
ARCH=$(uname -m)

apt update && apt install -y curl protobuf-compiler build-essential

if [ ! -f "$HOME/.cargo/env" ]; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
fi
. "$HOME/.cargo/env"

# Function to build for aarch64
build_aarch64() {
    apt install -y gcc-aarch64-linux-gnu
    sed -i "/^targets = \[/d" rust-toolchain.toml
    RUSTFLAGS='-C target-feature=+crt-static -C linker=aarch64-linux-gnu-gcc' cargo build --release --target aarch64-unknown-linux-gnu
    sed -i "/targets = \['aarch64-unknown-linux-gnu'\]/d" rust-toolchain.toml
}

# Function to build for x86_64
build_x86_64() {
    apt install -y gcc-x86-64-linux-gnu
    sed -i "/^targets = \[/d" rust-toolchain.toml
    echo "targets = ['x86_64-unknown-linux-gnu']" >> rust-toolchain.toml
    RUSTFLAGS='-C target-feature=+crt-static -C linker=x86_64-linux-gnu-gcc' cargo build --release --target x86_64-unknown-linux-gnu
    sed -i "/targets = \['x86_64-unknown-linux-gnu'\]/d" rust-toolchain.toml
}

build_aarch64
build_x86_64
