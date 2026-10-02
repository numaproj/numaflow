#!/usr/bin/env bash
#
# End-to-end smoke test for nfcli against real Rust SDK example servers.
#
# NOT wired to CI initially (spinning up SDK example servers is slow and network-dependent).
# Run manually from the `rust/` directory:
#
#     ./numaflow-cli/e2e.sh
#
# It builds nfcli, serves a handful of numaflow-rs example UDFs over UDS sockets in temp dirs,
# runs the subcommand matrix, and asserts on the JSONL output.
#
# Requires: cargo, a checkout of numaproj/numaflow-rs at the pinned rev (SDK_DIR below), and
# the examples buildable locally.

set -euo pipefail

# Point this at a local numaflow-rs checkout (matching the pinned rev in numaflow-core/Cargo.toml).
SDK_DIR="${SDK_DIR:-$HOME/numaflow-rs}"

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d)"
PIDS=()

cleanup() {
    for pid in "${PIDS[@]:-}"; do
        kill "$pid" 2>/dev/null || true
    done
    rm -rf "$WORK"
}
trap cleanup EXIT

echo "==> Building nfcli"
( cd "$ROOT" && cargo build -p numaflow-cli )
NFCLI="$ROOT/target/debug/nfcli"

# Serve one SDK example over a UDS socket in its own temp dir; returns the socket path.
serve_example() {
    local example="$1"
    local sock_dir="$WORK/$example"
    mkdir -p "$sock_dir"
    # The example binaries default to /var/run/numaflow paths; override via env the examples read,
    # or rely on the SDK's default. Most examples let you set the socket via NUMAFLOW_* — adjust
    # per example as needed. Here we assume the example honors a socket dir override.
    ( cd "$SDK_DIR" && NUMAFLOW_UDS_SOCK_DIR="$sock_dir" cargo run --example "$example" >"$sock_dir/log" 2>&1 ) &
    PIDS+=("$!")
    echo "$sock_dir"
}

fail() { echo "FAIL: $*" >&2; exit 1; }

# ---- map (cat) ----
echo "==> map-cat"
MAP_DIR="$(serve_example map-cat)"
sleep 2
cat >"$WORK/map.yaml" <<'YAML'
payload: hello
keys: [k1]
YAML
OUT="$("$NFCLI" map --socket "$MAP_DIR/map.sock" -f "$WORK/map.yaml" -o json)"
echo "$OUT" | grep -q '"type":"result"' || fail "map produced no result: $OUT"
echo "$OUT" | grep -q '"type":"summary"' || fail "map produced no summary"

# ---- reduce (counter) ----
echo "==> reduce-counter"
RED_DIR="$(serve_example reduce-counter)"
sleep 2
cat >"$WORK/reduce.yaml" <<'YAML'
payload: "1"
keys: [k1]
eventTime: "+1s"
---
payload: "2"
keys: [k1]
eventTime: "+2s"
YAML
OUT="$("$NFCLI" reduce --socket "$RED_DIR/reduce.sock" -f "$WORK/reduce.yaml" \
    --window fixed --length 60s -o json)" || fail "reduce failed"
echo "$OUT" | grep -q '"type":"result"' || fail "reduce produced no result: $OUT"

# ---- sink (log) ----
echo "==> sink-log"
SINK_DIR="$(serve_example sink-log)"
sleep 2
OUT="$("$NFCLI" sink --socket "$SINK_DIR/sink.sock" --payload hello -o json)" || fail "sink failed"
echo "$OUT" | grep -q '"type":"summary"' || fail "sink produced no summary"

echo "==> ALL PASSED"
