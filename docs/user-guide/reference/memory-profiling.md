# Memory Profiling

## Numa container memory profiling

This section explores attaching [bytehound](https://github.com/koute/bytehound) (heap profiler via `LD_PRELOAD`) 
mem profiler to the numaflow-core process in the **`numa` main container** and captures a `.dat` for analysis offline

The stock numaflow image **cannot** be profiled as-is since the rust binary is built with `-C target-feature=+crt-static` 
(fully static → no dynamic loader) and the release image is `FROM scratch` (no `ld.so`/libc). 
Both make `LD_PRELOAD` silently do nothing. We need a *dynamic glibc* binary on a *glibc* base.

### Prerequisites

- bytehound prebuilt is **x86_64 only** (v0.11.0). Cluster nodes must be x86_64, or build bytehound from source for arm64. 
- kubectl + permission to edit the numaflow controller Deployment (`numaflow-system`).
- docker/buildx to build & push the numaflow image.

### Step 1 — Get `libbytehound.so`

Download `bytehound-x86_64-unknown-linux-gnu.tgz` from [link](https://github.com/koute/bytehound/releases) and extract. 
We get `libbytehound.so` (goes into the image) and `bytehound` (the CLI/server, run locally to analyze).

### Step 2 — Build a *profilable* numaflow image

Edit `numaflow/Dockerfile`:

- Remove `-C target-feature=+crt-static` from **both** `RUSTFLAGS='...'` lines (the `cargo chef cook`
  line and the `cargo build` line) → dynamic glibc binary.
- Add `COPY libbytehound.so /usr/share/libbytehound.so` into the final `numaflow` stage (drop
  `libbytehound.so` at the build context root first). Use the same path for `LD_PRELOAD` in Step 4.

Edit `numaflow/Makefile`:

- Builder is `lukemathwalker/cargo-chef:latest-rust-1.95` = Debian **trixie** (glibc 2.41). 
  Make the final `numaflow` stage base glibc **and ≥ the builder's glibc**. 
- If using `make image` update `DEV_BASE_IMAGE` to `debian:trixie-slim`
- Otherwise, if using `make image-multi` update `RELEASE_BASE_IMAGE` to `debian:trixie-slim`


Verify it's dynamic (in the rust-builder stage or a throwaway container):

```bash
ldd /bin/numaflow-rs      # must list libc.so.6, NOT "not a dynamic executable"
```

### Step 3 — Point the controller at the profiling image

```bash
kubectl -n <numaflow-ns> set env deploy/<controller> NUMAFLOW_IMAGE=<your-registry>/<img>:<tag>
kubectl -n <numaflow-ns> rollout restart deploy/<controller>
```

### Step 4 — Pipeline spec: profiling env on the reduce vertex

```yaml
  - name: <vertex-name>
    containerTemplate:
      env:
        - { name: LD_PRELOAD, value: /usr/share/libbytehound.so }            # match Step 2 path
        - { name: MEMORY_PROFILER_TRACK_CHILD_PROCESSES, value: "1" }        # REQUIRED (see note)
        - { name: MEMORY_PROFILER_OUTPUT, value: /var/numaflow/pbq/memory-profiling_%e_%t_%p.dat }
        - { name: MEMORY_PROFILER_LOG, value: info }
        - { name: MEMORY_PROFILER_CULL_TEMPORARY_ALLOCATIONS, value: "1" }   # drop <10s allocs, shrink .dat
        - { name: MEMORY_PROFILER_GRAB_BACKTRACES_ON_FREE, value: "0" }      # cut overhead
```

Then `kubectl apply -f <pipeline>.yaml && kubectl delete pod <accum-pod>`.

**Critical: `MEMORY_PROFILER_TRACK_CHILD_PROCESSES=1`.** The image ENTRYPOINT is `/bin/entrypoint`
(a Rust dispatcher) which `execve`s `/bin/numaflow-rs`. With the default (`0`), bytehound *unsets*
`LD_PRELOAD` during its own init in `entrypoint` (to avoid following children), so the exec'd
`numaflow-rs` starts **unprofiled**. Setting `1` keeps `LD_PRELOAD` across the exec.

### Step 5 — Verify bytehound is attached

```bash
kubectl exec <accum-pod> -c numa -- env LD_PRELOAD= sh -c '
  echo "mapped: $(grep -c bytehound /proc/1/maps)";  # expect: >=1
  ldd /bin/numaflow-rs | grep libc;                  # expect: libc.so.6 (dynamic)
  ls -la /var/numaflow/pbq/memory-profiling_numaflow-rs_*'   # a growing .dat (MBs, binary embedded)
```

`env LD_PRELOAD=` clears the var for *your* shell/commands only.

### Step 6 — Collect the `.dat` (do NOT use `kubectl cp`)

`kubectl cp` runs `tar` *inside* the pod; under `TRACK_CHILD_PROCESSES=1` bytehound injects into that
`tar` and it **SIGSEGVs**. Stream it out with `LD_PRELOAD` cleared:

```bash
kubectl exec <accum-pod> -c numa -- \
  env LD_PRELOAD= tar cf - -C /var/numaflow/pbq memory-profiling_numaflow-rs_<t>_1.dat | tar xf -
# optional clean/flushed snapshot: kill -USR1 1 before (flush+pause) and again after (resume)
kubectl exec <accum-pod> -c numa -- env LD_PRELOAD= kill -USR1 1
```

### Step 7 — Analyze

```bash
./bytehound server memory-profiling_numaflow-rs_<t>_1.dat   # → http://localhost:8080
./bytehound strip --threshold 60 -o stripped.dat <file>.dat # if the file is huge / server strains
```

## UD container memory profiling

### Rust SDK

We again use bytehound for memory profiling Rust SDK images.

#### Image

(a source/sink example — adapt to your UDF).

Drop `bytehound-x86_64-unknown-linux-gnu.tgz` (from [link](https://github.com/koute/bytehound/releases)) and `start.sh` into the build context:

```dockerfile
FROM rust:1.76-bookworm AS build
RUN apt-get update && apt-get install -y protobuf-compiler
WORKDIR /source-sink
COPY ./ ./
RUN cargo build --release                       # dynamic glibc binary — LD_PRELOAD-able

FROM debian:bookworm AS simple-source
COPY --from=build /source-sink/target/release/sourcer-sinker .
COPY --from=build /source-sink/bytehound-x86_64-unknown-linux-gnu.tgz .
COPY --from=build /source-sink/start.sh .
RUN chmod +x start.sh && tar -xvzf bytehound-x86_64-unknown-linux-gnu.tgz
ENV MEMORY_PROFILER_LOG=info
CMD ["./start.sh"]
```

**`start.sh`** — set `LD_PRELOAD` **scoped to the app only**:

```bash
#!/bin/bash
LD_PRELOAD=./libbytehound.so ./sourcer-sinker
```

#### Build & push

```bash
docker buildx create --name multiarch --driver docker-container --use          # once
IMAGE_NAMESPACE=quay.io/$(whoami) DOCKER_PUSH=true VERSION=<tag> make image-multi  # or plain buildx build of your SDK image
docker buildx rm multiarch                                                       # cleanup
```

(bytehound prebuilt is **x86_64-only**; for arm64 nodes build `libbytehound.so` from source and COPY the
arch-matching `.so`.)

#### Collect

```bash
kubectl cp --retries 999 <pod>:/memory-profiling_<exe>_<t>_<pid>.dat /tmp/prof.dat
```
