# `bench` — benchmark harness for copperdb

A Rust-native benchmark binary that drives `LsmEngine` directly (no HTTP /
JNI), measures per-operation latency with [`hdrhistogram`], supports
YCSB-style workloads with Zipfian / uniform / latest key distributions, and
optionally runs in a multi-threaded, rate-targeted closed loop with
coordinated-omission correction.

[`hdrhistogram`]: https://crates.io/crates/hdrhistogram

## Build

```sh
cargo build --release --bin bench
```

Always build in `--release`. Debug builds will mislead you on every number.

## Quick start

Drop-in single-threaded run that mirrors the smallest useful Phase 1 invocation:

```sh
cargo run --release --bin bench -- \
    --dir /tmp/copperdb-bench \
    --keys 5000 \
    --value-size 256 \
    --duration 10
```

The directory is created if missing. **Use a fresh directory per run** unless
you specifically want to benchmark on top of stale state.

## The two run modes

### Open-loop (default)

Each worker thread fires operations as fast as the engine accepts them. Good
for "what's the peak throughput?" Bad for tail latency claims (the open-loop
view is subject to coordinated omission — stalls under-report).

```sh
cargo run --release --bin bench -- \
    --dir /tmp/bench-open \
    --threads 4 \
    --duration 30
```

### Closed-loop with coordinated-omission correction (`--target-rate`)

Each worker schedules ops at fixed intervals (total `--target-rate` divided
across `--threads`). Latency is measured from the *intended* fire time, and
`record_correct` backfills synthetic samples for any interval that fell inside
a stall. **This is the mode to use for honest tail-latency numbers.**

```sh
cargo run --release --bin bench -- \
    --dir /tmp/bench-closed \
    --threads 4 \
    --target-rate 50000 \
    --duration 30
```

Sleep-precision caveat on macOS: scheduler jitter can extend a `thread::sleep`
by several milliseconds. CO correction attributes those overshoots to the
histogram tail, which is correct semantically but produces a floor around
~7 ms at P99 in low-rate runs. Move to Linux for cleaner tail-latency numbers.

## Workload presets

Five YCSB-style presets are shipped under `workloads/` at the repo root. See
[`workloads/README.md`](../../workloads/README.md) for the table of
preset → op mix → distribution mappings.

```sh
cargo run --release --bin bench -- \
    --dir /tmp/bench-a \
    --config workloads/ycsb-a.toml \
    --threads 4 \
    --duration 30
```

CLI flags can override TOML fields: `--keys`, `--value-size`,
`--key-distribution`, `--zipfian-theta`, and the four `--*-pct` flags. Useful
for smoke-testing a preset at small scale before committing to a long run:

```sh
cargo run --release --bin bench -- \
    --dir /tmp/bench-d-smoke \
    --config workloads/ycsb-d.toml \
    --keys 5000 --value-size 256 --duration 10
```

The TOML presets default to 1 M × 1 KB, which now works as-is thanks to
the two-level SSTable index (see `src/sstable/DESIGN.md`). Smaller overrides
remain useful for quick smoke tests during development.

## CLI reference

### Run-time flags (always CLI)

| Flag                | Default      | Meaning                                                            |
|---------------------|--------------|--------------------------------------------------------------------|
| `--dir <path>`      | (required)   | Data directory for the engine. Created if missing.                 |
| `--duration <secs>` | `30`         | Run-phase wall time.                                               |
| `--threads <N>`     | `1`          | Worker thread count. `1` ≡ Phase 1 single-threaded behaviour.      |
| `--target-rate <r>` | (open-loop)  | Total ops/sec across all threads. Enables closed-loop + CO correction. |
| `--seed <u64>`      | `42`         | PRNG seed. Each thread uses `seed + thread_id`.                    |
| `--cooldown <secs>` | `1`          | Sleep between load and run phases so background work drains.       |
| `--memtable-size`   | `204800`     | Memtable byte budget. 200 KB default keeps flushes brisk for development; raise for larger runs. |

### Workload flags (CLI or TOML)

| Flag                       | Default (CLI mode) | Meaning                              |
|----------------------------|--------------------|--------------------------------------|
| `--config <path>`          | (custom mode)      | Path to a TOML workload preset.      |
| `--keys <N>`               | `100_000`          | Number of keys loaded before the run.|
| `--value-size <bytes>`     | `1024`             | Bytes per value.                     |
| `--key-distribution <kind>`| `uniform`          | `uniform` \| `zipfian` \| `latest`   |
| `--zipfian-theta <θ>`      | `0.99`             | Skew exponent for `zipfian` / `latest`. |
| `--read-pct <f>`           | `0.5`              | Fraction of ops that are reads.      |
| `--update-pct <f>`         | `0.5`              | Fraction of ops that are updates.    |
| `--insert-pct <f>`         | `0.0`              | Fraction that are inserts (grows key space). |
| `--rmw-pct <f>`            | `0.0`              | Fraction that are read-modify-write. |

The four `--*-pct` flags must sum to `1.0`. If `--config` is set, the TOML
provides the workload and CLI flags act as field-level overrides; setting
*any* `--*-pct` flag replaces the entire op mix wholesale (to keep the
sum-to-1 invariant).

## Reading the output

The summary block has three sections:

```
workload:    <name, description, keys, value_size, key_dist, op_mix>
config:      <dir, threads, mode, duration, memtable, seed>
load:        <elapsed, throughput>
run:         <total ops, per-op breakdown, key-space initial→final, throughput>
latency:     <per-op-kind percentiles (read, update, insert, rmw)>
```

Per-op-kind percentile blocks tell you which path (read vs write vs RMW) is
dragging the tail. If `reads.p99.9` looks fine but `updates.p99.9` is several
orders of magnitude worse, the write path is the bottleneck.

The `key space` line shows growth from inserts — useful for YCSB-D
verification (read-latest only makes sense if the key space actually grows).

## Recipes

### Reproduce two runs side by side

```sh
cargo run --release --bin bench -- --dir /tmp/run-a --config workloads/ycsb-a.toml \
    --keys 5000 --value-size 256 --duration 30 --seed 42 > run-a.txt
cargo run --release --bin bench -- --dir /tmp/run-b --config workloads/ycsb-a.toml \
    --keys 5000 --value-size 256 --duration 30 --seed 42 > run-b.txt
diff run-a.txt run-b.txt
```

Op counts per kind should be identical; latency numbers will differ slightly
because of system noise.

### Profile with flamegraph

```sh
cargo install flamegraph
cargo flamegraph --release --bin bench -- \
    --dir /tmp/bench-flame --keys 10000 --duration 30
```

Open the resulting `flamegraph.svg` in a browser.

### Find the open-loop peak, then measure tail at 80% capacity

```sh
# 1. Find peak throughput
cargo run --release --bin bench -- --dir /tmp/peak --config workloads/ycsb-c.toml \
    --keys 5000 --value-size 256 --threads 4 --duration 10
# … note the open-loop throughput, say 150,000 ops/sec.

# 2. Close the loop at 80% of peak.
cargo run --release --bin bench -- --dir /tmp/sustain --config workloads/ycsb-c.toml \
    --keys 5000 --value-size 256 --threads 4 --duration 30 --target-rate 120000
# Compare the per-op-kind P99.9 between (1) and (2).
```

## Known gaps

- **No time-series output.** Each run reports a single summary; you can't see
  throughput or P99 over time. Phase 4 will add a per-second CSV.
- **No engine counter snapshots.** When tail latency spikes, the harness
  doesn't yet tell you whether a compaction or an L0 fan-out caused it.
  Phase 4 will plumb `LsmEngine::stats()`.
- **Sleep precision on macOS.** See the closed-loop caveat above. Linux is
  the answer.
- **No `scan` workload (YCSB-E).** `LsmEngine` has no public `scan` API;
  adding one is a separate engine feature, not part of the harness phasing.
