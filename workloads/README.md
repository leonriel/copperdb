# Workload presets

TOML files in this directory describe canonical YCSB-style workloads that the
benchmark harness can run via `--config`:

```
cargo run --release --bin bench -- \
    --dir /tmp/bench-a \
    --config workloads/ycsb-a.toml \
    --duration 30
```

| File          | YCSB name | Op mix                       | Key distribution | Notes                              |
|---------------|-----------|------------------------------|-------------------|------------------------------------|
| `ycsb-a.toml` | A         | 50% read / 50% update         | Zipfian (θ=0.99)  | Update-heavy session store         |
| `ycsb-b.toml` | B         | 95% read / 5% update          | Zipfian (θ=0.99)  | Read-mostly with light updates     |
| `ycsb-c.toml` | C         | 100% read                     | Zipfian (θ=0.99)  | Read-only baseline                 |
| `ycsb-d.toml` | D         | 95% read / 5% insert          | Latest            | Read-latest with growing key space |
| `ycsb-e.toml` | E         | 95% scan / 5% insert          | Zipfian (θ=0.99)  | Short range scans (length 1–100)   |
| `ycsb-f.toml` | F         | 50% read / 50% RMW            | Zipfian (θ=0.99)  | Read-modify-write                  |

YCSB-E uses range scans with per-op length sampled uniformly from
`[1, max_scan_length]` (default `100`, matching the YCSB paper). Override
via `--max-scan-length`.

All presets default to **1 million keys × 1 KB values**. Override any field
at run time with the corresponding CLI flag (e.g. `--keys 10000`), or copy a
TOML preset and edit it. Run-time knobs (`--dir`, `--duration`, `--seed`,
`--cooldown`, `--memtable-size`) are always supplied via CLI; the TOML
describes the *workload*, not the *run*.

The SSTable writer uses a two-level index (see `src/sstable/DESIGN.md`), so
the TOML defaults run as written. Smaller overrides remain useful for fast
smoke tests during development.

Reference: B. F. Cooper et al., *Benchmarking Cloud Serving Systems with
YCSB* (SoCC '10). Our numbers aren't directly comparable to the paper's
because we drive `LsmEngine` in-process rather than through YCSB's Java
client, but the workload definitions are intentionally aligned.
