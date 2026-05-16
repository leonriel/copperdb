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
| `ycsb-f.toml` | F         | 50% read / 50% RMW            | Zipfian (θ=0.99)  | Read-modify-write                  |

YCSB-E (short range scans) is intentionally omitted — `LsmEngine` does not
yet expose a public scan API. Adding one is a separate engine feature, not
part of the benchmark harness.

All presets default to **1 million keys × 1 KB values**. Override any field
at run time with the corresponding CLI flag (e.g. `--keys 10000`), or copy a
TOML preset and edit it. Run-time knobs (`--dir`, `--duration`, `--seed`,
`--cooldown`, `--memtable-size`) are always supplied via CLI; the TOML
describes the *workload*, not the *run*.

> **Note on current engine limits.** The SSTable writer uses a single-level
> 1 MB index block (see `src/sstable/DESIGN.md`). At the full 1M × 1KB
> defaults, the L0→L1 compaction output's index exceeds 1 MB and the
> compaction fails. Until copperdb grows a multi-level index, smoke-test the
> presets with smaller overrides — e.g.
> `--keys 5000 --value-size 256` — or use smaller per-value sizes
> (`--value-size 64`) at the full key count.

Reference: B. F. Cooper et al., *Benchmarking Cloud Serving Systems with
YCSB* (SoCC '10). Our numbers aren't directly comparable to the paper's
because we drive `LsmEngine` in-process rather than through YCSB's Java
client, but the workload definitions are intentionally aligned.
