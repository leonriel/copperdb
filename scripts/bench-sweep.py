#!/usr/bin/env python3
"""Run a YCSB workload N times and aggregate the statistics.

Usage:
  scripts/bench-sweep.py <workload-letter> <N> [--base-seed S] -- <bench args>

Example:
  scripts/bench-sweep.py a 5 -- --threads 4 --duration 30
  scripts/bench-sweep.py e 10 --base-seed 100 -- --keys 5000 --value-size 256

The script invokes target/release/bench once per iteration with seed =
base_seed + i, parses the printed summary, and prints a table of
per-metric min / median / max / stdev / spread across the runs.

Each iteration uses a fresh tmpdir (passed via --dir) so runs don't share
on-disk state; the temp dir is cleaned up afterwards.
"""

from __future__ import annotations

import argparse
import re
import statistics
import subprocess
import sys
import tempfile
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
BENCH_BIN = REPO_ROOT / "target" / "release" / "bench"
WORKLOADS = REPO_ROOT / "workloads"

OP_KINDS = ["reads", "updates", "inserts", "rmw", "scans"]
PERCENTILE_KEYS = ["p50", "p99", "p99.9", "p99.99", "max"]


def parse_summary(stdout: str) -> dict[str, float | None]:
    """Extract run-phase throughput and per-op-kind percentiles.

    The bench prints a `load:` block with its own throughput and then a
    `run:` block with the throughput we care about. We track which section
    we're inside via a tiny state machine so the "wrong" throughput value
    is never captured.

    Missing metrics (op kinds with zero ops, etc.) come back as `None` and
    are filtered out by the aggregation step.
    """
    metrics: dict[str, float | None] = {"throughput": None}
    for op in OP_KINDS:
        for pk in PERCENTILE_KEYS:
            metrics[f"{op}.{pk}"] = None

    in_run = False
    lines = stdout.splitlines()
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped == "run:":
            in_run = True
            continue
        if stripped.startswith("latency"):
            in_run = False  # percentile section starts; throughput is past

        if in_run and metrics["throughput"] is None:
            m = re.match(r"\s*throughput:\s+(\d+)\s+ops/sec", line)
            if m:
                metrics["throughput"] = float(m.group(1))

        # Per-op-kind header line: "  reads   (160001 ops):"
        # Followed by a percentile line like:
        # "    p50= 667  p95= ... p99= ... p99.9= ... p99.99= ... max= ..."
        for op in OP_KINDS:
            head = re.match(rf"\s+{op}\s*\(\d+ ops\):", line)
            if head and i + 1 < len(lines):
                pct_line = lines[i + 1]
                for pk in PERCENTILE_KEYS:
                    # Escape the dot in keys like "p99.9" so it matches a
                    # literal character; the `\b` keeps "p99=" from
                    # matching "p99.9=".
                    esc = re.escape(pk)
                    m = re.search(rf"{esc}=\s*(\d+)\b", pct_line)
                    if m:
                        metrics[f"{op}.{pk}"] = float(m.group(1))
    return metrics


def run_one(
    workload_path: Path,
    extra_args: list[str],
    seed: int,
) -> dict[str, float | None]:
    """Invoke the bench once with a fresh tmpdir; return parsed metrics."""
    with tempfile.TemporaryDirectory(prefix="copperdb-sweep-") as tmp:
        cmd = [
            str(BENCH_BIN),
            "--dir", tmp,
            "--config", str(workload_path),
            "--seed", str(seed),
            *extra_args,
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        if proc.returncode != 0:
            sys.stderr.write(
                f"bench exited with code {proc.returncode}\n"
                f"--- stderr ---\n{proc.stderr}\n"
                f"--- stdout (tail) ---\n{proc.stdout[-2000:]}\n"
            )
            raise SystemExit(proc.returncode)
        return parse_summary(proc.stdout)


def fmt(value: float | None, width: int = 14) -> str:
    """Right-align a number with thousands separators; '—' for None."""
    if value is None:
        return "—".rjust(width)
    if value >= 100:
        return f"{value:,.0f}".rjust(width)
    return f"{value:.2f}".rjust(width)


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Run a YCSB workload N times and aggregate stats.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument(
        "workload",
        help="Workload letter (a, b, c, d, e, f).",
    )
    ap.add_argument(
        "n",
        type=int,
        help="Number of iterations.",
    )
    ap.add_argument(
        "--base-seed",
        type=int,
        default=1,
        help="Seed for the first run; subsequent runs use base + i. "
             "Use the same value across A/B comparisons.",
    )
    ap.add_argument(
        "bench_args",
        nargs=argparse.REMAINDER,
        help="Args forwarded to the bench binary after `--` "
             "(e.g. --threads, --duration, --keys, --value-size).",
    )
    args = ap.parse_args()

    workload_path = WORKLOADS / f"ycsb-{args.workload.lower()}.toml"
    if not workload_path.is_file():
        sys.exit(f"workload file not found: {workload_path}")
    if not BENCH_BIN.is_file():
        sys.exit(
            f"bench binary not found at {BENCH_BIN}. "
            "Run `cargo build --release --bin bench` first."
        )
    if args.n < 1:
        sys.exit("N must be ≥ 1")

    extra_args = args.bench_args or []
    if extra_args and extra_args[0] == "--":
        extra_args = extra_args[1:]

    print(
        f"Running YCSB-{args.workload.upper()} × {args.n} "
        f"with bench args: {extra_args or '(none)'}\n"
    )

    all_metrics: list[dict[str, float | None]] = []
    for i in range(args.n):
        seed = args.base_seed + i
        metrics = run_one(workload_path, extra_args, seed)
        all_metrics.append(metrics)
        thr = metrics["throughput"]
        thr_str = f"{thr:,.0f}" if thr is not None else "—"
        print(f"  [{i+1:>2}/{args.n}] seed={seed:<4} throughput={thr_str} ops/sec")

    # ---- Aggregate ----
    print(f"\nSummary across {args.n} runs:\n")

    series = ["throughput"] + [
        f"{op}.{pk}" for op in OP_KINDS for pk in PERCENTILE_KEYS
    ]

    rows: list[tuple[str, float, float, float, float, float]] = []
    for key in series:
        vals = [m[key] for m in all_metrics if m[key] is not None]
        if not vals:
            continue
        v_min = min(vals)
        v_med = statistics.median(vals)
        v_max = max(vals)
        v_sd = statistics.stdev(vals) if len(vals) > 1 else 0.0
        spread = (v_max - v_min) / v_med if v_med != 0 else 0.0
        rows.append((key, v_min, v_med, v_max, v_sd, spread))

    if not rows:
        sys.exit("no metrics parsed — check the bench output format")

    # Column widths.
    name_w = max(len(r[0]) for r in rows) + 2
    num_w = 14
    spread_w = 10

    # Header.
    print(
        f"  {'Metric'.ljust(name_w)}"
        f"{'min'.rjust(num_w)}"
        f"{'median'.rjust(num_w)}"
        f"{'max'.rjust(num_w)}"
        f"{'stdev'.rjust(num_w)}"
        f"{'spread':>{spread_w}}"
    )
    print(
        f"  {'-' * (name_w - 1)}"
        f"{'-' * num_w * 4}"
        f"{'-' * spread_w}"
    )

    for name, v_min, v_med, v_max, v_sd, spread in rows:
        print(
            f"  {name.ljust(name_w)}"
            f"{fmt(v_min, num_w)}"
            f"{fmt(v_med, num_w)}"
            f"{fmt(v_max, num_w)}"
            f"{fmt(v_sd, num_w)}"
            f"{spread * 100:>{spread_w - 1}.1f}%"
        )


if __name__ == "__main__":
    main()
