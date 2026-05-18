#!/usr/bin/env python3
"""Run a YCSB workload N times and aggregate the statistics.

Usage:
  scripts/bench-sweep.py <workload-letter> <N> [--base-seed S] [-o [PATH]] -- <bench args>

Example:
  scripts/bench-sweep.py a 5 -- --threads 4 --duration 30
  scripts/bench-sweep.py e 10 --base-seed 100 -- --keys 5000 --value-size 256
  scripts/bench-sweep.py a 5 -o -- --threads 4   # auto-named file under bench-results/
  scripts/bench-sweep.py a 5 -o foo.txt          # explicit path

The script invokes target/release/bench once per iteration with seed =
base_seed + i, parses the printed summary, and prints a table of
per-metric min / median / max / stdev / spread across the runs.

Each iteration uses a fresh tmpdir (passed via --dir) so runs don't share
on-disk state; the temp dir is cleaned up afterwards.

With `--output`, the same content is also written to disk. Without an
explicit path, the file lands at
`bench-results/ycsb-<letter>_N<n>_<bench-args>_<timestamp>.txt` under the
repo root.
"""

from __future__ import annotations

import argparse
import datetime
import re
import statistics
import subprocess
import sys
import tempfile
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
BENCH_BIN = REPO_ROOT / "target" / "release" / "bench"
WORKLOADS = REPO_ROOT / "workloads"
RESULTS_DIR = REPO_ROOT / "bench-results"
# Sentinel returned by argparse when `--output` is passed without a value;
# main() resolves it to an auto-generated path before writing.
AUTO_OUTPUT = "<auto>"

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


def slugify_args(extra_args: list[str]) -> str:
    """Turn a bench arg list into a compact filename-safe slug.

    `["--threads", "4", "--duration", "30"]` → `"threads4_duration30"`.
    Only the alphanumerics + dashes survive; everything else is stripped.
    Returns an empty string when there are no args so callers can avoid
    the leading `_` separator.
    """
    if not extra_args:
        return ""
    tokens: list[str] = []
    for a in extra_args:
        cleaned = re.sub(r"[^A-Za-z0-9\-]", "", a.lstrip("-"))
        if cleaned:
            tokens.append(cleaned)
    # Heuristic pairing: every flag is immediately followed by its value
    # in our usage, so collapse them ("threads", "4") → "threads4". For
    # an odd number of tokens (e.g. a bare boolean flag), the trailing
    # token stays standalone.
    slug_parts: list[str] = []
    i = 0
    while i < len(tokens):
        if i + 1 < len(tokens) and tokens[i + 1].lstrip("-").replace(".", "").isdigit():
            slug_parts.append(f"{tokens[i]}{tokens[i + 1]}")
            i += 2
        else:
            slug_parts.append(tokens[i])
            i += 1
    return "_".join(slug_parts)


def default_output_path(
    workload_letter: str, n: int, extra_args: list[str], when: datetime.datetime
) -> Path:
    """Construct `bench-results/ycsb-<letter>_N<n>_<args>_<timestamp>.txt`."""
    ts = when.strftime("%Y%m%dT%H%M%S")
    slug = slugify_args(extra_args)
    name_parts = [f"ycsb-{workload_letter}", f"N{n}"]
    if slug:
        name_parts.append(slug)
    name_parts.append(ts)
    return RESULTS_DIR / (f"{'_'.join(name_parts)}.txt")


class TeeBuffer:
    """Collect lines for later disk write while also printing to stdout."""

    def __init__(self) -> None:
        self.lines: list[str] = []

    def emit(self, line: str = "") -> None:
        print(line)
        self.lines.append(line)

    def text(self) -> str:
        return "\n".join(self.lines) + "\n"


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
        "-o", "--output",
        nargs="?",
        const=AUTO_OUTPUT,
        default=None,
        metavar="PATH",
        help="Write the run output (per-iteration log + summary table) "
             "to a file. With no value, the path is auto-generated under "
             "`bench-results/` with workload + N + bench args + timestamp "
             "in the filename. With a value, use that exact path.",
    )
    ap.epilog = (
        "Pass bench args after `--`, e.g. "
        "`scripts/bench-sweep.py a 5 -o -- --threads 4 --duration 30`."
    )

    # Manually split sys.argv at `--` so `argparse.REMAINDER`'s greediness
    # doesn't eat our own flags (e.g. `-o` ahead of a bench-side `--`).
    raw = sys.argv[1:]
    if "--" in raw:
        sep = raw.index("--")
        script_argv = raw[:sep]
        bench_args = raw[sep + 1:]
    else:
        script_argv = raw
        bench_args = []

    args = ap.parse_args(script_argv)
    extra_args = bench_args

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

    started_at = datetime.datetime.now()
    out = TeeBuffer()

    # Header block so a saved file is self-describing without the
    # caller having to remember which args produced it.
    out.emit(f"# bench-sweep — YCSB-{args.workload.upper()} × {args.n}")
    out.emit(f"# started_at:  {started_at.isoformat(timespec='seconds')}")
    out.emit(f"# base_seed:   {args.base_seed}")
    out.emit(f"# bench args:  {extra_args or '(none)'}")
    out.emit("")
    out.emit(
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
        out.emit(f"  [{i+1:>2}/{args.n}] seed={seed:<4} throughput={thr_str} ops/sec")

    # ---- Aggregate ----
    out.emit(f"\nSummary across {args.n} runs:\n")

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
    out.emit(
        f"  {'Metric'.ljust(name_w)}"
        f"{'min'.rjust(num_w)}"
        f"{'median'.rjust(num_w)}"
        f"{'max'.rjust(num_w)}"
        f"{'stdev'.rjust(num_w)}"
        f"{'spread':>{spread_w}}"
    )
    out.emit(
        f"  {'-' * (name_w - 1)}"
        f"{'-' * num_w * 4}"
        f"{'-' * spread_w}"
    )

    for name, v_min, v_med, v_max, v_sd, spread in rows:
        out.emit(
            f"  {name.ljust(name_w)}"
            f"{fmt(v_min, num_w)}"
            f"{fmt(v_med, num_w)}"
            f"{fmt(v_max, num_w)}"
            f"{fmt(v_sd, num_w)}"
            f"{spread * 100:>{spread_w - 1}.1f}%"
        )

    # ---- Optional file write ----
    if args.output is not None:
        if args.output == AUTO_OUTPUT:
            output_path = default_output_path(
                args.workload.lower(), args.n, extra_args, started_at
            )
        else:
            output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(out.text())
        # stderr so it doesn't pollute the captured stdout of the run
        # (some callers may pipe stdout into other tooling).
        sys.stderr.write(f"\n[bench-sweep] results written to {output_path}\n")


if __name__ == "__main__":
    main()
