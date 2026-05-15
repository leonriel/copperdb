//! Phase 1 of the copperdb benchmark harness.
//!
//! Single-threaded driver:
//!   1. Load phase — insert `--keys` keys with fixed-size random values.
//!   2. Run phase  — for `--duration` seconds, sample a uniform key and do
//!                   a get (with probability `--read-pct`) or a put.
//! Reports throughput plus a latency histogram via hdrhistogram.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use clap::Parser;
use hdrhistogram::Histogram;
use rand::{Rng, SeedableRng};
use rand::rngs::SmallRng;

use copperdb::engine::LsmEngine;

#[derive(Parser, Debug)]
#[command(name = "bench", about = "Rust-native benchmark harness for copperdb")]
struct Args {
    /// Data directory for the engine. Created if missing.
    #[arg(long)]
    dir: PathBuf,

    /// Number of keys to load before the run phase.
    #[arg(long, default_value_t = 100_000)]
    keys: u64,

    /// How long the run phase lasts, in seconds.
    #[arg(long, default_value_t = 30)]
    duration: u64,

    /// Fraction of run-phase ops that are reads (0.0–1.0); the rest are writes.
    #[arg(long, default_value_t = 0.5)]
    read_pct: f64,

    /// Size of each value, in bytes.
    #[arg(long, default_value_t = 1024)]
    value_size: usize,

    /// PRNG seed (set for reproducible runs).
    #[arg(long, default_value_t = 42)]
    seed: u64,

    /// Seconds to sleep between load and run phases so background flushes /
    /// compactions drain. Set to 0 to disable.
    #[arg(long, default_value_t = 1)]
    cooldown: u64,

    /// Memtable size in bytes. Default 200 KB to stay under the current
    /// 4 KB SSTable index-block limit (each flush produces ~50 data blocks).
    /// Raise this once the SSTable writer supports multi-level indices.
    #[arg(long, default_value_t = 200 * 1024)]
    memtable_size: usize,
}

fn make_key(i: u64) -> String {
    format!("key_{:020}", i)
}

fn make_value(rng: &mut SmallRng, size: usize, buf: &mut Vec<u8>) {
    buf.resize(size, 0);
    rng.fill(&mut buf[..]);
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    if !(0.0..=1.0).contains(&args.read_pct) {
        return Err(format!("--read-pct must be in [0.0, 1.0], got {}", args.read_pct).into());
    }

    std::fs::create_dir_all(&args.dir)?;
    eprintln!("[bench] opening copperdb at {:?} (memtable={} B)", args.dir, args.memtable_size);
    let engine: std::sync::Arc<LsmEngine> =
        LsmEngine::open_with_memtable_size(&args.dir, args.memtable_size)?;

    let mut rng = SmallRng::seed_from_u64(args.seed);
    let mut value_buf: Vec<u8> = Vec::with_capacity(args.value_size);

    // ---- Load phase -----------------------------------------------------
    eprintln!("[bench] load phase: inserting {} keys", args.keys);
    let load_start = Instant::now();
    for i in 0..args.keys {
        make_value(&mut rng, args.value_size, &mut value_buf);
        engine.put(make_key(i), value_buf.clone())?;
    }
    let load_elapsed = load_start.elapsed();
    let load_throughput = args.keys as f64 / load_elapsed.as_secs_f64();
    eprintln!(
        "[bench] load complete in {:.2}s ({:.0} ops/sec)",
        load_elapsed.as_secs_f64(),
        load_throughput,
    );

    if args.cooldown > 0 {
        eprintln!("[bench] cooldown {}s (let background work drain)", args.cooldown);
        std::thread::sleep(Duration::from_secs(args.cooldown));
    }

    // ---- Run phase ------------------------------------------------------
    eprintln!(
        "[bench] run phase: {}s @ read_pct={} value_size={}",
        args.duration, args.read_pct, args.value_size,
    );

    // Auto-resizing histogram with 3 significant figures; tracks
    // latencies in nanoseconds.
    let mut hist: Histogram<u64> = Histogram::new(3)?;

    let run_start = Instant::now();
    let run_deadline = run_start + Duration::from_secs(args.duration);
    let mut total_reads = 0u64;
    let mut total_writes = 0u64;
    let mut read_hits = 0u64;

    while Instant::now() < run_deadline {
        let key_idx = rng.random_range(0..args.keys);
        let key = make_key(key_idx);
        let is_read = rng.random::<f64>() < args.read_pct;

        let op_start = Instant::now();
        if is_read {
            let got = engine.get(&key);
            let elapsed_ns = op_start.elapsed().as_nanos() as u64;
            hist.record(elapsed_ns)?;
            if got.is_some() {
                read_hits += 1;
            }
            total_reads += 1;
        } else {
            make_value(&mut rng, args.value_size, &mut value_buf);
            engine.put(key, value_buf.clone())?;
            let elapsed_ns = op_start.elapsed().as_nanos() as u64;
            hist.record(elapsed_ns)?;
            total_writes += 1;
        }
    }
    let run_elapsed = run_start.elapsed();
    let total_ops = total_reads + total_writes;

    // ---- Report ---------------------------------------------------------
    println!("--- summary ---");
    println!("config:");
    println!("  dir:         {:?}", args.dir);
    println!("  keys:        {}", args.keys);
    println!("  duration:    {}s", args.duration);
    println!("  read_pct:    {}", args.read_pct);
    println!("  value_size:  {} B", args.value_size);
    println!("  memtable:    {} B", args.memtable_size);
    println!("  seed:        {}", args.seed);
    println!();
    println!("load:");
    println!("  elapsed:     {:.2}s", load_elapsed.as_secs_f64());
    println!("  throughput:  {:.0} ops/sec", load_throughput);
    println!();
    println!("run:");
    println!("  elapsed:     {:.2}s", run_elapsed.as_secs_f64());
    println!("  total ops:   {}", total_ops);
    println!("  reads:       {} (hits {}, miss rate {:.4})",
        total_reads,
        read_hits,
        if total_reads == 0 { 0.0 } else { 1.0 - (read_hits as f64 / total_reads as f64) },
    );
    println!("  writes:      {}", total_writes);
    println!("  throughput:  {:.0} ops/sec", total_ops as f64 / run_elapsed.as_secs_f64());
    println!();
    println!("latency (ns, all ops):");
    println!("  p50:    {:>12}", hist.value_at_quantile(0.50));
    println!("  p95:    {:>12}", hist.value_at_quantile(0.95));
    println!("  p99:    {:>12}", hist.value_at_quantile(0.99));
    println!("  p99.9:  {:>12}", hist.value_at_quantile(0.999));
    println!("  p99.99: {:>12}", hist.value_at_quantile(0.9999));
    println!("  max:    {:>12}", hist.max());
    println!("  mean:   {:>12.0}", hist.mean());

    Ok(())
}
