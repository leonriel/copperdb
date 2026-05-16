//! copperdb benchmark harness (Phase 3).
//!
//! Multi-threaded driver with optional rate-targeted closed loop and
//! coordinated-omission correction. Workloads can be supplied either as a
//! TOML preset (`--config workloads/ycsb-a.toml`) or via individual CLI
//! flags. Per-op-kind latency histograms are reported separately so reads
//! and writes can be analysed in isolation.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use clap::Parser;
use hdrhistogram::Histogram;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use rand_distr::{Distribution, Zipf};
use serde::Deserialize;

use copperdb::engine::LsmEngine;

// ---------------------------------------------------------------------------
// Workload model
// ---------------------------------------------------------------------------

#[derive(Deserialize, Debug, Clone)]
struct Workload {
    #[serde(default)]
    name: String,
    #[serde(default)]
    description: String,
    keys: u64,
    value_size: usize,
    key_distribution: KeyDistributionKind,
    #[serde(default = "default_theta")]
    zipfian_theta: f64,
    op_mix: OpMix,
}

fn default_theta() -> f64 {
    0.99
}

#[derive(Deserialize, Debug, Clone, Copy)]
#[serde(rename_all = "lowercase")]
enum KeyDistributionKind {
    Uniform,
    Zipfian,
    Latest,
}

impl std::str::FromStr for KeyDistributionKind {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "uniform" => Ok(KeyDistributionKind::Uniform),
            "zipfian" => Ok(KeyDistributionKind::Zipfian),
            "latest" => Ok(KeyDistributionKind::Latest),
            other => Err(format!("unknown key distribution: {}", other)),
        }
    }
}

#[derive(Deserialize, Debug, Clone, Copy, Default)]
struct OpMix {
    #[serde(default)]
    read: f64,
    #[serde(default)]
    update: f64,
    #[serde(default)]
    insert: f64,
    #[serde(default)]
    rmw: f64,
}

impl OpMix {
    fn validate(&self) -> Result<(), String> {
        for (name, v) in [
            ("read", self.read),
            ("update", self.update),
            ("insert", self.insert),
            ("rmw", self.rmw),
        ] {
            if v < 0.0 {
                return Err(format!("op_mix.{} = {} is negative", name, v));
            }
        }
        let sum = self.read + self.update + self.insert + self.rmw;
        if (sum - 1.0).abs() > 1e-6 {
            return Err(format!(
                "op_mix sums to {} (read={}, update={}, insert={}, rmw={}); expected 1.0",
                sum, self.read, self.update, self.insert, self.rmw,
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
enum OpKind {
    Read,
    Update,
    Insert,
    Rmw,
}

// ---------------------------------------------------------------------------
// Samplers
// ---------------------------------------------------------------------------

/// Per-op random key chooser, parameterised by the workload's distribution.
struct KeySampler {
    kind: KeyDistributionKind,
    zipf: Option<Zipf<f64>>,
}

impl KeySampler {
    fn new(kind: KeyDistributionKind, n_initial: u64, theta: f64) -> Result<Self, String> {
        let zipf = match kind {
            KeyDistributionKind::Uniform => None,
            KeyDistributionKind::Zipfian | KeyDistributionKind::Latest => Some(
                Zipf::new(n_initial as f64, theta)
                    .map_err(|e| format!("invalid Zipf parameters: {}", e))?,
            ),
        };
        Ok(Self { kind, zipf })
    }

    /// Sample a key index in `[0, n_current)`.
    fn sample(&self, rng: &mut SmallRng, n_current: u64) -> u64 {
        debug_assert!(n_current > 0);
        match self.kind {
            KeyDistributionKind::Uniform => rng.random_range(0..n_current),
            KeyDistributionKind::Zipfian => {
                // Zipf yields 1-indexed ranks; shift and clamp into [0, n_current).
                let r = self.zipf.as_ref().unwrap().sample(rng) as u64;
                r.saturating_sub(1).min(n_current - 1)
            }
            KeyDistributionKind::Latest => {
                // Reverse the rank so rank=1 (most likely) maps to the most
                // recently inserted key. Approximation: the Zipf was built
                // against the initial key space; as inserts grow n_current,
                // the distribution slightly under-weights the tail.
                let r = self.zipf.as_ref().unwrap().sample(rng) as u64;
                let rank = r.max(1).min(n_current);
                n_current - rank
            }
        }
    }
}

/// Cumulative-threshold sampler for the op mix.
struct OpSampler {
    read: f64,
    update_cum: f64,
    insert_cum: f64,
    // rmw fills the rest.
}

impl OpSampler {
    fn new(mix: &OpMix) -> Self {
        Self {
            read: mix.read,
            update_cum: mix.read + mix.update,
            insert_cum: mix.read + mix.update + mix.insert,
        }
    }

    fn sample(&self, r: f64) -> OpKind {
        if r < self.read {
            OpKind::Read
        } else if r < self.update_cum {
            OpKind::Update
        } else if r < self.insert_cum {
            OpKind::Insert
        } else {
            OpKind::Rmw
        }
    }
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser, Debug)]
#[command(name = "bench", about = "Rust-native benchmark harness for copperdb")]
struct Args {
    /// Data directory for the engine. Created if missing.
    #[arg(long)]
    dir: PathBuf,

    /// Path to a TOML workload preset (see workloads/). When set, the
    /// workload-detail flags below are overrides on top of the preset.
    #[arg(long)]
    config: Option<PathBuf>,

    /// How long the run phase lasts, in seconds.
    #[arg(long, default_value_t = 30)]
    duration: u64,

    /// PRNG seed (set for reproducible runs).
    #[arg(long, default_value_t = 42)]
    seed: u64,

    /// Seconds to sleep between load and run phases so background flushes /
    /// compactions drain.
    #[arg(long, default_value_t = 1)]
    cooldown: u64,

    /// Memtable size in bytes.
    #[arg(long, default_value_t = 200 * 1024)]
    memtable_size: usize,

    /// Number of concurrent worker threads driving the run phase.
    #[arg(long, default_value_t = 1)]
    threads: u32,

    /// Optional total target rate across all threads, in ops/sec. When set,
    /// switches to closed-loop mode with coordinated-omission correction.
    /// When absent, the run is open-loop max (each thread fires ops as fast
    /// as the engine accepts).
    #[arg(long)]
    target_rate: Option<f64>,

    // -- Workload-detail overrides. --
    /// Number of keys to load before the run phase.
    #[arg(long)]
    keys: Option<u64>,

    /// Size of each value, in bytes.
    #[arg(long)]
    value_size: Option<usize>,

    /// Key distribution: uniform | zipfian | latest.
    #[arg(long)]
    key_distribution: Option<String>,

    /// Zipfian exponent (also used by `latest`).
    #[arg(long)]
    zipfian_theta: Option<f64>,

    /// Fraction of ops that are reads.
    #[arg(long)]
    read_pct: Option<f64>,

    /// Fraction of ops that are updates.
    #[arg(long)]
    update_pct: Option<f64>,

    /// Fraction of ops that are inserts (grows the key space).
    #[arg(long)]
    insert_pct: Option<f64>,

    /// Fraction of ops that are read-modify-write (get followed by put).
    #[arg(long)]
    rmw_pct: Option<f64>,
}

fn default_workload() -> Workload {
    Workload {
        name: "custom".to_string(),
        description: "Workload built from CLI flags".to_string(),
        keys: 100_000,
        value_size: 1024,
        key_distribution: KeyDistributionKind::Uniform,
        zipfian_theta: 0.99,
        op_mix: OpMix {
            read: 0.5,
            update: 0.5,
            insert: 0.0,
            rmw: 0.0,
        },
    }
}

fn resolve_workload(args: &Args) -> Result<Workload, Box<dyn std::error::Error>> {
    // Start from the TOML preset if given, otherwise from built-in defaults.
    let mut workload = if let Some(path) = args.config.as_ref() {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| format!("failed to read {:?}: {}", path, e))?;
        toml::from_str::<Workload>(&raw)
            .map_err(|e| format!("failed to parse {:?}: {}", path, e))?
    } else {
        default_workload()
    };

    // Apply CLI overrides on top.
    if let Some(v) = args.keys {
        workload.keys = v;
    }
    if let Some(v) = args.value_size {
        workload.value_size = v;
    }
    if let Some(s) = &args.key_distribution {
        workload.key_distribution = s.parse()?;
    }
    if let Some(v) = args.zipfian_theta {
        workload.zipfian_theta = v;
    }
    // Op-mix override: if any pct flag is set, replace the whole mix so we
    // don't end up with a mix that doesn't sum to 1.0 from partial overrides.
    if args.read_pct.is_some()
        || args.update_pct.is_some()
        || args.insert_pct.is_some()
        || args.rmw_pct.is_some()
    {
        workload.op_mix = OpMix {
            read: args.read_pct.unwrap_or(0.0),
            update: args.update_pct.unwrap_or(0.0),
            insert: args.insert_pct.unwrap_or(0.0),
            rmw: args.rmw_pct.unwrap_or(0.0),
        };
    }

    workload.op_mix.validate()?;
    if workload.keys == 0 {
        return Err("workload.keys must be > 0".into());
    }
    if args.threads == 0 {
        return Err("--threads must be > 0".into());
    }
    if let Some(r) = args.target_rate {
        if r <= 0.0 {
            return Err("--target-rate must be > 0".into());
        }
    }
    Ok(workload)
}

// ---------------------------------------------------------------------------
// Op helpers
// ---------------------------------------------------------------------------

fn make_key(i: u64) -> String {
    format!("key_{:020}", i)
}

fn make_value(rng: &mut SmallRng, size: usize, buf: &mut Vec<u8>) {
    buf.resize(size, 0);
    rng.fill(&mut buf[..]);
}

// ---------------------------------------------------------------------------
// Worker
// ---------------------------------------------------------------------------

struct ThreadResult {
    reads: u64,
    updates: u64,
    inserts: u64,
    rmws: u64,
    read_hits: u64,
    read_hist: Histogram<u64>,
    update_hist: Histogram<u64>,
    insert_hist: Histogram<u64>,
    rmw_hist: Histogram<u64>,
}

/// Drive the run phase from a single worker thread until the deadline.
///
/// `per_thread_rate = Some(r)` ⇒ closed-loop mode, schedule ops at fixed
/// intervals `1/r` and use coordinated-omission correction. `None` ⇒
/// open-loop max (fire ops as fast as the engine accepts).
fn run_worker(
    thread_id: u32,
    engine: Arc<LsmEngine>,
    workload: Workload,
    next_insert_idx: Arc<AtomicU64>,
    seed: u64,
    run_start: Instant,
    deadline: Instant,
    per_thread_rate: Option<f64>,
) -> Result<ThreadResult, String> {
    let mut rng = SmallRng::seed_from_u64(seed.wrapping_add(thread_id as u64));
    let key_sampler = KeySampler::new(
        workload.key_distribution,
        workload.keys,
        workload.zipfian_theta,
    )?;
    let op_sampler = OpSampler::new(&workload.op_mix);
    let mut value_buf: Vec<u8> = Vec::with_capacity(workload.value_size);

    let mut read_hist: Histogram<u64> = Histogram::new(3).map_err(|e| e.to_string())?;
    let mut update_hist: Histogram<u64> = Histogram::new(3).map_err(|e| e.to_string())?;
    let mut insert_hist: Histogram<u64> = Histogram::new(3).map_err(|e| e.to_string())?;
    let mut rmw_hist: Histogram<u64> = Histogram::new(3).map_err(|e| e.to_string())?;

    let mut reads: u64 = 0;
    let mut updates: u64 = 0;
    let mut inserts: u64 = 0;
    let mut rmws: u64 = 0;
    let mut read_hits: u64 = 0;

    // Per-thread interval in nanoseconds for closed-loop scheduling.
    let interval_ns: Option<u64> = per_thread_rate.map(|r| (1e9 / r).max(1.0) as u64);

    let mut iteration: u64 = 0;
    loop {
        let now = Instant::now();
        if now >= deadline {
            break;
        }

        // Closed-loop scheduling: compute the intended fire time for this
        // iteration. If we're ahead, sleep; if we're behind, fire immediately
        // and let `record_correct` backfill missed intervals.
        let intended: Option<Instant> = interval_ns.map(|ivl| {
            run_start + Duration::from_nanos(iteration.saturating_mul(ivl))
        });
        if let Some(t) = intended {
            if let Some(wait) = t.checked_duration_since(now) {
                thread::sleep(wait);
            }
        }

        // Sample the op and current key-space size.
        let op = op_sampler.sample(rng.random::<f64>());
        let n_current = next_insert_idx.load(Ordering::Relaxed);

        let op_started = Instant::now();
        let kind = match op {
            OpKind::Read => {
                let k = make_key(key_sampler.sample(&mut rng, n_current));
                let got = engine.get(&k);
                if got.is_some() {
                    read_hits += 1;
                }
                reads += 1;
                OpKind::Read
            }
            OpKind::Update => {
                let k = make_key(key_sampler.sample(&mut rng, n_current));
                make_value(&mut rng, workload.value_size, &mut value_buf);
                engine.put(k, value_buf.clone()).map_err(|e| e.to_string())?;
                updates += 1;
                OpKind::Update
            }
            OpKind::Insert => {
                let idx = next_insert_idx.fetch_add(1, Ordering::Relaxed);
                let k = make_key(idx);
                make_value(&mut rng, workload.value_size, &mut value_buf);
                engine.put(k, value_buf.clone()).map_err(|e| e.to_string())?;
                inserts += 1;
                OpKind::Insert
            }
            OpKind::Rmw => {
                let k = make_key(key_sampler.sample(&mut rng, n_current));
                let _ = engine.get(&k);
                make_value(&mut rng, workload.value_size, &mut value_buf);
                engine.put(k, value_buf.clone()).map_err(|e| e.to_string())?;
                rmws += 1;
                OpKind::Rmw
            }
        };
        let op_complete = Instant::now();

        // Latency: from the intended fire time in closed-loop, else from the
        // actual start. `saturating_duration_since` keeps this safe if the
        // sleep slightly overshot.
        let latency_ns = match intended {
            Some(t) => op_complete.saturating_duration_since(t).as_nanos() as u64,
            None => op_complete.duration_since(op_started).as_nanos() as u64,
        };

        let hist = match kind {
            OpKind::Read => &mut read_hist,
            OpKind::Update => &mut update_hist,
            OpKind::Insert => &mut insert_hist,
            OpKind::Rmw => &mut rmw_hist,
        };
        match interval_ns {
            Some(ivl) => hist
                .record_correct(latency_ns, ivl)
                .map_err(|e| e.to_string())?,
            None => hist.record(latency_ns).map_err(|e| e.to_string())?,
        }

        iteration += 1;
    }

    Ok(ThreadResult {
        reads,
        updates,
        inserts,
        rmws,
        read_hits,
        read_hist,
        update_hist,
        insert_hist,
        rmw_hist,
    })
}

// ---------------------------------------------------------------------------
// Report helpers
// ---------------------------------------------------------------------------

fn print_per_op_block(label: &str, count: u64, hist: &Histogram<u64>) {
    if count == 0 || hist.len() == 0 {
        println!("  {} (no samples)", label);
        return;
    }
    println!(
        "  {} ({} ops):",
        label,
        count,
    );
    println!(
        "    p50={:>10}  p95={:>10}  p99={:>10}  p99.9={:>10}  p99.99={:>10}  max={:>10}  mean={:>10.0}",
        hist.value_at_quantile(0.50),
        hist.value_at_quantile(0.95),
        hist.value_at_quantile(0.99),
        hist.value_at_quantile(0.999),
        hist.value_at_quantile(0.9999),
        hist.max(),
        hist.mean(),
    );
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let workload = resolve_workload(&args)?;

    std::fs::create_dir_all(&args.dir)?;
    eprintln!(
        "[bench] workload: {} — {}",
        workload.name, workload.description,
    );
    eprintln!(
        "[bench] opening copperdb at {:?} (memtable={} B)",
        args.dir, args.memtable_size,
    );
    let engine: Arc<LsmEngine> =
        LsmEngine::open_with_memtable_size(&args.dir, args.memtable_size)?;

    let mut rng = SmallRng::seed_from_u64(args.seed);
    let mut value_buf: Vec<u8> = Vec::with_capacity(workload.value_size);

    // ---- Load phase (single-threaded) -----------------------------------
    eprintln!("[bench] load phase: inserting {} keys", workload.keys);
    let load_start = Instant::now();
    for i in 0..workload.keys {
        make_value(&mut rng, workload.value_size, &mut value_buf);
        engine.put(make_key(i), value_buf.clone())?;
    }
    let load_elapsed = load_start.elapsed();
    let load_throughput = workload.keys as f64 / load_elapsed.as_secs_f64();
    eprintln!(
        "[bench] load complete in {:.2}s ({:.0} ops/sec)",
        load_elapsed.as_secs_f64(),
        load_throughput,
    );

    if args.cooldown > 0 {
        eprintln!(
            "[bench] cooldown {}s (let background work drain)",
            args.cooldown,
        );
        thread::sleep(Duration::from_secs(args.cooldown));
    }

    // ---- Run phase (multi-threaded) -------------------------------------
    let per_thread_rate = args.target_rate.map(|r| r / args.threads as f64);
    let mode_str = match per_thread_rate {
        Some(r) => format!(
            "closed-loop (target {} ops/sec total, {:.0} per thread)",
            args.target_rate.unwrap(),
            r,
        ),
        None => "open-loop max".to_string(),
    };
    eprintln!(
        "[bench] run phase: {}s, {} threads, mode={}",
        args.duration, args.threads, mode_str,
    );

    let next_insert_idx = Arc::new(AtomicU64::new(workload.keys));
    let run_start = Instant::now();
    let deadline = run_start + Duration::from_secs(args.duration);

    let mut handles = Vec::with_capacity(args.threads as usize);
    for t in 0..args.threads {
        let engine = Arc::clone(&engine);
        let workload = workload.clone();
        let next_insert_idx = Arc::clone(&next_insert_idx);
        let seed = args.seed;
        handles.push(thread::spawn(move || {
            run_worker(
                t,
                engine,
                workload,
                next_insert_idx,
                seed,
                run_start,
                deadline,
                per_thread_rate,
            )
        }));
    }

    let mut combined_read: Histogram<u64> = Histogram::new(3)?;
    let mut combined_update: Histogram<u64> = Histogram::new(3)?;
    let mut combined_insert: Histogram<u64> = Histogram::new(3)?;
    let mut combined_rmw: Histogram<u64> = Histogram::new(3)?;
    let mut reads = 0u64;
    let mut updates = 0u64;
    let mut inserts = 0u64;
    let mut rmws = 0u64;
    let mut read_hits = 0u64;

    for h in handles {
        let result = h
            .join()
            .map_err(|_| "worker thread panicked".to_string())??;
        combined_read.add(result.read_hist)?;
        combined_update.add(result.update_hist)?;
        combined_insert.add(result.insert_hist)?;
        combined_rmw.add(result.rmw_hist)?;
        reads += result.reads;
        updates += result.updates;
        inserts += result.inserts;
        rmws += result.rmws;
        read_hits += result.read_hits;
    }

    let run_elapsed = run_start.elapsed();
    let total_ops = reads + updates + inserts + rmws;
    let n_final = next_insert_idx.load(Ordering::Relaxed);

    // ---- Report ---------------------------------------------------------
    let pct = |n: u64| -> f64 {
        if total_ops == 0 {
            0.0
        } else {
            100.0 * n as f64 / total_ops as f64
        }
    };

    println!("--- summary ---");
    println!("workload:");
    println!("  name:        {}", workload.name);
    println!("  description: {}", workload.description);
    println!("  keys:        {}", workload.keys);
    println!("  value_size:  {} B", workload.value_size);
    println!("  key_dist:    {:?}", workload.key_distribution);
    if !matches!(workload.key_distribution, KeyDistributionKind::Uniform) {
        println!("  zipf_theta:  {}", workload.zipfian_theta);
    }
    println!(
        "  op_mix:      read={:.2} update={:.2} insert={:.2} rmw={:.2}",
        workload.op_mix.read,
        workload.op_mix.update,
        workload.op_mix.insert,
        workload.op_mix.rmw,
    );
    println!("config:");
    println!("  dir:         {:?}", args.dir);
    println!("  threads:     {}", args.threads);
    println!("  mode:        {}", mode_str);
    println!("  duration:    {}s", args.duration);
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
    println!(
        "  reads:       {} ({:.1}%, hits {}, miss rate {:.4})",
        reads,
        pct(reads),
        read_hits,
        if reads == 0 {
            0.0
        } else {
            1.0 - (read_hits as f64 / reads as f64)
        },
    );
    println!("  updates:     {} ({:.1}%)", updates, pct(updates));
    println!("  inserts:     {} ({:.1}%)", inserts, pct(inserts));
    println!("  rmw:         {} ({:.1}%)", rmws, pct(rmws));
    println!(
        "  key space:   {} (initial) → {} (final)",
        workload.keys, n_final,
    );
    println!(
        "  throughput:  {:.0} ops/sec",
        total_ops as f64 / run_elapsed.as_secs_f64(),
    );
    println!();
    println!("latency (ns), per op kind:");
    print_per_op_block("reads  ", reads, &combined_read);
    print_per_op_block("updates", updates, &combined_update);
    print_per_op_block("inserts", inserts, &combined_insert);
    print_per_op_block("rmw    ", rmws, &combined_rmw);

    Ok(())
}
