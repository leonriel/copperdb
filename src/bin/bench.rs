//! copperdb benchmark harness (Phase 2).
//!
//! Single-threaded driver supporting Zipfian / uniform / latest key
//! distributions and a configurable op mix (read / update / insert /
//! read-modify-write). Workloads can be supplied either as a TOML preset
//! (`--config workloads/ycsb-a.toml`) or via individual CLI flags.

use std::path::PathBuf;
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
    /// workload-detail flags below are ignored.
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

    /// Memtable size in bytes. 200 KB by default to stay within the SSTable
    /// writer's per-flush index budget.
    #[arg(long, default_value_t = 200 * 1024)]
    memtable_size: usize,

    // -- Workload-detail overrides. When --config is set, these override
    // individual fields of the loaded TOML. When --config is absent, missing
    // values fall back to the built-in defaults in `default_workload()`. --
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

    /// Fraction of ops that are updates (writes to an existing key).
    #[arg(long)]
    update_pct: Option<f64>,

    /// Fraction of ops that are inserts (writes to a new key, growing the
    /// key space).
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
    let engine: std::sync::Arc<LsmEngine> =
        LsmEngine::open_with_memtable_size(&args.dir, args.memtable_size)?;

    let mut rng = SmallRng::seed_from_u64(args.seed);
    let mut value_buf: Vec<u8> = Vec::with_capacity(workload.value_size);

    // ---- Load phase -----------------------------------------------------
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
        std::thread::sleep(Duration::from_secs(args.cooldown));
    }

    // ---- Run phase ------------------------------------------------------
    eprintln!(
        "[bench] run phase: {}s @ distribution={:?} mix={:?}",
        args.duration, workload.key_distribution, workload.op_mix,
    );

    let key_sampler =
        KeySampler::new(workload.key_distribution, workload.keys, workload.zipfian_theta)?;
    let op_sampler = OpSampler::new(&workload.op_mix);

    let mut hist: Histogram<u64> = Histogram::new(3)?;

    let run_start = Instant::now();
    let run_deadline = run_start + Duration::from_secs(args.duration);

    let mut next_insert_idx: u64 = workload.keys;
    let mut n_current: u64 = workload.keys;
    let mut reads = 0u64;
    let mut updates = 0u64;
    let mut inserts = 0u64;
    let mut rmws = 0u64;
    let mut read_hits = 0u64;

    while Instant::now() < run_deadline {
        let op = op_sampler.sample(rng.random::<f64>());
        match op {
            OpKind::Read => {
                let k = make_key(key_sampler.sample(&mut rng, n_current));
                let t = Instant::now();
                let got = engine.get(&k);
                let elapsed_ns = t.elapsed().as_nanos() as u64;
                hist.record(elapsed_ns)?;
                if got.is_some() {
                    read_hits += 1;
                }
                reads += 1;
            }
            OpKind::Update => {
                let k = make_key(key_sampler.sample(&mut rng, n_current));
                make_value(&mut rng, workload.value_size, &mut value_buf);
                let t = Instant::now();
                engine.put(k, value_buf.clone())?;
                let elapsed_ns = t.elapsed().as_nanos() as u64;
                hist.record(elapsed_ns)?;
                updates += 1;
            }
            OpKind::Insert => {
                let k = make_key(next_insert_idx);
                next_insert_idx += 1;
                n_current = next_insert_idx;
                make_value(&mut rng, workload.value_size, &mut value_buf);
                let t = Instant::now();
                engine.put(k, value_buf.clone())?;
                let elapsed_ns = t.elapsed().as_nanos() as u64;
                hist.record(elapsed_ns)?;
                inserts += 1;
            }
            OpKind::Rmw => {
                let k = make_key(key_sampler.sample(&mut rng, n_current));
                let t = Instant::now();
                let _ = engine.get(&k);
                make_value(&mut rng, workload.value_size, &mut value_buf);
                engine.put(k, value_buf.clone())?;
                let elapsed_ns = t.elapsed().as_nanos() as u64;
                hist.record(elapsed_ns)?;
                rmws += 1;
            }
        }
    }
    let run_elapsed = run_start.elapsed();
    let total_ops = reads + updates + inserts + rmws;

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
        workload.keys, n_current,
    );
    println!(
        "  throughput:  {:.0} ops/sec",
        total_ops as f64 / run_elapsed.as_secs_f64(),
    );
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
