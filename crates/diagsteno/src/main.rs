//! diagsteno: the Diagnostic Stenographer command line.
//!
//! ```text
//! diagsteno synth  --out FILE [--seed N] [--seconds S]
//! diagsteno ingest --store DIR [--listen ADDR] [--connections K] [--batch-ms MS] [--queue FRAMES]
//! diagsteno send   --to ADDR (--dataset FILE | [--seed N] [--seconds S]) [--realtime]
//! diagsteno replay --dataset FILE
//! diagsteno check-store --store DIR
//! ```
//!
//! Every command uses the default 64-channel layout. `ingest` prints
//! `listening on ADDR` once bound, and each command prints a JSON summary
//! on stdout when it finishes.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, HashMap};
use std::net::TcpListener;
use std::process::ExitCode;
use std::time::Duration;

use diagsteno::config::{default_layout, samples_per_second};
use diagsteno::features::{Engine, Window, Windower, reference};
use diagsteno::ingest::{Ingest, IngestConfig};
use diagsteno::synth::{Synth, ticks_for_seconds};
use diagsteno::{dataset, sender};
use serde_json::json;

type Error = Box<dyn std::error::Error + Send + Sync>;

struct Args(HashMap<String, String>);

impl Args {
    fn parse(rest: &[String]) -> Result<Args, Error> {
        let mut m = HashMap::new();
        let mut i = 0;
        while i < rest.len() {
            let k = rest[i]
                .strip_prefix("--")
                .ok_or_else(|| format!("unexpected argument {}", rest[i]))?;
            if k == "realtime" {
                m.insert(k.to_string(), "true".to_string());
                i += 1;
            } else {
                let v = rest
                    .get(i + 1)
                    .ok_or_else(|| format!("--{k} needs a value"))?;
                m.insert(k.to_string(), v.clone());
                i += 2;
            }
        }
        Ok(Args(m))
    }

    fn get(&self, k: &str) -> Option<&str> {
        self.0.get(k).map(String::as_str)
    }

    fn req(&self, k: &str) -> Result<&str, Error> {
        self.get(k)
            .ok_or_else(|| format!("--{k} is required").into())
    }

    fn num<T: std::str::FromStr>(&self, k: &str, default: T) -> Result<T, Error> {
        match self.get(k) {
            None => Ok(default),
            Some(v) => v
                .parse()
                .map_err(|_| format!("--{k}: bad number {v}").into()),
        }
    }
}

fn synth(a: &Args) -> Result<(), Error> {
    let seed = a.num("seed", 1u64)?;
    let seconds = a.num("seconds", 10u64)?;
    let layout = default_layout();
    let n = dataset::write(
        a.req("out")?,
        Synth::new(&layout, seed, Some(ticks_for_seconds(seconds))),
    )?;
    println!(
        "{}",
        json!({"frames": n, "seed": seed, "seconds": seconds, "channels": layout.len(),
               "samples_per_second": samples_per_second(&layout)})
    );
    Ok(())
}

fn ingest(a: &Args) -> Result<(), Error> {
    let listener = TcpListener::bind(a.get("listen").unwrap_or("127.0.0.1:7600"))?;
    println!("listening on {}", listener.local_addr()?);
    let mut cfg = IngestConfig::new(a.req("store")?, default_layout());
    cfg.batch = Duration::from_millis(a.num("batch-ms", 100u64)?);
    cfg.queue = a.num("queue", cfg.queue)?;
    let connections = match a.get("connections") {
        Some(v) => Some(v.parse::<usize>()?),
        None => None,
    };
    let mut ing = Ingest::open(cfg)?;
    ing.serve(&listener, connections)?;
    let s = ing.close()?;
    println!(
        "{}",
        json!({"frames": s.frames, "samples": s.samples, "missing_frames": s.missing_frames,
               "missing_samples": s.missing_samples, "duplicate_frames": s.duplicate_frames,
               "unknown_channel_frames": s.unknown_channel_frames, "feature_rows": s.feature_rows,
               "window_gaps": s.window_gaps, "batches": s.batches,
               "put_samples_ms": s.put_samples_ms, "features_ms": s.features_ms,
               "put_features_ms": s.put_features_ms, "max_flush_ms": s.max_flush_ms})
    );
    Ok(())
}

fn send(a: &Args) -> Result<(), Error> {
    let layout = default_layout();
    let frames = match a.get("dataset") {
        Some(p) => dataset::read(p)?,
        None => {
            let ticks = ticks_for_seconds(a.num("seconds", 10u64)?);
            Synth::new(&layout, a.num("seed", 1u64)?, Some(ticks)).collect()
        }
    };
    let realtime = a.get("realtime").is_some();
    let r = sender::send(a.req("to")?, frames, layout.len(), realtime)?;
    let acked_all = layout
        .iter()
        .all(|c| r.acked.get(&c.id).is_some_and(|&s| s + 1 == r.ticks));
    println!(
        "{}",
        json!({"frames": r.frames, "samples": r.samples, "ticks": r.ticks,
               "late_ticks": r.late_ticks, "realtime": realtime,
               "send_s": r.send_elapsed.as_secs_f64(),
               "sent_samples_per_second": r.samples as f64 / r.send_elapsed.as_secs_f64(),
               "until_last_ack_s": r.elapsed.as_secs_f64(),
               "all_frames_acked": acked_all, "error": r.error,
               "ack_delay_us": {"p50": r.ack_delay_percentile(50.0), "p99": r.ack_delay_percentile(99.0),
                                "max": r.ack_delay_percentile(100.0)}})
    );
    if !acked_all {
        return Err("not every frame was acknowledged".into());
    }
    Ok(())
}

fn replay(a: &Args) -> Result<(), Error> {
    let layout = default_layout();
    let frames = dataset::read(a.req("dataset")?)?;
    let mut w = Windower::new(&layout);
    let windows: Vec<Window> = frames.iter().flat_map(|f| w.push(f)).collect();
    let mut engine = Engine::new(bytelanes::cpu::CpuBackend::new());
    let mut equal = 0u64;
    let mut differ = 0u64;
    for chunk in windows.chunks(256) {
        let got = engine.compute(chunk)?;
        for (g, win) in got.iter().zip(chunk) {
            if *g == reference(win) {
                equal += 1;
            } else {
                differ += 1;
            }
        }
    }
    println!(
        "{}",
        json!({"frames": frames.len(), "windows": windows.len(), "window_gaps": w.gaps(),
               "features_equal_reference": equal, "features_differ": differ})
    );
    if differ > 0 {
        return Err(format!("{differ} windows differ from the CPU reference").into());
    }
    Ok(())
}

fn check_store(a: &Args) -> Result<(), Error> {
    let store = diagsteno_store::Store::open(a.req("store")?)?;
    let blocks = store.read_samples()?;
    let mut per: BTreeMap<u16, Vec<u64>> = BTreeMap::new();
    for b in &blocks {
        per.entry(b.channel).or_default().push(b.seq);
    }
    let mut gaps = 0u64;
    let mut dups = 0u64;
    for seqs in per.values_mut() {
        seqs.sort_unstable();
        for w in seqs.windows(2) {
            match w[1].cmp(&(w[0] + 1)) {
                std::cmp::Ordering::Less => dups += 1,
                std::cmp::Ordering::Greater => gaps += w[1] - w[0] - 1,
                std::cmp::Ordering::Equal => {}
            }
        }
    }
    let features = store.read_features()?;
    println!(
        "{}",
        json!({"sample_frames": blocks.len(), "channels": per.len(), "missing_frames": gaps,
               "duplicate_frames": dups, "feature_rows": features.len()})
    );
    Ok(())
}

fn main() -> ExitCode {
    let argv: Vec<String> = std::env::args().collect();
    let cmd = argv.get(1).map(String::as_str).unwrap_or("");
    let run = || -> Result<(), Error> {
        let a = Args::parse(argv.get(2..).unwrap_or(&[]))?;
        match cmd {
            "synth" => synth(&a),
            "ingest" => ingest(&a),
            "send" => send(&a),
            "replay" => replay(&a),
            "check-store" => check_store(&a),
            _ => Err("usage: diagsteno <synth|ingest|send|replay|check-store> [options]".into()),
        }
    };
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("diagsteno {cmd}: {e}");
            ExitCode::FAILURE
        }
    }
}
