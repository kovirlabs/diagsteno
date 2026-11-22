//! Socket-locality experiment on the Dell guest (not a milestone; the full
//! M6 pipeline benchmark belongs to agent B).
//!
//! ```text
//! socket-locality memory   [--secs S]   local vs remote memory: bandwidth and latency
//! socket-locality pipeline [--secs S]   diagsteno feature stage under four placements
//! ```
//!
//! Prints one JSON object per measurement on stdout.

#![forbid(unsafe_code)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use bytelanes::conformance::SplitMix64;
use diagsteno::config::{ChannelSpec, Kind};
use diagsteno::features::{Window, reference};
use serde_json::json;
use stagelanes_arch::{OsCpu, Topology, pin_current_thread};
use stagelanes_mem::{LargePages, Region, RegionConfig};

type Error = Box<dyn std::error::Error + Send + Sync>;

/// One OS CPU per physical core on `node`, in core order.
fn cores_on(topo: &Topology, node: u32) -> Vec<OsCpu> {
    topo.cores
        .iter()
        .filter(|c| c.numa_node == Some(node))
        .map(|c| topo.cpus[c.cpus[0]].os)
        .collect()
}

fn region(bytes: usize, node: u32) -> Result<Region, Error> {
    Ok(Region::new(&RegionConfig {
        bytes,
        numa_node: Some(node),
        large_pages: LargePages::Off,
        lock: false,
        max_bytes: 4 << 30,
    })?)
}

// ---------- memory: bandwidth and latency ----------

const BW_BYTES: usize = 512 << 20; // per thread, far above the 16.5 MB L3
const CHASE_BYTES: usize = 512 << 20;
const LINE: usize = 64;

/// Sum the region as u64 words, `passes` times. Returns bytes read and the
/// sum (so the reads are not optimised away).
fn read_pass(buf: &[u8], passes: usize) -> (u64, u64) {
    let mut acc = 0u64;
    for _ in 0..passes {
        for chunk in buf.chunks_exact(64) {
            let mut lane = [0u64; 8];
            for (l, w) in lane.iter_mut().zip(chunk.chunks_exact(8)) {
                *l = u64::from_ne_bytes([w[0], w[1], w[2], w[3], w[4], w[5], w[6], w[7]]);
            }
            acc = acc.wrapping_add(lane.iter().fold(0u64, |a, &x| a.wrapping_add(x)));
        }
    }
    ((buf.len() * passes) as u64, acc)
}

/// Fill `buf` with a single random cycle through its cache lines (Sattolo),
/// each line holding the byte offset of the next.
fn build_chase(buf: &mut [u8], seed: u64) {
    let lines = buf.len() / LINE;
    let mut order: Vec<u32> = (0..lines as u32).collect();
    let mut rng = SplitMix64::new(seed);
    for i in (1..lines).rev() {
        let j = rng.below(i as u64) as usize;
        order.swap(i, j);
    }
    for w in 0..lines {
        let from = order[w] as usize * LINE;
        let to = (order[(w + 1) % lines] as usize * LINE) as u64;
        buf[from..from + 8].copy_from_slice(&to.to_ne_bytes());
    }
}

/// Follow the chain for `steps` dependent loads; returns ns per load.
fn chase(buf: &[u8], steps: u64) -> (f64, u64) {
    let mut at = 0usize;
    let t = Instant::now();
    for _ in 0..steps {
        let w = &buf[at..at + 8];
        at = u64::from_ne_bytes([w[0], w[1], w[2], w[3], w[4], w[5], w[6], w[7]]) as usize;
    }
    (t.elapsed().as_nanos() as f64 / steps as f64, at as u64)
}

fn memory(topo: &Topology, secs: u64) -> Result<(), Error> {
    for threads in [1usize, 12] {
        for cpu_node in [0u32, 1] {
            for mem_node in [0u32, 1] {
                let cpus = cores_on(topo, cpu_node);
                if cpus.len() < threads {
                    return Err(format!("node {cpu_node} has {} cores", cpus.len()).into());
                }
                // Bandwidth: every thread streams its own region on mem_node.
                let start = Arc::new(std::sync::Barrier::new(threads));
                let stop = Arc::new(AtomicBool::new(false));
                let bytes = Arc::new(AtomicU64::new(0));
                let mut hs = Vec::new();
                for &cpu in cpus.iter().take(threads) {
                    let (start, stop, bytes) = (start.clone(), stop.clone(), bytes.clone());
                    hs.push(std::thread::spawn(move || -> Result<u64, Error> {
                        pin_current_thread(cpu)?;
                        let r = region(BW_BYTES, mem_node)?;
                        start.wait();
                        let mut sink = 0u64;
                        while !stop.load(Ordering::Relaxed) {
                            let (b, s) = read_pass(r.as_slice(), 1);
                            sink ^= s;
                            bytes.fetch_add(b, Ordering::Relaxed);
                        }
                        Ok(sink)
                    }));
                }
                std::thread::sleep(Duration::from_millis(500)); // allocation and touch
                let b0 = bytes.load(Ordering::Relaxed);
                let t0 = Instant::now();
                std::thread::sleep(Duration::from_secs(secs));
                let gbs =
                    (bytes.load(Ordering::Relaxed) - b0) as f64 / t0.elapsed().as_secs_f64() / 1e9;
                stop.store(true, Ordering::Relaxed);
                for h in hs {
                    h.join().map_err(|_| "thread panicked")??;
                }
                // Latency: one thread on cpu_node chasing pointers in mem_node.
                let lat = if threads == 1 {
                    let cpu = cpus[0];
                    let h = std::thread::spawn(move || -> Result<f64, Error> {
                        pin_current_thread(cpu)?;
                        let mut r = region(CHASE_BYTES, mem_node)?;
                        build_chase(r.as_mut_slice(), 7);
                        chase(r.as_slice(), 2_000_000); // warm-up
                        Ok(chase(r.as_slice(), 20_000_000).0)
                    });
                    Some(h.join().map_err(|_| "thread panicked")??)
                } else {
                    None
                };
                println!(
                    "{}",
                    json!({"test": "memory", "threads": threads, "cpu_node": cpu_node,
                           "mem_node": mem_node, "local": cpu_node == mem_node,
                           "read_gb_s": (gbs * 100.0).round() / 100.0,
                           "chase_ns_per_load": lat.map(|x| (x * 10.0).round() / 10.0)})
                );
            }
        }
    }
    Ok(())
}

// ---------- pipeline: producer stage -> feature workers ----------

/// A placement: where the producer runs and where each worker runs.
struct Layout {
    name: &'static str,
    producer: Option<OsCpu>,
    workers: Vec<Option<OsCpu>>,
}

fn layouts(topo: &Topology) -> Vec<Layout> {
    let n0 = cores_on(topo, 0);
    let n1 = cores_on(topo, 1);
    // 10 workers per variant so all layouts do the same amount of compute;
    // the producer takes one more core.
    vec![
        Layout {
            name: "same-socket: producer and 10 workers on node 0",
            producer: Some(n0[0]),
            workers: n0[1..11].iter().map(|&c| Some(c)).collect(),
        },
        Layout {
            name: "cross-socket: producer on node 0, 10 workers on node 1",
            producer: Some(n0[0]),
            workers: n1[1..11].iter().map(|&c| Some(c)).collect(),
        },
        Layout {
            name: "split: producer on node 0, 5 workers on each node",
            producer: Some(n0[0]),
            workers: n0[1..6].iter().chain(&n1[1..6]).map(|&c| Some(c)).collect(),
        },
        Layout {
            name: "unpinned: OS scheduler places producer and 10 workers",
            producer: None,
            workers: vec![None; 10],
        },
    ]
}

fn pool(n: usize) -> Vec<Window> {
    let spec = ChannelSpec {
        id: 0,
        kind: Kind::Accel,
        rate_hz: 25_600,
    };
    let mut rng = SplitMix64::new(2026);
    (0..n)
        .map(|i| Window {
            spec,
            index: i as u64,
            samples: (0..spec.window_len())
                .map(|_| rng.below(65_535) as i32 - 32_767)
                .collect(),
        })
        .collect()
}

fn pipeline(topo: &Topology, secs: u64) -> Result<(), Error> {
    for layout in layouts(topo) {
        let stop = Arc::new(AtomicBool::new(false));
        let done = Arc::new(AtomicU64::new(0));
        let queue_ns = Arc::new(AtomicU64::new(0));
        let mut txs = Vec::new();
        let mut workers = Vec::new();
        for cpu in layout.workers.iter().copied() {
            let (tx, rx) = mpsc::sync_channel::<(Instant, Window)>(32);
            txs.push(tx);
            let (done, queue_ns) = (done.clone(), queue_ns.clone());
            workers.push(std::thread::spawn(move || -> Result<(), Error> {
                if let Some(c) = cpu {
                    pin_current_thread(c)?;
                }
                let mut sink = 0f64;
                for (sent, w) in rx {
                    queue_ns.fetch_add(sent.elapsed().as_nanos() as u64, Ordering::Relaxed);
                    let row = reference(&w);
                    sink += row.band_energy[0];
                    done.fetch_add(1, Ordering::Relaxed);
                }
                std::hint::black_box(sink);
                Ok(())
            }));
        }
        let producer = {
            let stop = stop.clone();
            let cpu = layout.producer;
            std::thread::spawn(move || -> Result<(), Error> {
                if let Some(c) = cpu {
                    pin_current_thread(c)?;
                }
                // The producer owns its data: windows are built and copied on
                // its own core, so their memory is local to the producer.
                let src = pool(512);
                let mut i = 0usize;
                while !stop.load(Ordering::Relaxed) {
                    let w = src[i % src.len()].clone();
                    if txs[i % txs.len()].send((Instant::now(), w)).is_err() {
                        break;
                    }
                    i += 1;
                }
                Ok(())
            })
        };
        std::thread::sleep(Duration::from_secs(1)); // warm-up
        let (d0, q0) = (
            done.load(Ordering::Relaxed),
            queue_ns.load(Ordering::Relaxed),
        );
        let t0 = Instant::now();
        std::thread::sleep(Duration::from_secs(secs));
        let (d1, q1) = (
            done.load(Ordering::Relaxed),
            queue_ns.load(Ordering::Relaxed),
        );
        let el = t0.elapsed().as_secs_f64();
        stop.store(true, Ordering::Relaxed);
        producer.join().map_err(|_| "producer panicked")??;
        for w in workers {
            w.join().map_err(|_| "worker panicked")??;
        }
        let windows = d1 - d0;
        println!(
            "{}",
            json!({"test": "pipeline", "layout": layout.name,
                   "windows_per_s": (windows as f64 / el).round(),
                   "samples_per_s": (windows as f64 * 4096.0 / el).round(),
                   "mean_queue_us": ((q1 - q0) as f64 / windows.max(1) as f64 / 1e3 * 10.0).round() / 10.0,
                   "seconds": (el * 100.0).round() / 100.0})
        );
    }
    Ok(())
}

fn main() -> Result<(), Error> {
    let args: Vec<String> = std::env::args().collect();
    let secs = args
        .iter()
        .position(|a| a == "--secs")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(10);
    let topo = Topology::detect()?;
    match args.get(1).map(String::as_str) {
        Some("memory") => memory(&topo, secs),
        Some("pipeline") => pipeline(&topo, secs),
        _ => Err("usage: socket-locality <memory|pipeline> [--secs S]".into()),
    }
}
