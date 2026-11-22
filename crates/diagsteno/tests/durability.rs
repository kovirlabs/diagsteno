//! R8: kill the ingest process mid-stream, restart it on the same store, and
//! check that no acknowledged frame was lost. Then resend everything: the
//! restarted ingest stores exactly the missing frames, without duplicates.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use diagsteno::config::default_layout;
use diagsteno::sender;
use diagsteno::synth::{Synth, ticks_for_seconds};
use diagsteno_store::Store;

fn tmp(name: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("diagsteno-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    p
}

/// Start `diagsteno ingest` on an ephemeral port; returns the child and its
/// address.
fn start_ingest(store: &Path, batch_ms: u32) -> (Child, String) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_diagsteno"))
        .args(["ingest", "--listen", "127.0.0.1:0", "--connections", "1"])
        .args(["--batch-ms", &batch_ms.to_string()])
        // A small queue keeps the sender mid-stream when the kill lands.
        .args(["--queue", "1024"])
        .arg("--store")
        .arg(store)
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn diagsteno ingest");
    let mut line = String::new();
    BufReader::new(child.stdout.as_mut().expect("stdout"))
        .read_line(&mut line)
        .expect("read listen line");
    let addr = line
        .trim()
        .strip_prefix("listening on ")
        .unwrap_or_else(|| panic!("unexpected first line {line:?}"))
        .to_string();
    (child, addr)
}

#[test]
fn killed_ingest_loses_no_acknowledged_frame() {
    let layout = default_layout();
    let dir = tmp("r8");
    // 20 s of data, sent as fast as possible, so the kill lands mid-stream.
    let frames: Vec<_> = Synth::new(&layout, 5, Some(ticks_for_seconds(20))).collect();

    let (mut child, addr) = start_ingest(&dir, 20);
    let progress = Arc::new(AtomicU64::new(0));
    let send = {
        let frames = frames.clone();
        let n = layout.len();
        let progress = Arc::clone(&progress);
        std::thread::spawn(move || {
            sender::send_observed(addr, frames, n, false, &progress).unwrap()
        })
    };
    // Kill once acknowledgements are flowing: at least 1 s of data (100
    // ticks) acknowledged, out of 20 s sent.
    let deadline = Instant::now() + Duration::from_secs(120);
    while progress.load(Ordering::Relaxed) < 100 {
        assert!(
            Instant::now() < deadline,
            "no acknowledgements within 120 s"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
    child.kill().expect("kill ingest"); // SIGKILL on Unix, TerminateProcess on Windows
    let _ = child.wait();
    let report = send.join().unwrap();
    let acked: BTreeMap<u16, u64> = report.acked.iter().map(|(&c, &s)| (c, s)).collect();
    let last_tick = ticks_for_seconds(20) - 1;
    assert!(
        acked.values().any(|&s| s < last_tick),
        "the kill should land before everything is acknowledged"
    );

    // Every acknowledged frame is in the store, intact, after a restart.
    let store = Store::open(&dir).unwrap();
    let stored: BTreeMap<(u16, u64), Vec<i32>> = store
        .read_samples()
        .unwrap()
        .into_iter()
        .map(|b| ((b.channel, b.seq), b.samples))
        .collect();
    let mut checked = 0;
    for f in &frames {
        if acked.get(&f.channel).is_some_and(|&a| f.seq <= a) {
            assert_eq!(
                stored.get(&(f.channel, f.seq)),
                Some(&f.samples),
                "lost ch {} seq {}",
                f.channel,
                f.seq
            );
            checked += 1;
        }
    }
    assert!(checked > 0);
    drop(store);

    // Restart and resend everything: duplicates are dropped, the rest stored.
    let (mut child, addr) = start_ingest(&dir, 20);
    let report = sender::send(addr, frames.clone(), layout.len(), false).unwrap();
    assert_eq!(report.error, None);
    let _ = child.wait();
    let store = Store::open(&dir).unwrap();
    let mut all: Vec<(u16, u64)> = store
        .read_samples()
        .unwrap()
        .iter()
        .map(|b| (b.channel, b.seq))
        .collect();
    all.sort_unstable();
    let mut want: Vec<(u16, u64)> = frames.iter().map(|f| (f.channel, f.seq)).collect();
    want.sort_unstable();
    assert_eq!(all, want, "after resend: every frame exactly once");
    eprintln!(
        "R8: {checked} acknowledged frames survived the kill; {} frames total after resend",
        all.len()
    );
    let _ = std::fs::remove_dir_all(&dir);
}
