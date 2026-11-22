//! Ingest end to end, in process: real-time send of the full 64-channel
//! layout, then check the store against the generator and the reference.

use std::net::TcpListener;
use std::path::PathBuf;

use diagsteno::config::default_layout;
use diagsteno::features::{Windower, reference};
use diagsteno::ingest::{Ingest, IngestConfig};
use diagsteno::sender;
use diagsteno::synth::{Synth, ticks_for_seconds};
use diagsteno_store::Store;

fn tmp(name: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!("diagsteno-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    p
}

#[test]
fn realtime_ingest_stores_everything_and_features_equal_reference() {
    let layout = default_layout();
    let dir = tmp("e2e");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let cfg = IngestConfig::new(&dir, layout.clone());
    let server = std::thread::spawn(move || {
        let mut ing = Ingest::open(cfg).unwrap();
        ing.serve(&listener, Some(1)).unwrap();
        ing.close().unwrap()
    });
    let ticks = ticks_for_seconds(2);
    let frames: Vec<_> = Synth::new(&layout, 11, Some(ticks)).collect();
    let report = sender::send(addr, frames.clone(), layout.len(), true).unwrap();
    let stats = server.join().unwrap();

    assert_eq!(report.error, None);
    assert_eq!(report.ticks, ticks);
    for c in &layout {
        assert_eq!(
            report.acked.get(&c.id),
            Some(&(ticks - 1)),
            "channel {}",
            c.id
        );
    }
    assert_eq!(stats.missing_frames, 0);
    assert_eq!(stats.duplicate_frames, 0);
    assert_eq!(stats.frames, frames.len() as u64);

    let store = Store::open(&dir).unwrap();
    let mut stored = store.read_samples().unwrap();
    stored.sort_by_key(|b| (b.seq, b.channel));
    assert_eq!(stored.len(), frames.len());
    for (b, f) in stored.iter().zip(&frames) {
        assert_eq!(
            (b.channel, b.seq, &b.samples),
            (f.channel, f.seq, &f.samples)
        );
    }

    // Stored features equal the CPU reference over the same windows.
    let mut w = Windower::new(&layout);
    let want: Vec<_> = frames
        .iter()
        .flat_map(|f| w.push(f))
        .map(|x| reference(&x))
        .collect();
    let mut got = store.read_features().unwrap();
    let key = |r: &diagsteno_store::FeatureRow| (r.channel, r.window);
    got.sort_by_key(key);
    let mut want_sorted = want.clone();
    want_sorted.sort_by_key(key);
    assert_eq!(got, want_sorted);
    let _ = std::fs::remove_dir_all(&dir);
}
