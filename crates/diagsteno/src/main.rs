//! diagsteno: the Diagnostic Stenographer.
//!
//! Stream ingest, FFT and band features (through `bytelanes`), anomaly scoring
//! per channel, an HTTP query API and export to an existing time-series database.
//! Stages run on `stagelanes`.
//!
//! Status: stub. Built in M7 and M8 (replaces the spec's `cf-txn`).

#![forbid(unsafe_code)]

fn main() {
    eprintln!("diagsteno: not implemented yet (M7)");
    std::process::exit(2);
}
