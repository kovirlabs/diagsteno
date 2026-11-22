//! diagsteno: the Diagnostic Stenographer.
//!
//! An edge ingest and feature engine for strain gauge and accelerometer
//! data. Frames arrive over TCP ([`ingest`]), are stored durably before they
//! are acknowledged ([`diagsteno_store`]), and are cut into windows whose FFT
//! and band features run through a `bytelanes` channel backend
//! ([`features`]). [`synth`] generates the synthetic datasets it is
//! developed against. Anomaly scoring, the query API and export to a
//! time-series database come in M8.

#![forbid(unsafe_code)]

pub mod config;
pub mod features;
pub mod frame;
pub mod synth;
