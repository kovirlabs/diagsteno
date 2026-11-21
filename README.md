# diagsteno

**Status: pre-alpha.** Nothing here is built or measured yet.

The Diagnostic Stenographer: open, high-rate processing of strain gauge and accelerometer data at the edge. Stream ingest, FFT and band features on the GPU through `bytelanes`, anomaly scoring per channel, storage and an HTTP query API, with stages running on `stagelanes`. It is an ingest and feature engine that exports to existing time-series databases, not a database itself.

Part of the paralanes project: `paralanes` (umbrella: spec, plan, atlas,
network join, appliance image), `bytelanes` (GPU data ops), `stagelanes`
(stage placement runtime) and `diagsteno` (strain and vibration processing).

## Licence

GNU General Public License v3.0 only. See [LICENSE](LICENSE).
