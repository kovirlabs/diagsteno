# data

Generated (synthetic) strain and vibration datasets only, or data Evan confirms
he owns. Never commit machine data from an employer or customer.

## Generating the M7 dataset

Datasets are not committed. They are fully defined by the generator in
`crates/diagsteno/src/synth.rs`, a seed and a length: integer arithmetic
only, so every machine produces the same bytes (a unit test pins the
output).

```
cargo run --release -p diagsteno -- synth --out data/ds-2026-60s.bin --seed 2026 --seconds 60
```

The M7 gate dataset is seed 2026, 60 s: 384,000 frames, 51,072,000 samples,
SHA-256 `8302a7934dd3d3dbcc606b41cc8d28b45150494f7b23a7047e3466d60b20c98f`.
