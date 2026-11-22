# socket-locality (experiment)

Does keeping a stage's memory on its own socket speed up diagsteno on the
Dell? Not a milestone: the full M6 pipeline benchmark is agent B's. This
crate is standalone (its own `[workspace]`), so it does not affect the
diagsteno build or CI. It needs `../../../stagelanes` and `../../../bytelanes`
checked out next to diagsteno.

```
cargo run --release -- memory   --secs 8    # bandwidth and latency, local vs remote memory
cargo run --release -- pipeline --secs 15   # feature stage under four placements
```

- `memory`: threads pinned (stagelanes-arch) to one node's cores read a
  512 MiB `stagelanes-mem` `Region` bound to node 0 or 1. Sequential read
  bandwidth for 1 and 12 threads; dependent-load latency (a random cycle
  through cache lines) for 1 thread.
- `pipeline`: one producer stage copies 4096-sample accelerometer windows
  (memory local to the producer) to 10 workers that run diagsteno's feature
  reference (Q31 FFT and band energies). Layouts: everything on node 0;
  workers on node 1; workers split across both; unpinned.

Results are only valid with the guest's vCPUs pinned 1:1 to host cores and
its NUMA memory bound to the matching host nodes (see the run file).
