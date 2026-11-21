# diagsteno

The Diagnostic Stenographer: scientific signal processing for strain gauge
and accelerometer data. Ingest, FFT and band features on the GPU through
`bytelanes`, anomaly scoring, storage and a query API. Stages run on
`stagelanes`. It is the paralanes demo workload.

Read `../paralanes/docs/PLAN.md` and `../paralanes/docs/SPEC.md` before any
change. The plan sets milestone order and which machine does what. The spec is
the technical source of truth and uses the real crate names; the earlier
`cf-` placeholder names appear in the table below for reference. Where plan and spec disagree, the
plan is current; note the conflict in your status file.

> DRAFT. Items marked TBD are open decisions for Evan.

## Repo map

| Repo | Role | Depends on | Licence |
| --- | --- | --- | --- |
| `paralanes` | Spec, plan, atlas, network join, appliance image, all status files | All three below | GPL-3.0-only |
| `bytelanes` | Bulk data ops on any GPU through wgpu, each with a CPU reference. Owns the op format, which is the contract between all repos | Nothing above it | Apache-2.0 |
| `stagelanes` | Topology-aware stage runtime for Windows and Linux: pinning, rings, pinned memory, stage placement | Nothing above it | Apache-2.0 |
| `diagsteno` | The Diagnostic Stenographer: strain gauge and accelerometer signal processing, the demo workload | `bytelanes`, `stagelanes` | GPL-3.0-only |

Sibling repos are expected as checkouts next to each other (`../paralanes`,
`../bytelanes`, `../stagelanes`, `../diagsteno`). Use path dependencies during
development; switch to published versions only when Evan says so.

## Crates

| Crate | Spec name | Kind | Owns |
| --- | --- | --- | --- |
| `diagsteno` | replaces `cf-txn` | lib + bin | Ingest, band features, anomaly scoring, HTTP query API, export |
| `diagsteno-store` | `cf-store` | lib | Storage of samples and features |
| `data/` | none | data | Generated datasets only |

`bytelanes` and `stagelanes` are added as path dependencies
(`../bytelanes/crates/...`, `../stagelanes/crates/...`) when M7 first needs
them. CI must then check out the sibling repos.

## Milestones in this repo

| # | Milestone | Agent |
| --- | --- | --- |
| M7 | Signal ingest and features: stream ingest, an FFT op added to `bytelanes`, band features, storage | A (Dell); FFT op checked by B and C |
| M8 | Scoring and query: anomaly score per channel, HTTP query API, export to a time-series database. Release 3: `diagsteno` 0.1 | A (Dell) |

## Which agent are you

Three agents work in parallel, one per machine. Identify your machine first
(`uname -sm`, `lscpu` or `sysctl -a | grep machdep.cpu`, `systeminfo` on
Windows, `vulkaninfo --summary`) and act only as that agent. If the milestone
you were given is not assigned to your machine in
`../paralanes/docs/PLAN.md`, stop and say so.

| Agent | Machine | OS |
| --- | --- | --- |
| A | Dell PowerEdge, 2x Xeon Silver (10 cores each), no GPU | Linux guest on Proxmox, cores pinned, both NUMA nodes exposed |
| B | Desktop, Ryzen 7 5700X (8 cores, AM4 Mini-ITX), Radeon RX 7800 XT 16 GB on PCIe 3.0 x16, 1 GbE (details: `docs/machines.md` in paralanes) | Windows |
| C | MacBook Pro, M5 Pro, 24 GB unified memory | macOS, AArch64 |

## Hard rules

- **Agents never merge, tag or publish.** Prepare the pull request, changelog
  and version bump; Evan does the rest. Crates carry `publish = false` until
  Evan removes it.
- **One milestone per session.** Stop at the gate. Do not start the next
  milestone unprompted.
- **No unmeasured numbers.** A performance figure appears in a README, the
  atlas or a status file only with the run file that produced it. Every run
  file records machine, OS, kernel or build number, GPU driver version, wgpu
  backend, GPU host link (PCIe generation and width, or unified memory),
  network link type and speed for runs across machines, Rust version and the
  exact command.
- **A negative result is a result.** Publish it plainly.
- **Data rule.** Use generated data or data Evan confirms he owns. Never
  commit machine data from an employer or customer.
- **No `#[cfg(target_os)]` or `#[cfg(target_arch)]`** outside a module named
  `platform`.
- **Look up current docs** before using wgpu, tokio, io_uring or any
  networking crate API. Do not write them from memory.
- **Do not write NVIDIA or CUDA code.** That path is deferred until NVIDIA
  hardware exists.
- **Licence compatibility.** This repo is GPL-3.0-only. Check the licence of
  every new dependency before adding it: it must be GPL-3.0 compatible (MIT,
  Apache-2.0, BSD, ISC, Zlib, MPL-2.0, LGPL/GPL-3.0 are fine; GPL-2.0-only,
  SSPL, BUSL and proprietary licences are not).
- **Ops belong to `bytelanes`.** The FFT and any other bulk op is added there
  with a CPU reference, not here. Do not redefine the op format.
- **Features equal the CPU reference.** A replayed dataset produces the same
  features on GPU and CPU.
- **Durability does not depend on a GPU (R8).** A record is acknowledged only
  after `diagsteno-store` has flushed it; a kill-and-restart test loses no
  acknowledged record.
- **No dropped samples.** Ingest holds the target channel count and sample
  rate, and reports any drop.
- **Not a database.** Position and build it as an edge ingest and feature
  engine that exports to existing time-series databases.
- **No `unsafe`** in this repo; every crate has `#![forbid(unsafe_code)]`.

## Status files

All status files live in the umbrella repo: write
`../paralanes/docs/status/M<n>.md` (shape defined in `../paralanes/CLAUDE.md`),
whichever repo the work was in, and commit it on your milestone branch there.

At the start of every session: rebase your branch on `main`, then read every
file in `../paralanes/docs/status/`.

## Branches

- `main` is Evan's. Never push to it.
- Work on `m<n>-<short-topic>` (for example `m3-rx7800-ops`). One branch per
  milestone per repo.
- Small commits, one logical change each. Run fmt, clippy and tests before
  each commit.

## Commands

```
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

## Release rule

Agents prepare releases (changelog, README, version bump in a pull request).
Only Evan merges, tags and publishes.

## Open decisions (TBD)

- GitHub owner for the four repos
- Target channel count, sample rate and sensor types for the synthetic data (M7)
- Storage format (suggested: Parquet) (M7)
- Which time-series database to export to (M8)
