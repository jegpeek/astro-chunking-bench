# Joint sky-tiled LSST + Roman pixels vs the official stacks — AWS side-by-side (v4)

**Question.** v2/v3 ran against an S3 emulator and minimal-byte readers. Does the advantage of a joint HEALPix sky-tile store survive on real S3, against the *official* software that most users will run: the LSST Science Pipelines Butler (`butler.get(..., parameters={'bbox': ...})`) and `roman_datamodels`?

**Answer.** Yes. On real S3 in us-east-2, the joint tile-major store answers a cold all-band cutout query across 50–200 Rubin visits plus 18 Roman SCAs as follows:
- **2–9× faster than the official stacks at their best**: 1 MiB reads, multiprocess, Rubin and Roman run in parallel.
- **10–88× faster than the official stacks out of the box.**
- **11–200× fewer bytes** in every case.

The emulator predicted the tiled-store bytes and request counts exactly. It *under*-predicted the latency of native-format reads by 1.3–2.9×, so the earlier emulator-based speedups were conservative.

![Cold all-band query time and bytes received, real S3 (us-east-2), for the official stacks (out of the box and best effort), a minimal-byte native reader, and the joint tile-major store. Medians over 5 sky positions (tiled: 10; best-effort Butler: 3). Rubin: DP1 visit_image (FITS, GZIP_2); Roman: I-Sim L2 cal.asdf (lz4).](../figures/fig_aws_official_vs_tiled.png)

## 1. Setup

| item | value |
|---|---|
| compute | EC2 m7i.2xlarge (8 vCPU, 31.5 GB), on-demand, Ubuntu 24.04, us-east-2 (Ohio), new-experience project with $25 spend limit |
| storage | one S3 bucket in us-east-2: Butler datastore `dp1repo/` (200 DP1 visit_image, 21.3 GB), `roman_l2/` (18 cal.asdf, 3.72 GB), tiled stores `stores/` (12.0 GB); 409 objects, 36.99 GB total |
| Rubin official | LSST Science Pipelines `w_2026_47` (`lsst_distrib`), DirectButler with SQLite registry over an S3 datastore, populated with `transfer_from` the DP1 remote Butler |
| Roman official | `roman_datamodels` 1.2.0 / `asdf` 6.0.0 over `s3fs` 2026.9.0 |
| joint stores | HEALPix NESTED order 14 (13″) tiles, order-10 partitions, HATS-style Parquet; tile-major (`tiled_tm`) and per-band (`tiled_pb`) layouts; built on the instance from the same S3 objects |
| query grid | box side 5″/60″/120″; Rubin depth N = 50, 200; all-band (Rubin + Roman) and r-only; cold (fresh reader, no cache) and warm |
| bytes | instance NIC counters (`/proc/net/dev`); for the tiled stores, NIC/client-counted ratio = 1.01 |
| validation | tiled stores vs native files bit-identical: 8/8 checks (1,144 exposure cutouts). Butler cutouts vs minimal FITS reader: identical, 100/100 |

**"Official" tiers** (all-band time = Rubin part + Roman part; *sum* = run in sequence, *max* = ideal parallel):
1. **Out of the box.** `butler.get(bbox)` with 24 threads in one process, plus `roman_datamodels` with 24 threads; library defaults.
2. **Tuned reads.** Same, with the `s3fs` read block set to 1 MiB (§3).
3. **Best effort.** 1 MiB block *and* process parallelism: 8 Butler processes, 18 Roman processes, max of the two parts.

The tiled readers and the minimal-byte native reader stay single-process (threads only), so the comparison is conservative in the official stacks' favour.

## 2. Headline numbers (cold, all-band)

From [summary_aws_allband.csv](../results/summary_aws_allband.csv):

| N | box | joint tile-major | per-band | minimal native | official out of box (sum) | official best effort (max) | speed-up vs best effort | bytes: tile-major / best effort |
|---|---|---|---|---|---|---|---|---|
| 50 | 5″ | 0.38 s | 0.41 s | 8.78 s | 17.88 s | 3.56 s | 9.3× | 0.011 / 2.17 GB (200×) |
| 50 | 60″ | 1.03 s | 1.72 s | 10.54 s | 26.71 s | 3.60 s | 3.5× | 0.154 / 2.55 GB (17×) |
| 50 | 120″ | 2.76 s | 4.42 s | 12.68 s | 27.96 s | 5.77 s | 2.1× | 0.50 / 7.33 GB (15×) |
| 200 | 5″ | 0.59 s | 0.62 s | 12.28 s | 51.93 s | 4.90 s | 8.3× | 0.026 / 2.89 GB (112×) |
| 200 | 60″ | 2.40 s | 3.93 s | 19.37 s | 77.79 s | 13.31 s | 5.5× | 0.41 / 4.37 GB (11×) |
| 200 | 120″ | 7.49 s | 11.88 s | 27.86 s | 89.93 s | 21.38 s | 2.9× | 1.34 / 23.5 GB (18×) |

**Single-band (r-only, Rubin only).** From [summary_aws_ronly.csv](../results/summary_aws_ronly.csv):
- The per-band store is fastest: 0.12–0.62 s at N=50 and 0.17–1.84 s at N=200.
- Single-process Butler: 2.0–7.6 s at N=50 and 6.6–25.3 s at N=200.
- Bytes: 0.002–0.30 GB (per-band store) vs 0.08–5.9 GB (Butler).
- Best-effort multiprocess was not run for r-only.

**Warm** (reader and footers already open) changes the tiled numbers modestly; tile-major N=200 5″ drops from 0.59 to 0.40 s. It does not change the official paths: Butler and `roman_datamodels` re-open files on each `get`.

## 3. What limits the official stacks

**Rubin Butler — the `s3fs` 50 MiB read block.**
- The obs_base `FitsExposureFormatter` *does* take its remote partial-read path for boxes ≤ 500 px (traced: `read_from_uri` returns the `ExposureF` directly).
- It opens the file with `fs.open(fspath)` and no `block_size`, so it inherits `s3fs`'s 50 MiB default. Header walks and section reads then pull 50 MiB per seek, about 101 MB for a 106 MB visit_image, even for a 25 px box.
- One visit, 25 px box, by block size: 50 MiB → 101.3 MB, 2.78 s; 8 MiB → 34.2 MB, 0.92 s; 2 MiB → 8.8 MB; 1 MiB → 4.6 MB, 0.65 s; 256 KiB → 2.5 MB, 0.97 s.
- A one-line change (e.g. `block_size=1<<20` in the formatter, or a datastore-level fsspec setting) would cut Butler cutout bandwidth by roughly 20×.
- Boxes > 500 px (120″ at 0.2″/px) still download whole files by design.

**Rubin Butler — CPU and the GIL.**
- With threads in one process, CPU time is 1.1–1.7× wall time on 8 cores.
- Each `get` rebuilds a full `ExposureF`: header, WCS, PSF, calibration and aperture-correction HDUs. That costs about 0.24 s of CPU per visit even for a 5″ box.
- So the 1 MiB block alone halves the 5″ time (11.1 → 5.6 s, N=50) but does not help at 60″ (20.1 → 21.9 s).
- Eight processes bring N=50 to 1.37 / 3.60 / 5.78 s and N=200 to 4.90 / 13.31 / 21.38 s at 5/60/120″.

**Rubin Butler — local file cache.**
- Enabling the Butler file cache makes the first 120″ query download all 50 files (5.44 GB, 20.9 s).
- Later queries are cache hits at about 7.0 s, CPU-bound on local FITS decompression.

**Roman — format, not software.**
- Every L2 array is a single whole-array lz4 block, so any cutout must fetch and decompress all of `data`, `err` and `dq`, about 108 MB per SCA.
- Out of the box `roman_datamodels` reads about 170 MB per SCA (3.07 GB per query) in 6.6–6.9 s, regardless of box size.
- A 1 MiB block cuts that to 1.94 GB, the format floor, but is slightly slower in one process (7.3–7.7 s).
- 18 processes give 3.4–3.6 s at either block size.
- No reader setting can get below this floor; only re-chunking the arrays can.

**The remote DP1 Butler** (Rubin's hosted service on Google Cloud, read from AWS Ohio, N=50, 3 points):
- 5″ in 6.60 s with 1.15 GB moved (about 23 MB per visit).
- 60″ in 20.07 s.
- It moves fewer bytes than the in-AWS Butler's defaults, consistent with fsspec's smaller default block for HTTP (inferred, not traced).

## 4. Emulator vs real S3

From [aws_vs_emulator.csv](../results/aws_vs_emulator.csv); same sky positions, joined per position:

| reader | bytes (S3 / emulator) | requests (S3 / emulator) | wall (S3 / emulator) |
|---|---|---|---|
| joint tile-major | 1.000 | 1.000 | 0.87–1.20 |
| per-band | 1.000 | 1.000 | 0.68–1.19 (r-only); 1.57–2.41 (all-band) |
| minimal native | 1.00–1.19 | 1.0–1.6 | 1.29–2.90 |

Consequences:
- **Bytes and request counts are exact predictions** for the tiled stores. The cost model from v1–v3 transfers directly.
- **The emulator was optimistic for native reads**, so the v2/v3 native-vs-tiled speedups were underestimates.
- **On real S3, tile-major beats per-band for all-band queries** (the reverse of v3). An all-band query fans out over many per-band objects and pays S3's per-request latency tail. Per-band still wins single-band (r-only) queries.
- **v3 recommendation, revised:** use tile-major as the primary layout. Keep per-band only if single-band access dominates.

## 5. Other checks

- **Cross-region in-place Roman reads** (STScI `stpubdata`, us-east-1, anonymous) vs the in-region Ohio copy: 7.94 / 7.68 s vs 8.15 / 7.81 s at 5″/60″. There is no measurable penalty, so copying Roman data in-region is unnecessary for this workload.
- **Instance NIC vs client-counted bytes:** median 1.01–1.02. Protocol overhead is negligible.

## 5b. WCS fidelity of the tiled store

How WCS is carried:
- The tiled store keeps one WCS per exposure in `exposures.parquet` (a FITS TAN-SIP header string).
- Cutouts come back as un-resampled pixel blocks tagged with exposure id and box origin.
- A cutout's WCS is the parent WCS with CRPIX shifted by the box origin, which is exact because nothing is resampled.
- The benchmark used these headers for pixel selection in every format, including the official ones.

How the stored headers compare with the native WCS objects (measured on the instance):

| survey | native WCS | stored header | residual vs native |
|---|---|---|---|
| Roman I-Sim L2 | GWCS pipeline: detector → v2v3 → v2v3vacorr → v2v3corr → world; detector → v2v3 is a degree-5 2D polynomial (distortion up to 14.7 px vs pure TAN) | TAN-SIP order 5 fitted by `to_fits_sip` | max 3.9×10⁻⁷ px; 0.000 mas on all 18 SCAs |
| Rubin DP1 visit_image | `SkyWcs` (AST FrameSet in its own HDU) | TAN-SIP order 5 approximation from the image header | max 0.04–0.66 mas, rms 0.02–0.17 mas (≤ 0.003 px; 8 visits) |

For these files the SIP headers are effectively exact. A production store should still carry the native WCS objects verbatim, alongside the SIP header for interoperability:
- Roman: the GWCS subtree, serialized as ASDF.
- Rubin: the `SkyWcs` / AST serialization.
- A cutout's WCS is then the native WCS composed with a pixel shift (GWCS `Shift` models; `SkyWcs.copyAtShiftedPixelOrigin`).
- This adds a few KB per exposure, read once per query, and nothing to per-tile bytes.

Outputs: `wcs_check.txt`, `wcs_sanity.txt`.

## 6. Caveats

- Anchor points only: 3–10 positions per cell; medians reported. Cross-region and remote-Butler rows have 3 points each.
- One instance type (8 vCPU). The best-effort official tier scales with cores; the tiled readers were not given extra processes.
- The joint-time *max* assumes Rubin and Roman parts run perfectly in parallel on the same box. Both are CPU-heavy, so the true joint time lies between max and sum.
- Butler `get` returns a richer object (`ExposureF`, with PSF, WCS and calibration) than the tiled store (pixel arrays plus WCS per exposure). Part of the Butler CPU is that extra work, which an AI pipeline that wants pixels may not need.
- Roman I-Sim WCS was shifted onto the DP1 patch (CRVAL only). Same-sky is not physically real, but file formats and sizes are.
- Cost figures in §7 are estimates; the AWS Billing console is authoritative.

## 7. Cost (estimate)

- **EC2:** m7i.2xlarge on-demand, roughly $0.40/h in us-east-2, for the hours the instance existed.
- **EBS:** 116 GB gp3, roughly $9 per month pro rata while the volume exists.
- **S3:** 37 GB stored, about $0.85 per month pro rata. GET request charges are cents.
- **Transfer:** in-region S3→EC2 is free; DP1 transfer from Rubin's service and anonymous reads of `stpubdata` incur no requester charge.
- Expected total: a few dollars, well inside the $25 limit.

## 8. Artifacts

- Results:
  - [aws_results_roman.parquet](../results/aws_results_roman.parquet): tiled, minimal native, `roman_official`, cross-region.
  - [aws_results_butler.parquet](../results/aws_results_butler.parquet): Butler out of the box, cache, remote.
  - [aws_results_butler_tuned.parquet](../results/aws_results_butler_tuned.parquet).
  - [aws_results_roman_tuned.parquet](../results/aws_results_roman_tuned.parquet).
  - [aws_results_butler_mp.parquet](../results/aws_results_butler_mp.parquet).
  - [aws_results_roman_mp.parquet](../results/aws_results_roman_mp.parquet).
- Benchmarks:
  - [aws_bench_roman.py](../code/aws_bench_roman.py).
  - [aws_bench_butler.py](../code/aws_bench_butler.py).
  - [aws_bench_butler_tuned.py](../code/aws_bench_butler_tuned.py).
  - [aws_bench_roman_tuned.py](../code/aws_bench_roman_tuned.py).
  - [aws_bench_butler_mp.py](../code/aws_bench_butler_mp.py).
  - [aws_bench_roman_mp.py](../code/aws_bench_roman_mp.py).
  - [bench_common.py](../code/bench_common.py).
- Data staging:
  - [aws_butler_transfer.py](../code/aws_butler_transfer.py).
  - [aws_butler_to_s3.py](../code/aws_butler_to_s3.py). Note: `Butler.makeRepo(..., forceConfigRoot=False)` is required for an S3 datastore root.
  - [aws_copy_roman.sh](../code/aws_copy_roman.sh).
  - [aws_build_stores.py](../code/aws_build_stores.py).
- Libraries: [chunkbench.py](../code/chunkbench.py), [readers.py](../code/readers.py).
- Setup: [aws_runbook.md](../aws/aws_runbook.md).
- Previous report: [report_v3.md](report_v3.md).
