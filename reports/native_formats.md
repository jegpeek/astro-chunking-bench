# Native storage layout of the benchmark sample

Sample: 24 Rubin `calexp` (OpenUniverse2024 preview, LSST Science Pipelines Butler output, `w_2024_12`) and 18 Roman WAS `simple_model` images (OpenUniverse2024 preview), all overlapping a 6′×6′ patch at RA 9.718°, Dec −44.001°.
Source: `s3://nasa-irsa-simulations/openuniverse2024/` (IRSA, AWS Open Data, us-east-1).

| survey | plane | dtype | shape (rows×cols) | codec | compression tile | raw MB | stored MB |
|---|---|---|---|---|---|---|---|
| roman | DQ | uint32 | 4088x4088 | whole-file gzip (.fits.gz) | entire file | 66.8 |  |
| roman | ERR | float32 | 4088x4088 | whole-file gzip (.fits.gz) | entire file | 66.8 |  |
| roman | SCI | float64 | 4088x4088 | whole-file gzip (.fits.gz) | entire file | 133.7 |  |
| rubin | IMAGE | BITPIX -32 | 4004x4096 | FITS tile GZIP_2 lossless | 1x4096 | 65.6 | 59.72 |
| rubin | MASK | BITPIX 32 | 4004x4096 | FITS tile GZIP_2 lossless | 1x4096 | 65.6 | 0.64 |
| rubin | VARIANCE | BITPIX -32 | 4004x4096 | FITS tile GZIP_2 lossless | 1x4096 | 65.6 | 47.56 |

Mean file size: Rubin calexp 109.0 MB (17 HDUs; pixel planes ≈ 98% of bytes). Roman `.fits.gz` 38.7 MB. Roman ASDF copy (lz4, roman_datamodels default since 0.23) 78.3 MB.

## What this implies for a small-region read

* **Rubin calexp**: FITS tile compression (`GZIP_2`, i.e. byte-shuffled gzip, lossless, no quantization) with tiles **one full row wide (4096×1)**. You can read part of a file: the tile descriptor table (8 bytes per row) gives a byte offset for each row in the heap. But a cutout of *h* rows costs *h* full 4096-pixel rows in each of the 3 planes, whatever the cutout width. Read amplification ≈ 4096 / cutout width. Rubin's float32 image and variance planes compress poorly losslessly (ratio 1.10 and 1.38); the mask compresses about 100×.
* **Roman WAS sims, `.fits.gz`**: the **whole file is one gzip stream**. Random access is impossible. A reader must stream and decompress from byte 0 up to the last row it needs. DQ is the last HDU, so in practice a full read is needed. Note that these simulations store SCI as integer-valued float64, ERR as integer-valued float32 and DQ as all zeros, which is why gzip reaches about 6.9×. Real Roman L2 data will be float32 rates with much higher entropy.
* **Roman ASDF (lz4)**: this is how real Roman data will be delivered. Each array is one compressed block. The Python `asdf` library decompresses a whole block on access, so a cutout costs the full compressed data+err+dq blocks. The lz4 framing is internally chunked, but no current reader exploits that for random access.
* Neither native layout knows anything about the sky. Finding which files cover a position needs an external index (the Butler registry `visit_detector_region` table for Rubin; header WCS for Roman). The files themselves are organised by exposure and detector, not by sky region.
