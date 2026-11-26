"""Time native Rubin calexp FITS + Roman .fits.gz cutouts read in place from IRSA's public S3
(laptop -> us-east-1), with the same readers used on the emulator. Counts are client-side."""
import os, sys, time, json
sys.path.append(os.getcwd())
os.environ.setdefault('XDG_CONFIG_HOME', os.path.abspath('astropy_cfg')); os.environ.setdefault('XDG_CACHE_HOME', os.path.abspath('astropy_cache'))
import numpy as np, pandas as pd
from concurrent.futures import ThreadPoolExecutor
from astropy.io import fits
from astropy.wcs import WCS
import chunkbench as cb, readers as rd

BASE = 'https://nasa-irsa-simulations.s3.us-east-1.amazonaws.com/'
expo = pd.read_parquet('stores/exposures.parquet')
qpos = json.load(open('handoff/qpos.json'))
sess = rd.CountingSession(pool=48)
outer, inner = ThreadPoolExecutor(12), ThreadPoolExecutor(24)
rd.XFER = ThreadPoolExecutor(24)
E = {r.exp_id: (WCS(fits.Header.fromstring(r.wcs_header)), (r.ny, r.nx), r.survey, BASE + r.src_key) for r in expo.itertuples()}
rows = []
for side, n_rub, n_rom in ((5, 4, 2), (30, 4, 2), (120, 4, 1)):
    for qi, (ra, dec) in enumerate(qpos[str(side)][:n_rub]):
        for fmt in ('rubin_fits', 'roman_gz'):
            if fmt == 'roman_gz' and qi >= n_rom: continue
            sess.reset(); t0 = time.perf_counter(); jobs = []
            for e, (w, shp, s, url) in E.items():
                b = cb.query_box(w, shp, ra, dec, float(side))
                if b is None: continue
                if fmt == 'rubin_fits' and s == 'rubin': jobs.append(outer.submit(rd.RubinFitsReader(url, sess).cutout, b, inner))
                if fmt == 'roman_gz' and s == 'roman': jobs.append(outer.submit(rd.RomanGzReader(url, sess).cutout, b))
            res = [j.result() for j in jobs]
            rows.append(dict(fmt=fmt, side=side, qi=qi, wall_s=time.perf_counter() - t0, requests=sess.requests,
                             bytes=sess.bytes, n_exp=len(res), useful_bytes=sum(a.nbytes for d in res for a in d.values())))
            print(rows[-1], flush=True)
pd.DataFrame(rows).to_parquet('real_s3_results.parquet')
