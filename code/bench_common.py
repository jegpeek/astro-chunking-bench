"""Shared pieces for the AWS anchor-point benchmark: query positions (identical to v2/v3 for
5/15/30/60", extra 120" positions drawn afterwards), exposure WCS table, per-query native boxes,
instance NIC counters, and an incremental result writer."""
import os, json, time, resource
import numpy as np, pandas as pd
from astropy.io import fits
from astropy.wcs import WCS

P2 = json.load(open('patch2.json'))
SIDES_V3 = [5, 15, 30, 60]

def positions():
    rng = np.random.default_rng(2026)
    def draw(side, n):
        hw = (P2['patch_deg'] / 2 * 3600 - side / np.sqrt(2) - 4) / 3600
        return list(zip(P2['patch_ra'] + rng.uniform(-hw, hw, n) / np.cos(np.radians(P2['patch_dec'])), P2['patch_dec'] + rng.uniform(-hw, hw, n)))
    QP = {s: draw(s, 20) for s in SIDES_V3}   # same generator/order as bench_v3 -> same positions
    QP[120] = draw(120, 20)
    return QP

def load_expo(path):
    ex = pd.read_parquet(path)
    W = {r.exp_id: (WCS(fits.Header.fromstring(r.wcs_header)), (int(r.ny), int(r.nx)), r.survey, r.filt) for r in ex.itertuples()}
    return ex, W

def subset(ex, W, N):
    ids = set(ex[(ex.survey == 'rubin') & (ex.depth_rank < N)].exp_id) | set(ex[ex.survey != 'rubin'].exp_id)
    return {k: v for k, v in W.items() if k in ids}

def nic_bytes():
    rx = 0
    for line in open('/proc/net/dev').read().splitlines()[2:]:
        name, data = line.split(':', 1)
        if name.strip() != 'lo': rx += int(data.split()[0])
    return rx

def peak_rss_gb(): return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e6

class Writer:
    def __init__(self, path): self.path = path; self.rows = []
    def add(self, **kw): self.rows.append(kw)
    def flush(self): pd.DataFrame(self.rows).to_parquet(self.path)
