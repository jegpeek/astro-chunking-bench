"""Exact-ownership order-14 HEALPix stamps for the 18 Roman L2 cal.asdf files (WCS re-anchored onto the
Rubin patch; pixels untouched) -> data/roman_stamps_o14.parquet"""
import os, sys, json, time
sys.path.append(os.getcwd())
os.environ.setdefault('XDG_CONFIG_HOME', os.path.abspath('astropy_cfg')); os.environ.setdefault('XDG_CACHE_HOME', os.path.abspath('astropy_cache'))
import numpy as np, pandas as pd, asdf, warnings; warnings.filterwarnings('ignore')
from astropy.io import fits
from astropy.wcs import WCS
import chunkbench as cb
p2 = json.load(open('patch2.json')); rex = pd.read_parquet('roman_l2_expo.parquet'); rows = []; t0 = time.time()
for r in rex.itertuples():
    with asdf.open('data/roman_l2/' + os.path.basename(r.local), lazy_load=True, memmap=False) as af:
        planes = {k: np.ascontiguousarray(np.asarray(af['roman'][k])) for k in ('data', 'err', 'dq')}
    w = WCS(fits.Header.fromstring(r.wcs_header)); reg = cb.patch_region(w, (4088, 4088), p2['patch_ra'], p2['patch_dec'], p2['patch_deg'])
    rows += cb.stamps_from_ownership(planes, 'roman_l2', r.exp_id, r.filt, reg, cb.ownership_map(w, reg), 14)
pd.DataFrame(rows).to_parquet('data/roman_stamps_o14.parquet'); print('roman stamps', len(rows), round(time.time() - t0), 's')
