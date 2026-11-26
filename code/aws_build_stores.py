"""Build the joint HEALPix order-14 stores on the EC2 host from the in-AWS copies:
DP1 visit_images (Butler datastore in the private bucket; only the patch rows are range-read) and the
18 Roman L2 cal.asdf copies (s3://<bucket>/roman_l2/). Writes tile-major and per-band stores for
N = 50 and 200 Rubin visits to ~/stores and syncs them to s3://<bucket>/stores/.
Usage (roman env): python aws_build_stores.py <bucket> <butler_transfer.json>"""
import os, sys, json, time, subprocess, shutil
sys.path.insert(0, os.getcwd())
import numpy as np, pandas as pd, asdf, warnings; warnings.filterwarnings('ignore')
from concurrent.futures import ThreadPoolExecutor
from astropy.io import fits
from astropy.wcs import WCS
import chunkbench as cb, readers as rd, s3session

bucket = sys.argv[1]; bt = json.load(open(sys.argv[2]))
p2 = json.load(open('patch2.json'))
man = pd.read_parquet('dp1_manifest.parquet'); rex = pd.read_parquet('roman_l2_expo.parquet')
S = s3session.S3Session(region='us-east-2', pool=64); inner = ThreadPoolExecutor(12)
rd.XFER = ThreadPoolExecutor(24)
OUT = os.path.expanduser('~/stores'); os.makedirs(OUT, exist_ok=True)
t0 = time.time(); rows = []; expo = []

def rubin(rw):
    e, band = rw.exp_id, rw.lsst_band
    url = bt['uris'][e].replace('s3://', 's3://', 1)
    rdr = rd.RubinFitsReader(url, S); rdr.index(); h = rdr.header
    w = WCS(h); shp = (rdr.idx['IMAGE']['ny'], rdr.idx['IMAGE']['nx'])
    reg = cb.patch_region(w, shp, p2['patch_ra'], p2['patch_dec'], p2['patch_deg'])
    planes = rdr.cutout(reg, inner)
    st = cb.stamps_from_ownership(planes, 'rubin', e, band, reg, cb.ownership_map(w, reg), 14, origin=(reg[0], reg[2]))
    return st, dict(exp_id=e, survey='rubin', filt=band, ny=shp[0], nx=shp[1], wcs_header=h.tostring(), url=url, depth_rank=int(rw.depth_rank))

with ThreadPoolExecutor(6) as ex:
    for st, ex_ in ex.map(rubin, man.itertuples()):
        rows += st; expo.append(ex_)
print(f'rubin stamps: {len(expo)} visits, {len(rows)} rows, {time.time()-t0:.0f}s, {S.bytes/1e9:.2f} GB read', flush=True)

for r in rex.itertuples():
    fn = os.path.basename(r.local); url = f's3://{bucket}/roman_l2/{fn}'; tmp = f'/tmp/{fn}'
    S.signed.download_file(bucket, f'roman_l2/{fn}', tmp)
    with asdf.open(tmp, lazy_load=True, memmap=False) as af:
        planes = {k: np.ascontiguousarray(np.asarray(af['roman'][k])) for k in ('data', 'err', 'dq')}
    os.remove(tmp)
    w = WCS(fits.Header.fromstring(r.wcs_header)); reg = cb.patch_region(w, (4088, 4088), p2['patch_ra'], p2['patch_dec'], p2['patch_deg'])
    rows += cb.stamps_from_ownership(planes, 'roman_l2', r.exp_id, r.filt, reg, cb.ownership_map(w, reg), 14)
    expo.append(dict(exp_id=r.exp_id, survey='roman_l2', filt=r.filt, ny=4088, nx=4088, wcs_header=r.wcs_header, url=url, depth_rank=-1))
print(f'roman stamps done, {len(rows)} rows total, {time.time()-t0:.0f}s', flush=True)

st = pd.DataFrame(rows); ex = pd.DataFrame(expo); ex.to_parquet(f'{OUT}/exposures.parquet')
for N in (50, 200):
    ids = set(ex[(ex.survey == 'rubin') & (ex.depth_rank < N)].exp_id) | set(ex[ex.survey == 'roman_l2'].exp_id)
    sub = st[st.exp_id.isin(ids)].reset_index(drop=True)
    for lay, fn in (('tm', cb.write_hats_parquet_v2), ('pb', cb.write_hats_parquet_perband)):
        root = f'{OUT}/{lay}_N{N}'; shutil.rmtree(root, ignore_errors=True); fn(sub, root, 14, 10)
        sz = sum(os.path.getsize(os.path.join(d, f)) for d, _, fs in os.walk(root) for f in fs)
        print(f'  {lay}_N{N}: {sz/1e9:.2f} GB', flush=True)
subprocess.run(['aws', 's3', 'sync', OUT, f's3://{bucket}/stores/', '--region', 'us-east-2', '--only-show-errors'], check=True)
print(f'synced to s3://{bucket}/stores/  total {time.time()-t0:.0f}s', flush=True)
