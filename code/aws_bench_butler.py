"""AWS anchor-point benchmark, LSST-stack half: Rubin's own software reading the native DP1 files.
  rubin_butler_local   butler.get('visit_image', ref, parameters={'bbox': ...}) on the in-AWS DirectButler
                       (SQLite registry on disk, datastore in the private Ohio bucket)
  rubin_butler_remote  the same call against Rubin's DP1 remote Butler (Google Cloud), few points only
The cache is disabled for cold runs. A second 120" pass with a local file cache shows the whole-file fallback.
Bytes come from the instance NIC counters (the Butler uses its own S3 client).
Usage (inside `setup lsst_distrib`): python aws_bench_butler.py <repo> <butler_transfer.json> [NQN]"""
import os, sys, time, json, shutil
sys.path.insert(0, os.getcwd())
import numpy as np, warnings; warnings.filterwarnings('ignore')
from concurrent.futures import ThreadPoolExecutor
import chunkbench as cb
from bench_common import positions, load_expo, subset, nic_bytes, peak_rss_gb, Writer

repo, btj = sys.argv[1], sys.argv[2]; NQN = int(sys.argv[3]) if len(sys.argv) > 3 else 5
os.environ['DAF_BUTLER_CACHE_EXPIRATION_MODE'] = 'disabled'
from lsst.daf.butler import Butler
import lsst.geom as geom
bt = json.load(open(btj))
ex, W = load_expo(os.path.expanduser('~/stores/exposures.parquet'))
QP = positions(); out = Writer('aws_results_butler.parquet'); pool = ThreadPoolExecutor(24)

def refs_for(b, collections):
    m = {}
    for r in b.query_datasets('visit_image', collections=collections, where="instrument='LSSTComCam'", limit=None) if collections else []:
        m[f"dp1_{int(r.dataId['visit'])}_{int(r.dataId['detector'])}"] = r
    return m

def butler_query(b, R, Wn, ra, dec, side, bands=None):
    jobs = []
    for e, (w, shp, sv, band) in Wn.items():
        if sv != 'rubin' or (bands is not None and band not in bands): continue
        box = cb.query_box(w, shp, ra, dec, side)
        if box is None: continue
        y0, y1, x0, x1 = box
        bbox = geom.Box2I(geom.Point2I(x0, y0), geom.Extent2I(x1 - x0, y1 - y0))
        jobs.append((e, pool.submit(b.get, R[e], parameters={'bbox': bbox})))
    return {e: j.result() for e, j in jobs}

def measure(b, R, Wn, ra, dec, sz, bands, **meta):
    n0 = nic_bytes(); t0 = time.perf_counter(); c0 = time.process_time()
    res = butler_query(b, R, Wn, ra, dec, sz, bands)
    wall = time.perf_counter() - t0
    ub = sum(x.image.array.nbytes + x.mask.array.nbytes + x.variance.array.nbytes for x in res.values())
    out.add(**meta, wall_s=wall, cpu_s=time.process_time() - c0, nic_rx_bytes=nic_bytes() - n0, useful_bytes=ub,
            n_exp=len(res), peak_rss_gb=peak_rss_gb())
    return res

t_all = time.time()
local = Butler(repo); R = refs_for(local, bt['runs'])
print('local refs', len(R), flush=True)
for N in (50, 200):
    Wn = subset(ex, W, N)
    for mode in ('cold', 'warm'):
        if mode == 'warm': butler_query(local, R, Wn, QP[5][-1][0], QP[5][-1][1], 5.0)
        for q, bands in (('all', None), ('r_only', {'r'})):
            for s in (5, 60, 120):
                for qi, (ra, dec) in enumerate(QP[s][:NQN]):
                    measure(local, R, Wn, ra, dec, float(s), bands, N=N, mode=mode, fmt='rubin_butler_local', query=q, side=s, qi=qi)
        out.flush(); print(f'N={N} {mode} butler_local done t={time.time()-t_all:.0f}s', flush=True)
# validation: Butler cutout pixels == minimal FITS reader pixels (same S3 objects)
import readers as rd, s3session
S = s3session.S3Session(region='us-east-2'); URL = dict(zip(ex.exp_id, ex.url)); Wn = subset(ex, W, 50)
ok = True; nchk = 0
for s in (5, 60):
    ra, dec = QP[s][0]; got = butler_query(local, R, Wn, ra, dec, float(s))
    for e, x in got.items():
        w, shp, sv, b = Wn[e]; nat = rd.RubinFitsReader(URL[e], S).cutout(cb.query_box(w, shp, ra, dec, float(s)), pool)
        ok &= np.array_equal(x.image.array, nat['IMAGE']) and np.array_equal(x.variance.array, nat['VARIANCE']) and np.array_equal(x.mask.array, nat['MASK'])
        nchk += 1
out.add(mode='validation', fmt='rubin_butler_local', ok=bool(ok), n_checked=nchk); out.flush()
print('butler vs minimal reader pixels identical:', ok, nchk, flush=True)
# 120" with a local file cache: first pass fills it, second pass reads from it
os.environ['DAF_BUTLER_CACHE_EXPIRATION_MODE'] = 'size=40000000000'; os.environ['DAF_BUTLER_CACHE_DIRECTORY'] = '/home/ubuntu/butler_cache'
shutil.rmtree('/home/ubuntu/butler_cache', ignore_errors=True)
local_c = Butler(repo); Rc = refs_for(local_c, bt['runs']); Wn = subset(ex, W, 50)
for p in ('cache_fill', 'cache_hit'):
    for qi, (ra, dec) in enumerate(QP[120][:3]):
        measure(local_c, Rc, Wn, ra, dec, 120.0, None, N=50, mode=p, fmt='rubin_butler_local_cache', query='all', side=120, qi=qi)
out.flush()
# remote DP1 Butler (Google Cloud -> AWS Ohio), a few cold points
os.environ['DAF_BUTLER_CACHE_EXPIRATION_MODE'] = 'disabled'
os.environ['BUTLER_RUBIN_ACCESS_TOKEN'] = open(os.path.expanduser('~/.rsp_token')).read().strip()
remote = Butler('https://data.lsst.cloud/api/butler/repo/dp1/butler.yaml')
Rr = refs_for(remote, ['LSSTComCam/DP1']); Rr = {k: v for k, v in Rr.items() if k in R}
print('remote refs', len(Rr), flush=True)
for s in (5, 60):
    for qi, (ra, dec) in enumerate(QP[s][:3]):
        measure(remote, Rr, Wn, ra, dec, float(s), None, N=50, mode='cold', fmt='rubin_butler_remote', query='all', side=s, qi=qi)
out.flush(); print(f'ALL DONE {time.time()-t_all:.0f}s', flush=True)
