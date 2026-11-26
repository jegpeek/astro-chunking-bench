"""Butler half, tuned variant: identical to aws_bench_butler.py cold runs, except the s3fs default read
block is set to 1 MiB (out of the box it is 50 MiB, so every fsspec seek pulls 50 MiB; the obs_base
formatter calls fs.open(fspath) without a block_size). 120" boxes (600 px) exceed the formatter's
500 px partial-read limit and still download whole files.
Usage (inside `setup lsst_distrib`): python aws_bench_butler_tuned.py <repo> <butler_transfer.json> [NQN] [block_bytes]"""
import os, sys, time, json
sys.path.insert(0, os.getcwd())
import numpy as np, warnings; warnings.filterwarnings('ignore')
from concurrent.futures import ThreadPoolExecutor
import chunkbench as cb
from bench_common import positions, load_expo, subset, nic_bytes, peak_rss_gb, Writer
repo, btj = sys.argv[1], sys.argv[2]; NQN = int(sys.argv[3]) if len(sys.argv) > 3 else 5
BLOCK = int(sys.argv[4]) if len(sys.argv) > 4 else 1 << 20
os.environ['DAF_BUTLER_CACHE_EXPIRATION_MODE'] = 'disabled'
import s3fs; s3fs.S3FileSystem.default_block_size = BLOCK
from lsst.daf.butler import Butler
import lsst.geom as geom
bt = json.load(open(btj))
ex, W = load_expo(os.path.expanduser('~/stores/exposures.parquet'))
QP = positions(); out = Writer('aws_results_butler_tuned.parquet'); pool = ThreadPoolExecutor(24)
b = Butler(repo)
R = {f"dp1_{int(r.dataId['visit'])}_{int(r.dataId['detector'])}": r
     for r in b.query_datasets('visit_image', collections=bt['runs'], where="instrument='LSSTComCam'", limit=None)}
def query(Wn, ra, dec, side, bands):
    jobs = []
    for e, (w, shp, sv, band) in Wn.items():
        if sv != 'rubin' or (bands is not None and band not in bands): continue
        box = cb.query_box(w, shp, ra, dec, side)
        if box is None: continue
        y0, y1, x0, x1 = box
        jobs.append((e, pool.submit(b.get, R[e], parameters={'bbox': geom.Box2I(geom.Point2I(x0, y0), geom.Extent2I(x1 - x0, y1 - y0))})))
    return {e: j.result() for e, j in jobs}
t_all = time.time()
for N in (50, 200):
    Wn = subset(ex, W, N)
    for q, bands in (('all', None), ('r_only', {'r'})):
        for s in (5, 60, 120):
            for qi, (ra, dec) in enumerate(QP[s][:NQN]):
                n0 = nic_bytes(); t0 = time.perf_counter(); c0 = time.process_time()
                res = query(Wn, ra, dec, float(s), bands)
                out.add(N=N, mode='cold', fmt='rubin_butler_tuned', query=q, side=s, qi=qi, block=BLOCK,
                        wall_s=time.perf_counter() - t0, cpu_s=time.process_time() - c0, nic_rx_bytes=nic_bytes() - n0,
                        useful_bytes=sum(x.image.array.nbytes + x.mask.array.nbytes + x.variance.array.nbytes for x in res.values()),
                        n_exp=len(res), peak_rss_gb=peak_rss_gb())
    out.flush(); print(f'N={N} done t={time.time()-t_all:.0f}s', flush=True)
print('ALL DONE', flush=True)
