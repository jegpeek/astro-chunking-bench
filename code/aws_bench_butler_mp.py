"""Butler half, best-effort variant: 1 MiB s3fs block AND 8 worker processes, each with its own Butler
(sidesteps the GIL; single-process threaded gets use only ~1-1.7 cores). Pool and Butler start-up are
excluded from timing; data are cold (cache disabled, each position a new sky box).
Usage (inside `setup lsst_distrib`): python aws_bench_butler_mp.py <repo> <butler_transfer.json> [NQN] [NPROC]"""
import os, sys, time, json
sys.path.insert(0, os.getcwd())
import numpy as np, warnings; warnings.filterwarnings('ignore')
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
import multiprocessing as mp
import chunkbench as cb
from bench_common import positions, load_expo, subset, nic_bytes, peak_rss_gb, Writer
os.environ['DAF_BUTLER_CACHE_EXPIRATION_MODE'] = 'disabled'
_B = _R = _T = None
def init(repo, runs):
    global _B, _R, _T
    import s3fs; s3fs.S3FileSystem.default_block_size = 1 << 20
    from lsst.daf.butler import Butler
    _B = Butler(repo)
    _R = {f"dp1_{int(r.dataId['visit'])}_{int(r.dataId['detector'])}": r
          for r in _B.query_datasets('visit_image', collections=runs, where="instrument='LSSTComCam'", limit=None)}
    _T = ThreadPoolExecutor(3)
def work(tasks):
    import lsst.geom as geom
    def one(t):
        e, (y0, y1, x0, x1) = t
        x = _B.get(_R[e], parameters={'bbox': geom.Box2I(geom.Point2I(x0, y0), geom.Extent2I(x1 - x0, y1 - y0))})
        return x.image.array.nbytes + x.mask.array.nbytes + x.variance.array.nbytes
    return sum(_T.map(one, tasks))
def warm(_): return 0
if __name__ == '__main__':
    repo, btj = sys.argv[1], sys.argv[2]; NQN = int(sys.argv[3]) if len(sys.argv) > 3 else 3
    NP = int(sys.argv[4]) if len(sys.argv) > 4 else 8
    bt = json.load(open(btj)); ex, W = load_expo(os.path.expanduser('~/stores/exposures.parquet'))
    QP = positions(); out = Writer('aws_results_butler_mp.parquet')
    P = ProcessPoolExecutor(NP, mp_context=mp.get_context('spawn'), initializer=init, initargs=(repo, bt['runs']))
    list(P.map(warm, range(NP * 2)))
    for N in (50, 200):
        Wn = subset(ex, W, N)
        for s in (5, 60, 120):
            for qi, (ra, dec) in enumerate(QP[s][:NQN]):
                tasks = []
                for e, (w, shp, sv, band) in Wn.items():
                    if sv != 'rubin': continue
                    box = cb.query_box(w, shp, ra, dec, float(s))
                    if box is not None: tasks.append((e, box))
                chunks = [tasks[i::NP * 2] for i in range(NP * 2)]
                n0 = nic_bytes(); t0 = time.perf_counter()
                ub = sum(P.map(work, [c for c in chunks if c]))
                out.add(N=N, mode='cold', fmt='rubin_butler_tuned_mp', query='all', side=s, qi=qi, nproc=NP,
                        wall_s=time.perf_counter() - t0, nic_rx_bytes=nic_bytes() - n0, useful_bytes=ub, n_exp=len(tasks))
        out.flush(); print(f'N={N} done', flush=True)
    print('ALL DONE', flush=True)
