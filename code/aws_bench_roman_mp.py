"""Roman official reader, best-effort variant: roman_datamodels over s3fs (default and 1 MiB block) with one worker
process per SCA (18), sidestepping the GIL. Pool start-up excluded; cold data. The Roman part of a query is the
same 18 SCAs at every Rubin depth N. Usage: python aws_bench_roman_mp.py [NQN]"""
import os, sys, time
sys.path.insert(0, os.getcwd())
import numpy as np, warnings; warnings.filterwarnings('ignore')
from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp
import chunkbench as cb
from bench_common import positions, load_expo, subset, nic_bytes, Writer
_FS = None
def init():
    global _FS
    import s3fs; _FS = s3fs.S3FileSystem(client_kwargs={'region_name': 'us-east-2'})
def work(a):
    import roman_datamodels as rdm
    url, (y0, y1, x0, x1), block = a
    kw = {} if block is None else {'block_size': block}
    with _FS.open(url[5:], 'rb', **kw) as f, rdm.open(f) as m:
        return sum(np.asarray(getattr(m, k)[y0:y1, x0:x1]).nbytes for k in ('data', 'err', 'dq'))
def warm(_): return 0
if __name__ == '__main__':
    NQN = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    ex, W = load_expo(os.path.expanduser('~/stores/exposures.parquet')); URL = dict(zip(ex.exp_id, ex.url))
    QP = positions(); out = Writer('aws_results_roman_mp.parquet')
    P = ProcessPoolExecutor(18, mp_context=mp.get_context('spawn'), initializer=init); list(P.map(warm, range(36)))
    Wn = subset(ex, W, 50)
    for block in (None, 1 << 20):
        for s in (5, 60, 120):
            for qi, (ra, dec) in enumerate(QP[s][:NQN]):
                tasks = [(URL[e], cb.query_box(w, shp, ra, dec, float(s)), block) for e, (w, shp, sv, b) in Wn.items() if sv != 'rubin']
                tasks = [t for t in tasks if t[1] is not None]
                n0 = nic_bytes(); t0 = time.perf_counter(); ub = sum(P.map(work, tasks))
                out.add(N=50, mode='cold', fmt='roman_official_mp', query='all', side=s, qi=qi, block=block or 0, nproc=18,
                        wall_s=time.perf_counter() - t0, nic_rx_bytes=nic_bytes() - n0, useful_bytes=ub, n_exp=len(tasks))
    out.flush(); print('ALL DONE', flush=True)
