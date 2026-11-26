"""v5 grid on real S3 (cold, all-band unless noted): single-process vs process-parallel tiled readers, for the
baseline order-14 zstd-5 tile-major stores (N=50, 200) and the N=200 layout variants in stores_v5/.
Validation: every format's cutouts == baseline single-process tile-major cutouts (bit-identical, NaN-aware).
Usage (roman env): python aws_bench_v5.py <bucket> [NQ] [NPROC]  -> aws_results_v5.parquet"""
import os, sys, time
os.environ.setdefault('ARROW_DEFAULT_MEMORY_POOL', 'system'); os.environ.setdefault('MALLOC_ARENA_MAX', '4')
sys.path.insert(0, os.getcwd())
import numpy as np, warnings; warnings.filterwarnings('ignore')
from concurrent.futures import ThreadPoolExecutor
import chunkbench as cb, readers as rd, s3session, joint2
from bench_common import positions, load_expo, subset, nic_bytes, Writer
if __name__ == '__main__':
    bucket = sys.argv[1]; NQ = int(sys.argv[2]) if len(sys.argv) > 2 else 10; NP = int(sys.argv[3]) if len(sys.argv) > 3 else 8
    B = f's3://{bucket}'; EXP = os.path.expanduser('~/stores/exposures.parquet')
    S = s3session.S3Session(region='us-east-2', pool=128); rd.XFER = ThreadPoolExecutor(64); inner = ThreadPoolExecutor(32)
    ex, W = load_expo(EXP); QP = positions(); out = Writer('aws_results_v5.parquet')
    FORMATS = [  # name, N, kind, base, order
        ('tm_o14_zstd5', 50, 'hats', f'{B}/stores/tm_N50', 14), ('tm_o14_zstd5', 200, 'hats', f'{B}/stores/tm_N200', 14),
        ('tm_o14_lz4', 200, 'hats', f'{B}/stores_v5/tm_o14_lz4', 14), ('tm_o14_zstd1', 200, 'hats', f'{B}/stores_v5/tm_o14_zstd1', 14),
        ('tm_o13', 200, 'hats', f'{B}/stores_v5/tm_o13', 13), ('tm_o12', 200, 'hats', f'{B}/stores_v5/tm_o12', 12),
        ('blob_o14', 200, 'blob', f'{B}/stores_v5/blob_o14', 14), ('blob_o14_lz4', 200, 'blob', f'{B}/stores_v5/blob_o14_lz4', 14)]
    only = os.environ.get('V5_ONLY'); FORMATS = [f for f in FORMATS if not only or f[0] in only.split(',')]
    def single(kind, base, order, Wn): return (joint2.BlobReader if kind == 'blob' else rd.HatsPixelReader)(base, order, 10, S, Wn)
    def same(a, b):
        if set(a) != set(b): return False
        return all(set(a[e]) == set(b[e]) and all(np.array_equal(a[e][p], b[e][p], equal_nan=a[e][p].dtype.kind == 'f') for p in a[e]) for e in a)
    ref = {}
    for N in (50, 200):
        Wn = subset(ex, W, N); h = rd.HatsPixelReader(f'{B}/stores/tm_N{N}', 14, 10, S, Wn)
        for s in (5, 60): ra, dec = QP[s][0]; ref[(N, s)] = h.cutouts(ra, dec, float(s), inner)
    t_all = time.time()
    for name, N, kind, base, order in FORMATS:
        Wn = subset(ex, W, N)
        for par in (False, True):
            P = joint2.ParallelTiled(kind, base, order, 10, EXP, N, Wn, nproc=NP, nthreads=8) if par else None
            fmt = name + ('_par' if par else '')
            for q, bands in (('all', None), ('r_only', {'r'})):
                for s in (5, 60, 120):
                    if q == 'r_only' and s == 120: continue
                    for qi, (ra, dec) in enumerate(QP[s][:NQ]):
                        S.reset(); n0 = nic_bytes(); t0 = time.perf_counter(); c0 = time.process_time()
                        res = P.cutouts(ra, dec, float(s), bands, cold=True) if par else single(kind, base, order, Wn).cutouts(ra, dec, float(s), inner, bands=bands)
                        wall = time.perf_counter() - t0
                        out.add(N=N, mode='cold', fmt=fmt, layout=name, parallel=par, query=q, side=s, qi=qi, wall_s=wall,
                                cpu_parent_s=time.process_time() - c0, requests=None if par else S.requests, nic_rx_bytes=nic_bytes() - n0,
                                useful_bytes=sum(a.nbytes for d in res.values() for a in d.values()), n_exp=len(res))
                    del res; joint2.trim()
            okv = True
            for s in (5, 60):
                ra, dec = QP[s][0]
                got = P.cutouts(ra, dec, float(s), None, cold=True) if par else single(kind, base, order, Wn).cutouts(ra, dec, float(s), inner)
                okv &= same(got, ref[(N, s)])
            out.add(N=N, mode='validation', fmt=fmt, layout=name, parallel=par, ok=bool(okv)); out.flush()
            print(f'{fmt} N={N} done valid={okv} t={time.time()-t_all:.0f}s', flush=True)
            if P: P.close()
    print('ALL DONE', flush=True)
