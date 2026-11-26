"""Roman official reader, tuned variant: roman_datamodels over s3fs with a 1 MiB read block instead of the
50 MiB default (cold, Roman part of all-band queries). Usage: python aws_bench_roman_tuned.py <bucket> [NQN] [block_bytes]"""
import os, sys, time
sys.path.insert(0, os.getcwd())
import numpy as np, warnings; warnings.filterwarnings('ignore')
from concurrent.futures import ThreadPoolExecutor
import chunkbench as cb
from bench_common import positions, load_expo, subset, nic_bytes, peak_rss_gb, Writer
import s3fs, roman_datamodels as rdm
bucket = sys.argv[1]; NQN = int(sys.argv[2]) if len(sys.argv) > 2 else 5
BLOCK = int(sys.argv[3]) if len(sys.argv) > 3 else 1 << 20
ex, W = load_expo(os.path.expanduser('~/stores/exposures.parquet')); URL = dict(zip(ex.exp_id, ex.url))
QP = positions(); out = Writer('aws_results_roman_tuned.parquet'); outer = ThreadPoolExecutor(24)
FS = s3fs.S3FileSystem(client_kwargs={'region_name': 'us-east-2'})
def official(e, box):
    y0, y1, x0, x1 = box
    with FS.open(URL[e][5:], 'rb', block_size=BLOCK) as f, rdm.open(f) as m:
        return {k: np.asarray(getattr(m, k)[y0:y1, x0:x1]) for k in ('data', 'err', 'dq')}
for N in (50, 200):
    Wn = subset(ex, W, N)
    for s in (5, 60, 120):
        for qi, (ra, dec) in enumerate(QP[s][:NQN]):
            n0 = nic_bytes(); t0 = time.perf_counter(); c0 = time.process_time(); jobs = []
            for e, (w, shp, sv, b) in Wn.items():
                if sv == 'rubin': continue
                box = cb.query_box(w, shp, ra, dec, float(s))
                if box is not None: jobs.append(outer.submit(official, e, box))
            res = [j.result() for j in jobs]
            out.add(N=N, mode='cold', fmt='roman_official_tuned', query='all', side=s, qi=qi, block=BLOCK,
                    wall_s=time.perf_counter() - t0, cpu_s=time.process_time() - c0, nic_rx_bytes=nic_bytes() - n0,
                    useful_bytes=sum(a.nbytes for d in res for a in d.values()), n_exp=len(res), peak_rss_gb=peak_rss_gb())
    out.flush(); print(f'N={N} done', flush=True)
print('ALL DONE', flush=True)
