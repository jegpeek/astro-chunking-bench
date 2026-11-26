"""AWS anchor-point benchmark, Roman-env half. Formats (all read from real S3 in us-east-2 unless noted):
  tiled_tm      joint tile-major store           s3://<bucket>/stores/tm_N{N}
  tiled_pb      per-band store                   s3://<bucket>/stores/pb_N{N}
  native_best   minimal-byte readers on native files (DP1 FITS in the Butler datastore + Roman ASDF copies)
  roman_official roman_datamodels over s3fs on the Roman ASDF copies (Roman part of a joint query only)
  roman_inplace_xregion  minimal-byte ASDF reader on STScI's bucket in us-east-1 (anonymous), few points
Usage: python aws_bench_roman.py <bucket> [NQ] [NQN]   -> aws_results_roman.parquet"""
import os, sys, time
sys.path.insert(0, os.getcwd())
import numpy as np, warnings; warnings.filterwarnings('ignore')
from concurrent.futures import ThreadPoolExecutor
import chunkbench as cb, readers as rd, s3session
from bench_common import positions, load_expo, subset, nic_bytes, peak_rss_gb, Writer

bucket = sys.argv[1]; NQ = int(sys.argv[2]) if len(sys.argv) > 2 else 10; NQN = int(sys.argv[3]) if len(sys.argv) > 3 else 5
B = f's3://{bucket}'
S = s3session.S3Session(region='us-east-2', pool=128)
rd.XFER = ThreadPoolExecutor(64)
outer, inner, bandpool = ThreadPoolExecutor(24), ThreadPoolExecutor(32), ThreadPoolExecutor(16)
os.makedirs(os.path.expanduser('~/stores'), exist_ok=True)
S.signed.download_file(bucket, 'stores/exposures.parquet', os.path.expanduser('~/stores/exposures.parquet'))
ex, W = load_expo(os.path.expanduser('~/stores/exposures.parquet'))
URL = dict(zip(ex.exp_id, ex.url))
QP = positions(); out = Writer('aws_results_roman.parquet')

import s3fs, roman_datamodels as rdm, asdf
FS = s3fs.S3FileSystem(client_kwargs={'region_name': 'us-east-2'})

def roman_official(e, box):
    y0, y1, x0, x1 = box
    with FS.open(URL[e][5:], 'rb') as f:
        with rdm.open(f) as m:
            return {k: np.asarray(getattr(m, k)[y0:y1, x0:x1]) for k in ('data', 'err', 'dq')}

def native(Wn, ra, dec, side, cache, bands, fmt, roman_base=None):
    jobs = []
    for e, (w, shp, sv, b) in Wn.items():
        if bands is not None and b not in bands: continue
        box = cb.query_box(w, shp, ra, dec, side)
        if box is None: continue
        if sv == 'rubin':
            if fmt == 'roman_official' or fmt == 'roman_inplace_xregion': continue
            rdr = cache.get(e) or rd.RubinFitsReader(URL[e], S); cache[e] = rdr
            jobs.append(outer.submit(rdr.cutout, box, inner))
        else:
            if fmt == 'roman_official': jobs.append(outer.submit(roman_official, e, box))
            else:
                url = URL[e] if roman_base is None else f'{roman_base}/{URL[e].rsplit("/", 1)[1]}'
                jobs.append(outer.submit(rd.RomanAsdfReader(url, S).cutout, box))
    return [j.result() for j in jobs]

def make_tiled(fmt, N, Wn):
    if fmt == 'tiled_pb': return rd.PerBandReader(f'{B}/stores/pb_N{N}', 14, 10, S, Wn, bandpool)
    return rd.HatsPixelReader(f'{B}/stores/tm_N{N}', 14, 10, S, Wn)

def measure(fn, **meta):
    S.reset(); n0 = nic_bytes(); t0 = time.perf_counter(); c0 = time.process_time()
    res = fn()
    wall = time.perf_counter() - t0; cpu = time.process_time() - c0; nic = nic_bytes() - n0
    vals = res.values() if isinstance(res, dict) else res
    out.add(**meta, wall_s=wall, cpu_s=cpu, requests=S.requests, bytes=S.bytes, nic_rx_bytes=nic,
            useful_bytes=sum(a.nbytes for d in vals for a in d.values()), n_exp=len(res), peak_rss_gb=peak_rss_gb())
    return res

QUERIES = (('all', None), ('r_only', {'r'}))
t_all = time.time()
for N in (50, 200):
    Wn = subset(ex, W, N)
    for mode in ('cold', 'warm'):
        for fmt in ('tiled_tm', 'tiled_pb', 'native_best', 'roman_official'):
            shared = make_tiled(fmt, N, Wn) if fmt.startswith('tiled') else {}
            if mode == 'warm':
                if fmt.startswith('tiled'): shared.cutouts(QP[5][-1][0], QP[5][-1][1], 5.0, inner)
                else: native(Wn, QP[5][-1][0], QP[5][-1][1], 5.0, shared, None, fmt)
            for q, bands in QUERIES:
                if fmt == 'roman_official' and q == 'r_only': continue
                for s in (5, 60, 120):
                    n = NQ if fmt.startswith('tiled') else NQN
                    for qi, (ra, dec) in enumerate(QP[s][:n]):
                        if fmt.startswith('tiled'):
                            h = shared if mode == 'warm' else make_tiled(fmt, N, Wn)
                            measure(lambda: h.cutouts(ra, dec, float(s), inner, bands=bands), N=N, mode=mode, fmt=fmt, query=q, side=s, qi=qi)
                        else:
                            cch = shared if mode == 'warm' else {}
                            measure(lambda: native(Wn, ra, dec, float(s), cch, bands, fmt), N=N, mode=mode, fmt=fmt, query=q, side=s, qi=qi)
            out.flush(); print(f'N={N} {mode} {fmt} done t={time.time()-t_all:.0f}s', flush=True)
# cross-region in-place Roman reads (STScI bucket, us-east-1, anonymous) vs the Ohio copies, few points
Wn = subset(ex, W, 50)
for s in (5, 60):
    for qi, (ra, dec) in enumerate(QP[s][:3]):
        measure(lambda: native(Wn, ra, dec, float(s), {}, None, 'roman_inplace_xregion', 's3://stpubdata/roman/nexus/soc_simulations/r00342/l2'),
                N=50, mode='cold', fmt='roman_inplace_xregion', query='all', side=s, qi=qi)
        measure(lambda: native(Wn, ra, dec, float(s), {}, None, 'roman_inplace_xregion'), N=50, mode='cold', fmt='roman_copy_inregion', query='all', side=s, qi=qi)
out.flush()
# bit-identical validation: tiled stores vs minimal native readers on the same S3 objects
ok = True; nchk = 0
for N in (50, 200):
    Wn = subset(ex, W, N)
    for s in (5, 60):
        ra, dec = QP[s][0]
        nat = {}
        for e, (w, shp, sv, b) in Wn.items():
            box = cb.query_box(w, shp, ra, dec, float(s))
            if box is None: continue
            nat[e] = rd.RubinFitsReader(URL[e], S).cutout(box, inner) if sv == 'rubin' else rd.RomanAsdfReader(URL[e], S).cutout(box)
        for fmt in ('tiled_tm', 'tiled_pb'):
            G = make_tiled(fmt, N, Wn).cutouts(ra, dec, float(s), inner)
            good = set(G) == set(nat) and all(np.asarray(G[e][k]).tobytes() == np.ascontiguousarray(np.asarray(v).astype(G[e][k].dtype)).tobytes()
                                              for e, d in nat.items() for k, v in d.items())
            ok &= good; nchk += 1; out.add(N=N, mode='validation', fmt=fmt, side=s, ok=bool(good), n_checked=len(nat))
out.flush(); print('validation all bit-identical:', ok, f'({nchk} checks)  total {time.time()-t_all:.0f}s', flush=True)
