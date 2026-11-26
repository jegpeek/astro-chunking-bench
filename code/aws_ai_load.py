"""AI-style concurrent load on real S3: random 12.8" all-band cutouts at fixed concurrency, DataLoader-style
(NPROC long-lived worker processes x INFL queries in flight per worker). Readers are long-lived per worker
(index/footers cached after first use), as in a training loop.
Formats: tiled (hats/blob store; optional '1p' = one process, NPROC*INFL threads), native_best (minimal-byte readers),
roman_official (roman_datamodels, Roman part only), butler (lsst env; Rubin part only; 1 MiB s3fs block).
Usage: python aws_ai_load.py <fmt> <N> <NQ> <NPROC> <INFL> [store_base] [order] [kind] -> appends aws_results_ai.parquet"""
import os, sys, time, json
sys.path.insert(0, os.getcwd())
import numpy as np, warnings; warnings.filterwarnings('ignore')
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import multiprocessing as mp
SIDE = 12.8
def qpos(n):
    P2 = json.load(open('patch2.json')); rng = np.random.default_rng(7)
    hw = (P2['patch_deg'] / 2 * 3600 - SIDE / np.sqrt(2) - 4) / 3600
    return list(zip(P2['patch_ra'] + rng.uniform(-hw, hw, n) / np.cos(np.radians(P2['patch_dec'])), P2['patch_dec'] + rng.uniform(-hw, hw, n)))
G = {}
def init(fmt, N, base, order, kind, infl):
    import chunkbench as cb
    from bench_common import load_expo, subset
    ex, W = load_expo(os.path.expanduser('~/stores/exposures.parquet')); Wn = subset(ex, W, N); URL = dict(zip(ex.exp_id, ex.url))
    if os.environ.get('AI_WCS_LOCK'):  # astropy WCS objects are not thread-safe; serialize the (cheap) WCS math
        import threading; _lk = threading.Lock(); _qb = cb.query_box
        def _locked(*a, **k):
            with _lk: return _qb(*a, **k)
        cb.query_box = _locked
    G.update(fmt=fmt, Wn=Wn, cb=cb, qpool=ThreadPoolExecutor(infl), io=ThreadPoolExecutor(8 if fmt in ('native_best', 'roman_official') else 16), sub=ThreadPoolExecutor(32))
    if fmt == 'butler':
        import s3fs; s3fs.S3FileSystem.default_block_size = 1 << 20
        from lsst.daf.butler import Butler
        import lsst.geom as geom
        bt = json.load(open(os.path.expanduser('~/bench/butler_transfer.json'))); b = Butler('/home/ubuntu/dp1repo_s3')
        R = {f"dp1_{int(r.dataId['visit'])}_{int(r.dataId['detector'])}": r for r in b.query_datasets('visit_image', collections=bt['runs'], where="instrument='LSSTComCam'", limit=None)}
        G.update(b=b, R=R, geom=geom); return
    import readers as rd, s3session
    S = s3session.S3Session(region='us-east-2', pool=128); rd.XFER = ThreadPoolExecutor(32); G.update(S=S, rd=rd, URL=URL)
    if fmt == 'tiled':
        import joint2
        G['reader'] = (joint2.BlobReader if kind == 'blob' else rd.HatsPixelReader)(base, order, 10, S, Wn)
    if fmt == 'native_best': G['rcache'] = {}
    if fmt == 'roman_official':
        import s3fs, roman_datamodels as rdm
        G.update(FS=s3fs.S3FileSystem(client_kwargs={'region_name': 'us-east-2'}), rdm=rdm)
def one(ra, dec):
    t0 = time.perf_counter(); f = G['fmt']; cb = G['cb']; Wn = G['Wn']; ub = 0
    if f == 'tiled':
        res = G['reader'].cutouts(ra, dec, SIDE, G['sub']); ub = sum(a.nbytes for d in res.values() for a in d.values())
    else:
        jobs = []
        for e, (w, shp, sv, band) in Wn.items():
            box = cb.query_box(w, shp, ra, dec, SIDE)
            if box is None: continue
            if f == 'butler':
                if sv != 'rubin': continue
                y0, y1, x0, x1 = box; g = G['geom']
                jobs.append(G['io'].submit(lambda r, bb: (lambda x: x.image.array.nbytes + x.mask.array.nbytes + x.variance.array.nbytes)(G['b'].get(r, parameters={'bbox': bb})),
                                           G['R'][e], g.Box2I(g.Point2I(x0, y0), g.Extent2I(x1 - x0, y1 - y0))))
            elif f == 'roman_official':
                if sv == 'rubin': continue
                def ro(url, bx):
                    y0, y1, x0, x1 = bx
                    with G['FS'].open(url[5:], 'rb') as fh, G['rdm'].open(fh) as m:
                        return sum(np.asarray(getattr(m, k)[y0:y1, x0:x1]).nbytes for k in ('data', 'err', 'dq'))
                jobs.append(G['io'].submit(ro, G['URL'][e], box))
            else:  # native_best
                rd = G['rd']
                if sv == 'rubin':
                    r = G['rcache'].get(e) or rd.RubinFitsReader(G['URL'][e], G['S']); G['rcache'][e] = r
                    jobs.append(G['io'].submit(lambda r, bx: sum(a.nbytes for a in r.cutout(bx, G['sub']).values()), r, box))
                else:
                    jobs.append(G['io'].submit(lambda u, bx: sum(a.nbytes for a in rd.RomanAsdfReader(u, G['S']).cutout(bx).values()), G['URL'][e], box))
        ub = sum(j.result() for j in jobs)
    return time.perf_counter() - t0, ub
def batch(qs): return list(G['qpool'].map(lambda p: one(*p), qs))
def ping(_): return os.getpid()
if __name__ == '__main__':
    from bench_common import nic_bytes, Writer
    fmt, N, NQ, NP, INFL = sys.argv[1], int(sys.argv[2]), int(sys.argv[3]), int(sys.argv[4]), int(sys.argv[5])
    base = sys.argv[6] if len(sys.argv) > 6 else ''; order = int(sys.argv[7]) if len(sys.argv) > 7 else 14; kind = sys.argv[8] if len(sys.argv) > 8 else 'hats'
    label = os.environ.get('AI_LABEL', fmt); Q = qpos(NQ)
    out = Writer('aws_results_ai.parquet')
    if os.path.exists('aws_results_ai.parquet'):
        import pandas as pd; out.rows = pd.read_parquet('aws_results_ai.parquet').to_dict('records')
    if NP == 1:
        init(fmt, N, base, order, kind, INFL); n0 = nic_bytes(); t0 = time.perf_counter(); res = batch(Q)
    else:
        P = ProcessPoolExecutor(NP, mp_context=mp.get_context('spawn'), initializer=init, initargs=(fmt, N, base, order, kind, INFL))
        list(P.map(ping, range(NP * 2)))
        n0 = nic_bytes(); t0 = time.perf_counter()
        res = [r for b in P.map(batch, [Q[i:i + INFL] for i in range(0, NQ, INFL)]) for r in b]
    wall = time.perf_counter() - t0; nic = nic_bytes() - n0
    lat = np.array([r[0] for r in res]); ub = sum(r[1] for r in res)
    out.add(label=label, fmt=fmt, N=N, nq=NQ, nproc=NP, inflight=NP * INFL if NP > 1 else INFL, wall_s=wall, qps=NQ / wall,
            lat_p50=float(np.median(lat)), lat_p95=float(np.percentile(lat, 95)), nic_rx_bytes=nic, nic_per_q=nic / NQ, useful_bytes=ub)
    out.flush(); print(f'{label} N={N} nq={NQ} procs={NP} inflight={NP*INFL if NP>1 else INFL}: {NQ/wall:.2f} q/s, p50 {np.median(lat):.2f}s p95 {np.percentile(lat,95):.2f}s, {nic/NQ/1e6:.1f} MB/q', flush=True)
