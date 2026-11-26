"""v2 benchmark: real DP1 visit_images (Rubin) + STScI Roman I-Sim L2 cal.asdf (re-anchored WCS).
For each Rubin depth N, build the joint HEALPix order-14 store (row group = tile x survey x band), validate,
and benchmark on the in-process S3 emulator against native files. Results -> results_v2/*.parquet"""
import os, sys, json, time, glob, shutil
sys.path.append(os.getcwd())
os.environ.setdefault('XDG_CONFIG_HOME', os.path.abspath('astropy_cfg')); os.environ.setdefault('XDG_CACHE_HOME', os.path.abspath('astropy_cache'))
import numpy as np, pandas as pd, warnings; warnings.filterwarnings('ignore')
from concurrent.futures import ThreadPoolExecutor
from astropy.io import fits
from astropy.wcs import WCS
import chunkbench as cb, readers as rd

DEPTHS = [int(x) for x in os.environ.get('DEPTHS', '10,25,50,100,200').split(',')]
KEEP = int(os.environ.get('KEEP', '200'))
SIDES = [5, 15, 30, 60]; NQ = int(os.environ.get('NQ', '20')); NQ_NATIVE = int(os.environ.get('NQN', '5'))
os.makedirs('results_v2', exist_ok=True); os.makedirs('stores2/native', exist_ok=True)
p2 = json.load(open('patch2.json'))
man = pd.read_parquet('dp1_manifest.parquet'); dex = pd.read_parquet('data/dp1_expo_part.parquet').merge(man[['exp_id', 'lsst_band', 'depth_rank', 'local', 't_min']], on='exp_id')
rex = pd.read_parquet('roman_l2_expo.parquet')
# native files visible to the emulator
for f in glob.glob('data/dp1/*.fits') + glob.glob('data/roman_l2/*.asdf'):
    d = 'stores2/native/' + os.path.basename(f)
    if not os.path.exists(d): os.symlink(os.path.abspath(f), d)
W = {r.exp_id: (WCS(fits.Header.fromstring(r.wcs_header)), (r.ny, r.nx), 'rubin', r.lsst_band) for r in dex.itertuples()}
W.update({r.exp_id: (WCS(fits.Header.fromstring(r.wcs_header)), (4088, 4088), 'roman_l2', r.filt) for r in rex.itertuples()})
local_by_band = {b: sorted(g.exp_id) for b, g in dex[dex.local].groupby('lsst_band')}
proxy = {}  # virtual DP1 visit -> local file of same band (cyclic); its own WCS defines the box
for b, g in dex.sort_values('depth_rank').groupby('lsst_band'):
    for i, e in enumerate(g.exp_id): proxy[e] = local_by_band[b][i % len(local_by_band[b])]
rom_stamps = pd.read_parquet('data/roman_stamps_o14.parquet')
expo_tbl = pd.concat([dex.assign(survey='rubin', filt=dex.lsst_band)[['exp_id', 'survey', 'filt', 'ny', 'nx', 'wcs_header']],
                      rex.assign(ny=4088, nx=4088)[['exp_id', 'survey', 'filt', 'ny', 'nx', 'wcs_header']]])
expo_tbl.to_parquet('stores2/exposures.parquet')

emu = rd.EmulatedStore(os.path.abspath('stores2'), latency_ms=30, stream_MBps=90, link_MBps=1250)
outer, inner = ThreadPoolExecutor(24), ThreadPoolExecutor(96)
rng = np.random.default_rng(2026)
def positions(side, n):
    hw = (p2['patch_deg'] / 2 * 3600 - side / np.sqrt(2) - 4) / 3600
    return list(zip(p2['patch_ra'] + rng.uniform(-hw, hw, n) / np.cos(np.radians(p2['patch_dec'])), p2['patch_dec'] + rng.uniform(-hw, hw, n)))
QP = {s: positions(s, NQ) for s in SIDES}; QB = positions(12.8, 200)
json.dump({'QP': {str(k): v for k, v in QP.items()}, 'QB': QB}, open('results_v2/positions.json', 'w'))

def run_native(ex, ra, dec, side, cache, bands=None):
    jobs = []
    for e, (w, shp, s, b) in ex.items():
        if bands is not None and b not in bands: continue
        box = cb.query_box(w, shp, ra, dec, side)
        if box is None: continue
        if s == 'rubin':
            pe = proxy[e]; rdr = cache.get(pe) or rd.RubinFitsReader(f'native/{pe}.fits', emu); cache[pe] = rdr
            jobs.append(outer.submit(rdr.cutout, box, inner))
        else:
            jobs.append(outer.submit(rd.RomanAsdfReader('native/' + os.path.basename(rex.set_index('exp_id').local[e]), emu).cutout, box))
    return [j.result() for j in jobs]

def measure(fn, **meta):
    emu.reset(); t0 = time.perf_counter(); c0 = time.process_time(); res = fn()
    vals = res.values() if isinstance(res, dict) else res
    ub = sum(a.nbytes for d in vals for a in d.values()); ne = len(res)
    return dict(meta, wall_s=time.perf_counter() - t0, cpu_s=time.process_time() - c0, requests=emu.requests, bytes=emu.bytes, useful_bytes=ub, n_exp=ne)

rows = []
def flush(): pd.DataFrame(rows).to_parquet('results_v2/emu_results.parquet')
t_all = time.time()
for N in DEPTHS:
    ids = set(dex[dex.depth_rank < N].exp_id)
    ex = {e: v for e, v in W.items() if (v[2] == 'roman_l2') or e in ids}
    root = f'stores2/joint_N{N}'; shutil.rmtree(root, ignore_errors=True); tb = time.time()
    parts = [pd.read_parquet(f'data/dp1_stamps/{e}.parquet', filters=[('order', '==', 14)]) for e in sorted(ids)]
    st = pd.concat(parts + [rom_stamps], ignore_index=True).drop(columns=['order'])
    cb.write_hats_parquet_v2(st, root, 14, 10); del st, parts
    size = sum(os.path.getsize(f) for f in glob.glob(root + '/**/*.parquet', recursive=True))
    print(f'N={N} store {size/1e9:.2f} GB built in {time.time()-tb:.0f}s', flush=True)
    hr = lambda: rd.HatsPixelReader(f'joint_N{N}', 14, 10, emu, ex)
    for mode in ('cold', 'warm'):
        cacheH = hr(); cacheN = {}
        if mode == 'warm':
            cacheH.cutouts(p2['patch_ra'], p2['patch_dec'], 5.0, inner); run_native(ex, p2['patch_ra'], p2['patch_dec'], 5.0, cacheN)
        for s in SIDES:
            for qi, (ra, dec) in enumerate(QP[s]):
                h = cacheH if mode == 'warm' else hr()
                rows.append(measure(lambda: h.cutouts(ra, dec, float(s), inner), N=N, mode=mode, fmt='joint_o14', query='all', side=s, qi=qi, store_bytes=size))
                if qi < NQ_NATIVE:
                    c = cacheN if mode == 'warm' else {}
                    rows.append(measure(lambda: run_native(ex, ra, dec, float(s), c), N=N, mode=mode, fmt='native', query='all', side=s, qi=qi))
            # single-band time series (Rubin r only), cold+warm
            for qi, (ra, dec) in enumerate(QP[s][:min(NQ, 10)]):
                h = cacheH if mode == 'warm' else hr()
                rows.append(measure(lambda: h.cutouts(ra, dec, float(s), inner, surveys={'rubin'}, bands={'r'}), N=N, mode=mode, fmt='joint_o14', query='r_only', side=s, qi=qi))
                if qi < NQ_NATIVE:
                    c = cacheN if mode == 'warm' else {}
                    rows.append(measure(lambda: run_native(ex, ra, dec, float(s), c, bands={'r'}), N=N, mode=mode, fmt='native', query='r_only', side=s, qi=qi))
        flush(); print(f'  N={N} {mode} done, t={time.time()-t_all:.0f}s', flush=True)
    # validation: bit-identical vs direct local reads on a few queries (local DP1 subset + all Roman)
    for s in (5, 60):
        ra, dec = QP[s][0]; G = hr().cutouts(ra, dec, float(s), inner)
        ok = True; nchk = 0
        for e, (w, shp, sv, b) in ex.items():
            box = cb.query_box(w, shp, ra, dec, float(s))
            if box is None: continue
            y0, y1, x0, x1 = box
            if sv == 'rubin' and e in set(dex[dex.local].exp_id):
                with fits.open(f'data/dp1/{e}.fits') as hl:
                    T = {'IMAGE': hl[1].section[y0:y1, x0:x1], 'MASK': hl[2].section[y0:y1, x0:x1], 'VARIANCE': hl[3].section[y0:y1, x0:x1]}
            elif sv == 'roman_l2':
                import asdf
                with asdf.open(rex.set_index('exp_id').local[e], lazy_load=True, memmap=False) as af:
                    T = {k: np.asarray(af['roman'][k][y0:y1, x0:x1]) for k in ('data', 'err', 'dq')}
            else: continue
            nchk += 1; ok &= all(np.asarray(G[e][k]).tobytes() == np.ascontiguousarray(np.asarray(v).astype(G[e][k].dtype)).tobytes() for k, v in T.items())
        rows.append(dict(N=N, fmt='validation', side=s, ok=bool(ok), n_checked=nchk)); print(f'  validate N={N} side={s}: {ok} ({nchk} exposures)', flush=True)
    flush()
    if N != KEEP: shutil.rmtree(root)

if os.environ.get("SKIP_AI"): flush(); sys.exit(0)
# AI batch at full depth (store kept)
N = KEEP; ids = set(dex[dex.depth_rank < N].exp_id); ex = {e: v for e, v in W.items() if (v[2] == 'roman_l2') or e in ids}
drv = ThreadPoolExecutor(16)
for fmt, n in (('joint_o14', 200), ('native', 16)):
    h = rd.HatsPixelReader(f'joint_N{N}', 14, 10, emu, ex); cN = {}
    if fmt == 'joint_o14': h.cutouts(p2['patch_ra'], p2['patch_dec'], 5.0, inner)
    else: run_native(ex, p2['patch_ra'], p2['patch_dec'], 5.0, cN)
    def task(pq_):
        res = h.cutouts(pq_[0], pq_[1], 12.8, inner) if fmt == 'joint_o14' else run_native(ex, pq_[0], pq_[1], 12.8, cN)
        vals = res.values() if isinstance(res, dict) else res
        return sum(a.nbytes for d in vals for a in d.values()), len(res)
    emu.reset(); t0 = time.perf_counter(); out = list(drv.map(task, QB[:n])); wall = time.perf_counter() - t0
    rows.append(dict(N=N, fmt=fmt, query='ai_batch', side=12.8, n=n, wall_s=wall, requests=emu.requests, bytes=emu.bytes,
                     useful_bytes=sum(o[0] for o in out), n_exp=float(np.mean([o[1] for o in out])), cutouts_per_s=n / wall))
    print('AI', fmt, round(n / wall, 2), 'cutouts/s', round(emu.bytes / n / 1e6, 1), 'MB/cutout', flush=True)
flush(); print('ALL DONE', round(time.time() - t_all), 's')
