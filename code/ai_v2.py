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

DEPTHS = []
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
outer, inner = ThreadPoolExecutor(int(os.environ.get('OUTER','8'))), ThreadPoolExecutor(32)
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
N = 200; ids = set(dex[dex.depth_rank < N].exp_id); ex = {e: v for e, v in W.items() if (v[2] == 'roman_l2') or e in ids}
import resource
for fmt, n, inflight in (('joint_o14', 200, 16), ('joint_o14', 100, 4), ('native', 12, 4)):
    drv = ThreadPoolExecutor(inflight)
    h = rd.HatsPixelReader(f'joint_N{N}', 14, 10, emu, ex); cN = {}
    if fmt == 'joint_o14': h.cutouts(p2['patch_ra'], p2['patch_dec'], 5.0, inner)
    else: run_native(ex, p2['patch_ra'], p2['patch_dec'], 5.0, cN)
    def task(pq_):
        res = h.cutouts(pq_[0], pq_[1], 12.8, inner) if fmt == 'joint_o14' else run_native(ex, pq_[0], pq_[1], 12.8, cN)
        vals = res.values() if isinstance(res, dict) else res
        return sum(a.nbytes for d in vals for a in d.values()), len(res)
    emu.reset(); t0 = time.perf_counter(); c0 = time.process_time(); out = list(drv.map(task, QB[:n])); wall = time.perf_counter() - t0
    rows.append(dict(N=N, fmt=fmt, query='ai_batch', side=12.8, n=n, inflight=inflight, wall_s=wall, cpu_s=time.process_time() - c0, requests=emu.requests, bytes=emu.bytes,
                     useful_bytes=sum(o[0] for o in out), n_exp=float(np.mean([o[1] for o in out])), cutouts_per_s=n / wall,
                     peak_rss_GB=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1e9))
    pd.DataFrame(rows).to_parquet('results_v2/ai_results.parquet')
    print('AI', fmt, inflight, round(n / wall, 2), 'cutouts/s', round(emu.bytes / n / 1e6, 1), 'MB/cutout', round(rows[-1]['peak_rss_GB'], 1), 'GB rss', flush=True)
print('done')
