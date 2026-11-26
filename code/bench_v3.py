"""v3: band ordering inside the joint HEALPix store. Same inputs as v2 (200 real DP1 visit_images +
18 Roman I-Sim L2 cal.asdf, order-14 exact-ownership stamps). For each Rubin depth N, build and benchmark:
  tile_major  - v2 layout: rows ordered (tile, survey, band); one row group per tile x survey x band
  tile_gap    - same files, reader bridges gaps <= 1 MiB (object_store default coalescing)
  band_major  - same row groups, ordered (survey, band, tile)
  per_band    - one HATS tree per band (tile-major inside), bands fetched in parallel
plus native files (DP1 FITS + Roman ASDF), for all-band and r-band-only box queries.
Results -> results_v3/emu_results.parquet"""
import os, sys, json, time, glob, shutil
sys.path.append(os.getcwd())
os.environ.setdefault('XDG_CONFIG_HOME', os.path.abspath('astropy_cfg')); os.environ.setdefault('XDG_CACHE_HOME', os.path.abspath('astropy_cache'))
import numpy as np, pandas as pd, asdf, warnings; warnings.filterwarnings('ignore')
from concurrent.futures import ThreadPoolExecutor
from astropy.io import fits
from astropy.wcs import WCS
import chunkbench as cb, readers as rd

DEPTHS = [int(x) for x in os.environ.get('DEPTHS', '10,25,50,100,200').split(',')]
SIDES = [5, 15, 30, 60]; NQ = int(os.environ.get('NQ', '10')); NQN = int(os.environ.get('NQN', '5'))
VARIANTS = os.environ.get('VARIANTS', 'tile_major,tile_gap,tile_gap8,band_major,per_band').split(',')
GAP = 1 << 20
ROOT = os.path.abspath('stores3'); os.makedirs('results_v3', exist_ok=True); os.makedirs(f'{ROOT}/native', exist_ok=True)
p2 = json.load(open('patch2.json'))
man = pd.read_parquet('dp1_manifest.parquet')
dex = pd.read_parquet('data/dp1_expo_part.parquet').merge(man[['exp_id', 'lsst_band', 'depth_rank', 'local']], on='exp_id')
rex = pd.read_parquet('roman_l2_expo.parquet'); rex['fn'] = rex.local.map(os.path.basename)
for f in glob.glob('data/dp1/*.fits') + glob.glob('data/roman_l2/*.asdf'):
    d = f'{ROOT}/native/' + os.path.basename(f)
    if not os.path.exists(d): os.symlink(os.path.abspath(f), d)
W = {r.exp_id: (WCS(fits.Header.fromstring(r.wcs_header)), (r.ny, r.nx), 'rubin', r.lsst_band) for r in dex.itertuples()}
W.update({r.exp_id: (WCS(fits.Header.fromstring(r.wcs_header)), (4088, 4088), 'roman_l2', r.filt) for r in rex.itertuples()})
local_ids = set(dex[dex.local].exp_id)
local_by_band = {b: sorted(g.exp_id) for b, g in dex[dex.local].groupby('lsst_band')}
proxy = {}  # deeper DP1 visits read natively via a byte-identical local file of the same band; box from the real WCS
for b, g in dex.sort_values('depth_rank').groupby('lsst_band'):
    for i, e in enumerate(g.exp_id): proxy[e] = local_by_band[b][i % len(local_by_band[b])]
rfn = rex.set_index('exp_id').fn.to_dict()
rom_stamps = pd.read_parquet('data/roman_stamps_o14.parquet')

emu = rd.EmulatedStore(ROOT, latency_ms=30, stream_MBps=90, link_MBps=1250)
outer, inner, bandpool = ThreadPoolExecutor(24), ThreadPoolExecutor(32), ThreadPoolExecutor(16)   # outer=24 as in v2
rng = np.random.default_rng(2026)   # same generator/order as v2 -> first NQ of v2's 20 positions per size
def positions(side, n):
    hw = (p2['patch_deg'] / 2 * 3600 - side / np.sqrt(2) - 4) / 3600
    return list(zip(p2['patch_ra'] + rng.uniform(-hw, hw, n) / np.cos(np.radians(p2['patch_dec'])), p2['patch_dec'] + rng.uniform(-hw, hw, n)))
QP = {s: positions(s, 20)[:NQ] for s in SIDES}
json.dump({str(k): v for k, v in QP.items()}, open('results_v3/positions.json', 'w'))

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
            jobs.append(outer.submit(rd.RomanAsdfReader('native/' + rfn[e], emu).cutout, box))
    return [j.result() for j in jobs]

def make_reader(variant, N, ex):
    if variant == 'per_band': return rd.PerBandReader(f'pb_N{N}', 14, 10, emu, ex, bandpool)
    h = rd.HatsPixelReader(f'{"bm" if variant == "band_major" else "tm"}_N{N}', 14, 10, emu, ex)
    if variant == 'tile_gap': h.max_gap = GAP
    if variant == 'tile_gap8': h.max_gap = 8 * GAP
    return h

def measure(fn, **meta):
    emu.reset(); t0 = time.perf_counter(); c0 = time.process_time(); res = fn()
    vals = res.values() if isinstance(res, dict) else res
    return dict(meta, wall_s=time.perf_counter() - t0, cpu_s=time.process_time() - c0, requests=emu.requests, bytes=emu.bytes,
                useful_bytes=sum(a.nbytes for d in vals for a in d.values()), n_exp=len(res))

def truth(ex, ra, dec, side, bands=None):
    T = {}
    for e, (w, shp, sv, b) in ex.items():
        if bands is not None and b not in bands: continue
        box = cb.query_box(w, shp, ra, dec, side)
        if box is None: continue
        y0, y1, x0, x1 = box
        if sv == 'rubin' and e in local_ids:
            with fits.open(f'data/dp1/{e}.fits') as hl:
                T[e] = {'IMAGE': hl[1].section[y0:y1, x0:x1], 'MASK': hl[2].section[y0:y1, x0:x1], 'VARIANCE': hl[3].section[y0:y1, x0:x1]}
        elif sv == 'roman_l2':
            with asdf.open('data/roman_l2/' + rfn[e], lazy_load=True, memmap=False) as af:
                T[e] = {k: np.asarray(af['roman'][k][y0:y1, x0:x1]) for k in ('data', 'err', 'dq')}
    return T

def same(G, T):
    return all(e in G and all(np.asarray(G[e][k]).tobytes() == np.ascontiguousarray(np.asarray(v).astype(G[e][k].dtype)).tobytes() for k, v in t.items()) for e, t in T.items())

QUERIES = (('all', None), ('r_only', {'r'}))
rows = []
def flush(): pd.DataFrame(rows).to_parquet('results_v3/emu_results.parquet')
t_all = time.time()
for N in DEPTHS:
    ids = set(dex[dex.depth_rank < N].exp_id)
    ex = {e: v for e, v in W.items() if v[2] == 'roman_l2' or e in ids}
    st = pd.concat([pd.read_parquet(f'data/dp1_stamps/{e}.parquet', filters=[('order', '==', 14)]).drop(columns=['order']) for e in sorted(ids)] + [rom_stamps], ignore_index=True)
    # native baseline (independent of layout)
    for mode in ('cold', 'warm'):
        cN = {}
        if mode == 'warm': run_native(ex, p2['patch_ra'], p2['patch_dec'], 5.0, cN)
        for q, bands in QUERIES:
            for s in SIDES:
                for qi, (ra, dec) in enumerate(QP[s][:NQN]):
                    c = cN if mode == 'warm' else {}
                    rows.append(measure(lambda: run_native(ex, ra, dec, float(s), c, bands), N=N, mode=mode, fmt='native', query=q, side=s, qi=qi))
    flush(); print(f'N={N} native done t={time.time()-t_all:.0f}s', flush=True)
    built = {}
    for variant in VARIANTS:
        if variant in ('tile_major', 'tile_gap', 'tile_gap8'):
            key = 'tm'
            if key not in built:
                tb = time.time(); cb.write_hats_parquet_v2(st, f'{ROOT}/tm_N{N}', 14, 10); built[key] = time.time() - tb
        elif variant == 'band_major':
            key = 'bm'; tb = time.time(); cb.write_hats_parquet_bandmajor(st, f'{ROOT}/bm_N{N}', 14, 10); built[key] = time.time() - tb
        else:
            key = 'pb'; tb = time.time(); cb.write_hats_parquet_perband(st, f'{ROOT}/pb_N{N}', 14, 10); built[key] = time.time() - tb
        sroot = f'{ROOT}/{key}_N{N}'
        files = glob.glob(sroot + '/**/*.parquet', recursive=True); size = sum(os.path.getsize(f) for f in files)
        for mode in ('cold', 'warm'):
            hw = make_reader(variant, N, ex)
            if mode == 'warm': hw.cutouts(p2['patch_ra'], p2['patch_dec'], 5.0, inner)
            for q, bands in QUERIES:
                for s in SIDES:
                    for qi, (ra, dec) in enumerate(QP[s]):
                        h = hw if mode == 'warm' else make_reader(variant, N, ex)
                        rows.append(measure(lambda: h.cutouts(ra, dec, float(s), inner, bands=bands), N=N, mode=mode, fmt=variant, query=q, side=s, qi=qi,
                                            store_bytes=size, n_files=len(files)))
        # bit-identical validation: one all-band and one r-only query at 30"
        for q, bands in QUERIES:
            ra, dec = QP[30][0]; G = make_reader(variant, N, ex).cutouts(ra, dec, 30.0, inner, bands=bands); T = truth(ex, ra, dec, 30.0, bands)
            ok = same(G, T); rows.append(dict(N=N, fmt=variant, query=q, side=30, mode='validation', ok=bool(ok), n_checked=len(T)))
            print(f'  N={N} {variant} {q}: valid={ok} ({len(T)} exp)', flush=True)
        flush(); print(f'N={N} {variant} done ({size/1e9:.2f} GB, {len(files)} files) t={time.time()-t_all:.0f}s', flush=True)
        if key != 'tm': shutil.rmtree(sroot, ignore_errors=True)
    shutil.rmtree(f'{ROOT}/tm_N{N}', ignore_errors=True); del st
flush(); print('ALL DONE', round(time.time() - t_all), 's')
