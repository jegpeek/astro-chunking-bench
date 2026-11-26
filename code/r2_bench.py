"""Upload benchmark objects to Cloudflare R2 (presigned PUTs) and time every layout from the
same real endpoint (presigned GETs with Range). Usage: python r2_bench.py [upload|bench|both]"""
import os, sys, time, json
sys.path.append(os.getcwd())
os.environ.setdefault('XDG_CONFIG_HOME', os.path.abspath('astropy_cfg')); os.environ.setdefault('XDG_CACHE_HOME', os.path.abspath('astropy_cache'))
import numpy as np, pandas as pd, requests
from concurrent.futures import ThreadPoolExecutor
from astropy.io import fits
from astropy.wcs import WCS
import chunkbench as cb, readers as rd

URLS = json.load(open('handoff/r2_urls.json'))
mode = sys.argv[1] if len(sys.argv) > 1 else 'both'

if mode in ('upload', 'both'):
    s = requests.Session(); s.mount('https://', requests.adapters.HTTPAdapter(pool_maxsize=16))
    def put(rel):
        fn = os.path.join('stores', rel)
        for k in range(4):
            try:
                with open(fn, 'rb') as f: r = s.put(URLS[rel]['put'], data=f, timeout=1800)
                if r.status_code == 200: return os.path.getsize(fn)
            except requests.RequestException: pass
            time.sleep(2 * (k + 1))
        raise RuntimeError('upload failed ' + rel)
    t0 = time.time()
    with ThreadPoolExecutor(8) as ex: tot = sum(ex.map(put, list(URLS)))
    print(f'uploaded {len(URLS)} objects, {tot/1e9:.2f} GB in {time.time()-t0:.0f} s', flush=True)

class MappedSession(rd.CountingSession):
    def get(self, url, rng=None, stream=False):
        if url not in URLS:
            r = requests.Response(); r.status_code = 404; raise requests.HTTPError(response=r)
        return super().get(URLS[url]['get'], rng, stream)

if mode in ('bench', 'both'):
    expo = pd.read_parquet('stores/exposures.parquet')
    qpos = json.load(open('handoff/qpos.json'))
    E = {r.exp_id: (WCS(fits.Header.fromstring(r.wcs_header)), (r.ny, r.nx), r.survey) for r in expo.itertuples()}
    Erub = {k: v for k, v in E.items() if v[2] == 'rubin'}; Erom = {k: v for k, v in E.items() if v[2] == 'roman'}
    src = {r.exp_id: os.path.basename(r.src_key) for r in expo.itertuples()}
    sess = MappedSession(pool=48)
    outer, inner = ThreadPoolExecutor(12), ThreadPoolExecutor(24); rd.XFER = ThreadPoolExecutor(24)
    def native(ra, dec, side, rom):
        jobs = []
        for e, (w, shp, s) in E.items():
            b = cb.query_box(w, shp, ra, dec, side)
            if b is None: continue
            if s == 'rubin': jobs.append(outer.submit(rd.RubinFitsReader('native/' + src[e], sess).cutout, b, inner))
            elif rom == 'gz': jobs.append(outer.submit(rd.RomanGzReader('native/' + src[e], sess).cutout, b))
            elif rom == 'asdf': jobs.append(outer.submit(rd.RomanAsdfReader('native/' + src[e].replace('.fits.gz', '.asdf'), sess).cutout, b))
        return [j.result() for j in jobs]
    def tiled(ra, dec, side, lay, o, cache):
        names = ['joint'] if lay == 'joint' else ['rubin', 'roman']
        rs = []
        for n in names:
            if (n, o) not in cache: cache[(n, o)] = rd.HatsPixelReader(f'{n}_o{o}', o, 10, sess, E if n == 'joint' else (Erub if n == 'rubin' else Erom))
            rs.append(cache[(n, o)])
        out = {}
        for f in [outer.submit(r.cutouts, ra, dec, side, inner) for r in rs]: out.update(f.result())
        return list(out.values())
    rows = []
    plan = [('joint:13', 4), ('joint:14', 4), ('persurvey:13', 4), ('native:gz', 2), ('native:asdf', 1)]
    for side in (5, 30, 120):
        for fmt, n in plan:
            for qi, (ra, dec) in enumerate(qpos[str(side)][:n]):
                for m in (('cold', 'warm') if not fmt.startswith('native') else ('cold',)):
                    cache = {}
                    if m == 'warm': tiled(ra, dec, side, *fmt.split(':')[0:1], int(fmt.split(':')[1]), cache) if False else None
                    if m == 'warm':
                        lay, o = fmt.split(':'); tiled(qpos['5'][-1][0], qpos['5'][-1][1], 5.0, lay, int(o), cache)
                    sess.reset(); t0 = time.perf_counter()
                    if fmt.startswith('native'): res = native(ra, dec, float(side), fmt.split(':')[1])
                    else:
                        lay, o = fmt.split(':'); res = tiled(ra, dec, float(side), lay, int(o), cache)
                    rows.append(dict(fmt=fmt, mode=m, side=side, qi=qi, wall_s=time.perf_counter() - t0, requests=sess.requests, bytes=sess.bytes,
                                     n_exp=len(res), useful_bytes=sum(a.nbytes for d in res for a in d.values())))
                    print(rows[-1], flush=True)
            pd.DataFrame(rows).to_parquet('r2_results.parquet')
    pd.DataFrame(rows).to_parquet('r2_results.parquet'); print('done')
