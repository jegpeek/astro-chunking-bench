"""Stream-and-discard: for each selected DP1 visit_image, range-read only the row tiles covering the
patch from Rubin's GCS bucket (native GZIP_2 tiles, decoded bit-exactly), build exact-ownership HEALPix
stamps for orders 13 and 14, and write one small Parquet per visit. Nothing else is kept."""
import os, sys, time, json
sys.path.append(os.getcwd())
os.environ.setdefault('XDG_CONFIG_HOME', os.path.abspath('astropy_cfg')); os.environ.setdefault('XDG_CACHE_HOME', os.path.abspath('astropy_cache'))
import numpy as np, pandas as pd
from concurrent.futures import ThreadPoolExecutor
from astropy.wcs import WCS
import chunkbench as cb, readers as rd, dp1access as dpa

man = pd.read_parquet('dp1_manifest.parquet'); pat = json.load(open('patch2.json'))
os.makedirs('data/dp1_stamps', exist_ok=True)
sess = rd.CountingSession(pool=48); inner = ThreadPoolExecutor(12)
rd.XFER = ThreadPoolExecutor(24)
log = []
def work(args):
    e, band, url = args
    out = f'data/dp1_stamps/{e}.parquet'
    if os.path.exists(out): return None
    for attempt in range(3):
        try:
            rdr = rd.RubinFitsReader(url, sess); rdr.index(); h = rdr.header
            w = WCS(h); shp = (rdr.idx['IMAGE']['ny'], rdr.idx['IMAGE']['nx'])
            reg = cb.patch_region(w, shp, pat['patch_ra'], pat['patch_dec'], pat['patch_deg'])
            planes = rdr.cutout(reg, inner)
            own = cb.ownership_map(w, reg)
            rows = []
            for o in (14,):
                rr = cb.stamps_from_ownership(planes, 'rubin', e, band, reg, own, o, origin=(reg[0], reg[2]))
                for r in rr: r['order'] = o
                rows += rr
            df = pd.DataFrame(rows); df.to_parquet(out + '.tmp'); os.replace(out + '.tmp', out)
            return dict(exp_id=e, ny=shp[0], nx=shp[1], wcs_header=h.tostring(), ry0=reg[0], ry1=reg[1], rx0=reg[2], rx1=reg[3],
                        pixscale=cb.pixscale_arcsec(w), ctype=w.wcs.ctype[0], sip=w.sip is not None)
        except Exception as ex:
            err = repr(ex)[:200]; time.sleep(3)
    return dict(exp_id=e, error=err)

t0 = time.time(); B = 25
for i in range(0, len(man), B):
    chunk = man.iloc[i:i + B]
    todo = [(e, b, dl) for e, b, dl in zip(chunk.exp_id, chunk.lsst_band, chunk.access_url) if not os.path.exists(f'data/dp1_stamps/{e}.parquet')]
    if not todo: continue
    urls = dpa.resolve([t[2] for t in todo])          # fresh signed URLs per chunk (they expire)
    with ThreadPoolExecutor(6) as ex:
        res = list(ex.map(work, [(e, b, u) for (e, b, _), u in zip(todo, urls)]))
    log += [r for r in res if r]
    pd.DataFrame(log).to_parquet(f'data/dp1_expo_part.parquet')
    print(i + len(chunk), 'done', round(time.time() - t0), 's', round(sess.bytes / 1e9, 2), 'GB read', sum('error' in r for r in res if r), 'errors', flush=True)
print('total GB', sess.bytes / 1e9, 'requests', sess.requests)
