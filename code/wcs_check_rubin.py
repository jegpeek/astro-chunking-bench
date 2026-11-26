"""Rubin: exact SkyWcs (AST FrameSet from the visit_image, via the Butler 'wcs' component) vs the TAN-SIP header the
tiled store carries (exposures.parquet wcs_header). Run inside setup lsst_distrib."""
import os, sys, json, numpy as np, pandas as pd, warnings; warnings.filterwarnings('ignore')
from astropy.io import fits
from astropy.wcs import WCS
import lsst.geom as geom
from lsst.daf.butler import Butler
os.environ['DAF_BUTLER_CACHE_EXPIRATION_MODE'] = 'disabled'
b = Butler('/home/ubuntu/dp1repo_s3'); bt = json.load(open(os.path.expanduser('~/bench/butler_transfer.json')))
ex = pd.read_parquet(os.path.expanduser('~/stores/exposures.parquet')); ex = ex[ex.survey == 'rubin'].set_index('exp_id')
refs = list(b.query_datasets('visit_image', collections=bt['runs'], where="instrument='LSSTComCam'", limit=None))
rng = np.random.default_rng(0); pick = rng.choice(len(refs), 8, replace=False)
yy, xx = np.mgrid[0:4000:25, 0:4072:25].reshape(2, -1).astype(float)
for j in pick:
    r = refs[j]; eid = f"dp1_{int(r.dataId['visit'])}_{int(r.dataId['detector'])}"
    sw = b.get(r.makeComponentRef('wcs'))
    w = WCS(fits.Header.fromstring(ex.loc[eid, 'wcs_header']))
    sky = sw.pixelToSkyArray(xx, yy, degrees=True); ra, de = np.asarray(sky[0]), np.asarray(sky[1])
    r2, d2 = w.all_pix2world(xx, yy, 0)
    sep = np.hypot(((r2 - ra + 180) % 360 - 180) * np.cos(np.radians(de)), d2 - de) * 3.6e6
    print(f"RUBIN {eid} ctype {w.wcs.ctype[0]} sip {w.sip is not None and w.sip.a_order} fwd max {sep.max():.3f} mas rms {np.sqrt((sep**2).mean()):.3f} mas (0.2\"/px => {sep.max()/200:.4f} px)", flush=True)
