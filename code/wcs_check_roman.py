"""Roman: full GWCS (from the cal.asdf copies in S3) vs the TAN-SIP header carried by the tiled store.
The store header = to_fits_sip(max_pix_error=0.05) fit with CRVAL shifted; compare against the GWCS with the
same sky shift removed, i.e. refit here and also check pipeline contents."""
import os, sys, numpy as np, warnings; warnings.filterwarnings('ignore')
import s3fs, asdf
from astropy.wcs import WCS
fs = s3fs.S3FileSystem(client_kwargs={'region_name': 'us-east-2'})
keys = sorted(fs.ls('jegp-chunkbench-20261125/roman_l2/'))
yy, xx = np.mgrid[0:4088:16, 0:4088:16].reshape(2, -1).astype(float)
e = np.r_[0, 4087.]; ex_, ey_ = np.meshgrid(np.linspace(0, 4087, 512), e); xx = np.r_[xx, ex_.ravel(), ey_.ravel()]; yy = np.r_[yy, ey_.ravel(), ex_.ravel()]
for i, k in enumerate(keys):
    with fs.open(k, 'rb', block_size=8 << 20) as f, asdf.open(f, lazy_load=True) as af:
        g = af['roman']['meta']['wcs']
        if i == 0:
            print('FRAMES', [str(fr) for fr in g.available_frames])
            for step in g.pipeline: print('STEP', step.frame.name if hasattr(step.frame, 'name') else step.frame, '->', type(step.transform).__name__ if step.transform else None, getattr(step.transform, 'n_submodels', ''))
            print('BBOX', g.bounding_box)
        h = g.to_fits_sip(bounding_box=((0, 4087), (0, 4087)), max_pix_error=0.05, degree=None, max_inv_pix_error=0.05, verbose=False)
        w = WCS(h); ra, de = g(xx, yy)
        xs, ys = w.all_world2pix(ra, de, 0); r = np.hypot(xs - xx, ys - yy)
        r2, d2 = w.all_pix2world(xx, yy, 0); sep = np.hypot((r2 - ra) * np.cos(np.radians(de)), d2 - de) * 3.6e6
        print(f"SCA {os.path.basename(k)[:40]} sip_deg {h.get('A_ORDER')} fwd max {sep.max():.3f} mas rms {np.sqrt((sep**2).mean()):.3f} mas | inv max {r.max():.5f} px")
