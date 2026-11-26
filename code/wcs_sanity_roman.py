import numpy as np, warnings, s3fs, asdf; warnings.filterwarnings('ignore')
from astropy.wcs import WCS
fs = s3fs.S3FileSystem(client_kwargs={'region_name': 'us-east-2'}); k = sorted(fs.ls('jegp-chunkbench-20261125/roman_l2/'))[0]
with fs.open(k, 'rb', block_size=8 << 20) as f, asdf.open(f, lazy_load=True) as af:
    g = af['roman']['meta']['wcs']
    print('DET->V2V3 model:', g.pipeline[0].transform.__repr__()[:600].replace('\n', ' '))
    h = g.to_fits_sip(bounding_box=((0, 4087), (0, 4087)), max_pix_error=0.05, degree=None, max_inv_pix_error=0.05, verbose=False)
    w = WCS(h); lin = WCS(h); lin.sip = None
    yy, xx = np.mgrid[0:4088:16, 0:4088:16].reshape(2, -1).astype(float); ra, de = g(xx, yy)
    print('nan in gwcs eval:', int(np.isnan(ra).sum()), 'of', ra.size)
    xs, ys = w.all_world2pix(ra, de, 0); xl, yl = lin.all_world2pix(ra, de, 0)
    print('SIP residual max px: %.3e' % np.hypot(xs - xx, ys - yy).max())
    print('distortion captured (gwcs vs pure TAN, no SIP) max px: %.2f' % np.hypot(xl - xx, yl - yy).max())
    print('pixel scale arcsec:', np.round(np.sqrt(np.abs(np.linalg.det(w.pixel_scale_matrix))) * 3600, 4))
