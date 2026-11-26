"""Core geometry, store builders and readers for the LSST+Roman joint-chunking benchmark.

All pixel coordinates are 0-based numpy (row=y, col=x); stamps and cutouts are
native-pixel subarrays [y0:y1, x0:x1] of the parent image (no resampling).
"""
import os, io, json, zlib, struct, math, time
import numpy as np, pandas as pd
try:
    import blosc2
except ImportError:   # not needed for geometry-only use (e.g. inside the LSST stack)
    blosc2 = None
import pyarrow as pa, pyarrow.parquet as pq
from astropy.io import fits
from astropy.wcs import WCS
try:
    from astropy_healpix import HEALPix
except ImportError:
    HEALPix = None
import astropy.units as u
import warnings; warnings.filterwarnings('ignore')

PLANES = {'rubin': ('IMAGE', 'VARIANCE', 'MASK'), 'roman': ('SCI', 'ERR', 'DQ'), 'roman_l2': ('data', 'err', 'dq')}

# ---------------------------------------------------------------- geometry
def pixscale_arcsec(w):
    return float(np.sqrt(abs(np.linalg.det(w.pixel_scale_matrix))) * 3600)

def query_box(w, shape, ra, dec, side_arcsec):
    """Native-pixel cutout box (y0,y1,x0,x1), axis-aligned in the image grid, side = side_arcsec.
    Returns None if no overlap. Same definition used for every format."""
    x, y = w.all_world2pix([[ra, dec]], 0)[0]
    h = 0.5 * side_arcsec / pixscale_arcsec(w)
    x0, x1 = int(np.floor(x - h + 0.5)), int(np.floor(x + h + 0.5))
    y0, y1 = int(np.floor(y - h + 0.5)), int(np.floor(y + h + 0.5))
    x0, y0 = max(x0, 0), max(y0, 0); x1, y1 = min(x1, shape[1]), min(y1, shape[0])
    if x1 <= x0 or y1 <= y0: return None
    return (y0, y1, x0, x1)

def tile_boundary_lonlat(hp, tiles, step=4):
    lon, lat = hp.boundaries_lonlat(np.atleast_1d(tiles), step=step)
    return lon.to_value(u.deg), lat.to_value(u.deg)   # (n, 4*step)

def tile_stamp_boxes(w, shape, hp, tiles, pad=1):
    """Pixel bbox of each HEALPix tile in this image (pixel centres inside tile ⊂ box)."""
    lon, lat = tile_boundary_lonlat(hp, tiles)
    n, m = lon.shape
    xy = w.all_world2pix(np.c_[lon.ravel(), lat.ravel()], 0).reshape(n, m, 2)
    out = []
    for t, p in zip(tiles, xy):
        x0 = int(np.ceil(p[:, 0].min() - 0.5)) - pad; x1 = int(np.floor(p[:, 0].max() + 0.5)) + 1 + pad
        y0 = int(np.ceil(p[:, 1].min() - 0.5)) - pad; y1 = int(np.floor(p[:, 1].max() + 0.5)) + 1 + pad
        x0, y0 = max(x0, 0), max(y0, 0); x1, y1 = min(x1, shape[1]), min(y1, shape[0])
        out.append(None if (x1 <= x0 or y1 <= y0) else (y0, y1, x0, x1))
    return out

def tiles_for_query(hp, ra, dec, side_arcsec, max_pix_arcsec=0.2, step=32, R=None):
    """Exact set of HEALPix tiles intersecting the disk that bounds the query box (radius = half-diagonal
    plus one pixel): breadth-first search over tile neighbours from the tile containing the centre, keeping
    tiles whose densely sampled boundary enters the disk. (astropy_healpix cone_search_lonlat is not
    inclusive for the elongated tiles at these latitudes.)"""
    from astropy.coordinates import SkyCoord
    if R is None: R = side_arcsec / np.sqrt(2) + 2 * max_pix_arcsec
    c0 = SkyCoord(ra * u.deg, dec * u.deg)
    start = int(hp.lonlat_to_healpix(ra * u.deg, dec * u.deg))
    keep = {start}; seen = {start}; frontier = [start]
    while frontier:
        nb = np.unique(hp.neighbours(np.array(frontier)).ravel()); nb = [int(t) for t in nb if t >= 0 and int(t) not in seen]
        seen.update(nb); frontier = []
        if not nb: break
        lon, lat = hp.boundaries_lonlat(np.array(nb), step=step)
        d = c0.separation(SkyCoord(lon, lat)).arcsec.min(axis=1)
        for t, dd in zip(nb, d):
            if dd <= R: keep.add(t); frontier.append(t)
    return np.array(sorted(keep), dtype=np.int64)

# ---------------------------------------------------------------- loading natives
def load_exposure(row):
    """Return dict(planes={name: native-endian array}, header=WCS-bearing header)."""
    if row['survey'] == 'rubin':
        with fits.open(row['local']) as hl:
            hdr = hl[1].header.copy()
            planes = {n: np.asarray(hl[i].data) for i, n in ((1, 'IMAGE'), (3, 'VARIANCE'), (2, 'MASK'))}
    else:
        with fits.open(row['local']) as hl:
            hdr = hl[0].header.copy()
            planes = {n: np.asarray(hl[n].data) for n in ('SCI', 'ERR', 'DQ')}
    planes = {k: np.ascontiguousarray(v.astype(v.dtype.newbyteorder('='))) for k, v in planes.items()}
    return planes, hdr

def wcs_header_string(hdr):
    w = WCS(hdr)
    h = w.to_header(relax=True)
    return h.tostring()

# ---------------------------------------------------------------- codec
def enc(a):
    return blosc2.compress2(np.ascontiguousarray(a), codec=blosc2.Codec.ZSTD, clevel=5,
                            filters=[blosc2.Filter.BITSHUFFLE], typesize=a.dtype.itemsize, nthreads=1)

def dec(b, dtype, shape):
    return np.frombuffer(blosc2.decompress2(b), dtype=dtype).reshape(shape)

# ---------------------------------------------------------------- stamp extraction
def extract_stamps(man, order, tiles_all, log=print):
    """Return DataFrame rows: tile, survey, exp_id, filt, plane, y0,y1,x0,x1, dtype, data(bytes), raw_nbytes."""
    hp = HEALPix(nside=2 ** order, order='nested')
    rows = []
    for ei, r in man.iterrows():
        planes, hdr = load_exposure(r)
        w = WCS(hdr); shape = next(iter(planes.values())).shape
        boxes = tile_stamp_boxes(w, shape, hp, tiles_all)
        for t, b in zip(tiles_all, boxes):
            if b is None: continue
            y0, y1, x0, x1 = b
            for pn in PLANES[r['survey']]:
                a = planes[pn][y0:y1, x0:x1]
                rows.append(dict(tile=int(t), survey=r['survey'], exp_id=r['exp_id'], filt=r['filt'], plane=pn,
                                 y0=y0, y1=y1, x0=x0, x1=x1, dtype=a.dtype.str, raw_nbytes=a.nbytes, data=enc(a)))
    df = pd.DataFrame(rows)
    sorder = {'rubin': 0, 'roman': 1}
    df['_s'] = df.survey.map(sorder)
    df = df.sort_values(['tile', '_s', 'exp_id', 'plane']).drop(columns='_s').reset_index(drop=True)
    return df

# ---------------------------------------------------------------- HATS-style parquet writer
PQ_SCHEMA = pa.schema([('tile', pa.int64()), ('survey', pa.string()), ('exp_id', pa.string()), ('filt', pa.string()),
                       ('plane', pa.string()), ('y0', pa.int32()), ('y1', pa.int32()), ('x0', pa.int32()), ('x1', pa.int32()),
                       ('dtype', pa.string()), ('raw_nbytes', pa.int64()), ('data', pa.binary())])

def write_hats_parquet(df, root, order, part_order=10):
    """One Parquet file per HEALPix(part_order) partition (HATS naming); one row group per order-`order` tile,
    tiles in NESTED order so neighbours are contiguous on disk. Pixel blobs are pre-compressed (blosc2
    zstd+bitshuffle) so the data column is stored uncompressed; metadata columns use zstd."""
    shift = 2 * (order - part_order)
    df = df.assign(part=(df.tile.to_numpy(np.int64) >> shift))
    files = {}
    for part, g in df.groupby('part'):
        d = f"{root}/Norder={part_order}/Dir={(part // 10000) * 10000}"
        os.makedirs(d, exist_ok=True); fn = f"{d}/Npix={part}.parquet"
        comp = {c: ('none' if c == 'data' else 'zstd') for c in PQ_SCHEMA.names}
        with pq.ParquetWriter(fn, PQ_SCHEMA, compression=comp, use_dictionary=['survey', 'filt', 'plane', 'dtype', 'exp_id'],
                              write_statistics=['tile']) as wtr:
            for t, gt in g.groupby('tile', sort=True):
                wtr.write_table(pa.Table.from_pandas(gt.drop(columns='part'), schema=PQ_SCHEMA, preserve_index=False))
        files[int(part)] = fn
    return files


# ---------------------------------------------------------------- ownership-based stamps (exact, no duplication)
try:
    from scipy import ndimage
except ImportError:
    ndimage = None

def patch_region(w, shape, patch_ra, patch_dec, patch_deg, pad=2):
    g = np.linspace(-patch_deg / 2, patch_deg / 2, 9)
    GX, GY = np.meshgrid(g, g)
    ra = patch_ra + GX.ravel() / np.cos(np.radians(patch_dec)); de = patch_dec + GY.ravel()
    x, y = w.all_world2pix(np.c_[ra, de], 0).T
    x0 = max(int(np.floor(x.min())) - pad, 0); x1 = min(int(np.ceil(x.max())) + pad + 1, shape[1])
    y0 = max(int(np.floor(y.min())) - pad, 0); y1 = min(int(np.ceil(y.max())) + pad + 1, shape[0])
    return (y0, y1, x0, x1) if (x1 > x0 and y1 > y0) else None

def ownership_map(w, region, order_max=14):
    """HEALPix(order_max, NESTED) index of every native pixel centre in region."""
    y0, y1, x0, x1 = region
    yy, xx = np.mgrid[y0:y1, x0:x1]
    ra, de = w.all_pix2world(xx.ravel().astype(float), yy.ravel().astype(float), 0)
    hp = HEALPix(nside=2 ** order_max, order='nested')
    return hp.lonlat_to_healpix(ra * u.deg, de * u.deg).reshape(yy.shape).astype(np.int64)

def stamps_from_ownership(planes, survey, exp_id, filt, region, own_max, order, order_max=14, origin=(0, 0)):
    """Yield stamp rows (one per tile x plane plus an OWN bitmask row) for one exposure at `order`."""
    y0r, y1r, x0r, x1r = region
    t = own_max >> (2 * (order_max - order))
    uniq, lab = np.unique(t, return_inverse=True); lab = lab.reshape(t.shape) + 1
    rows = []
    for k, sl in enumerate(ndimage.find_objects(lab)):
        if sl is None: continue
        m = lab[sl] == (k + 1)
        y0, y1 = sl[0].start + y0r, sl[0].stop + y0r; x0, x1 = sl[1].start + x0r, sl[1].stop + x0r
        common = dict(tile=int(uniq[k]), survey=survey, exp_id=exp_id, filt=filt, y0=y0, y1=y1, x0=x0, x1=x1)
        for pn in PLANES[survey]:
            a = planes[pn][y0 - origin[0]:y1 - origin[0], x0 - origin[1]:x1 - origin[1]].copy(); a[~m] = 0
            rows.append(dict(common, plane=pn, dtype=a.dtype.str, raw_nbytes=int(m.sum()) * a.dtype.itemsize, data=enc(a)))
        pk = np.packbits(m, axis=None)
        rows.append(dict(common, plane='OWN', dtype='|b1', raw_nbytes=0,
                         data=blosc2.compress2(pk, codec=blosc2.Codec.ZSTD, clevel=5, typesize=1, nthreads=1)))
    return rows

def dec_own(b, shape):
    return np.unpackbits(np.frombuffer(blosc2.decompress2(b), np.uint8), count=shape[0] * shape[1]).reshape(shape).astype(bool)

def sort_stamps(df):
    s = df.survey.map({'rubin': 0, 'roman': 1})
    return df.assign(_s=s).sort_values(['tile', '_s', 'exp_id', 'plane']).drop(columns='_s').reset_index(drop=True)


def query_radius(expo, ra, dec, side_arcsec):
    """Exact bounding radius (arcsec) of all exposures' native-pixel query boxes, from the sky
    positions of the outer pixel corners (includes field distortion), plus 0.1 arcsec."""
    from astropy.coordinates import SkyCoord
    c0 = SkyCoord(ra * u.deg, dec * u.deg); R = 0.0
    for e, (w, shp, s, *_r) in expo.items():
        b = query_box(w, shp, ra, dec, side_arcsec)
        if b is None: continue
        y0, y1, x0, x1 = b
        cx = np.array([x0 - .5, x1 - .5, x1 - .5, x0 - .5]); cy = np.array([y0 - .5, y0 - .5, y1 - .5, y1 - .5])
        r_, d_ = w.all_pix2world(cx, cy, 0)
        R = max(R, c0.separation(SkyCoord(r_ * u.deg, d_ * u.deg)).arcsec.max())
    return R + 0.1


def write_hats_parquet_v2(df, root, order, part_order=10):
    """HATS-style Parquet, one file per HEALPix(part_order) partition; one row group per
    (tile, survey, band) in NESTED tile order, so all-band reads of a tile stay contiguous while
    single-band/single-survey reads can skip row groups via footer statistics."""
    shift = 2 * (order - part_order)
    so = {'rubin': 0, 'roman': 1, 'roman_l2': 1}
    df = df.assign(part=(df.tile.to_numpy(np.int64) >> shift), _s=df.survey.map(so)).sort_values(['tile', '_s', 'filt', 'exp_id', 'plane'])
    files = {}
    for part, g in df.groupby('part'):
        d = f"{root}/Norder={part_order}/Dir={(part // 10000) * 10000}"
        os.makedirs(d, exist_ok=True); fn = f"{d}/Npix={part}.parquet"
        comp = {c: ('none' if c == 'data' else 'zstd') for c in PQ_SCHEMA.names}
        with pq.ParquetWriter(fn, PQ_SCHEMA, compression=comp, use_dictionary=['survey', 'filt', 'plane', 'dtype', 'exp_id'],
                              write_statistics=['tile', 'survey', 'filt']) as wtr:
            for _, gt in g.groupby(['tile', '_s', 'filt'], sort=True):
                wtr.write_table(pa.Table.from_pandas(gt.drop(columns=['part', '_s']), schema=PQ_SCHEMA, preserve_index=False))
        files[int(part)] = fn
    return files


def write_hats_parquet_bandmajor(df, root, order, part_order=10):
    """Same files and row groups as write_hats_parquet_v2 (one row group per tile x survey x band),
    but ordered (survey, band, tile) inside each partition file: a single band's tiles are contiguous
    in NESTED order, so single-band box queries coalesce into a few ranges."""
    shift = 2 * (order - part_order)
    so = {'rubin': 0, 'roman': 1, 'roman_l2': 1}
    df = df.assign(part=(df.tile.to_numpy(np.int64) >> shift), _s=df.survey.map(so)).sort_values(['_s', 'filt', 'tile', 'exp_id', 'plane'])
    files = {}
    for part, g in df.groupby('part'):
        d = f"{root}/Norder={part_order}/Dir={(part // 10000) * 10000}"
        os.makedirs(d, exist_ok=True); fn = f"{d}/Npix={part}.parquet"
        comp = {c: ('none' if c == 'data' else 'zstd') for c in PQ_SCHEMA.names}
        with pq.ParquetWriter(fn, PQ_SCHEMA, compression=comp, use_dictionary=['survey', 'filt', 'plane', 'dtype', 'exp_id'],
                              write_statistics=['tile', 'survey', 'filt']) as wtr:
            for _, gt in g.groupby(['_s', 'filt', 'tile'], sort=True):
                wtr.write_table(pa.Table.from_pandas(gt.drop(columns=['part', '_s']), schema=PQ_SCHEMA, preserve_index=False))
        files[int(part)] = fn
    return files

def write_hats_parquet_perband(df, root, order, part_order=10):
    """One HATS-style tree per band: root/band=<filt>/Norder=../Dir=../Npix=N.parquet, tile-major inside."""
    out = {}
    for b, g in df.groupby('filt'):
        out[b] = write_hats_parquet_v2(g, f'{root}/band={b}', order, part_order)
    return out
