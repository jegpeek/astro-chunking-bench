"""Joint-store optimizations (v5):
  * BlobStore: per-partition binary shard (`Npix=p.bin`, the same blosc2 stamp blobs as the Parquet store, same
    tile-major order) + compact offset index (`Npix=p.idx`, numpy npz). No Parquet footer, no pyarrow decode.
  * assemble_records(): stamp paste that optionally returns per-plane coverage masks (for exact parallel merge).
  * ParallelTiled: process-parallel cutouts for HatsPixelReader or BlobReader; tiles split into NESTED-contiguous
    chunks, one per worker; workers fetch+decode+paste, the parent merges with the coverage masks (bit-identical).
"""
import io, os, json, math, ctypes, gc
import numpy as np
def trim():
    """Return freed heap pages to the OS (glibc); keeps long-lived workers' RSS bounded."""
    gc.collect()
    try: ctypes.CDLL('libc.so.6').malloc_trim(0)
    except Exception: pass
import chunkbench as cb, readers as rd

PLANE_CODES = ['IMAGE', 'MASK', 'VARIANCE', 'data', 'err', 'dq', 'OWN']
IDX_DT = np.dtype([('tile', 'i8'), ('exp', 'i2'), ('plane', 'u1'), ('dtype', 'u1'), ('y0', 'i4'), ('y1', 'i4'),
                   ('x0', 'i4'), ('x1', 'i4'), ('off', 'i8'), ('len', 'i4')])

def write_blob_store(df, root, order, part_order=10):
    """Same row order as cb.write_hats_parquet_v2 (tile, survey, band, exp, plane)."""
    shift = 2 * (order - part_order); so = {'rubin': 0, 'roman': 1, 'roman_l2': 1}
    df = df.assign(part=(df.tile.to_numpy(np.int64) >> shift), _s=df.survey.map(so)).sort_values(['tile', '_s', 'filt', 'exp_id', 'plane'])
    sizes = {}
    for part, g in df.groupby('part'):
        d = f"{root}/Norder={part_order}/Dir={(part // 10000) * 10000}"; os.makedirs(d, exist_ok=True)
        exps = sorted(g.exp_id.unique()); ei = {e: i for i, e in enumerate(exps)}
        dts = sorted(g.dtype.unique())
        idx = np.zeros(len(g), IDX_DT); off = 0
        with open(f"{d}/Npix={part}.bin", 'wb') as fb:
            for k, r in enumerate(g.itertuples()):
                b = r.data; fb.write(b)
                idx[k] = (r.tile, ei[r.exp_id], PLANE_CODES.index(r.plane), dts.index(r.dtype), r.y0, r.y1, r.x0, r.x1, off, len(b)); off += len(b)
        bio = io.BytesIO(); np.savez_compressed(bio, idx=idx, exps=np.array(exps), dtypes=np.array(dts))
        with open(f"{d}/Npix={part}.idx", 'wb') as fi: fi.write(bio.getvalue())
        sizes[int(part)] = (off, bio.tell())
    return sizes

class BlobReader:
    def __init__(self, base_url, order, part_order, sess, expo):
        self.base, self.order, self.po, self.sess, self.expo = base_url, order, part_order, sess, expo; self.meta = {}
    def _u(self, part, ext): return f"{self.base}/Norder={self.po}/Dir={(part // 10000) * 10000}/Npix={part}.{ext}"
    def _index(self, part):
        if part in self.meta: return self.meta[part]
        try: b = self.sess.get(self._u(part, 'idx')).content
        except Exception: self.meta[part] = None; return None
        z = np.load(io.BytesIO(b)); m = dict(idx=z['idx'], exps=[str(x) for x in z['exps']], dtypes=[np.dtype(str(x)) for x in z['dtypes']])
        self.meta[part] = m; return m
    def fetch_records(self, tiles, pool, bands=None):
        shift = 2 * (self.order - self.po); tiles = np.asarray(sorted(set(int(t) for t in tiles)), np.int64)
        parts = sorted(set((tiles >> shift).tolist()))
        metas = dict(zip(parts, pool.map(self._index, parts)))
        jobs = []
        for p in parts:
            m = metas[p]
            if m is None: continue
            ix = m['idx']; sel = np.isin(ix['tile'], tiles[(tiles >> shift) == p])
            if bands is not None:
                ok = np.array([self.expo.get(e, (None, None, None, None))[3] in bands for e in m['exps']])
                sel &= ok[ix['exp']]
            w = np.nonzero(sel)[0]
            if not len(w): continue
            # contiguous runs of selected index rows -> one byte range each
            br = np.nonzero(np.diff(w) != 1)[0] + 1
            for run in np.split(w, br):
                jobs.append((p, run, int(ix['off'][run[0]]), int(ix['off'][run[-1]] + ix['len'][run[-1]])))
        datas = list(pool.map(lambda j: rd.get_range(self.sess, self._u(j[0], 'bin'), j[2], j[3] - 1), jobs))
        recs = []
        for (p, run, a, b), d in zip(jobs, datas):
            m = metas[p]; ix = m['idx'][run]; mv = memoryview(d)
            for r in ix:
                o = int(r['off']) - a
                recs.append((m['exps'][r['exp']], PLANE_CODES[r['plane']], m['dtypes'][r['dtype']], int(r['tile']),
                             int(r['y0']), int(r['y1']), int(r['x0']), int(r['x1']), mv[o:o + int(r['len'])]))
        return recs
    def cutouts(self, ra, dec, side, pool, surveys=None, bands=None, tiles=None, with_mask=False):
        ex = self.expo if bands is None else {k: v for k, v in self.expo.items() if v[3] in bands}
        if tiles is None:
            R = cb.query_radius(ex, ra, dec, side); tiles = cb.tiles_for_query(cb.HEALPix(nside=2 ** self.order, order='nested'), ra, dec, side, R=R)
        return assemble_records(self.fetch_records(tiles, pool, bands), ex, ra, dec, side, with_mask)

def table_records(tbl):
    if tbl is None: return []
    d = tbl.to_pydict()
    return list(zip(d['exp_id'], d['plane'], [np.dtype(x) for x in d['dtype']], d['tile'], d['y0'], d['y1'], d['x0'], d['x1'], d['data']))

def assemble_records(recs, expo, ra, dec, side, with_mask=False):
    """Paste ownership-masked stamps into per-exposure native-pixel cutouts (same result as readers.assemble)."""
    out, cov = {}, {}
    boxes = {}
    for e, (w, shp, survey, *_r) in expo.items():
        b = cb.query_box(w, shp, ra, dec, side)
        if b is not None: boxes[e] = b
    own = {}
    for (e, pl, dt, t, y0, y1, x0, x1, data) in recs:
        if pl == 'OWN': own[(e, t)] = cb.dec_own(bytes(data), (y1 - y0, x1 - x0))
    for (e, pl, dt, t, y0, y1, x0, x1, data) in recs:
        if pl == 'OWN' or e not in boxes: continue
        qy0, qy1, qx0, qx1 = boxes[e]
        iy0, iy1, ix0, ix1 = max(qy0, y0), min(qy1, y1), max(qx0, x0), min(qx1, x1)
        if iy1 <= iy0 or ix1 <= ix0: continue
        a = cb.dec(bytes(data), dt, (y1 - y0, x1 - x0))
        m = own[(e, t)][iy0 - y0:iy1 - y0, ix0 - x0:ix1 - x0]
        o = out.setdefault(e, {}).get(pl)
        if o is None:
            o = np.zeros((qy1 - qy0, qx1 - qx0), a.dtype); out[e][pl] = o
            if with_mask: cov.setdefault(e, {})[pl] = np.zeros(o.shape, bool)
        np.copyto(o[iy0 - qy0:iy1 - qy0, ix0 - qx0:ix1 - qx0], a[iy0 - y0:iy1 - y0, ix0 - x0:ix1 - x0], where=m)
        if with_mask: cov[e][pl][iy0 - qy0:iy1 - qy0, ix0 - qx0:ix1 - qx0] |= m
    return (out, cov) if with_mask else out

# ------------------------------------------------------------------ process-parallel wrapper
_R = None; _POOL = None
def _w_init(kind, base, order, po, expo_path, N, region, nthreads):
    global _R, _POOL
    import sys; sys.path.insert(0, os.getcwd())
    from concurrent.futures import ThreadPoolExecutor
    import s3session
    from bench_common import load_expo, subset
    ex, W = load_expo(expo_path); Wn = subset(ex, W, N)
    S = s3session.S3Session(region=region, pool=max(32, nthreads * 2)); rd.XFER = ThreadPoolExecutor(nthreads)
    _R = (BlobReader if kind == 'blob' else rd.HatsPixelReader)(base, order, po, S, Wn); _R.kind = kind
    _POOL = ThreadPoolExecutor(nthreads)
    os.environ.setdefault('ARROW_DEFAULT_MEMORY_POOL', 'system')
def _w_ping(_): return os.getpid()
def _w_work(a):
    tiles, ra, dec, side, bands, cold = a
    if cold: _R.meta.clear()
    ex = _R.expo if bands is None else {k: v for k, v in _R.expo.items() if v[3] in bands}
    if _R.kind == 'blob': recs = _R.fetch_records(tiles, _POOL, bands)
    else: recs = table_records(_R.fetch_tiles(tiles, _POOL, None, bands))
    out, cov = assemble_records(recs, ex, ra, dec, side, with_mask=True); del recs
    res = {}
    for e, d in out.items():                      # crop each plane to its coverage bbox (workers own compact tile chunks)
        for pl, a in d.items():
            m = cov[e][pl]; ys = np.nonzero(m.any(1))[0]; xs = np.nonzero(m.any(0))[0]
            if not len(ys): res.setdefault(e, {})[pl] = (a.shape, 0, 0, a[:0, :0].copy(), np.packbits(np.zeros(0, bool))); continue
            y0, y1, x0, x1 = ys[0], ys[-1] + 1, xs[0], xs[-1] + 1
            res.setdefault(e, {})[pl] = (a.shape, y0, x0, np.ascontiguousarray(a[y0:y1, x0:x1]), np.packbits(m[y0:y1, x0:x1]))
    del out, cov; trim(); return res

class ParallelTiled:
    def __init__(self, kind, base, order, po, expo_path, N, Wn, nproc=8, nthreads=8, region='us-east-2'):
        import multiprocessing as mp
        from concurrent.futures import ProcessPoolExecutor
        self.order, self.Wn, self.nproc = order, Wn, nproc
        self.P = ProcessPoolExecutor(nproc, mp_context=mp.get_context('spawn'), initializer=_w_init,
                                     initargs=(kind, base, order, po, expo_path, N, region, nthreads))
        list(self.P.map(_w_ping, range(nproc * 2)))
    def cutouts(self, ra, dec, side, bands=None, cold=True):
        ex = self.Wn if bands is None else {k: v for k, v in self.Wn.items() if v[3] in bands}
        R = cb.query_radius(ex, ra, dec, side)
        tiles = sorted(int(t) for t in cb.tiles_for_query(cb.HEALPix(nside=2 ** self.order, order='nested'), ra, dec, side, R=R))
        k = max(1, min(self.nproc, math.ceil(len(tiles) / 2)))
        chunks = [tiles[i * len(tiles) // k:(i + 1) * len(tiles) // k] for i in range(k)]
        out = {}
        for part in self.P.map(_w_work, [(c, ra, dec, side, bands, cold) for c in chunks]):
            for e, d in part.items():
                for pl, (shape, y0, x0, a, pm) in d.items():
                    m = np.unpackbits(pm, count=a.size).reshape(a.shape).astype(bool)
                    o = out.setdefault(e, {}).get(pl)
                    if o is None: o = out[e][pl] = np.zeros(shape, a.dtype)
                    np.copyto(o[y0:y0 + a.shape[0], x0:x0 + a.shape[1]], a, where=m)
            del part
        return out
    def close(self): self.P.shutdown()
