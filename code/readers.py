"""Remote cutout readers for each storage layout. All talk HTTP(S) to an object store
(the local emulator or real S3/R2) through requests; byte/request counts are taken
server-side (emulator) or client-side (CountingSession) for real clouds."""
import io, os, zlib, json, threading, time, bisect
import numpy as np, requests
import pyarrow as pa, pyarrow.parquet as pq
from concurrent.futures import ThreadPoolExecutor
from astropy.io import fits
from astropy.wcs import WCS
import chunkbench as cb

_tls = threading.local()
class CountingSession:
    """Thread-local requests sessions with global client-side byte/request counters."""
    def __init__(self, pool=64):
        self.lock = threading.Lock(); self.requests = 0; self.bytes = 0; self.pool = pool
    def _s(self):
        s = getattr(_tls, 's', None)
        if s is None:
            from urllib3.util.retry import Retry
            s = requests.Session(); a = requests.adapters.HTTPAdapter(pool_connections=self.pool, pool_maxsize=self.pool,
                max_retries=Retry(total=4, backoff_factor=0.3, status_forcelist=(500, 502, 503, 504), allowed_methods=('GET', 'PUT', 'HEAD')))
            s.mount('http://', a); s.mount('https://', a); _tls.s = s
        return s
    def get(self, url, rng=None, stream=False):
        h = {'Range': rng} if rng else {}
        r = self._s().get(url, headers=h, stream=stream, timeout=(20, 120)); r.raise_for_status()
        with self.lock: self.requests += 1
        if not stream:
            with self.lock: self.bytes += len(r.content)
        return r
    def add_bytes(self, n):
        with self.lock: self.bytes += n
    def reset(self):
        with self.lock: self.requests = 0; self.bytes = 0

PART = 8 * 2 ** 20
XFER = ThreadPoolExecutor(128)   # transfer-only pool (tasks never submit further work)
def _get1(sess, url, a, b): return sess.get(url, f'bytes={a}-{b}').content
def get_range(sess, url, a, b):            # inclusive [a, b]; large ranges -> parallel 8 MiB parts (s3transfer-style)
    n = b - a + 1
    if n <= PART: return _get1(sess, url, a, b)
    cuts = list(range(a, b + 1, PART))
    return b''.join(XFER.map(lambda s: _get1(sess, url, s, min(s + PART - 1, b)), cuts))

# ------------------------------------------------------------------ FITS header helpers
def parse_header_at(buf, off):
    """Parse a FITS header starting at buf[off]; returns (Header, header_len) or (None, needed_bytes)."""
    i = off
    while i + 2880 <= len(buf):
        blk = buf[i:i + 2880]
        for c in range(0, 2880, 80):
            if blk[c:c + 8] == b'END     ' or blk[c:c + 80].rstrip() == b'END':
                hl = i + 2880 - off
                return fits.Header.fromstring(buf[off:off + hl].decode('ascii', 'replace')), hl
        i += 2880
    return None, (i - off) + 2880 * 4

def pad2880(n): return (n + 2879) // 2880 * 2880

# ------------------------------------------------------------------ Rubin: FITS tile-compressed calexp
class RubinFitsReader:
    """Minimal-bytes reader for LSST calexp FITS (GZIP_2 row tiles). index() discovers HDU layout with
    range reads (cold); cutout() fetches only the row tiles covering the box, one GET per plane."""
    HDUS = ((1, 'IMAGE'), (2, 'MASK'), (3, 'VARIANCE'))
    def __init__(self, url, sess): self.url, self.sess, self.idx = url, sess, None
    def index(self):
        if self.idx is not None: return self.idx
        buf = b''; base = 0; idx = {}
        def ensure(a, b):  # make buf cover absolute [a, b)
            nonlocal buf, base
            if a >= base and b <= base + len(buf): return
            buf = get_range(self.sess, self.url, a, max(b, a + 65536) - 1); base = a
        ensure(0, 65536)
        h, hl = parse_header_at(buf, 0); pos = pad2880(hl)  # primary (no data)
        for k, name in self.HDUS:
            ensure(pos, pos + 2880 * 8)
            h, hl = parse_header_at(buf, pos - base)
            if h is None: ensure(pos, pos + hl); h, hl = parse_header_at(buf, pos - base)
            d0 = pos + hl; n1, n2 = h['NAXIS1'], h['NAXIS2']
            ensure(d0, d0 + n1 * n2)
            tab = buf[d0 - base:d0 - base + n1 * n2]
            fmt = h['TFORM1']; q = 'Q' in fmt
            desc = np.frombuffer(tab, dtype='>i8' if q else '>i4').reshape(n2, -1)[:, :2].astype(np.int64)
            heap = d0 + h.get('THEAP', n1 * n2)
            if name == 'IMAGE': self.header = h
            idx[name] = dict(heap=heap, desc=desc, nx=h['ZNAXIS1'], ny=h['ZNAXIS2'], bitpix=h['ZBITPIX'],
                             cmp=h['ZCMPTYPE'].strip(), tile=(h['ZTILE2'], h['ZTILE1']))
            pos = d0 + pad2880(n1 * n2 + h['PCOUNT'])
        self.idx = idx; return idx
    def _plane(self, name, y0, y1, x0, x1):
        p = self.idx[name]; assert p['tile'] == (1, p['nx']) and p['cmp'] == 'GZIP_2'
        d = p['desc'][y0:y1]; a = p['heap'] + d[:, 1].min(); b = p['heap'] + (d[:, 1] + d[:, 0]).max()
        raw = get_range(self.sess, self.url, a, b - 1)
        dt = {-32: '>f4', 32: '>i4', 16: '>i2', -64: '>f8'}[p['bitpix']]; isz = abs(p['bitpix']) // 8
        out = np.empty((y1 - y0, x1 - x0), dtype=np.dtype(dt).newbyteorder('='))
        for i, (n, o) in enumerate(d):
            s = zlib.decompress(raw[o - (a - p['heap']):o - (a - p['heap']) + n], 47)
            row = np.frombuffer(s, np.uint8).reshape(isz, p['nx']).T.copy().view(dt).ravel()
            out[i] = row[x0:x1]
        return out
    def cutout(self, box, pool):
        self.index(); y0, y1, x0, x1 = box
        futs = {n: pool.submit(self._plane, n, y0, y1, x0, x1) for _, n in self.HDUS}
        return {n: f.result() for n, f in futs.items()}

# ------------------------------------------------------------------ Roman: whole-file gzip FITS
class _PartIter:
    """Fetch a whole object as 8 MiB ranged parts with a lookahead window, yielding in order."""
    def __init__(self, sess, url, window=6):
        self.sess, self.url, self.w = sess, url, window
        r = sess.get(url, f'bytes=0-{PART - 1}'); self.size = int(r.headers['Content-Range'].split('/')[-1])
        self.first = r.content; self.next = PART; self.q = []; self.closed = False
    def _submit(self):
        while len(self.q) < self.w and self.next < self.size:
            a = self.next; b = min(a + PART, self.size) - 1; self.next = b + 1
            self.q.append(XFER.submit(_get1, self.sess, self.url, a, b))
    def iter_content(self, n):
        yield self.first
        while True:
            self._submit()
            if not self.q or self.closed: return
            yield self.q.pop(0).result()
    def close(self):
        self.closed = True
        for f in self.q: f.cancel()

class _Stream:
    def __init__(self, resp, sess):
        self.it = resp.iter_content(1 << 18); self.d = zlib.decompressobj(31); self.buf = bytearray(); self.resp = resp
        self.sess = sess; self.pos = 0
    def _fill(self, n):
        while len(self.buf) < n:
            c = next(self.it, None)
            if c is None: self.buf += self.d.flush(); break
            self.buf += self.d.decompress(c)  # bytes counted by the session that fetched the part
    def read(self, n):
        self._fill(n); out = bytes(self.buf[:n]); del self.buf[:n]; self.pos += n; return out
    def skip(self, n):
        while n > 0:
            self._fill(min(n, 1 << 22)); k = min(n, len(self.buf)); del self.buf[:k]; n -= k; self.pos += k
            if k == 0: break
    def read_header(self):
        raw = b''
        while True:
            raw += self.read(2880); h, hl = parse_header_at(raw, 0)
            if h is not None: return h
    def close(self): self.resp.close()

class RomanGzReader:
    """Best-case reader for whole-file gzip FITS: stream-decompress from byte 0, keep only the rows
    needed from SCI/ERR/DQ, and abort the transfer right after the last needed row."""
    def __init__(self, url, sess): self.url, self.sess = url, sess
    def index(self): return None
    def cutout(self, box, pool=None):
        y0, y1, x0, x1 = box; st = _Stream(_PartIter(self.sess, self.url), self.sess); out = {}
        h = st.read_header()  # primary; NAXIS=0
        for name in ('SCI', 'ERR', 'DQ'):
            h = st.read_header(); nx, ny, bp = h['NAXIS1'], h['NAXIS2'], h['BITPIX']
            isz = abs(bp) // 8; dt = {-64: '>f8', -32: '>f4', 32: '>i4', 16: '>i2'}[bp]
            st.skip(y0 * nx * isz); blk = st.read((y1 - y0) * nx * isz)
            a = np.frombuffer(blk, dt).reshape(y1 - y0, nx)[:, x0:x1].astype(np.dtype(dt).newbyteorder('='))
            bz = h.get('BZERO', 0)
            if bp == 32 and bz == 2 ** 31: a = (a.view(np.uint32) ^ np.uint32(2 ** 31))
            elif bz or h.get('BSCALE', 1) != 1: a = a * h.get('BSCALE', 1) + bz
            out[name] = a
            if name != 'DQ': st.skip(pad2880(nx * ny * isz) - y1 * nx * isz)
        st.close(); return out

# ------------------------------------------------------------------ Roman: ASDF (lz4 blocks)
class RomanAsdfReader:
    """Reads via the asdf library over fsspec HTTP (lazy tree; each array block is fetched and
    decompressed whole on access, as in roman_datamodels)."""
    def __init__(self, url, sess, block_size=5 * 2 ** 20): self.url, self.sess, self.bs = url, sess, block_size  # fsspec default readahead
    def index(self): return None
    def cutout(self, box, pool=None):
        import asdf
        y0, y1, x0, x1 = box
        f = RangeFile(self.sess, self.url, self.bs)
        if True:
            with asdf.open(f, lazy_load=True, memmap=False) as af:
                r = af['roman']; return {k: np.asarray(r[k][y0:y1, x0:x1]) for k in ('data', 'err', 'dq')}

# ------------------------------------------------------------------ sparse file for pyarrow
class SparseFile(io.RawIOBase):
    def __init__(self, size, segs):
        self.size = size; self.segs = sorted(segs); self.starts = [s for s, _ in self.segs]; self.p = 0
    def readable(self): return True
    def seekable(self): return True
    def tell(self): return self.p
    def seek(self, o, wh=0):
        self.p = o if wh == 0 else (self.p + o if wh == 1 else self.size + o); return self.p
    def read(self, n=-1):
        if n is None or n < 0: n = self.size - self.p
        i = bisect.bisect_right(self.starts, self.p) - 1
        s, b = self.segs[i]
        if not (s <= self.p and self.p + n <= s + len(b)):
            raise IOError(f'sparse miss at {self.p}+{n}')
        out = b[self.p - s:self.p - s + n]; self.p += n; return out
    def readinto(self, bb):
        d = self.read(len(bb)); bb[:len(d)] = d; return len(d)

# ------------------------------------------------------------------ HATS-style pixel parquet (joint or per-survey)
class HatsPixelReader:
    def __init__(self, base_url, order, part_order, sess, expo):
        self.base, self.order, self.po, self.sess = base_url, order, part_order, sess
        self.meta = {}; self.expo = expo  # expo: dict exp_id -> (WCS, shape, survey)
    def _furl(self, part): return f"{self.base}/Norder={self.po}/Dir={(part // 10000) * 10000}/Npix={part}.parquet"
    def _footer(self, part):
        if part in self.meta: return self.meta[part]
        url = self._furl(part)
        try: r = self.sess.get(url, 'bytes=-65536')
        except (requests.HTTPError, FileNotFoundError): self.meta[part] = None; return None
        tail = r.content; size = int(r.headers['Content-Range'].split('/')[-1])
        mlen = int.from_bytes(tail[-8:-4], 'little')
        if mlen + 8 > len(tail):
            tail = get_range(self.sess, url, size - mlen - 8, size - 1)
        sf = SparseFile(size, [(size - len(tail), tail)])
        md = pq.read_metadata(sf)
        rg = {}
        for i in range(md.num_row_groups):
            g = md.row_group(i); t = g.column(0).statistics.min
            st = min((c.dictionary_page_offset or c.data_page_offset) if c.has_dictionary_page else c.data_page_offset
                     for c in (g.column(j) for j in range(g.num_columns)))
            en = max(g.column(j).data_page_offset + g.column(j).total_compressed_size for j in range(g.num_columns))
            en = max(en, max(((g.column(j).dictionary_page_offset or 0) + g.column(j).total_compressed_size) for j in range(g.num_columns)))
            sv = g.column(1).statistics.min if g.column(1).statistics is not None else None
            bd = g.column(3).statistics.min if g.column(3).statistics is not None else None
            rg.setdefault(int(t), []).append((i, st, en, sv, bd))
        self.meta[part] = dict(url=url, size=size, tail=(size - len(tail), tail), md=md, rg=rg); return self.meta[part]
    def fetch_tiles(self, tiles, pool, surveys=None, bands=None):
        shift = 2 * (self.order - self.po); parts = sorted(set(int(t) >> shift for t in tiles))
        metas = dict(zip(parts, pool.map(self._footer, parts)))
        jobs = []
        for part in parts:
            m = metas[part]
            if m is None: continue
            sel = sorted((i, a, b) for t in tiles if (int(t) >> shift) == part and int(t) in m['rg'] for (i, a, b, sv, bd) in m['rg'][int(t)]
                         if (surveys is None or sv in surveys) and (bands is None or bd in bands))
            if not sel: continue
            runs = []; gap = getattr(self, 'max_gap', 0)
            for i, a, b in sel:  # merge contiguous row groups (or ones separated by <= max_gap bytes) into one range
                if runs and ((a <= runs[-1][2] + 1 and i == runs[-1][3][-1] + 1) or (gap and a - runs[-1][2] <= gap)):
                    runs[-1][2] = max(b, runs[-1][2]); runs[-1][3].append(i)
                else: runs.append([part, a, b, [i]])
            jobs += runs
        datas = list(pool.map(lambda r: get_range(self.sess, metas[r[0]]['url'], r[1], r[2] - 1), jobs))
        tables = []
        for (part, a, b, rgs), d in zip(jobs, datas):
            m = metas[part]; sf = SparseFile(m['size'], [m['tail'], (a, d)])
            tables.append(pq.ParquetFile(sf, metadata=m['md']).read_row_groups(rgs))
        return pa.concat_tables(tables) if tables else None
    def cutouts(self, ra, dec, side, pool, surveys=None, bands=None):
        ex = self.expo if bands is None else {k: v for k, v in self.expo.items() if v[3] in bands}
        R = cb.query_radius(ex, ra, dec, side)
        tiles = cb.tiles_for_query(cb.HEALPix(nside=2 ** self.order, order='nested'), ra, dec, side, R=R)
        tbl = self.fetch_tiles(tiles, pool, surveys, bands)
        return assemble(tbl, ex, ra, dec, side)

def assemble(tbl, expo, ra, dec, side):
    """Paste ownership-masked stamps into per-exposure native-pixel cutouts."""
    out = {}
    if tbl is None: return out
    d = tbl.to_pydict(); n = len(d['tile'])
    boxes = {}
    for e, (w, shp, survey, *_rest) in expo.items():
        if e not in out:
            b = cb.query_box(w, shp, ra, dec, side)
            if b is not None: boxes[e] = b
    own = {}
    for i in range(n):
        if d['plane'][i] == 'OWN':
            sh = (d['y1'][i] - d['y0'][i], d['x1'][i] - d['x0'][i])
            own[(d['exp_id'][i], d['tile'][i])] = cb.dec_own(d['data'][i], sh)
    for i in range(n):
        e = d['exp_id'][i]; pl = d['plane'][i]
        if pl == 'OWN' or e not in boxes: continue
        qy0, qy1, qx0, qx1 = boxes[e]; y0, y1, x0, x1 = d['y0'][i], d['y1'][i], d['x0'][i], d['x1'][i]
        iy0, iy1, ix0, ix1 = max(qy0, y0), min(qy1, y1), max(qx0, x0), min(qx1, x1)
        if iy1 <= iy0 or ix1 <= ix0: continue
        a = cb.dec(d['data'][i], np.dtype(d['dtype'][i]), (y1 - y0, x1 - x0))
        m = own[(e, d['tile'][i])][iy0 - y0:iy1 - y0, ix0 - x0:ix1 - x0]
        o = out.setdefault(e, {}).get(pl)
        if o is None:
            o = np.zeros((qy1 - qy0, qx1 - qx0), a.dtype); out[e][pl] = o
        sub = o[iy0 - qy0:iy1 - qy0, ix0 - qx0:ix1 - qx0]
        np.copyto(sub, a[iy0 - y0:iy1 - y0, ix0 - x0:ix1 - x0], where=m)
    return out


# ------------------------------------------------------------------ in-process object-store emulator
class _Resp:
    def __init__(self, content=None, headers=None, it=None, closer=None):
        self.content = content; self.headers = headers or {}; self._it = it; self._closer = closer
    def iter_content(self, n): return self._it(n)
    def close(self):
        if self._closer: self._closer()

class EmulatedStore:
    """Serves 'url' = '<root-relative path>' from local disk with S3-like costs:
    time-to-first-byte `latency_ms` per GET, per-stream bandwidth `stream_MBps`, and a shared link of
    `link_MBps` (all concurrent transfers share it). Counts GETs and body bytes delivered."""
    def __init__(self, root, latency_ms=30.0, stream_MBps=90.0, link_MBps=1250.0):
        self.root = root; self.lat = latency_ms / 1000; self.sr = stream_MBps * 1e6; self.lr = link_MBps * 1e6
        self.lock = threading.Lock(); self.link_free = 0.0; self.requests = 0; self.bytes = 0
    def reset(self):
        with self.lock: self.requests = 0; self.bytes = 0
    def add_bytes(self, n): pass   # counted inside get()
    def _xfer_time(self, n):
        now = time.perf_counter()
        with self.lock:
            start = max(now + self.lat, self.link_free); self.link_free = start + n / self.lr
            link_done = self.link_free
        return max(now + self.lat + n / self.sr, link_done) - now
    def _path(self, url):
        return self.root + '/' + url.split('://', 1)[-1].split('/', 1)[-1] if '://' in url else self.root + '/' + url.lstrip('/')
    def get(self, url, rng=None, stream=False):
        fn = self._path(url); size = os.path.getsize(fn)
        a, b = 0, size - 1
        if rng:
            s, e = rng[6:].split('-')
            if s == '': a = max(size - int(e), 0)
            else: a = int(s); b = min(int(e), size - 1) if e else size - 1
        with self.lock: self.requests += 1
        hdr = {'Content-Range': f'bytes {a}-{b}/{size}'}
        if not stream:
            with open(fn, 'rb') as f: f.seek(a); d = f.read(b - a + 1)
            time.sleep(self._xfer_time(len(d)))
            with self.lock: self.bytes += len(d)
            return _Resp(content=d, headers=hdr)
        state = {'closed': False}
        def it(n):
            time.sleep(self.lat)
            with open(fn, 'rb') as f:
                f.seek(a); left = b - a + 1
                while left > 0 and not state['closed']:
                    d = f.read(min(n, left)); left -= len(d)
                    with self.lock:
                        start = max(time.perf_counter(), self.link_free); self.link_free = start + len(d) / self.lr
                        done = self.link_free
                    time.sleep(max(len(d) / self.sr, done - time.perf_counter()))
                    with self.lock: self.bytes += len(d)
                    yield d
        return _Resp(headers=hdr, it=it, closer=lambda: state.update(closed=True))

class RangeFile(io.RawIOBase):
    """Seekable read-only file over a store with a simple readahead block cache (for asdf)."""
    def __init__(self, sess, url, block=1 << 18):
        self.sess, self.url, self.block = sess, url, block
        r = sess.get(url, 'bytes=0-%d' % (block - 1)); self.size = int(r.headers['Content-Range'].split('/')[-1])
        self.cache = (0, r.content); self.p = 0
    def readable(self): return True
    def seekable(self): return True
    def tell(self): return self.p
    def seek(self, o, wh=0):
        self.p = o if wh == 0 else (self.p + o if wh == 1 else self.size + o); return self.p
    def read(self, n=-1):
        if n is None or n < 0: n = self.size - self.p
        n = max(0, min(n, self.size - self.p))
        s, b = self.cache
        if not (s <= self.p and self.p + n <= s + len(b)):
            e = min(self.p + max(n, self.block), self.size) - 1
            self.cache = (self.p, get_range(self.sess, self.url, self.p, e)); s, b = self.cache
        out = b[self.p - s:self.p - s + n]; self.p += n; return out
    def readinto(self, bb):
        d = self.read(len(bb)); bb[:len(d)] = d; return len(d)


class PerBandReader:
    """One HatsPixelReader per band tree (root/band=<filt>); bands are fetched in parallel."""
    def __init__(self, base_url, order, part_order, sess, expo, pool):
        self.pool = pool; self.rd = {}
        for b in sorted(set(v[3] for v in expo.values())):
            self.rd[b] = HatsPixelReader(f'{base_url}/band={b}', order, part_order, sess, {k: v for k, v in expo.items() if v[3] == b})
    def cutouts(self, ra, dec, side, pool, surveys=None, bands=None):
        use = [b for b in self.rd if bands is None or b in bands]
        out = {}
        for f in [self.pool.submit(self.rd[b].cutouts, ra, dec, side, pool) for b in use]: out.update(f.result())
        return out
