"""Build v5 layout variants of the joint store at N=200 (all 200 DP1 visits + 18 Roman SCAs) on the EC2 host.
Phase 1: extract each exposure's patch pixels + order-14 ownership map once, cache to ~/vcache (npz).
Phase 2: per variant, re-stamp in 8 processes (codec / HEALPix order), write tile-major Parquet or blob store,
         sync to s3://<bucket>/stores_v5/<variant>/ and delete the local copy.
Usage (roman env): python aws_build_variants.py <bucket> <butler_transfer.json> [variant ...]"""
import os, sys, json, time, subprocess, shutil, glob
sys.path.insert(0, os.getcwd())
import numpy as np, pandas as pd, warnings; warnings.filterwarnings('ignore')
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import multiprocessing as mp

VARIANTS = {  # name: (layout, order, codec)
    'tm_o14_lz4': ('tm', 14, 'lz4'), 'tm_o14_zstd1': ('tm', 14, 'zstd1'),
    'tm_o13': ('tm', 13, 'zstd5'), 'tm_o12': ('tm', 12, 'zstd5'),
    'blob_o14': ('blob', 14, 'zstd5'), 'blob_o14_lz4': ('blob', 14, 'lz4')}
CACHE = os.path.expanduser('~/vcache')

def make_enc(codec):
    import blosc2
    c, lv = {'zstd5': (blosc2.Codec.ZSTD, 5), 'zstd1': (blosc2.Codec.ZSTD, 1), 'lz4': (blosc2.Codec.LZ4, 5)}[codec]
    def enc(a):
        return blosc2.compress2(np.ascontiguousarray(a), codec=c, clevel=lv, filters=[blosc2.Filter.BITSHUFFLE],
                                typesize=a.dtype.itemsize, nthreads=1)
    return enc

def stamp_one(args):
    path, order, codec = args
    import chunkbench as cb
    cb.enc = make_enc(codec)
    z = np.load(path, allow_pickle=True); m = z['meta'].item()
    planes = {k: z[k] for k in m['planes']}
    return cb.stamps_from_ownership(planes, m['survey'], m['exp_id'], m['filt'], tuple(m['reg']), z['own'], order,
                                    origin=(m['reg'][0], m['reg'][2]))

def extract(bucket, bt):
    import chunkbench as cb, readers as rd, s3session, asdf
    from astropy.io import fits
    from astropy.wcs import WCS
    os.makedirs(CACHE, exist_ok=True)
    p2 = json.load(open('patch2.json')); man = pd.read_parquet('dp1_manifest.parquet'); rex = pd.read_parquet('roman_l2_expo.parquet')
    S = s3session.S3Session(region='us-east-2', pool=64); inner = ThreadPoolExecutor(12); rd.XFER = ThreadPoolExecutor(24)
    def save(e, survey, filt, reg, planes, own):
        np.savez(f'{CACHE}/{e}.npz', own=own, meta=np.array(dict(exp_id=e, survey=survey, filt=filt, reg=list(map(int, reg)), planes=list(planes))), **planes)
    def rubin(rw):
        e = rw.exp_id; rdr = rd.RubinFitsReader(bt['uris'][e], S); rdr.index(); w = WCS(rdr.header)
        shp = (rdr.idx['IMAGE']['ny'], rdr.idx['IMAGE']['nx'])
        reg = cb.patch_region(w, shp, p2['patch_ra'], p2['patch_dec'], p2['patch_deg'])
        save(e, 'rubin', rw.lsst_band, reg, rdr.cutout(reg, inner), cb.ownership_map(w, reg))
    with ThreadPoolExecutor(6) as ex: list(ex.map(rubin, man.itertuples()))
    for r in rex.itertuples():
        fn = os.path.basename(r.local); tmp = f'/tmp/{fn}'; S.signed.download_file(bucket, f'roman_l2/{fn}', tmp)
        w = WCS(fits.Header.fromstring(r.wcs_header)); reg = cb.patch_region(w, (4088, 4088), p2['patch_ra'], p2['patch_dec'], p2['patch_deg'])
        with asdf.open(tmp, lazy_load=True, memmap=False) as af:
            planes = {k: np.ascontiguousarray(np.asarray(af['roman'][k])[reg[0]:reg[1], reg[2]:reg[3]]) for k in ('data', 'err', 'dq')}
        os.remove(tmp); save(r.exp_id, 'roman_l2', r.filt, reg, planes, cb.ownership_map(w, reg))
    return S.bytes

if __name__ == '__main__':
    bucket = sys.argv[1]; bt = json.load(open(sys.argv[2])); names = sys.argv[3:] or list(VARIANTS)
    t0 = time.time()
    if len(glob.glob(f'{CACHE}/*.npz')) < 218:
        nb = extract(bucket, bt); print(f'extract: {len(glob.glob(CACHE + "/*.npz"))} exposures, {nb/1e9:.2f} GB read, {time.time()-t0:.0f}s', flush=True)
    import chunkbench as cb, joint2
    files = sorted(glob.glob(f'{CACHE}/*.npz'))
    P = ProcessPoolExecutor(8, mp_context=mp.get_context('spawn'))
    for v in names:
        lay, order, codec = VARIANTS[v]; t1 = time.time(); rows = []
        for st in P.map(stamp_one, [(f, order, codec) for f in files], chunksize=4): rows += st
        df = pd.DataFrame(rows); del rows; t2 = time.time()
        root = os.path.expanduser(f'~/stores_v5/{v}'); shutil.rmtree(root, ignore_errors=True)
        (joint2.write_blob_store if lay == 'blob' else cb.write_hats_parquet_v2)(df, root, order, 10)
        sz = sum(os.path.getsize(os.path.join(d, f)) for d, _, fs in os.walk(root) for f in fs); n = len(df); del df
        subprocess.run(['aws', 's3', 'sync', root, f's3://{bucket}/stores_v5/{v}/', '--region', 'us-east-2', '--only-show-errors'], check=True)
        shutil.rmtree(root)
        print(f'{v}: {n} rows, {sz/1e9:.3f} GB, stamp {t2-t1:.0f}s, write+sync {time.time()-t2:.0f}s', flush=True)
    print(f'ALL DONE {time.time()-t0:.0f}s', flush=True)
