"""Resolve Rubin DP1 datalink IDs to short-lived signed GCS URLs (virtual-hosted form).
Needs the RSP token in env var RSP (declare the 'RSP' credential on the cell)."""
import os, io, requests, warnings
from concurrent.futures import ThreadPoolExecutor
from astropy.io.votable import parse_single_table
warnings.filterwarnings('ignore')

def _session():
    s = requests.Session(); s.headers['Authorization'] = 'Bearer ' + os.environ['RSP']; return s

def vhost(u):
    a, b = u.split('://storage.googleapis.com/', 1); bkt, rest = b.split('/', 1)
    return f'https://{bkt}.storage.googleapis.com/{rest}'

def resolve(datalink_urls, workers=8):
    s = _session()
    def one(dl):
        import time
        for k in range(8):
            try:
                r = s.get(dl, timeout=120); r.raise_for_status()
                t = parse_single_table(io.BytesIO(r.content)).to_table(use_names_over_ids=True).to_pandas()
                return vhost(t[t.semantics == '#this'].access_url.iloc[0])
            except Exception:
                if k == 7: raise
                time.sleep(2 * (k + 1))
    with ThreadPoolExecutor(workers) as ex: return list(ex.map(one, datalink_urls))
