"""Download the 18 Roman L2 files (stpubdata, r00342) and the 24-file DP1 local subset with parallel
8 MiB ranged GETs. Usage: python fetch_v3.py roman|dp1"""
import os, sys, time, requests
sys.path.append(os.getcwd())
os.environ.setdefault('XDG_CONFIG_HOME', os.path.abspath('astropy_cfg')); os.environ.setdefault('XDG_CACHE_HOME', os.path.abspath('astropy_cache'))
import pandas as pd
from concurrent.futures import ThreadPoolExecutor
PART = 8 * 2 ** 20
S = requests.Session(); S.mount('https://', requests.adapters.HTTPAdapter(pool_maxsize=32))
P = ThreadPoolExecutor(16)
def fetch(url, fn):
    for k in range(10):
        try:
            r = S.get(url, headers={'Range': 'bytes=0-0'}, timeout=(20, 60)); r.raise_for_status(); break
        except requests.RequestException:
            if k == 9: raise
            time.sleep(2 * (k + 1))
    size = int(r.headers['Content-Range'].split('/')[-1])
    if os.path.exists(fn) and os.path.getsize(fn) == size: return size
    def part(a):
        for k in range(10):
            try:
                r = S.get(url, headers={'Range': f'bytes={a}-{min(a + PART, size) - 1}'}, timeout=(20, 120)); r.raise_for_status(); return r.content
            except requests.RequestException: time.sleep(2 * (k + 1))
        raise RuntimeError('part failed')
    with open(fn + '.part', 'wb') as f:
        for b in P.map(part, range(0, size, PART)): f.write(b)
    os.replace(fn + '.part', fn); return size
t0 = time.time()
if sys.argv[1] == 'roman':
    os.makedirs('data/roman_l2', exist_ok=True)
    rex = pd.read_parquet('roman_l2_expo.parquet'); tot = 0
    for loc in rex.local:
        b = os.path.basename(loc)
        tot += fetch('https://stpubdata.s3.us-east-1.amazonaws.com/roman/nexus/soc_simulations/r00342/l2/' + b, 'data/roman_l2/' + b)
    print('roman', len(rex), round(tot / 1e9, 2), 'GB', round(time.time() - t0), 's')
else:
    import dp1access as dpa
    man = pd.read_parquet('dp1_manifest.parquet'); loc = man[man.local]
    os.makedirs('data/dp1', exist_ok=True)
    urls = dpa.resolve(loc.access_url.tolist()); tot = 0
    for e, u in zip(loc.exp_id, urls): tot += fetch(u, f'data/dp1/{e}.fits')
    print('dp1', len(loc), round(tot / 1e9, 2), 'GB', round(time.time() - t0), 's')
