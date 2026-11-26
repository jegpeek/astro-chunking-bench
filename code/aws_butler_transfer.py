"""Create a private DirectButler in AWS (SQLite registry on the instance disk, datastore in the
private S3 bucket) and copy the 200 benchmark DP1 visit_images into it from the DP1 remote Butler
with Butler.transfer_from (files + dimension records). Run inside the LSST stack (setup lsst_distrib).
Usage: python aws_butler_transfer.py <bucket> <dp1_manifest.parquet> [repo_dir]"""
import os, sys, time, json
import pandas as pd
from lsst.daf.butler import Butler, Config

bucket, manifest = sys.argv[1], sys.argv[2]
repo = sys.argv[3] if len(sys.argv) > 3 else os.path.expanduser('~/dp1repo')
DP1 = 'https://data.lsst.cloud/api/butler/repo/dp1/butler.yaml'
tok = open(os.path.expanduser('~/.rsp_token')).read().strip()
assert tok, '~/.rsp_token is empty'
os.environ['BUTLER_RUBIN_ACCESS_TOKEN'] = tok

if not os.path.exists(os.path.join(repo, 'butler.yaml')):
    seed = Config({'datastore': {'root': f's3://{bucket}/dp1repo/'},
                   'registry': {'db': f'sqlite:///{repo}/gen3.sqlite3'}})
    Butler.makeRepo(repo, config=seed)
    print('created repo', repo, '-> datastore', f's3://{bucket}/dp1repo/', flush=True)

remote = Butler(DP1)
local = Butler(repo, writeable=True)
man = pd.read_parquet(manifest)
pairs = set(zip(man.lsst_visit.astype(int), man.lsst_detector.astype(int)))
visits = sorted({v for v, _ in pairs})
t0 = time.time(); refs = []
for i in range(0, len(visits), 50):
    vs = ','.join(str(v) for v in visits[i:i + 50])
    for r in remote.query_datasets('visit_image', collections='LSSTComCam/DP1',
                                   where=f"instrument='LSSTComCam' AND visit IN ({vs})", limit=None):
        if (int(r.dataId['visit']), int(r.dataId['detector'])) in pairs: refs.append(r)
print(f'found {len(refs)} of {len(pairs)} visit_images in {time.time()-t0:.1f}s', flush=True)
t0 = time.time()
out = local.transfer_from(remote, refs, transfer='copy', register_dataset_types=True, transfer_dimensions=True)
print(f'transferred {len(out)} datasets in {time.time()-t0:.0f}s', flush=True)
runs = sorted({r.run for r in refs})
uris = {}
for r in local.query_datasets('visit_image', collections=runs, where="instrument='LSSTComCam'", limit=None):
    k = (int(r.dataId['visit']), int(r.dataId['detector']))
    if k in pairs: uris[f'dp1_{k[0]}_{k[1]}'] = str(local.getURI(r))
json.dump({'repo': repo, 'runs': runs, 'n': len(out), 'uris': uris}, open('butler_transfer.json', 'w'))
print('runs', runs, 'uris', len(uris), next(iter(uris.values()))[:80])
