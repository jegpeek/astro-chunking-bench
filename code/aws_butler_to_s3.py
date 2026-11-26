"""Create the S3-backed DirectButler properly (forceConfigRoot=False keeps the S3 datastore root) and
fill it by transfer_from the local file-backed repo made earlier (AWS-internal copy, no re-download).
Usage (lsst env): python aws_butler_to_s3.py <bucket> <local_repo> <s3_repo_dir>"""
import os, sys, time, json
from lsst.daf.butler import Butler, Config
bucket, src_repo, repo = sys.argv[1], sys.argv[2], sys.argv[3]
if not os.path.exists(os.path.join(repo, 'butler.yaml')):
    seed = Config({'datastore': {'root': f's3://{bucket}/dp1repo/'}, 'registry': {'db': f'sqlite:///{repo}/gen3.sqlite3'}})
    Butler.makeRepo(repo, config=seed, forceConfigRoot=False)
src = Butler(src_repo); dst = Butler(repo, writeable=True)
print('datastore root:', dst._datastore.root if hasattr(dst, '_datastore') else '?', flush=True)
bt = json.load(open('butler_transfer.json'))
refs = list(src.query_datasets('visit_image', collections=bt['runs'], where="instrument='LSSTComCam'", limit=None))
t0 = time.time()
out = dst.transfer_from(src, refs, transfer='copy', register_dataset_types=True, transfer_dimensions=True)
print(f'transferred {len(out)} to S3 datastore in {time.time()-t0:.0f}s', flush=True)
uris = {}
for r in dst.query_datasets('visit_image', collections=bt['runs'], where="instrument='LSSTComCam'", limit=None):
    uris[f"dp1_{int(r.dataId['visit'])}_{int(r.dataId['detector'])}"] = str(dst.getURI(r))
assert all(u.startswith('s3://') for u in uris.values()), list(uris.values())[:2]
bt.update(repo=repo, uris=uris); json.dump(bt, open('butler_transfer.json', 'w'))
print('uris', len(uris), next(iter(uris.values()))[:90])
