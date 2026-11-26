"""S3 transport for the benchmark readers: same interface as readers.CountingSession
(get(url, rng, stream) -> response with .content / .headers['Content-Range'] / iter_content),
but over boto3 so it works with private buckets via the EC2 instance role, and public
buckets (e.g. STScI 'stpubdata') anonymously. URLs are 's3://bucket/key'. Counts GETs and
body bytes client-side, thread-safe."""
import threading, boto3, botocore
from botocore import UNSIGNED
from botocore.config import Config

PUBLIC_BUCKETS = {'stpubdata': 'us-east-1', 'nasa-irsa-simulations': 'us-east-1'}   # public bucket -> its region

class _Resp:
    def __init__(self, content=None, headers=None, body=None, sess=None):
        self.content = content; self.headers = headers or {}; self._body = body; self._sess = sess
    def iter_content(self, n):
        for chunk in self._body.iter_chunks(n):
            with self._sess.lock: self._sess.bytes += len(chunk)
            yield chunk
    def close(self):
        if self._body is not None: self._body.close()
    def raise_for_status(self): pass

class S3Session:
    def __init__(self, region='us-east-2', pool=128):
        cfg = dict(region_name=region, max_pool_connections=pool, retries={'max_attempts': 5, 'mode': 'adaptive'},
                   s3={'addressing_style': 'virtual'})
        ep = f'https://s3.{region}.amazonaws.com'
        self.signed = boto3.client('s3', endpoint_url=ep, config=Config(**cfg))
        self.anon = {}
        for rg in set(PUBLIC_BUCKETS.values()):
            c2 = dict(cfg, region_name=rg)
            self.anon[rg] = boto3.client('s3', endpoint_url=f'https://s3.{rg}.amazonaws.com', config=Config(signature_version=UNSIGNED, **c2))
        self.lock = threading.Lock(); self.requests = 0; self.bytes = 0
    def _split(self, url):
        assert url.startswith('s3://'), url
        b, k = url[5:].split('/', 1); return b, k
    def _client(self, bucket): return self.anon[PUBLIC_BUCKETS[bucket]] if bucket in PUBLIC_BUCKETS else self.signed
    def get(self, url, rng=None, stream=False):
        b, k = self._split(url); kw = {'Bucket': b, 'Key': k}
        if rng: kw['Range'] = rng
        try:
            r = self._client(b).get_object(**kw)
        except botocore.exceptions.ClientError as e:
            if e.response.get('Error', {}).get('Code') in ('NoSuchKey', '404'): raise FileNotFoundError(url) from e
            raise
        with self.lock: self.requests += 1
        hdr = {'Content-Range': r.get('ContentRange') or f"bytes 0-{r['ContentLength']-1}/{r['ContentLength']}",
               'Content-Length': str(r['ContentLength'])}
        if stream: return _Resp(headers=hdr, body=r['Body'], sess=self)
        data = r['Body'].read()
        with self.lock: self.bytes += len(data)
        return _Resp(content=data, headers=hdr)
    def add_bytes(self, n): pass
    def reset(self):
        with self.lock: self.requests = 0; self.bytes = 0
    def put_file(self, path, url):
        b, k = self._split(url); self.signed.upload_file(path, b, k)
