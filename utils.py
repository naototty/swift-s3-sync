from swift.common.utils import FileLikeIter
import urllib

SWIFT_USER_META_PREFIX = 'x-object-meta-'

class FileWrapper(object):
    def __init__(self, swift_client, account, container, key, headers={}):
        self._swift = swift_client
        self._account = account
        self._container = container
        self._key = key
        self._s3_key = get_s3_name(account, container, key)
        self.swift_req_hdrs = headers
        self.open_object_stream()

    def open_object_stream(self):
        status, self._headers, body = self._swift.get_object(
            self._account, self._container, self._key,
            headers=self.swift_req_hdrs)
        if status != 200:
            raise RuntimeError('Failed to get the object')
        self._iter = FileLikeIter(body)
        self._s3_headers = convert_to_s3_headers(self._headers)       

    def seek(self, pos, flag=0):
        if pos != 0:
            raise RuntimeError('Arbitrary seeks are not supported')
        self.open_object_stream()

    def read(self, size=-1):
        return self._iter.read(size)

    def __len__(self):
        if 'Content-Length' not in self._headers:
            raise RuntimeError('Length is not implemented')
        return int(self._headers['Content-Length'])

    def __iter__(self):
        return self._iter

    def get_s3_headers(self):
        return self._s3_headers


def convert_to_s3_headers(swift_headers):
    s3_headers = {}
    for hdr in swift_headers.keys():
        if hdr.lower().startswith(SWIFT_USER_META_PREFIX):
            s3_header_name = hdr[len(SWIFT_USER_META_PREFIX):]
            s3_headers[s3_header_name] = urllib.quote(swift_headers[hdr])
    return s3_headers

def is_object_meta_synced(s3_meta, swift_meta):
    swift_keys = set([key[len(SWIFT_USER_META_PREFIX):] for key in swift_meta
                      if key.startswith(SWIFT_USER_META_PREFIX)])
    s3_keys = set([key.lower() for key in s3_meta.keys()])
    if set(swift_keys) != set(s3_keys):
        return False
    for key in s3_keys:
        swift_value = swift_meta[SWIFT_USER_META_PREFIX + key]
        if s3_meta[key] != swift_value:
            return False
    return True

def get_s3_name(account, container, key):
    return '%s/%s/%s' % (account, container, key)
