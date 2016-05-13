from swift.common.utils import FileLikeIter

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
        if hdr.startswith('X-Object-Meta-'):
            s3_header_name = hdr[len('X-Object-Meta-'):]
            s3_headers[s3_header_name] = swift_headers[hdr]
    return s3_headers

def get_s3_name(account, container, key):
    return '%s/%s/%s' % (account, container, key)
