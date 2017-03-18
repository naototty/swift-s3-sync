import hashlib
import urllib

from swift.common.utils import FileLikeIter


SWIFT_USER_META_PREFIX = 'x-object-meta-'
MANIFEST_HEADER = 'x-object-manifest'
SLO_HEADER = 'x-static-large-object'
SLO_ETAG_FIELD = 'swift-slo-etag'


class FileWrapper(object):
    def __init__(self, swift_client, account, container, key, headers={}):
        self._swift = swift_client
        self._account = account
        self._container = container
        self._key = key
        self.swift_req_hdrs = headers
        self._bytes_read = 0
        self.open_object_stream()

    def open_object_stream(self):
        status, self._headers, body = self._swift.get_object(
            self._account, self._container, self._key,
            headers=self.swift_req_hdrs)
        if status != 200:
            raise RuntimeError('Failed to get the object')
        self._swift_stream = body
        self._iter = FileLikeIter(body)
        self._s3_headers = convert_to_s3_headers(self._headers)

    def seek(self, pos, flag=0):
        if pos != 0:
            raise RuntimeError('Arbitrary seeks are not supported')
        if self._bytes_read == 0:
            return
        self._swift_stream.close()
        self.open_object_stream()

    def read(self, size=-1):
        if self._bytes_read == self.__len__():
            return ''

        data = self._iter.read(size)
        self._bytes_read += len(data)
        # TODO: we do not need to read an extra byte after
        # https://review.openstack.org/#/c/363199/ is released
        if self._bytes_read == self.__len__():
            self._iter.read(1)
            self._swift_stream.close()
        return data

    def __len__(self):
        if 'Content-Length' not in self._headers:
            raise RuntimeError('Length is not implemented')
        return int(self._headers['Content-Length'])

    def __iter__(self):
        return self._iter

    def get_s3_headers(self):
        return self._s3_headers

    def close(self):
        self._swift_stream.close()


class SLOFileWrapper(object):

    # For Google Cloud Storage, we convert SLO to a single object. We can't do
    # that easily with InternalClient, as it does not allow query parameters.
    # This means that if we turn on SLO in the pipeline, we will not be able to
    # retrieve the manifest object itself. In the future, this may be converted
    # to a resumable upload or we may resort to using compose.
    #
    # For the headers, we must also attach the Swift manifest ETag, as we have
    # no way of verifying the object has been uploaded otherwise.
    def __init__(self, swift_client, account, manifest, manifest_meta,
                 headers={}):
        self._swift = swift_client
        self._manifest = manifest
        self._account = account
        self._swift_req_headers = headers
        self._s3_headers = convert_to_s3_headers(manifest_meta)
        self._s3_headers[SLO_ETAG_FIELD] = manifest_meta['etag']
        self._segment = None
        self._segment_index = 0
        self._size = sum([int(segment['bytes']) for segment in self._manifest])

    def seek(self, pos, flag=0):
        if pos != 0:
            raise RuntimeError('Arbitrary seeks are not supported')
        if not self._segment:
            return
        self._segment.close()
        self._segment = None
        self._segment_index = 0

    def _open_next_segment(self):
        segment = self._manifest[self._segment_index]
        container, key = segment['name'].split('/', 2)[1:]
        self._segment = FileWrapper(self._swift, self._account, container,
                                    key, self._swift_req_headers)
        self._segment_index += 1

    def read(self, size=-1):
        if not self._segment:
            self._open_next_segment()
        data = self._segment.read(size)
        if not data:
            if self._segment_index < len(self._manifest):
                self._open_next_segment()
                data = self._segment.read(size)
        return data

    def next(self):
        data = self.read()
        if not data:
            raise StopIteration()

    def __iter__(self):
        return self

    def __len__(self):
        return self._size

    def get_s3_headers(self):
        return self._s3_headers


def convert_to_s3_headers(swift_headers):
    s3_headers = {}
    for hdr in swift_headers.keys():
        if hdr.lower().startswith(SWIFT_USER_META_PREFIX):
            s3_header_name = hdr[len(SWIFT_USER_META_PREFIX):].lower()
            s3_headers[s3_header_name] = urllib.quote(swift_headers[hdr])
        elif hdr.lower() == MANIFEST_HEADER:
            s3_headers[MANIFEST_HEADER] = urllib.quote(swift_headers[hdr])

    return s3_headers


def is_object_meta_synced(s3_meta, swift_meta):
    swift_keys = set([key.lower()[len(SWIFT_USER_META_PREFIX):]
                      for key in swift_meta
                      if key.lower().startswith(SWIFT_USER_META_PREFIX)])
    s3_keys = set([key.lower() for key in s3_meta.keys()])
    if SLO_HEADER in swift_meta and SLO_ETAG_FIELD in s3_keys:
        # We include the SLO ETag for Google SLO uploads for content
        # verification
        s3_keys.remove(SLO_ETAG_FIELD)
    if set(swift_keys) != set(s3_keys):
        return False
    for key in s3_keys:
        swift_value = urllib.quote(swift_meta[SWIFT_USER_META_PREFIX + key])
        if s3_meta[key] != swift_value:
            return False
    return True


def get_slo_etag(manifest):
    etags = [segment['hash'].decode('hex') for segment in manifest]
    md5_hash = hashlib.md5()
    md5_hash.update(''.join(etags))
    return md5_hash.hexdigest() + '-%d' % len(manifest)


def check_slo(swift_meta):
    if SLO_HEADER not in swift_meta:
        return False
    return swift_meta[SLO_HEADER] == 'True'
