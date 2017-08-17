import eventlet
import hashlib
import urllib

from swift.common.utils import FileLikeIter
from swift.common.swob import Request


SWIFT_USER_META_PREFIX = 'x-object-meta-'
S3_USER_META_PREFIX = 'x-amz-meta-'
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

    def get_headers(self):
        return self._headers

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


class BlobstorePutWrapper(object):
    def __init__(self, chunk_size, chunk_queue):
        self.chunk_size = chunk_size
        self.queue = chunk_queue

        self.chunk = None
        self.chunk_offset = 0
        self.closed = False

    def read(self, size=-1):
        if self.closed:
            return ''
        if size == -1 or size > self.chunk_size:
            size = self.chunk_size
        resp = ''
        while size:
            if size < 0:
                raise RuntimeError('Negative chunk size')
            if self.chunk == '':
                self.closed = True
                break
            if not self.chunk or self.chunk_offset == len(self.chunk):
                self.chunk = self.queue.get()
                self.chunk_offset = 0

            read_sz = min(size, len(self.chunk) - self.chunk_offset)
            new_offset = self.chunk_offset + read_sz
            resp += self.chunk[self.chunk_offset:new_offset]
            size -= read_sz
            self.chunk_offset = new_offset
        return resp

    def close(self):
        self.closed = True
        if self.chunk:
            self.chunk = None
            self.chunk_offset = 0
        return


class SwiftPutWrapper(object):
    CHUNK_SIZE = 65536

    def __init__(self, req, app):
        self.body = req.environ['wsgi.input']

        self.queue = eventlet.queue.Queue(maxsize=100)
        req.environ['wsgi.input'] = BlobstorePutWrapper(
            self.CHUNK_SIZE, self.queue)
        self.put_thread = eventlet.greenthread.spawn(req.call_application, app)

    def read(self, size=-1):
        if size == -1 or size > self.CHUNK_SIZE:
            size = self.CHUNK_SIZE
        if hasattr(self.body, 'read'):
            chunk = self.body.read(size)
        else:
            try:
                chunk = next(self.body)
            except StopIteration:
                chunk = ''
        self.queue.put(chunk)
        # Wait for the Swift write to complete
        if not chunk:
            status, headers, resp_iter = self.put_thread.wait()

            # TODO: Check the PUT status -- we can only log on error here, but
            # not raise, probably, as otherwise the client request will fail
            for _ in resp_iter:
                # Do not expect anything returned
                pass
        return chunk

    def __iter__(self):
        return self

    def next(self):
        chunk = self.read(self.CHUNK_SIZE)
        if not chunk:
            raise StopIteration
        return chunk


def convert_to_s3_headers(swift_headers):
    s3_headers = {}
    for hdr in swift_headers.keys():
        if hdr.lower().startswith(SWIFT_USER_META_PREFIX):
            s3_header_name = hdr[len(SWIFT_USER_META_PREFIX):].lower()
            s3_headers[s3_header_name] = urllib.quote(swift_headers[hdr])
        elif hdr.lower() == MANIFEST_HEADER:
            s3_headers[MANIFEST_HEADER] = urllib.quote(swift_headers[hdr])
        elif hdr.lower() == SLO_HEADER:
            s3_headers[SLO_HEADER] = urllib.quote(swift_headers[hdr])

    return s3_headers


def convert_to_swift_headers(s3_headers):
    swift_headers = {}
    for header, value in s3_headers.items():
        if header in ('x-amz-id-2', 'x-amz-request-id'):
            swift_headers['Remote-' + header] = value
        elif header.endswith((MANIFEST_HEADER, SLO_HEADER)):
            swift_headers[header[len(S3_USER_META_PREFIX):]] = value
        elif header.startswith(S3_USER_META_PREFIX):
            key = SWIFT_USER_META_PREFIX + header[len(S3_USER_META_PREFIX):]
            swift_headers[key] = value
        elif header == 'content-length':
            # Capitalize, so eventlet doesn't try to add its own
            swift_headers['Content-Length'] = value
        elif header == 'etag':
            # S3 returns ETag in quotes
            swift_headers['etag'] = value[1:-1]
        else:
            swift_headers[header] = value
    return swift_headers


def tee_response(request, headers, body, app):
    env = dict(request.environ)
    env['REQUEST_METHOD'] = 'PUT'
    put_headers = dict([(k, v) for k, v in headers
                        if not k.startswith('Remote-')])
    # Request requires body to implement __len__, which we will not have
    # implemented
    env['wsgi.input'] = body
    env['CONTENT_LENGTH'] = put_headers['Content-Length']
    put_req = Request.blank(
        '',  # should pick up the path from environ
        environ=env,
        headers=put_headers)
    return SwiftPutWrapper(put_req, app)


def get_slo_etag(manifest):
    etags = [segment['hash'].decode('hex') for segment in manifest]
    md5_hash = hashlib.md5()
    md5_hash.update(''.join(etags))
    return md5_hash.hexdigest() + '-%d' % len(manifest)


def check_slo(swift_meta):
    if SLO_HEADER not in swift_meta:
        return False
    return swift_meta[SLO_HEADER] == 'True'
