# -*- coding: UTF-8 -*-

from utils import FakeStream
from s3_sync import utils
import unittest


class TestUtilsFunctions(unittest.TestCase):
    def test_s3_headers_conversion(self):
        input_hdrs = {'x-object-meta-foo': 'Foo',
                      'x-object-meta-Bar': 'Bar',
                      'X-Object-Meta-upper': '1',
                      'X-ObJeCT-Meta-CraZy': 'CrAzY',
                      'X-Object-Manifest': 'container/key/123415/prefix'}
        out = utils.convert_to_s3_headers(input_hdrs)
        expected = dict([(key[len('x-object-meta-'):].lower(), value) for
                         key, value in input_hdrs.items() if
                         key.lower().startswith(utils.SWIFT_USER_META_PREFIX)])
        expected[utils.MANIFEST_HEADER] = input_hdrs['X-Object-Manifest']
        self.assertEqual(set(expected.keys()), set(out.keys()))
        for key in out.keys():
            self.assertEqual(expected[key], out[key])

    def test_is_object_meta_synced(self):
        # The structure for each entry is: swift meta, s3 meta, whether they
        # should be equal.
        test_metas = [({'x-object-meta-upper': 'UPPER',
                        'x-object-meta-lower': 'lower'},
                       {'upper': 'UPPER',
                        'lower': 'lower'},
                       True),
                      ({'x-object-meta-foo': 'foo',
                        'x-object-meta-foo': 'bar'},
                       {'foo': 'not foo',
                        'bar': 'bar'},
                       False),
                      ({'x-object-meta-unicode': '👍',
                        'x-object-meta-date': 'Wed, April 30 10:32:21 UTC'},
                       {'unicode': '%F0%9F%91%8D',
                        'date': 'Wed%2C%20April%2030%2010%3A32%3A21%20UTC'},
                       True)]
        for swift_meta, s3_meta, expected in test_metas:
            self.assertEqual(expected, utils.is_object_meta_synced(s3_meta,
                                                                   swift_meta))

    def test_get_slo_etag(self):
        sample_manifest = [{'hash': 'abcdef'}, {'hash': 'fedcba'}]
        # We expect the md5 sum of the concatenated strings (converted to hex
        # bytes) followed by the number of parts (segments)
        expected_tag = 'ce7989f0e2f1f3e4fdd2a01dda0844ae-2'
        self.assertEqual(expected_tag, utils.get_slo_etag(sample_manifest))


class TestFileWrapper(unittest.TestCase):
    class FakeSwift(object):
        def __init__(self):
            self.size = 1024
            self.status = 200

        def get_object(self, account, container, key, headers={}):
            self.fake_stream = FakeStream(self.size)
            return (self.status,
                    {'Content-Length': self.size},
                    self.fake_stream)

    def setUp(self):
        self.mock_swift = self.FakeSwift()

    def test_open(self):
        wrapper = utils.FileWrapper(self.mock_swift,
                                    'account',
                                    'container',
                                    'key')
        self.assertEqual(1024, len(wrapper))

    def test_seek(self):
        wrapper = utils.FileWrapper(self.mock_swift,
                                    'account',
                                    'container',
                                    'key')
        wrapper.read(256)
        wrapper.seek(0)
        self.assertEqual(0, self.mock_swift.fake_stream.current_pos)
