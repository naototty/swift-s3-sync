# -*- coding: UTF-8 -*-

from utils import FakeStream
from s3_sync import utils
import mock
import unittest


class TestUtilsFunctions(unittest.TestCase):
    def test_s3_headers_conversion(self):
        input_hdrs = {'x-object-meta-foo': 'Foo',
                      'x-object-meta-Bar': 'Bar',
                      'X-Object-Meta-upper': '1',
                      'X-ObJeCT-Meta-CraZy': 'CrAzY',
                      'X-Object-Manifest': 'container/key/123415/prefix',
                      'Content-Type': 'application/testing'}
        out = utils.convert_to_s3_headers(input_hdrs)
        expected = dict([(key[len('x-object-meta-'):].lower(), value) for
                         key, value in input_hdrs.items() if
                         key.lower().startswith(utils.SWIFT_USER_META_PREFIX)])
        expected[utils.MANIFEST_HEADER] = input_hdrs['X-Object-Manifest']
        self.assertEqual(set(expected.keys()), set(out.keys()))
        for key in out.keys():
            self.assertEqual(expected[key], out[key])

    def test_get_slo_etag(self):
        sample_manifest = [{'hash': 'abcdef'}, {'hash': 'fedcba'}]
        # We expect the md5 sum of the concatenated strings (converted to hex
        # bytes) followed by the number of parts (segments)
        expected_tag = 'ce7989f0e2f1f3e4fdd2a01dda0844ae-2'
        self.assertEqual(expected_tag, utils.get_slo_etag(sample_manifest))


class FakeSwift(object):
    def __init__(self):
        self.size = 1024
        self.status = 200

    def get_object(self, account, container, key, headers={}):
        self.fake_stream = FakeStream(self.size)
        return (self.status,
                {'Content-Length': self.size},
                self.fake_stream)


class TestFileWrapper(unittest.TestCase):
    def setUp(self):
        self.mock_swift = FakeSwift()

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


class TestSLOFileWrapper(unittest.TestCase):
    def setUp(self):
        self.manifest = [
            {'name': '/foo/part1',
             'bytes': 500},
            {'name': '/foo/part2',
             'bytes': 1000}
        ]
        self.swift = mock.Mock()

    def test_slo_length(self):
        slo = utils.SLOFileWrapper(self.swift, 'account', self.manifest,
                                   {'etag': 'deadbeef'})
        self.assertEqual(1500, len(slo))

    def test_slo_headers(self):
        slo = utils.SLOFileWrapper(self.swift, 'account', self.manifest,
                                   {'etag': 'deadbeef'})

        self.assertEqual(1500, len(slo))
        self.assertEqual(
            'deadbeef', slo.get_s3_headers()['swift-slo-etag'])

    def test_seek_after_read(self):
        fake_segment = FakeStream(content='A' * 500)
        self.assertEqual(False, fake_segment.closed)

        def get_object(account, container, key, headers={}):
            if account != 'account':
                raise RuntimeError('unknown account')
            if container != 'foo':
                raise RuntimeError('unknown container')
            if key == 'part1':
                return (200, {'Content-Length': 500}, fake_segment)
            raise RuntimeError('unknown key')

        self.swift.get_object.side_effect = get_object
        slo = utils.SLOFileWrapper(self.swift, 'account', self.manifest,
                                   {'etag': 'deadbeef'})
        data = slo.read()
        slo.seek(0)
        self.assertEqual(True, fake_segment.closed)
        self.assertEqual('A' * 500, data)
        self.swift.get_object.assert_called_once_with(
            'account', 'foo', 'part1', headers={})

    def test_read_manifest(self):
        part1_content = FakeStream(content='A' * 500)
        part2_content = FakeStream(content='B' * 1000)

        def get_object(account, container, key, headers={}):
            if account != 'account':
                raise RuntimeError('unknown account')
            if container != 'foo':
                raise RuntimeError('unknown container')
            if key == 'part1':
                return (200, {'Content-Length': 500}, part1_content)
            if key == 'part2':
                return (200, {'Content-Length': 1000}, part2_content)
            raise RuntimeError('unknown key')

        self.swift.get_object.side_effect = get_object
        slo = utils.SLOFileWrapper(self.swift, 'account', self.manifest,
                                   {'etag': 'deadbeef'})
        content = ''
        while True:
            data = slo.read()
            content += data
            if not data:
                break
        self.assertEqual(1500, len(content))
        self.assertEqual('A' * 500, content[0:500])
        self.assertEqual('B' * 1000, content[500:1500])

        self.swift.get_object.has_calls(
            mock.call('account', 'foo', 'part1', {}),
            mock.call('account', 'foo', 'part2', {}))
        self.assertEqual(True, part1_content.closed)
        self.assertEqual(True, part2_content.closed)
