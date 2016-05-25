from s3_sync import utils

import unittest


class TestUtilsFunctions(unittest.TestCase):
    def test_s3_headers_conversion(self):
        input_hdrs = {'x-object-meta-foo': 'Foo',
                      'x-object-meta-Bar': 'Bar',
                      'X-Object-Meta-upper': '1',
                      'X-ObJeCT-Meta-CraZy': 'CrAzY'}
        out = utils.convert_to_s3_headers(input_hdrs)
        expected = dict([(key[len('x-object-meta-'):].lower(), value) for
                         key, value in input_hdrs.items()])
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
                       False)]
        for swift_meta, s3_meta, expected in test_metas:
            self.assertEqual(expected, utils.is_object_meta_synced(s3_meta,
                                                                   swift_meta))


class TestFileWrapper(unittest.TestCase):
    class FakeStream(object):
        def __init__(self, size=1024):
            self.size = size
            self.current_pos = 0

        def read(self, size=0):
            if self.current_pos == self.size - 1:
                return ''
            if size == -1 or self.current_pos + size > self.size:
                ret = 'A'*(self.size - self.current_pos)
                self.current_pos = self.size - 1
                return ret
            self.current_pos += size
            return 'A'*size

        def next(self):
            if self.current_pos == self.size:
                raise StopIteration()
            self.current_pos += 1
            return 'A'

        def __iter__(self):
            return self

    class FakeSwift(object):
        def __init__(self):
            self.size = 1024
            self.status = 200

        def get_object(self, account, container, key, headers={}):
            self.fake_stream = TestFileWrapper.FakeStream(self.size)
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
