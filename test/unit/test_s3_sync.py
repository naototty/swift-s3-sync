import mock
from s3_sync import s3_sync
import unittest


class TestRing(object):
    pass

class TestInternalClient(object):
    pass

class TestS3Sync(unittest.TestCase):
    @mock.patch('s3_sync.s3_sync.InternalClient')
    @mock.patch('s3_sync.s3_sync.Ring')
    def setUp(self, mock_ring, mock_ic):
        self.mock_ring = TestRing()
        self.mock_ic = TestInternalClient()
        mock_ring.return_value = self.mock_ring
        mock_ic.return_value = self.mock_ic
        self.s3_sync = s3_sync.S3Sync({})

    def test_load_non_existent_meta(self):
        ret = self.s3_sync.load_sync_meta('foo', 'bar')
        self.assertEqual({}, ret)

    def test_load_existing_meta(self):
        pass
