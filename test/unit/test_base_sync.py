import mock
import unittest
from s3_sync.base_sync import BaseSync


class TestBaseSync(unittest.TestCase):
    def setUp(self):
        self.settings = {
            'account': 'account',
            'container': 'container',
            'aws_bucket': 'bucket'
        }

    @mock.patch('s3_sync.base_sync.BaseSync._get_client_factory')
    def test_http_pool_locking(self, factory_mock):
        factory_mock.return_value = mock.Mock()

        base = BaseSync(self.settings, max_conns=1)
        with base.client_pool.get_client():
            self.assertEqual(0, base.client_pool.get_semaphore.balance)
            self.assertEqual(
                0, base.client_pool.client_pool[0].semaphore.balance)
        self.assertEqual(1, base.client_pool.get_semaphore.balance)
