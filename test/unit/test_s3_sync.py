import mock
from s3_sync import s3_sync
import unittest


class TestS3Sync(unittest.TestCase):
    @mock.patch('s3_sync.s3_sync.Ring')
    def setUp(self, mock_ring):
        self.mock_ring = mock.Mock()
        mock_ring.return_value = self.mock_ring
        self.conf = {'devices': '/devices',
                     'items_chunk': 1000,
                     'scratch': '/var/scratch'}
        self.s3_sync = s3_sync.S3Sync(self.conf)

    def test_sync_items(self):
        total_rows = 20
        items = [{'ROWID': x} for x in range(0, total_rows)]
        sync_container_mock = mock.Mock()

        for nodes in range(1, 7):
            for node_id in range(0, nodes):
                self.s3_sync.sync_row = mock.Mock()
                sync_calls = filter(lambda x: x % nodes == node_id,
                                    range(0, total_rows))
                verify_calls = filter(lambda x: x not in sync_calls,
                                      range(0, total_rows))

                self.s3_sync.sync_items(sync_container_mock, items, nodes,
                                        node_id)
                expected = [mock.call(sync_container_mock, {'ROWID': x})
                            for x in sync_calls]
                expected += [mock.call(sync_container_mock, {'ROWID': x})
                             for x in verify_calls]
                self.assertTrue(expected ==
                                self.s3_sync.sync_row.call_args_list)
