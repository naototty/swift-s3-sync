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
                     'status_dir': '/var/scratch'}
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
                self.assertEqual(expected,
                                 self.s3_sync.sync_row.call_args_list)

    def test_exit_if_no_containers(self):
        with self.assertRaises(SystemExit):
            self.s3_sync.run_once()

    @mock.patch('s3_sync.s3_sync.SyncContainer')
    def test_processes_every_container(self, sync_container_mock):
        self.s3_sync.sync_container = mock.Mock()
        self.s3_sync.conf['containers'] = [
            {'account': 'foo',
             'container': 'foo',
             'aws_bucket': 'bucket',
             'aws_identity': 'id',
             'aws_secret': 'secret'},
            {'account': 'bar',
             'container': 'bar',
             'aws_bucket': 'other-bucket',
             'aws_identity': 'something',
             'aws_secret': 'else'}
        ]

        self.s3_sync.run_once()
        expected_calls = [mock.call(self.conf['status_dir'], container)
                          for container in self.s3_sync.conf['containers']]
        self.assertEquals(expected_calls, sync_container_mock.call_args_list)
