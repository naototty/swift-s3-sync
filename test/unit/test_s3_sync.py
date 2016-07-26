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

    def test_sync_items_errors(self):
        rows = 10
        items = [{'ROWID': x} for x in range(0, rows)]
        sync_container_mock = mock.Mock()

        for node_id in (0, 1):
            self.s3_sync.sync_row = mock.Mock()
            self.s3_sync.sync_row.side_effect = RuntimeError('oops')

            with self.assertRaises(RuntimeError):
                self.s3_sync.sync_items(sync_container_mock, items, 2, node_id)

            sync_calls = filter(lambda x: x % 2 == node_id,
                                range(0, rows))
            expected = [mock.call(sync_container_mock, {'ROWID': row_id})
                        for row_id in sync_calls]
            self.assertEqual(expected,
                             self.s3_sync.sync_row.call_args_list)

    def test_verify_items_errors(self):
        rows = 10
        items = [{'ROWID': x} for x in range(0, rows)]
        sync_container_mock = mock.Mock()

        for node_id in (0, 1):
            self.s3_sync.sync_row = mock.Mock()

            # only fail the verify calls
            def fail_verify(container, row):
                if row['ROWID'] % 2 != node_id:
                    raise RuntimeError('oops')
                return

            self.s3_sync.sync_row.side_effect = fail_verify

            with self.assertRaises(RuntimeError):
                self.s3_sync.sync_items(sync_container_mock, items, 2, node_id)

            sync_calls = filter(lambda x: x % 2 == node_id,
                                range(0, rows))
            verify_calls = filter(lambda x: x % 2 != node_id,
                                  range(0, rows))
            expected = [mock.call(sync_container_mock, {'ROWID': row_id})
                        for row_id in sync_calls]
            expected += [mock.call(sync_container_mock, {'ROWID': row_id})
                             for row_id in verify_calls]
            self.assertEqual(expected,
                             self.s3_sync.sync_row.call_args_list)

    def test_exit_if_no_containers(self):
        with self.assertRaises(SystemExit):
            self.s3_sync.run_once()

    @mock.patch('s3_sync.s3_sync.traceback.format_exc')
    @mock.patch('s3_sync.sync_container.InternalClient')
    def test_no_exception_on_failure(self, internal_client_mock, format_exc_mock):
        self.s3_sync.conf['containers'] = [
            {'account': 'foo',
             'container': 'bar',
             'aws_bucket': 'qux',
             'aws_identity': 'identity',
             'aws_secret': 'credential'
            },
            # missing parameters
            {'account': 'foo',
             'aws_bucket': 'qux'
            }
        ]

        self.s3_sync.logger = mock.Mock()
        format_exc_mock.return_value = 'traceback'

        self.s3_sync.sync_container = mock.Mock()
        self.s3_sync.sync_container.side_effect = RuntimeError('oops')
        self.s3_sync.run_once()

        self.assertEqual(1, len(self.s3_sync.sync_container.call_args_list))
        expected_logger_calls = [
            mock.call("Failed to sync foo/bar to qux: RuntimeError('oops',)"),
            mock.call('traceback'),
            mock.call("Failed to sync foo/N/A to qux: KeyError('container',)"),
            mock.call('traceback')
        ]
        self.assertEqual(expected_logger_calls,
                         self.s3_sync.logger.error.call_args_list)

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
