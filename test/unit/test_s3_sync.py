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

    @staticmethod
    def _create_test_rows(count, prefix, policy_index):
        return [{'ROWID': x,
                 'name': prefix + str(x),
                 'storage_policy_index': policy_index,
                 'deleted': False}
                for x in range(0, count)]

    def test_sync_items(self):
        total_rows = 20
        name_prefix = 'foo'
        policy_index = 1
        items = self._create_test_rows(total_rows, name_prefix, policy_index)

        for nodes in range(1, 7):
            for node_id in range(0, nodes):
                sync_container_mock = mock.Mock()
                sync_calls = filter(lambda x: x % nodes == node_id,
                                    range(0, total_rows))
                verify_calls = filter(lambda x: x not in sync_calls,
                                      range(0, total_rows))

                self.s3_sync.sync_items(sync_container_mock, items, nodes,
                                        node_id)
                expected = [mock.call(name_prefix + str(x), policy_index)
                            for x in sync_calls]
                expected += [mock.call(name_prefix + str(x), policy_index)
                             for x in verify_calls]
                self.assertEqual(
                    expected, sync_container_mock.upload_object.call_args_list)

    def test_sync_items_errors(self):
        rows = 10
        name_prefix = 'foo'
        policy_index = 1
        items = self._create_test_rows(rows, name_prefix, policy_index)

        for node_id in (0, 1):
            sync_container_mock = mock.Mock()
            sync_container_mock.upload_object.side_effect = RuntimeError(
                'oops')

            with self.assertRaises(RuntimeError):
                self.s3_sync.sync_items(sync_container_mock, items, 2, node_id)

            sync_calls = filter(lambda x: x % 2 == node_id,
                                range(0, rows))
            expected = [mock.call(name_prefix + str(row_id), policy_index)
                        for row_id in sync_calls]
            self.assertEqual(expected,
                             sync_container_mock.upload_object.call_args_list)

    def test_verify_items_errors(self):
        rows = 10
        name_prefix = 'foo'
        policy_index = 1
        items = self._create_test_rows(rows, name_prefix, policy_index)

        for node_id in (0, 1):
            sync_container_mock = mock.Mock()
            sync_container_mock.account = 'test account'
            sync_container_mock.container = 'test container'

            # only fail the verify calls
            def fail_upload(name, policy_index):
                row = int(name[len(name_prefix):])
                if row % 2 != node_id:
                    raise RuntimeError('oops')
                return

            sync_container_mock.upload_object.side_effect = fail_upload

            with self.assertRaises(RuntimeError):
                self.s3_sync.sync_items(sync_container_mock, items, 2, node_id)

            sync_calls = filter(lambda x: x % 2 == node_id,
                                range(0, rows))
            verify_calls = filter(lambda x: x % 2 != node_id,
                                  range(0, rows))
            expected = [mock.call(name_prefix + str(row_id), policy_index)
                        for row_id in sync_calls]
            expected += [mock.call(name_prefix + str(row_id), policy_index)
                         for row_id in verify_calls]
            self.assertEqual(expected,
                             sync_container_mock.upload_object.call_args_list)

    def test_exit_if_no_containers(self):
        with self.assertRaises(SystemExit):
            self.s3_sync.run_once()

    @mock.patch('s3_sync.s3_sync.traceback.format_exc')
    @mock.patch('s3_sync.sync_container.InternalClient')
    def test_no_exception_on_failure(self, internal_client_mock,
                                     format_exc_mock):
        self.s3_sync.containers = [
            {'account': 'foo',
             'container': 'bar',
             'aws_bucket': 'qux',
             'aws_identity': 'identity',
             'aws_secret': 'credential'},
            # missing parameters
            {'account': 'foo',
             'aws_bucket': 'qux'}
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
        self.s3_sync.containers = [
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
        expected_calls = [mock.call(self.conf['status_dir'], container,
                                    self.s3_sync.workers)
                          for container in self.s3_sync.containers]
        self.assertEquals(expected_calls, sync_container_mock.call_args_list)
