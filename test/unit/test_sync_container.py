import json
import mock
import time
import unittest

from container_crawler import RetryError
from s3_sync.sync_container import SyncContainer
from swift.common.utils import decode_timestamps, Timestamp


class TestSyncContainer(unittest.TestCase):
    class MockMetaConf(object):
        def __init__(self, fake_status):
            self.fake_status = fake_status
            self.write_buf = ''

        def read(self, size=-1):
            if size != -1:
                raise RuntimeError()
            return json.dumps(self.fake_status)

        def write(self, data):
            # Only support write at the beginning
            self.write_buf += data

        def truncate(self, size=None):
            if size:
                raise RuntimeError('Not supported')
            self.fake_status = json.loads(self.write_buf)
            self.write_buf = ''

        def __exit__(self, *args):
            if self.write_buf:
                self.fake_status = json.loads(self.write_buf)
                self.write_buf = ''

        def __enter__(self):
            return self

        def seek(self, offset, flags=None):
            if offset != 0:
                raise RuntimeError

    @mock.patch('s3_sync.sync_s3.boto3.session.Session')
    def setUp(self, mock_boto3):
        self.mock_boto3_session = mock.Mock()
        self.mock_boto3_client = mock.Mock()

        mock_boto3.return_value = self.mock_boto3_session
        self.mock_boto3_session.client.return_value = self.mock_boto3_client

        self.aws_bucket = 'bucket'
        self.scratch_space = 'scratch'
        self.sync_container = SyncContainer(
            self.scratch_space,
            {'storage_location': 'some-s3',
             'account': 'account',
             'container': 'container'},
            {'storage_locations': {'some-s3': {
                'aws_bucket': self.aws_bucket,
                'aws_identity': 'identity',
                'aws_secret': 'credential'}}})

    def test_load_non_existent_meta(self):
        ret = self.sync_container.get_last_row('db-id')
        self.assertEqual(0, ret)

    @mock.patch('__builtin__.open')
    @mock.patch('s3_sync.sync_container.os.path.exists')
    def test_load_upgrade_status(self, mock_exists, mock_open):
        mock_exists.return_value = True
        fake_status = dict(last_row=42)
        mock_open.return_value = self.MockMetaConf(fake_status)

        status = self.sync_container.get_last_row('db-id')
        self.assertEqual(fake_status['last_row'], status)

        mock_exists.assert_called_with('%s/%s/%s' % (
            self.scratch_space, self.sync_container._account,
            self.sync_container._container))

    @mock.patch('__builtin__.open')
    @mock.patch('s3_sync.sync_container.os.path.exists')
    def test_last_row_new_bucket(self, mock_exists, mock_open):
        db_id = 'db-id-test'
        new_bucket = 'new-bucket'
        self.sync_container.aws_bucket = 'bucket'
        fake_status = {db_id: dict(last_row=42, aws_bucket=new_bucket)}

        mock_exists.return_value = True
        mock_open.return_value = self.MockMetaConf(fake_status)

        status = self.sync_container.get_last_row(db_id)
        self.assertEqual(0, status)

        mock_exists.assert_called_with('%s/%s/%s' % (
            self.scratch_space, self.sync_container._account,
            self.sync_container._container))

    @mock.patch('__builtin__.open')
    @mock.patch('s3_sync.sync_container.os.path.exists')
    def test_last_row_new_db_id(self, mock_exists, mock_open):
        db_id = 'db-id-test'
        self.sync_container.aws_bucket = 'bucket'
        fake_status = {db_id: dict(last_row=42, aws_bucket='bucket')}

        mock_exists.return_value = True
        mock_open.return_value = self.MockMetaConf(fake_status)

        status = self.sync_container.get_last_row('other-db-id')
        self.assertEqual(0, status)

        mock_exists.assert_called_with('%s/%s/%s' % (
            self.scratch_space, self.sync_container._account,
            self.sync_container._container))

    @mock.patch('__builtin__.open')
    @mock.patch('s3_sync.sync_container.os.path.exists')
    def test_last_row(self, mock_exists, mock_open):
        db_entries = [{'id': 'db-id-1', 'aws_bucket': 'bucket', 'last_row': 5},
                      {'id': 'db-id-2', 'aws_bucket': 'bucket', 'last_row': 7}]
        for entry in db_entries:
            self.sync_container.aws_bucket = entry['aws_bucket']
            fake_status = {entry['id']: dict(last_row=entry['last_row'],
                                             aws_bucket=entry['aws_bucket'])}

            mock_exists.return_value = True
            mock_open.return_value = self.MockMetaConf(fake_status)

            status = self.sync_container.get_last_row(entry['id'])
            self.assertEqual(entry['last_row'], status)

            mock_exists.assert_called_with('%s/%s/%s' % (
                self.scratch_space, self.sync_container._account,
                self.sync_container._container))

    @mock.patch('__builtin__.open')
    def test_save_last_row(self, mock_open):
        db_entries = {'db-id-1': {'aws_bucket': 'bucket', 'last_row': 5},
                      'db-id-2': {'aws_bucket': 'bucket', 'last_row': 7}}
        new_row = 42
        for db_id, entry in db_entries.items():
            self.sync_container.aws_bucket = entry['aws_bucket']
            fake_conf_file = self.MockMetaConf(db_entries)
            mock_open.return_value = fake_conf_file

            with mock.patch('s3_sync.sync_container.os.path.exists')\
                    as mock_exists:
                mock_exists.return_value = True

                self.sync_container.save_last_row(new_row, db_id)
                file_entries = fake_conf_file.fake_status
                for file_db_id, status in file_entries.items():
                    if file_db_id == db_id:
                        self.assertEqual(new_row, status['last_row'])
                    else:
                        self.assertEqual(db_entries[file_db_id]['last_row'],
                                         status['last_row'])

                self.assertEqual(
                    [mock.call('%s/%s' % (self.scratch_space,
                                          self.sync_container._account)),
                     mock.call('%s/%s/%s' % (self.scratch_space,
                                             self.sync_container._account,
                                             self.sync_container._container))],
                    mock_exists.call_args_list)

    @mock.patch('__builtin__.open')
    @mock.patch('s3_sync.sync_container.os.path.exists')
    def test_save_no_prior_status(self, mock_exists, mock_open):
        def existence_check(path):
            if path == '%s/%s' % (self.scratch_space,
                                  self.sync_container._account):
                return True
            elif path == '%s/%s/%s' % (self.scratch_space,
                                       self.sync_container._account,
                                       self.sync_container._container):
                return False
            else:
                raise RuntimeError('Invalid path')

        self.sync_container.aws_bucket = 'bucket'
        fake_conf_file = self.MockMetaConf({})
        mock_exists.side_effect = existence_check
        mock_open.return_value = fake_conf_file

        self.sync_container.save_last_row(42, 'db-id')
        self.assertEqual(42, fake_conf_file.fake_status['db-id']['last_row'])
        self.assertEqual('bucket',
                         fake_conf_file.fake_status['db-id']['aws_bucket'])

        self.assertEqual(
            [mock.call('%s/%s' % (self.scratch_space,
                                  self.sync_container._account)),
             mock.call('%s/%s/%s' % (self.scratch_space,
                                     self.sync_container._account,
                                     self.sync_container._container))],
            mock_exists.call_args_list)

    @mock.patch('__builtin__.open')
    @mock.patch('s3_sync.sync_container.os.path.exists')
    def test_save_last_row_new_bucket(self, mock_exists, mock_open):
        db_entries = {'db-id-1': {'aws_bucket': 'bucket', 'last_row': 5},
                      'db-id-2': {'aws_bucket': 'old-bucket', 'last_row': 7}}
        new_row = 42
        for db_id, entry in db_entries.items():
            self.sync_container.aws_bucket = 'bucket'
            fake_conf_file = self.MockMetaConf(db_entries)
            mock_open.return_value = fake_conf_file

            with mock.patch('s3_sync.sync_container.os.path.exists')\
                    as mock_exists:
                mock_exists.return_value = True
                self.sync_container.save_last_row(new_row, db_id)
                file_entries = fake_conf_file.fake_status
                for file_db_id, status in file_entries.items():
                    if file_db_id == db_id:
                        self.assertEqual(new_row, status['last_row'])
                        self.assertEqual('bucket', status['aws_bucket'])
                    else:
                        self.assertEqual(db_entries[file_db_id]['last_row'],
                                         status['last_row'])
                        self.assertEqual(db_entries[file_db_id]['aws_bucket'],
                                         status['aws_bucket'])

                self.assertEqual(
                    [mock.call('%s/%s' % (self.scratch_space,
                                          self.sync_container._account)),
                     mock.call('%s/%s/%s' % (self.scratch_space,
                                             self.sync_container._account,
                                             self.sync_container._container))],
                    mock_exists.call_args_list)

    def test_s3_provider(self):
        sync_conf = {'account': 'account',
                     'container': 'container',
                     'storage_location': 'a-bucket'}
        global_conf = {'storage_locations': {'a-bucket': {
            'aws_bucket': self.aws_bucket,
            'aws_identity': 'identity',
            'aws_secret': 'credential'}}}
        global_conf_explicit = {'storage_locations': {'a-bucket': {
            'aws_bucket': self.aws_bucket,
            'aws_identity': 'identity',
            'aws_secret': 'credential',
            'protocol': 's3'}}}

        test_settings = [(sync_conf, global_conf),
                         (sync_conf, global_conf_explicit)]

        for sconf, gconf in test_settings:
            with mock.patch('s3_sync.sync_container.SyncS3') as mock_sync_s3:
                SyncContainer(self.scratch_space, sconf, gconf, max_conns=1)
                expected_conf = sconf.copy()
                expected_conf.update(gconf['storage_locations']['a-bucket'])
                mock_sync_s3.assert_called_once_with(expected_conf, 1)

    def test_swift_provider(self):
        settings = {'account': 'account',
                    'container': 'container',
                    'protocol': 'swift',
                    'storage_location': 'some-account'}
        global_conf = {'storage_locations': {'some-account': {
            'aws_bucket': self.aws_bucket,
            'aws_identity': 'identity',
            'aws_secret': 'credential'}}}
        with mock.patch('s3_sync.sync_container.SyncSwift') as mock_sync_swift:
            SyncContainer(self.scratch_space, settings, global_conf,
                          max_conns=1)
            expected_conf = settings.copy()
            expected_conf.update(
                global_conf['storage_locations']['some-account'])
            mock_sync_swift.assert_called_once_with(expected_conf, 1)

    def test_unknown_provider(self):
        settings = {'account': 'account',
                    'container': 'container',
                    'storage_location': 'mysterymeat'}
        global_conf = {'storage_locations': {'mysterymeat': {
            'aws_bucket': self.aws_bucket,
            'aws_identity': 'identity',
            'aws_secret': 'credential',
            'protocol': 'foo'}}}

        with self.assertRaises(NotImplementedError):
            SyncContainer(self.scratch_space, settings, global_conf, 1)

    @mock.patch('s3_sync.sync_s3.boto3.session.Session')
    def test_retry_copy_after(self, session_mock):
        settings = {
            'storage_location': 'a-bucket',
            'account': 'account',
            'container': 'container',
            'copy_after': 3600}
        global_conf = {'storage_locations': {'a-bucket': {
            'aws_bucket': self.aws_bucket,
            'aws_identity': 'identity',
            'aws_secret': 'credential'}}}
        with self.assertRaises(RetryError):
            sync = SyncContainer(self.scratch_space, settings, global_conf)
            sync.handle({'deleted': 0, 'created_at': str(time.time())}, None)

        current = time.time()
        with mock.patch('s3_sync.sync_container.time') as time_mock:
            time_mock.time.return_value = current + settings['copy_after'] + 1
            sync = SyncContainer(self.scratch_space, settings, global_conf)
            sync.provider = mock.Mock()
            sync.handle({'deleted': 0,
                         'created_at': str(time.time()),
                         'name': 'foo',
                         'storage_policy_index': 99}, None)
            sync.provider.upload_object.assert_called_once_with(
                'foo', 99, None)

    @mock.patch('s3_sync.sync_s3.boto3.session.Session')
    def test_retain_copy(self, session_mock):
        settings = {
            'storage_location': 'a-bucket',
            'account': 'account',
            'container': 'container',
            'retain_local': False}
        global_conf = {'storage_locations': {'a-bucket': {
            'aws_bucket': self.aws_bucket,
            'aws_identity': 'identity',
            'aws_secret': 'credential'}}}

        sync = SyncContainer(self.scratch_space, settings, global_conf)
        sync.provider = mock.Mock()
        swift_client = mock.Mock()
        row = {'deleted': 0,
               'created_at': str(time.time() - 5),
               'name': 'foo',
               'storage_policy_index': 99}
        sync.handle(row, swift_client)

        _, _, swift_ts = decode_timestamps(row['created_at'])
        swift_ts.offset += 1

        sync.provider.upload_object.assert_called_once_with(
            row['name'], 99, swift_client)
        swift_client.delete_object.assert_called_once_with(
            settings['account'], settings['container'], row['name'],
            headers={'X-Timestamp': Timestamp(swift_ts).internal})

    @mock.patch('s3_sync.sync_s3.boto3.session.Session')
    def test_no_propagate_delete(self, session_mock):
        settings = {
            'storage_location': 'a-bucket',
            'account': 'account',
            'container': 'container',
            'propagate_delete': False}
        global_conf = {'storage_locations': {'a-bucket': {
            'aws_bucket': self.aws_bucket,
            'aws_identity': 'identity',
            'aws_secret': 'credential'}}}

        sync = SyncContainer(self.scratch_space, settings, global_conf)
        sync.provider = mock.Mock()
        row = {'deleted': 1, 'name': 'tombstone'}
        sync.handle(row, None)

        # Make sure we do nothing with this row
        self.assertEqual([], sync.provider.mock_calls)

    @mock.patch('s3_sync.sync_s3.boto3.session.Session')
    def test_propagate_delete(self, session_mock):
        settings = {
            'storage_location': 'a-bucket',
            'account': 'account',
            'container': 'container',
            'propagate_delete': True}
        global_conf = {'storage_locations': {'a-bucket': {
            'aws_bucket': self.aws_bucket,
            'aws_identity': 'identity',
            'aws_secret': 'credential'}}}

        sync = SyncContainer(self.scratch_space, settings, global_conf)
        sync.provider = mock.Mock()
        row = {'deleted': 1, 'name': 'tombstone'}
        sync.handle(row, None)

        # Make sure that we do not make any additional calls
        self.assertEqual([mock.call.delete_object(row['name'], None)],
                         sync.provider.mock_calls)
