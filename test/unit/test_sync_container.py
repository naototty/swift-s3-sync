import hashlib
import json
import mock
from botocore.exceptions import ClientError
from s3_sync import utils
from s3_sync.sync_container import SyncContainer
import unittest


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

    @mock.patch('s3_sync.sync_container.boto3.session.Session')
    @mock.patch('s3_sync.sync_container.InternalClient')
    def setUp(self, mock_ic, mock_boto3):
        self.mock_ic = mock.Mock()
        self.mock_boto3_session = mock.Mock()
        self.mock_boto3_client = mock.Mock()

        mock_ic.return_value = self.mock_ic
        mock_boto3.return_value = self.mock_boto3_session
        self.mock_boto3_session.client.return_value = self.mock_boto3_client

        self.aws_bucket = 'bucket'
        self.scratch_space = 'scratch'
        self.sync_container = SyncContainer(self.scratch_space,
                                            {'aws_bucket': self.aws_bucket,
                                             'aws_identity': 'identity',
                                             'aws_secret': 'credential',
                                             'account': 'account',
                                             'container': 'container'})

    def test_load_non_existent_meta(self):
        ret = self.sync_container.load_status('db-id')
        self.assertEqual(0, ret)

    @mock.patch('s3_sync.sync_container.open')
    @mock.patch('s3_sync.sync_container.os.path.exists')
    def test_load_upgrade_status(self, mock_exists, mock_open):
        mock_exists.return_value = True
        fake_status = dict(last_row=42)
        mock_open.return_value = self.MockMetaConf(fake_status)

        status = self.sync_container.load_status('db-id')
        self.assertEqual(fake_status['last_row'], status)

        mock_exists.assert_called_with('%s/%s/%s' % (
            self.scratch_space, self.sync_container.account,
            self.sync_container.container))

    @mock.patch('s3_sync.sync_container.open')
    @mock.patch('s3_sync.sync_container.os.path.exists')
    def test_load_status_new_bucket(self, mock_exists, mock_open):
        db_id = 'db-id-test'
        new_bucket = 'new-bucket'
        self.sync_container.aws_bucket = 'bucket'
        fake_status = {db_id: dict(last_row=42, aws_bucket=new_bucket)}

        mock_exists.return_value = True
        mock_open.return_value = self.MockMetaConf(fake_status)

        status = self.sync_container.load_status(db_id)
        self.assertEqual(0, status)

        mock_exists.assert_called_with('%s/%s/%s' % (
            self.scratch_space, self.sync_container.account,
            self.sync_container.container))

    @mock.patch('s3_sync.sync_container.open')
    @mock.patch('s3_sync.sync_container.os.path.exists')
    def test_load_status_new_db_id(self, mock_exists, mock_open):
        db_id = 'db-id-test'
        self.sync_container.aws_bucket = 'bucket'
        fake_status = {db_id: dict(last_row=42, aws_bucket='bucket')}

        mock_exists.return_value = True
        mock_open.return_value = self.MockMetaConf(fake_status)

        status = self.sync_container.load_status('other-db-id')
        self.assertEqual(0, status)

        mock_exists.assert_called_with('%s/%s/%s' % (
            self.scratch_space, self.sync_container.account,
            self.sync_container.container))

    @mock.patch('s3_sync.sync_container.open')
    @mock.patch('s3_sync.sync_container.os.path.exists')
    def test_load_status(self, mock_exists, mock_open):
        db_entries = [{'id': 'db-id-1', 'aws_bucket': 'bucket', 'last_row': 5},
                      {'id': 'db-id-2', 'aws_bucket': 'bucket', 'last_row': 7}]
        for entry in db_entries:
            self.sync_container.aws_bucket = entry['aws_bucket']
            fake_status = {entry['id']: dict(last_row=entry['last_row'],
                                             aws_bucket=entry['aws_bucket'])}

            mock_exists.return_value = True
            mock_open.return_value = self.MockMetaConf(fake_status)

            status = self.sync_container.load_status(entry['id'])
            self.assertEqual(entry['last_row'], status)

            mock_exists.assert_called_with('%s/%s/%s' % (
                self.scratch_space, self.sync_container.account,
                self.sync_container.container))

    @mock.patch('s3_sync.sync_container.open')
    def test_save_status(self, mock_open):
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

                self.sync_container.save_status(new_row, db_id)
                file_entries = fake_conf_file.fake_status
                for file_db_id, status in file_entries.items():
                    if file_db_id == db_id:
                        self.assertEqual(new_row, status['last_row'])
                    else:
                        self.assertEqual(db_entries[file_db_id]['last_row'],
                                         status['last_row'])

                self.assertEqual(
                    [mock.call('%s/%s' % (self.scratch_space,
                                          self.sync_container.account)),
                     mock.call('%s/%s/%s' % (self.scratch_space,
                                             self.sync_container.account,
                                             self.sync_container.container))],
                    mock_exists.call_args_list)

    @mock.patch('s3_sync.sync_container.open')
    @mock.patch('s3_sync.sync_container.os.path.exists')
    def test_save_no_prior_status(self, mock_exists, mock_open):
        def existence_check(path):
            if path == '%s/%s' % (self.scratch_space,
                                  self.sync_container.account):
                return True
            elif path == '%s/%s/%s' % (self.scratch_space,
                                       self.sync_container.account,
                                       self.sync_container.container):
                return False
            else:
                raise RuntimeError('Invalid path')

        self.sync_container.aws_bucket = 'bucket'
        fake_conf_file = self.MockMetaConf({})
        mock_exists.side_effect = existence_check
        mock_open.return_value = fake_conf_file

        self.sync_container.save_status(42, 'db-id')
        self.assertEqual(42, fake_conf_file.fake_status['db-id']['last_row'])
        self.assertEqual('bucket',
                         fake_conf_file.fake_status['db-id']['aws_bucket'])

        self.assertEqual(
            [mock.call('%s/%s' % (self.scratch_space,
                                  self.sync_container.account)),
             mock.call('%s/%s/%s' % (self.scratch_space,
                                     self.sync_container.account,
                                     self.sync_container.container))],
            mock_exists.call_args_list)

    @mock.patch('s3_sync.sync_container.open')
    @mock.patch('s3_sync.sync_container.os.path.exists')
    def test_save_status_new_bucket(self, mock_exists, mock_open):
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
                self.sync_container.save_status(new_row, db_id)
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
                                          self.sync_container.account)),
                     mock.call('%s/%s/%s' % (self.scratch_space,
                                             self.sync_container.account,
                                             self.sync_container.container))],
                    mock_exists.call_args_list)

    @mock.patch('s3_sync.sync_container.open')
    @mock.patch('s3_sync.sync_container.os.path.exists')
    def test_save_old_status(self, mock_exists, mock_open):
        pass

    @mock.patch('s3_sync.sync_container.FileWrapper')
    def test_upload_new_object(self, mock_file_wrapper):
        key = 'key'
        storage_policy = 42
        swift_req_headers = {'X-Backend-Storage-Policy-Index': storage_policy,
                             'X-Newest': True}

        wrapper = mock.Mock()
        wrapper.__len__ = lambda s: 0
        wrapper.get_s3_headers.return_value = {}
        mock_file_wrapper.return_value = wrapper
        self.mock_boto3_client.head_object.side_effect = ClientError(
            {'Error': {'Code': 404}}, 'HEAD')

        self.sync_container.upload_object(key, storage_policy)

        mock_file_wrapper.assert_called_with(self.mock_ic,
                                             self.sync_container.account,
                                             self.sync_container.container,
                                             key, swift_req_headers)

        self.mock_boto3_client.put_object.assert_called_with(
            Bucket=self.aws_bucket,
            Key=self.sync_container.get_s3_name(key),
            Body=wrapper,
            Metadata={},
            ContentLength=0)

    def test_upload_changed_meta(self):
        key = 'key'
        storage_policy = 42
        etag = '1234'
        swift_object_meta = {'x-object-meta-new': 'new',
                             'x-object-meta-old': 'updated',
                             'etag': etag}
        self.mock_ic.get_object_metadata.return_value = swift_object_meta
        self.mock_boto3_client.head_object.return_value = {
            'Metadata': {'old': 'old'},
            'ETag': '"%s"' % etag
        }

        self.sync_container.upload_object(key, storage_policy)

        self.mock_boto3_client.copy_object.assert_called_with(
            CopySource={'Bucket': self.aws_bucket,
                        'Key': self.sync_container.get_s3_name(key)},
            MetadataDirective='REPLACE',
            Bucket=self.aws_bucket,
            Key=self.sync_container.get_s3_name(key),
            Metadata={'new': 'new', 'old': 'updated'})

    @mock.patch('s3_sync.sync_container.FileWrapper')
    def test_upload_replace_object(self, mock_file_wrapper):
        key = 'key'
        storage_policy = 42
        swift_object_meta = {'x-object-meta-new': 'new',
                             'x-object-meta-old': 'updated',
                             'etag': 2}
        self.mock_ic.get_object_metadata.return_value = swift_object_meta
        self.mock_boto3_client.head_object.return_value = {
            'Metadata': {'old': 'old'},
            'ETag': 1
        }

        wrapper = mock.Mock()
        wrapper.get_s3_headers.return_value = utils.convert_to_s3_headers(
            swift_object_meta)
        wrapper.__len__ = lambda s: 42
        mock_file_wrapper.return_value = wrapper

        self.sync_container.upload_object(key, storage_policy)

        self.mock_boto3_client.put_object.assert_called_with(
            Bucket=self.aws_bucket,
            Key=self.sync_container.get_s3_name(key),
            Metadata={'new': 'new', 'old': 'updated'},
            Body=wrapper,
            ContentLength=42)

    def test_upload_same_object(self):
        key = 'key'
        storage_policy = 42
        etag = '1234'
        swift_object_meta = {'x-object-meta-foo': 'foo',
                             'etag': etag}
        self.mock_ic.get_object_metadata.return_value = swift_object_meta
        self.mock_boto3_client.head_object.return_value = {
            'Metadata': {'foo': 'foo'},
            'ETag': '"%s"' % etag
        }

        self.sync_container.upload_object(key, storage_policy)

        self.mock_boto3_client.copy_object.assert_not_called()
        self.mock_boto3_client.put_object.assert_not_called()

    def test_delete_object(self):
        key = 'key'

        self.sync_container.delete_object(key)
        self.mock_boto3_client.delete_object.assert_called_with(
            Bucket=self.aws_bucket,
            Key=self.sync_container.get_s3_name(key))

    @mock.patch('s3_sync.sync_container.SyncContainer._init_s3')
    @mock.patch('s3_sync.sync_container.InternalClient')
    def test_s3_name(self, mock_ic, mock_init_s3):
        test_data = [('AUTH_test', 'container', 'key'),
                     ('swift', 'stuff', 'my/key')]
        for account, container, key in test_data:
            sync = SyncContainer('meta_dir', {'account': account,
                                              'container': container})
            # Verify that the get_s3_name function is deterministic
            self.assertEqual(sync.get_s3_name(key),
                             sync.get_s3_name(key))
            s3_key = sync.get_s3_name(key)
            prefix, remainder = s3_key.split('/', 1)
            self.assertEqual(remainder, '/'.join((account, container, key)))
            self.assertTrue(len(prefix) and
                            len(prefix) <= SyncContainer.PREFIX_LEN)
            # Check the prefix computation
            md5_prefix = hashlib.md5('%s/%s' % (account, container))
            expected_prefix = hex(long(md5_prefix.hexdigest(), 16) %
                                  SyncContainer.PREFIX_SPACE)[2:-1]
            self.assertEqual(expected_prefix, prefix)
