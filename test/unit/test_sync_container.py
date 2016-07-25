import hashlib
import mock
from botocore.exceptions import ClientError
from s3_sync import utils
from s3_sync.sync_container import SyncContainer
import unittest


class TestSyncContainer(unittest.TestCase):
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
        ret = self.sync_container.load_status()
        self.assertEqual({}, ret)

    @mock.patch('s3_sync.sync_container.open')
    @mock.patch('s3_sync.sync_container.os.path.exists')
    def test_load_existing_meta(self, mock_exists, mock_open):
        class MockMetaConf(object):
            def read(self, size=-1):
                if size != -1:
                    raise RuntimeError()
                return '{ "last_row": 42 }'

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return

        mock_exists.return_value = True
        mock_open.return_value = MockMetaConf()

        meta = self.sync_container.load_status()
        self.assertEqual(42, meta['last_row'])

        mock_exists.assert_called_with('%s/%s/%s' % (
            self.scratch_space, self.sync_container.account,
            self.sync_container.container))

    @mock.patch('s3_sync.sync_container.FileWrapper')
    def test_upload_new_object(self, mock_file_wrapper):
        key = 'key'
        storage_policy = 42
        swift_req_headers = {'X-Backend-Storage-Policy-Index': storage_policy,
                             'X-Newest': True}

        wrapper = mock.Mock()
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
            Metadata={})

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
            'ETag': etag
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
        mock_file_wrapper.return_value = wrapper

        self.sync_container.upload_object(key, storage_policy)

        self.mock_boto3_client.put_object.assert_called_with(
            Bucket=self.aws_bucket,
            Key=self.sync_container.get_s3_name(key),
            Metadata={'new': 'new', 'old': 'updated'},
            Body=wrapper)

    def test_upload_same_object(self):
        key = 'key'
        storage_policy = 42
        etag = '1234'
        swift_object_meta = {'x-object-meta-foo': 'foo',
                             'etag': etag}
        self.mock_ic.get_object_metadata.return_value = swift_object_meta
        self.mock_boto3_client.head_object.return_value = {
            'Metadata': {'foo': 'foo'},
            'ETag': etag
        }

        self.sync_container.upload_object(key, storage_policy)

        self.mock_boto3_client.copy_object.assert_not_called()
        self.mock_boto3_client.put_object.assert_not_called()

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
