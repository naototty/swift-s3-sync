import mock
from botocore.exceptions import ClientError
from s3_sync import s3_sync, utils
import unittest


class TestS3Sync(unittest.TestCase):
    @mock.patch('s3_sync.s3_sync.boto3.session.Session')
    @mock.patch('s3_sync.s3_sync.InternalClient')
    @mock.patch('s3_sync.s3_sync.Ring')
    def setUp(self, mock_ring, mock_ic, mock_boto3):
        self.mock_ring = mock.Mock()
        self.mock_ic = mock.Mock()
        self.mock_boto3_session = mock.Mock()
        self.mock_boto3_client = mock.Mock()

        mock_ring.return_value = self.mock_ring
        mock_ic.return_value = self.mock_ic
        mock_boto3.return_value = self.mock_boto3_session
        self.mock_boto3_session.client.return_value = self.mock_boto3_client

        self.aws_bucket = 'bucket'
        self.scratch_space = 'scratch'
        self.s3_sync = s3_sync.S3Sync({
            'aws_bucket': self.aws_bucket,
            'scratch': self.scratch_space})

    def test_load_non_existent_meta(self):
        ret = self.s3_sync.load_sync_meta('foo', 'bar')
        self.assertEqual({}, ret)

    @mock.patch('s3_sync.s3_sync.open')
    @mock.patch('s3_sync.s3_sync.os.path.exists')
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

        account = 'foo'
        container = 'bar'
        mock_exists.return_value = True
        mock_open.return_value = MockMetaConf()

        meta = self.s3_sync.load_sync_meta(account, container)
        self.assertEqual(42, meta['last_row'])

        mock_exists.assert_called_with('%s/%s/%s' % (
            self.scratch_space, account, container))

    @mock.patch('s3_sync.s3_sync.FileWrapper')
    def test_upload_new_object(self, mock_file_wrapper):
        account = 'foo'
        container = 'bar'
        key = 'key'
        storage_policy = 42
        swift_req_headers = {'X-Backend-Storage-Policy-Index': storage_policy,
                             'X-Newest': True}

        wrapper = mock.Mock()
        wrapper.get_s3_headers.return_value = {}
        mock_file_wrapper.return_value = wrapper
        self.mock_boto3_client.head_object.side_effect = ClientError(
            {'Error': {'Code': 404}}, 'HEAD')

        self.s3_sync.upload_object(account, container,
                                   {'name': key,
                                    'storage_policy_index': storage_policy})

        mock_file_wrapper.assert_called_with(self.mock_ic, account, container,
                                             key, swift_req_headers)

        self.mock_boto3_client.put_object.assert_called_with(
            Bucket=self.aws_bucket,
            Key=utils.get_s3_name(account, container, key),
            Body=wrapper,
            Metadata={})

    def test_upload_changed_meta(self):
        account = 'foo'
        container = 'bar'
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

        self.s3_sync.upload_object(account, container,
                                   {'name': key,
                                    'storage_policy_index': storage_policy,
                                    'etag': '1234'})

        self.mock_boto3_client.copy_object.assert_called_with(
            CopySource={'Bucket': self.aws_bucket,
                        'Key': utils.get_s3_name(account, container, key)},
            MetadataDirective='REPLACE',
            Bucket=self.aws_bucket,
            Key=utils.get_s3_name(account, container, key),
            Metadata={'new': 'new', 'old': 'updated'})

    @mock.patch('s3_sync.s3_sync.FileWrapper')
    def test_upload_replace_object(self, mock_file_wrapper):
        account = 'foo'
        container = 'bar'
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

        self.s3_sync.upload_object(account, container,
                                   {'name': key,
                                    'storage_policy_index': storage_policy,
                                    'etag': '1234'})

        self.mock_boto3_client.put_object.assert_called_with(
            Bucket=self.aws_bucket,
            Key=utils.get_s3_name(account, container, key),
            Metadata={'new': 'new', 'old': 'updated'},
            Body=wrapper)

    def test_upload_same_object(self):
        account = 'foo'
        container = 'bar'
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

        self.s3_sync.upload_object(account, container,
                                   {'name': key,
                                    'storage_policy_index': storage_policy,
                                    'etag': '1234'})

        self.mock_boto3_client.copy_object.assert_not_called()
        self.mock_boto3_client.put_object.assert_not_called()
