# -*- coding: UTF-8 -*-

from cStringIO import StringIO
import datetime
import hashlib
import json
import mock
import boto3
from botocore.exceptions import ClientError
from botocore.vendored.requests.exceptions import RequestException
from swift.common import swob
from utils import FakeStream
from s3_sync import utils
from s3_sync.sync_s3 import SyncS3
import unittest


class TestSyncS3(unittest.TestCase):
    @mock.patch('s3_sync.sync_s3.boto3.session.Session')
    def setUp(self, mock_boto3):
        self.mock_boto3_session = mock.Mock()
        self.mock_boto3_client = mock.Mock()

        mock_boto3.return_value = self.mock_boto3_session
        self.mock_boto3_session.client.return_value = self.mock_boto3_client

        self.aws_bucket = 'bucket'
        self.scratch_space = 'scratch'
        self.sync_s3 = SyncS3({'aws_bucket': self.aws_bucket,
                               'aws_identity': 'identity',
                               'aws_secret': 'credential',
                               'account': 'account',
                               'container': 'container'})

    @mock.patch('s3_sync.sync_s3.FileWrapper')
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
        self.sync_s3.check_slo = mock.Mock()
        self.sync_s3.check_slo.return_value = False
        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = {}

        self.sync_s3.upload_object(key, storage_policy, mock_ic)

        mock_file_wrapper.assert_called_with(mock_ic,
                                             self.sync_s3.account,
                                             self.sync_s3.container,
                                             key, swift_req_headers)

        self.mock_boto3_client.put_object.assert_called_with(
            Bucket=self.aws_bucket,
            Key=self.sync_s3.get_s3_name(key),
            Body=wrapper,
            Metadata={},
            ContentLength=0,
            ServerSideEncryption='AES256')

    @mock.patch('s3_sync.sync_s3.FileWrapper')
    def test_upload_object_without_encryption(self, mock_file_wrapper):
        key = 'key'
        storage_policy = 42
        swift_req_headers = {'X-Backend-Storage-Policy-Index': storage_policy,
                             'X-Newest': True}

        self.sync_s3.endpoint = 'http://127.0.0.1:8080'

        wrapper = mock.Mock()
        wrapper.__len__ = lambda s: 0
        wrapper.get_s3_headers.return_value = {}
        mock_file_wrapper.return_value = wrapper
        self.mock_boto3_client.head_object.side_effect = ClientError(
            {'Error': {'Code': 404}}, 'HEAD')
        self.sync_s3.check_slo = mock.Mock()
        self.sync_s3.check_slo.return_value = False
        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = {}

        self.sync_s3.upload_object(key, storage_policy, mock_ic)

        mock_file_wrapper.assert_called_with(mock_ic,
                                             self.sync_s3.account,
                                             self.sync_s3.container,
                                             key, swift_req_headers)

        self.mock_boto3_client.put_object.assert_called_with(
            Bucket=self.aws_bucket,
            Key=self.sync_s3.get_s3_name(key),
            Body=wrapper,
            Metadata={},
            ContentLength=0)

    @mock.patch('s3_sync.sync_s3.FileWrapper')
    def test_google_upload_encryption(self, mock_file_wrapper):
        key = 'key'
        storage_policy = 42
        swift_req_headers = {'X-Backend-Storage-Policy-Index': storage_policy,
                             'X-Newest': True}

        self.sync_s3.endpoint = 'https://storage.googleapis.com'

        wrapper = mock.Mock()
        wrapper.__len__ = lambda s: 0
        wrapper.get_s3_headers.return_value = {}
        mock_file_wrapper.return_value = wrapper
        self.mock_boto3_client.head_object.side_effect = ClientError(
            {'Error': {'Code': 404}}, 'HEAD')
        self.sync_s3.check_slo = mock.Mock()
        self.sync_s3.check_slo.return_value = False
        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = {}

        self.sync_s3.upload_object(key, storage_policy, mock_ic)

        mock_file_wrapper.assert_called_with(mock_ic,
                                             self.sync_s3.account,
                                             self.sync_s3.container,
                                             key, swift_req_headers)

        self.mock_boto3_client.put_object.assert_called_with(
            Bucket=self.aws_bucket,
            Key=self.sync_s3.get_s3_name(key),
            Body=wrapper,
            Metadata={},
            ContentLength=0)

    @mock.patch('s3_sync.sync_s3.boto3.session.Session')
    def test_encryption_option(self, mock_session):
        sync_s3 = SyncS3({'aws_bucket': 'bucket',
                          'aws_identity': 'id',
                          'aws_secret': 'key',
                          'account': 'account',
                          'container': 'container',
                          'encryption': True})
        self.assertTrue(sync_s3.encryption)

    @mock.patch('s3_sync.sync_s3.FileWrapper')
    def test_upload_unicode_object_name(self, mock_file_wrapper):
        key = 'monkey-\xf0\x9f\x90\xb5'
        storage_policy = 42
        swift_req_headers = {'X-Backend-Storage-Policy-Index': storage_policy,
                             'X-Newest': True}

        wrapper = mock.Mock()
        wrapper.__len__ = lambda s: 0
        wrapper.get_s3_headers.return_value = {}
        mock_file_wrapper.return_value = wrapper
        self.mock_boto3_client.head_object.side_effect = ClientError(
            {'Error': {'Code': 404}}, 'HEAD')
        self.sync_s3.check_slo = mock.Mock()
        self.sync_s3.check_slo.return_value = False
        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = {}

        self.sync_s3.upload_object(key, storage_policy, mock_ic)

        mock_file_wrapper.assert_called_with(mock_ic,
                                             self.sync_s3.account,
                                             self.sync_s3.container,
                                             key, swift_req_headers)

        self.mock_boto3_client.put_object.assert_called_with(
            Bucket=self.aws_bucket,
            Key=u"356b9d/account/container/" + key.decode('utf-8'),
            Body=wrapper,
            Metadata={},
            ContentLength=0,
            ServerSideEncryption='AES256')

    def test_upload_changed_meta(self):
        key = 'key'
        storage_policy = 42
        etag = '1234'
        swift_object_meta = {'x-object-meta-new': 'new',
                             'x-object-meta-old': 'updated',
                             'etag': etag}
        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = swift_object_meta
        self.mock_boto3_client.head_object.return_value = {
            'Metadata': {'old': 'old'},
            'ETag': '"%s"' % etag
        }

        self.sync_s3.upload_object(key, storage_policy, mock_ic)

        self.mock_boto3_client.copy_object.assert_called_with(
            CopySource={'Bucket': self.aws_bucket,
                        'Key': self.sync_s3.get_s3_name(key)},
            MetadataDirective='REPLACE',
            Bucket=self.aws_bucket,
            Key=self.sync_s3.get_s3_name(key),
            Metadata={'new': 'new', 'old': 'updated'},
            ServerSideEncryption='AES256')

    def test_upload_changed_meta_no_encryption(self):
        key = 'key'
        storage_policy = 42
        etag = '1234'
        swift_object_meta = {'x-object-meta-new': 'new',
                             'x-object-meta-old': 'updated',
                             'etag': etag}
        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = swift_object_meta
        self.mock_boto3_client.head_object.return_value = {
            'Metadata': {'old': 'old'},
            'ETag': '"%s"' % etag
        }
        self.sync_s3.endpoint = 'http://127.0.0.1:8080'

        self.sync_s3.upload_object(key, storage_policy, mock_ic)

        self.mock_boto3_client.copy_object.assert_called_with(
            CopySource={'Bucket': self.aws_bucket,
                        'Key': self.sync_s3.get_s3_name(key)},
            MetadataDirective='REPLACE',
            Bucket=self.aws_bucket,
            Key=self.sync_s3.get_s3_name(key),
            Metadata={'new': 'new', 'old': 'updated'})

    def test_upload_changed_meta_google_encryption(self):
        key = 'key'
        storage_policy = 42
        etag = '1234'
        swift_object_meta = {'x-object-meta-new': 'new',
                             'x-object-meta-old': 'updated',
                             'etag': etag}
        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = swift_object_meta
        self.mock_boto3_client.head_object.return_value = {
            'Metadata': {'old': 'old'},
            'ETag': '"%s"' % etag
        }
        self.sync_s3.endpoint = 'https://storage.googleapis.com'

        self.sync_s3.upload_object(key, storage_policy, mock_ic)

        self.mock_boto3_client.copy_object.assert_called_with(
            CopySource={'Bucket': self.aws_bucket,
                        'Key': self.sync_s3.get_s3_name(key)},
            MetadataDirective='REPLACE',
            Bucket=self.aws_bucket,
            Key=self.sync_s3.get_s3_name(key),
            Metadata={'new': 'new', 'old': 'updated'})

    @mock.patch('s3_sync.sync_s3.FileWrapper')
    def test_upload_changed_meta_glacier(self, mock_file_wrapper):
        key = 'key'
        storage_policy = 42
        swift_req_headers = {'X-Backend-Storage-Policy-Index': storage_policy,
                             'X-Newest': True}
        etag = '1234'
        swift_object_meta = {'x-object-meta-new': 'new',
                             'x-object-meta-old': 'updated',
                             'etag': etag}
        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = swift_object_meta
        self.mock_boto3_client.head_object.return_value = {
            'Metadata': {'old': 'old'},
            'ETag': '"%s"' % etag,
            'StorageClass': 'GLACIER'
        }

        wrapper = mock.Mock()
        wrapper.__len__ = lambda s: 0
        wrapper.get_s3_headers.return_value = {'new': 'new', 'old': 'updated'}
        mock_file_wrapper.return_value = wrapper

        self.sync_s3.upload_object(key, storage_policy, mock_ic)

        mock_file_wrapper.assert_called_with(mock_ic,
                                             self.sync_s3.account,
                                             self.sync_s3.container,
                                             key, swift_req_headers)

        self.mock_boto3_client.put_object.assert_called_with(
            Bucket=self.aws_bucket,
            Key=self.sync_s3.get_s3_name(key),
            Metadata={'new': 'new', 'old': 'updated'},
            Body=wrapper,
            ContentLength=0,
            ServerSideEncryption='AES256')

    @mock.patch('s3_sync.sync_s3.FileWrapper')
    def test_upload_replace_object(self, mock_file_wrapper):
        key = 'key'
        storage_policy = 42
        swift_object_meta = {'x-object-meta-new': 'new',
                             'x-object-meta-old': 'updated',
                             'etag': 2}
        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = swift_object_meta
        self.mock_boto3_client.head_object.return_value = {
            'Metadata': {'old': 'old'},
            'ETag': 1
        }

        wrapper = mock.Mock()
        wrapper.get_s3_headers.return_value = utils.convert_to_s3_headers(
            swift_object_meta)
        wrapper.__len__ = lambda s: 42
        mock_file_wrapper.return_value = wrapper

        self.sync_s3.upload_object(key, storage_policy, mock_ic)

        self.mock_boto3_client.put_object.assert_called_with(
            Bucket=self.aws_bucket,
            Key=self.sync_s3.get_s3_name(key),
            Metadata={'new': 'new', 'old': 'updated'},
            Body=wrapper,
            ContentLength=42,
            ServerSideEncryption='AES256')

    def test_upload_same_object(self):
        key = 'key'
        storage_policy = 42
        etag = '1234'
        swift_object_meta = {'x-object-meta-foo': 'foo',
                             'etag': etag}
        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = swift_object_meta
        self.mock_boto3_client.head_object.return_value = {
            'Metadata': {'foo': 'foo'},
            'ETag': '"%s"' % etag
        }

        self.sync_s3.upload_object(key, storage_policy, mock_ic)

        self.mock_boto3_client.copy_object.assert_not_called()
        self.mock_boto3_client.put_object.assert_not_called()

    def test_delete_object(self):
        key = 'key'

        self.sync_s3.delete_object(key)
        self.mock_boto3_client.delete_object.assert_called_with(
            Bucket=self.aws_bucket,
            Key=self.sync_s3.get_s3_name(key))

    def test_delete_missing_object(self):
        key = 'key'
        error = ClientError({'Error': {'Code': 404}}, None)
        self.mock_boto3_client.delete_object.side_effect = error
        self.sync_s3.delete_object(key)
        self.mock_boto3_client.delete_object.assert_called_with(
            Bucket=self.aws_bucket,
            Key=self.sync_s3.get_s3_name(key))

    @mock.patch('s3_sync.sync_s3.boto3.session.Session')
    def test_s3_name(self, mock_session):
        test_data = [('AUTH_test', 'container', 'key'),
                     ('swift', 'stuff', 'my/key')]
        for account, container, key in test_data:
            sync = SyncS3({'account': account,
                           'container': container,
                           'aws_bucket': 'bucket',
                           'aws_identity': 'identity',
                           'aws_secret': 'secret'})
            # Verify that the get_s3_name function is deterministic
            self.assertEqual(sync.get_s3_name(key),
                             sync.get_s3_name(key))
            s3_key = sync.get_s3_name(key)
            prefix, remainder = s3_key.split('/', 1)
            self.assertEqual(remainder, '/'.join((account, container, key)))
            self.assertTrue(len(prefix) and
                            len(prefix) <= SyncS3.PREFIX_LEN)
            # Check the prefix computation
            md5_prefix = hashlib.md5('%s/%s' % (account, container))
            expected_prefix = hex(long(md5_prefix.hexdigest(), 16) %
                                  SyncS3.PREFIX_SPACE)[2:-1]
            self.assertEqual(expected_prefix, prefix)

    @mock.patch('s3_sync.sync_s3.boto3.session.Session')
    def test_signature_version(self, session_mock):
        config_class = 's3_sync.sync_s3.boto3.session.Config'
        with mock.patch(config_class) as conf_mock:
            SyncS3({'aws_bucket': self.aws_bucket,
                    'aws_identity': 'identity',
                    'aws_secret': 'credential',
                    'account': 'account',
                    'container': 'container'})
            conf_mock.assert_called_once_with(signature_version='s3v4',
                                              s3={'aws_chunked': True})

        with mock.patch(config_class) as conf_mock:
            SyncS3({'aws_bucket': self.aws_bucket,
                    'aws_identity': 'identity',
                    'aws_secret': 'credential',
                    'account': 'account',
                    'container': 'container',
                    'aws_endpoint': 'http://test.com'})
            conf_mock.assert_called_once_with(s3={'addressing_style': 'path'})

    def test_slo_upload(self):
        slo_key = 'slo-object'
        storage_policy = 42
        swift_req_headers = {'X-Backend-Storage-Policy-Index': storage_policy,
                             'X-Newest': True}
        manifest = [{'name': '/segment_container/slo-object/part1',
                     'hash': 'deadbeef'},
                    {'name': '/segment_container/slo-object/part2',
                     'hash': 'beefdead'}]

        self.mock_boto3_client.head_object.side_effect = ClientError(
            {'Error': {'Code': 404}}, 'HEAD')

        def get_metadata(account, container, key, headers):
            if key == slo_key:
                return {utils.SLO_HEADER: 'True'}
            raise RuntimeError('Unknown key')

        def get_object(account, container, key, headers):
            if key == slo_key:
                return (200, {utils.SLO_HEADER: 'True'},
                        FakeStream(content=json.dumps(manifest)))
            raise RuntimeError('Unknown key!')

        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.side_effect = get_metadata
        mock_ic.get_object.side_effect = get_object
        self.sync_s3._upload_slo = mock.Mock()

        self.sync_s3.upload_object(slo_key, storage_policy, mock_ic)

        self.mock_boto3_client.head_object.assert_called_once_with(
            Bucket=self.aws_bucket,
            Key=self.sync_s3.get_s3_name(slo_key))
        mock_ic.get_object_metadata.assert_called_once_with(
            'account', 'container', slo_key, headers=swift_req_headers)
        mock_ic.get_object.assert_called_once_with(
            'account', 'container', slo_key, headers=swift_req_headers)

    @mock.patch('s3_sync.sync_s3.boto3.session.Session')
    def test_google_set(self, mock_session):
        session = mock.Mock()
        mock_session.return_value = session

        client = mock.Mock()
        session.client.return_value = client

        sync = SyncS3({'aws_bucket': self.aws_bucket,
                       'aws_identity': 'identity',
                       'aws_secret': 'credential',
                       'account': 'account',
                       'container': 'container',
                       'aws_endpoint': SyncS3.GOOGLE_API})
        # Connections are instantiated on-demand, so we have to submit a
        # request to check the boto session and client arguments.
        sync.delete_object('object')
        session.client.assert_has_calls([
            mock.call('s3',
                      config=mock.ANY,
                      endpoint_url=SyncS3.GOOGLE_API),
            mock.call().meta.events.unregister(
                'before-call.s3.PutObject', mock.ANY),
            mock.call().meta.events.unregister(
                'before-call.s3.UploadPart', mock.ANY),
            mock.call().meta.events.unregister(
                'before-parameter-build.s3.ListObjects', mock.ANY)])
        self.assertEqual(True, sync._google())
        client.delete_object.assert_called_once_with(
            Bucket=self.aws_bucket, Key=sync.get_s3_name('object'))

    def test_user_agent(self):
        boto3_ua = boto3.session.Session()._session.user_agent()
        endpoint_user_agent = {
            SyncS3.GOOGLE_API: 'CloudSync/5.0 (GPN:SwiftStack) %s' % (
                boto3_ua),
            's3.amazonaws.com': None,
            None: None,
            'other.s3-clone.com': None
        }

        session_class = 's3_sync.sync_s3.boto3.session.Session'
        for endpoint, ua in endpoint_user_agent.items():
            settings = {'aws_bucket': self.aws_bucket,
                        'aws_identity': 'identity',
                        'aws_secret': 'credential',
                        'account': 'account',
                        'container': 'container',
                        'aws_endpoint': endpoint}
            with mock.patch(session_class) as mock_session:
                session = mock.Mock()
                session._session.user_agent.return_value = boto3_ua
                mock_session.return_value = session

                client = mock.Mock()
                session.client.return_value = client

                sync = SyncS3(settings)
                # Connections are only instantiated when there is an object to
                # process. delete() is the simplest call to mock, so do so here
                sync.delete_object('object')

                if endpoint == SyncS3.GOOGLE_API:
                    session.client.assert_has_calls(
                        [mock.call('s3',
                                   config=mock.ANY,
                                   endpoint_url=endpoint),
                         mock.call().meta.events.unregister(
                            'before-call.s3.PutObject', mock.ANY),
                         mock.call().meta.events.unregister(
                            'before-call.s3.UploadPart', mock.ANY),
                         mock.call().meta.events.unregister(
                            'before-parameter-build.s3.ListObjects',
                            mock.ANY)])
                else:
                    session.client.assert_has_calls(
                        [mock.call('s3',
                                   config=mock.ANY,
                                   endpoint_url=endpoint),
                         mock.call().meta.events.unregister(
                            'before-call.s3.PutObject', mock.ANY),
                         mock.call().meta.events.unregister(
                            'before-call.s3.UploadPart', mock.ANY)])
                called_config = session.client.call_args[1]['config']

                if endpoint and not endpoint.endswith('amazonaws.com'):
                    self.assertEqual({'addressing_style': 'path'},
                                     called_config.s3)
                else:
                    self.assertEqual('s3v4', called_config.signature_version)
                    self.assertEqual({'aws_chunked': True}, called_config.s3)
                self.assertEqual(endpoint == SyncS3.GOOGLE_API,
                                 sync._google())

                self.assertEqual(ua, called_config.user_agent)
                client.delete_object.assert_called_once_with(
                    Bucket=settings['aws_bucket'],
                    Key=sync.get_s3_name('object'))

    def test_google_slo_upload(self):
        self.sync_s3._google = lambda: True
        slo_key = 'slo-object'
        storage_policy = 42
        swift_req_headers = {'X-Backend-Storage-Policy-Index': storage_policy,
                             'X-Newest': True}
        manifest = [{'name': '/segment_container/slo-object/part1',
                     'hash': 'deadbeef',
                     'bytes': 5 * SyncS3.MB},
                    {'name': '/segment_container/slo-object/part2',
                     'hash': 'beefdead',
                     'bytes': 200}]

        self.mock_boto3_client.head_object.side_effect = ClientError(
            {'Error': {'Code': 404}}, 'HEAD')

        def get_metadata(account, container, key, headers):
            if key == slo_key:
                return {utils.SLO_HEADER: 'True'}
            raise RuntimeError('Unknown key')

        def get_object(account, container, key, headers):
            if key == slo_key:
                return (200, {'etag': 'swift-slo-etag'},
                        FakeStream(content=json.dumps(manifest)))
            raise RuntimeError('Unknown key!')

        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.side_effect = get_metadata
        mock_ic.get_object.side_effect = get_object

        self.sync_s3.upload_object(slo_key, storage_policy, mock_ic)

        self.mock_boto3_client.head_object.assert_called_once_with(
            Bucket=self.aws_bucket,
            Key=self.sync_s3.get_s3_name(slo_key))

        args, kwargs = self.mock_boto3_client.put_object.call_args
        self.assertEqual(self.aws_bucket, kwargs['Bucket'])
        self.assertEqual(
            self.sync_s3.get_s3_name(slo_key), kwargs['Key'])
        self.assertEqual(5 * SyncS3.MB + 200, kwargs['ContentLength'])
        self.assertEqual(
            {utils.SLO_ETAG_FIELD: 'swift-slo-etag'},
            kwargs['Metadata'])
        self.assertEqual(utils.SLOFileWrapper, type(kwargs['Body']))

        mock_ic.get_object_metadata.assert_called_once_with(
            'account', 'container', slo_key, headers=swift_req_headers)
        mock_ic.get_object.assert_called_once_with(
            'account', 'container', slo_key, headers=swift_req_headers)

    def test_google_slo_metadata_update(self):
        self.sync_s3._google = lambda: True
        self.sync_s3._is_amazon = lambda: False
        s3_key = self.sync_s3.get_s3_name('slo-object')
        slo_key = 'slo-object'
        storage_policy = 42

        manifest = [{'name': '/segment_container/slo-object/part1',
                     'hash': 'deadbeef',
                     'bytes': 5 * SyncS3.MB},
                    {'name': '/segment_container/slo-object/part2',
                     'hash': 'beefdead',
                     'bytes': 200}]

        self.mock_boto3_client.head_object.return_value = {
            'Metadata': {utils.SLO_ETAG_FIELD: 'swift-slo-etag'}}

        def get_metadata(account, container, key, headers):
            if key == slo_key:
                return {utils.SLO_HEADER: 'True',
                        'x-object-meta-foo': 'bar'}
            raise RuntimeError('Unknown key')

        def get_object(account, container, key, headers):
            if key == slo_key:
                return (200, {'etag': 'swift-slo-etag',
                              'x-object-meta-foo': 'bar',
                              utils.SLO_HEADER: 'True'},
                        FakeStream(content=json.dumps(manifest)))
            raise RuntimeError('Unknown key!')

        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.side_effect = get_metadata
        mock_ic.get_object.side_effect = get_object

        self.sync_s3.upload_object(slo_key, storage_policy, mock_ic)

        self.mock_boto3_client.copy_object.assert_called_with(
            CopySource={'Bucket': self.aws_bucket,
                        'Key': s3_key},
            MetadataDirective='REPLACE',
            Bucket=self.aws_bucket,
            Key=s3_key,
            Metadata={utils.SLO_ETAG_FIELD: 'swift-slo-etag',
                      'foo': 'bar',
                      utils.SLO_HEADER: 'True'})

    @mock.patch('s3_sync.sync_s3.FileWrapper')
    def test_internal_slo_upload(self, mock_file_wrapper):
        slo_key = 'slo-object'
        slo_meta = {'x-object-meta-foo': 'bar'}
        s3_key = self.sync_s3.get_s3_name(slo_key)
        storage_policy = 42
        swift_req_headers = {'X-Backend-Storage-Policy-Index': storage_policy,
                             'X-Newest': True}
        manifest = [{'name': '/segment_container/slo-object/part1',
                     'hash': 'deadbeef',
                     'bytes': 100},
                    {'name': '/segment_container/slo-object/part2',
                     'hash': 'beefdead',
                     'bytes': 200}]
        fake_body = FakeStream(5 * SyncS3.MB)

        self.mock_boto3_client.create_multipart_upload.return_value = {
            'UploadId': 'mpu-key-for-slo'}

        def upload_part(**kwargs):
            if kwargs['PartNumber'] == 1:
                return {'ETag': '"deadbeef"'}
            elif kwargs['PartNumber'] == 2:
                return {'ETag': '"beefdead"'}
            else:
                raise RuntimeError('Unknown call to upload part')

        self.mock_boto3_client.upload_part.side_effect = upload_part
        mock_file_wrapper.return_value = fake_body

        mock_ic = mock.Mock()
        self.sync_s3._upload_slo(manifest, slo_meta, s3_key, swift_req_headers,
                                 mock_ic)

        self.mock_boto3_client.create_multipart_upload.assert_called_once_with(
            Bucket=self.aws_bucket,
            Key=self.sync_s3.get_s3_name(slo_key),
            Metadata={'foo': 'bar'},
            ServerSideEncryption='AES256')
        self.mock_boto3_client.upload_part.assert_has_calls([
            mock.call(Bucket=self.aws_bucket,
                      Key=self.sync_s3.get_s3_name(slo_key),
                      PartNumber=1,
                      ContentLength=len(fake_body),
                      Body=mock.ANY,
                      UploadId='mpu-key-for-slo'),
            mock.call(Bucket=self.aws_bucket,
                      Key=self.sync_s3.get_s3_name(slo_key),
                      PartNumber=2,
                      ContentLength=len(fake_body),
                      Body=mock.ANY,
                      UploadId='mpu-key-for-slo')
        ])
        self.mock_boto3_client.complete_multipart_upload\
            .assert_called_once_with(
                Bucket=self.aws_bucket,
                Key=self.sync_s3.get_s3_name(slo_key),
                UploadId='mpu-key-for-slo',
                MultipartUpload={'Parts': [
                    {'PartNumber': 1, 'ETag': 'deadbeef'},
                    {'PartNumber': 2, 'ETag': 'beefdead'}
                ]}
            )

    @mock.patch('s3_sync.sync_s3.FileWrapper')
    def test_internal_slo_upload_encryption(self, mock_file_wrapper):
        slo_key = 'slo-object'
        slo_meta = {'x-object-meta-foo': 'bar'}
        s3_key = self.sync_s3.get_s3_name(slo_key)
        storage_policy = 42
        swift_req_headers = {'X-Backend-Storage-Policy-Index': storage_policy,
                             'X-Newest': True}
        manifest = [{'name': '/segment_container/slo-object/part1',
                     'hash': 'deadbeef',
                     'bytes': 100}]
        fake_body = FakeStream(5 * SyncS3.MB)

        self.mock_boto3_client.create_multipart_upload.return_value = {
            'UploadId': 'mpu-key-for-slo'}

        def upload_part(**kwargs):
            if kwargs['PartNumber'] == 1:
                return {'ETag': '"deadbeef"'}
            else:
                raise RuntimeError('Unknown call to upload part')

        self.mock_boto3_client.upload_part.side_effect = upload_part
        mock_file_wrapper.return_value = fake_body

        mock_ic = mock.Mock()
        self.sync_s3.encryption = True
        self.sync_s3._upload_slo(manifest, slo_meta, s3_key, swift_req_headers,
                                 mock_ic)

        self.mock_boto3_client.create_multipart_upload.assert_called_once_with(
            Bucket=self.aws_bucket,
            Key=self.sync_s3.get_s3_name(slo_key),
            Metadata={'foo': 'bar'},
            ServerSideEncryption='AES256')

    @mock.patch('s3_sync.sync_s3.get_slo_etag')
    def test_slo_meta_changed(self, mock_get_slo_etag):
        slo_key = 'slo-object'
        storage_policy = 42
        swift_req_headers = {'X-Backend-Storage-Policy-Index': storage_policy,
                             'X-Newest': True}
        manifest = [{'name': '/segment_container/slo-object/part1',
                     'hash': 'deadbeef',
                     'bytes': 5 * SyncS3.MB},
                    {'name': '/segment_container/slo-object/part2',
                     'hash': 'beefdead',
                     'bytes': 5 * SyncS3.MB}]

        self.mock_boto3_client.head_object.return_value = {
            'Metadata': {},
            'ETag': '"etag-2"'}
        mock_get_slo_etag.return_value = 'etag-2'
        self.sync_s3.update_slo_metadata = mock.Mock()
        self.sync_s3._upload_slo = mock.Mock()
        slo_meta = {
            utils.SLO_HEADER: 'True',
            'x-object-meta-new-key': 'foo'
        }
        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = slo_meta
        mock_ic.get_object.return_value = (
            200, slo_meta, FakeStream(content=json.dumps(manifest)))

        self.sync_s3.upload_object(slo_key, storage_policy, mock_ic)

        self.sync_s3.update_slo_metadata.assert_called_once_with(
            slo_meta, manifest, self.sync_s3.get_s3_name(slo_key),
            swift_req_headers, mock_ic)
        self.assertEqual(0, self.sync_s3._upload_slo.call_count)
        mock_ic.get_object_metadata.assert_called_once_with(
            'account', 'container', slo_key, headers=swift_req_headers)
        mock_ic.get_object.assert_called_once_with(
            'account', 'container', slo_key, headers=swift_req_headers)

    @mock.patch('s3_sync.sync_s3.get_slo_etag')
    def test_slo_meta_update_glacier(self, mock_get_slo_etag):
        slo_key = 'slo-object'
        storage_policy = 42
        swift_req_headers = {'X-Backend-Storage-Policy-Index': storage_policy,
                             'X-Newest': True}
        manifest = [{'name': '/segment_container/slo-object/part1',
                     'hash': 'deadbeef',
                     'bytes': 5 * SyncS3.MB},
                    {'name': '/segment_container/slo-object/part2',
                     'hash': 'beefdead',
                     'bytes': 5 * SyncS3.MB}]

        self.mock_boto3_client.head_object.return_value = {
            'Metadata': {},
            'ETag': '"etag-2"',
            'StorageClass': 'GLACIER'}
        mock_get_slo_etag.return_value = 'etag-2'
        self.sync_s3.update_slo_metadata = mock.Mock()
        self.sync_s3._upload_slo = mock.Mock()
        slo_meta = {
            utils.SLO_HEADER: 'True',
            'x-object-meta-new-key': 'foo'
        }
        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = slo_meta
        mock_ic.get_object.return_value = (
            200, slo_meta, FakeStream(content=json.dumps(manifest)))

        self.sync_s3.upload_object(slo_key, storage_policy, mock_ic)

        self.assertEqual(0, self.sync_s3.update_slo_metadata.call_count)
        self.sync_s3._upload_slo.assert_called_once_with(
            manifest, slo_meta, self.sync_s3.get_s3_name(slo_key),
            swift_req_headers, mock_ic)
        mock_ic.get_object_metadata.assert_called_once_with(
            'account', 'container', slo_key, headers=swift_req_headers)
        mock_ic.get_object.assert_called_once_with(
            'account', 'container', slo_key, headers=swift_req_headers)

    @mock.patch('s3_sync.sync_s3.get_slo_etag')
    def test_slo_no_changes(self, mock_get_slo_etag):
        slo_key = 'slo-object'
        storage_policy = 42
        swift_req_headers = {'X-Backend-Storage-Policy-Index': storage_policy,
                             'X-Newest': True}
        manifest = [{'name': '/segment_container/slo-object/part1',
                     'hash': 'deadbeef'},
                    {'name': '/segment_container/slo-object/part2',
                     'hash': 'beefdead'}]

        self.mock_boto3_client.head_object.return_value = {
            'Metadata': {'new-key': 'foo'},
            'ETag': '"etag-2"'}
        mock_get_slo_etag.return_value = 'etag-2'
        self.sync_s3.update_slo_metadata = mock.Mock()
        self.sync_s3._upload_slo = mock.Mock()
        slo_meta = {
            utils.SLO_HEADER: 'True',
            'x-object-meta-new-key': 'foo'
        }

        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = slo_meta
        mock_ic.get_object.return_value = (
            200, slo_meta, FakeStream(content=json.dumps(manifest)))

        self.sync_s3.upload_object(slo_key, storage_policy, mock_ic)

        self.assertEqual(0, self.sync_s3.update_slo_metadata.call_count)
        self.assertEqual(0, self.sync_s3._upload_slo.call_count)
        mock_ic.get_object_metadata.assert_called_once_with(
            'account', 'container', slo_key, headers=swift_req_headers)
        mock_ic.get_object.assert_called_once_with(
            'account', 'container', slo_key, headers=swift_req_headers)

    def test_slo_metadata_update(self):
        slo_meta = {
            utils.SLO_HEADER: 'True',
            'x-object-meta-new-key': 'foo',
            'x-object-meta-other-key': 'bar'
        }
        manifest = [
            {'name': '/segments/slo-object/part1',
             'hash': 'abcdef'},
            {'name': '/segments/slo-object/part2',
             'hash': 'fedcba'}]
        s3_key = self.sync_s3.get_s3_name('slo-object')
        segment_lengths = [12 * SyncS3.MB, 14 * SyncS3.MB]
        storage_policy = 42
        swift_req_headers = {'X-Backend-Storage-Policy-Index': storage_policy,
                             'X-Newest': True}

        def get_object_metadata(account, container, key, headers={}):
            return {'content-length': segment_lengths[int(key[-1]) - 1]}
        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.side_effect = get_object_metadata

        self.mock_boto3_client.create_multipart_upload.return_value = {
            'UploadId': 'mpu-upload'}

        def upload_part_copy(**kwargs):
            if kwargs['PartNumber'] == 1:
                return {'CopyPartResult': {'ETag': '"abcdef"'}}
            elif kwargs['PartNumber'] == 2:
                return {'CopyPartResult': {'ETag': '"fedcba"'}}
            raise RuntimeError('Invalid part!')

        self.mock_boto3_client.upload_part_copy.side_effect = upload_part_copy

        self.sync_s3.update_slo_metadata(slo_meta, manifest, s3_key,
                                         swift_req_headers, mock_ic)

        self.mock_boto3_client.create_multipart_upload.assert_called_once_with(
            Bucket=self.aws_bucket, Key=s3_key,
            Metadata={'new-key': 'foo', 'other-key': 'bar',
                      utils.SLO_HEADER: 'True'},
            ServerSideEncryption='AES256')
        self.mock_boto3_client.upload_part_copy.assert_has_calls([
            mock.call(Bucket=self.aws_bucket, Key=s3_key, PartNumber=1,
                      CopySource={'Bucket': self.aws_bucket, 'Key': s3_key},
                      CopySourceRange='bytes=0-%d' % (12 * SyncS3.MB - 1),
                      UploadId='mpu-upload'),
            mock.call(Bucket=self.aws_bucket, Key=s3_key, PartNumber=2,
                      CopySource={'Bucket': self.aws_bucket, 'Key': s3_key},
                      CopySourceRange='bytes=%d-%d' % (
                          12 * SyncS3.MB,
                          26 * SyncS3.MB - 1),
                      UploadId='mpu-upload')
        ])
        self.mock_boto3_client.complete_multipart_upload\
            .assert_called_once_with(Bucket=self.aws_bucket, Key=s3_key,
                                     UploadId='mpu-upload',
                                     MultipartUpload={'Parts': [
                                         {'PartNumber': 1, 'ETag': 'abcdef'},
                                         {'PartNumber': 2, 'ETag': 'fedcba'}
                                     ]})
        mock_ic.get_object_metadata.assert_has_calls(
            [mock.call(self.sync_s3.account,
                       'segments',
                       'slo-object/part1',
                       headers=swift_req_headers),
             mock.call(self.sync_s3.account,
                       'segments',
                       'slo-object/part2',
                       headers=swift_req_headers)])

    def test_slo_metadata_update_encryption(self):
        slo_meta = {
            utils.SLO_HEADER: 'True',
            'x-object-meta-new-key': 'foo',
            'x-object-meta-other-key': 'bar'
        }
        manifest = [
            {'name': '/segments/slo-object/part1',
             'hash': 'abcdef'}]
        s3_key = self.sync_s3.get_s3_name('slo-object')
        segment_lengths = [12 * SyncS3.MB, 14 * SyncS3.MB]

        def get_object_metadata(account, container, key, headers):
            return {'content-length': segment_lengths[int(key[-1]) - 1]}
        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.side_effect = get_object_metadata

        self.mock_boto3_client.create_multipart_upload.return_value = {
            'UploadId': 'mpu-upload'}

        def upload_part_copy(**kwargs):
            if kwargs['PartNumber'] == 1:
                return {'CopyPartResult': {'ETag': '"abcdef"'}}
            raise RuntimeError('Invalid part!')

        self.mock_boto3_client.upload_part_copy.side_effect = upload_part_copy

        self.sync_s3.update_slo_metadata(slo_meta, manifest, s3_key, {},
                                         mock_ic)

        self.mock_boto3_client.create_multipart_upload.assert_called_once_with(
            Bucket=self.aws_bucket, Key=s3_key,
            Metadata={'new-key': 'foo', 'other-key': 'bar',
                      utils.SLO_HEADER: 'True'},
            ServerSideEncryption='AES256')

    def test_validate_manifest_too_many_parts(self):
        segments = [{'name': '/segment/%d' % i} for i in xrange(10001)]
        self.assertEqual(
            False, self.sync_s3._validate_slo_manifest(segments))

    def test_validate_manifest_small_part(self):
        segments = [{'name': '/segment/1',
                     'bytes': 10 * SyncS3.MB},
                    {'name': '/segment/2',
                     'bytes': 10},
                    {'name': '/segment/3',
                     'bytes': '10'}]
        self.assertEqual(
            False, self.sync_s3._validate_slo_manifest(segments))

    def test_validate_manifest_large_part(self):
        segments = [{'name': '/segment/1',
                     'bytes': 10 * SyncS3.MB},
                    {'name': '/segment/2',
                     'bytes': 10 * SyncS3.GB},
                    {'name': '/segment/3',
                     'bytes': '10'}]
        self.assertEqual(
            False, self.sync_s3._validate_slo_manifest(segments))

    def test_validate_manifest_small(self):
        segments = [{'name': '/segment/1',
                     'hash': 'abcdef',
                     'bytes': 10}]
        self.assertEqual(
            True, self.sync_s3._validate_slo_manifest(segments))

    def test_validate_manifest_range(self):
        segments = [{'name': '/segment/1',
                     'hash': 'abcdef',
                     'range': '102453-102462',
                     'bytes': 10}]
        self.assertEqual(
            False, self.sync_s3._validate_slo_manifest(segments))

    def test_is_object_meta_synced(self):
        # The structure for each entry is: swift meta, s3 meta, whether they
        # should be equal.
        test_metas = [({'x-object-meta-upper': 'UPPER',
                        'x-object-meta-lower': 'lower'},
                       {'upper': 'UPPER',
                        'lower': 'lower'},
                       True),
                      ({'x-object-meta-foo': 'foo',
                        'x-object-meta-bar': 'bar'},
                       {'foo': 'not foo',
                        'bar': 'bar'},
                       False),
                      ({'x-object-meta-unicode': '👍',
                        'x-object-meta-date': 'Wed, April 30 10:32:21 UTC'},
                       {'unicode': '%F0%9F%91%8D',
                        'date': 'Wed%2C%20April%2030%2010%3A32%3A21%20UTC'},
                       True),
                      ({'x-object-meta-foo': 'foo',
                        'x-object-meta-bar': 'bar',
                        'x-static-large-object': 'True'},
                       {'swift-slo-etag': 'deadbeef',
                        'foo': 'foo',
                        'bar': 'bar'},
                       True)]
        for swift_meta, s3_meta, expected in test_metas:
            self.assertEqual(expected,
                             SyncS3.is_object_meta_synced(s3_meta, swift_meta))

    def test_shunt_object(self):
        key = 'key'
        body = 'some fairly large content' * (1 << 16)
        # simulate s3proxy with filesystem backend response
        head_response = {
            u'ContentLength': len(body),
            u'ContentType': 'application/unknown',
            u'ETag': '"e06dd4228b3a7ab66aae5fbc9e4b905e"',
            u'Metadata': {'mtime': '1497315527.000000'},
            'ResponseMetadata': {
                'HTTPHeaders': {
                    'content-length': str(len(body)),
                    'content-type': 'application/unknown',
                    'date': 'Thu, 15 Jun 2017 00:09:25 GMT',
                    'etag': '"e06dd4228b3a7ab66aae5fbc9e4b905e"',
                    'last-modified': 'Wed, 14 Jun 2017 23:11:34 GMT',
                    'server': 'Jetty(9.2.z-SNAPSHOT)',
                    'x-amz-meta-mtime': '1497315527.000000'},
                'HTTPStatusCode': 200,
                'RetryAttempts': 0,
            }
        }
        get_response = dict(head_response)
        get_response[u'Body'] = StringIO(body)
        self.mock_boto3_client.get_object.return_value = get_response
        self.mock_boto3_client.head_object.return_value = head_response

        expected_headers = [
            # Content-Length must be properly capitalized,
            # or eventlet will try to be "helpful"
            ('Content-Length', str(len(body))),
            # everything else...
            ('content-type', 'application/unknown'),
            ('date', 'Thu, 15 Jun 2017 00:09:25 GMT'),
            ('etag', 'e06dd4228b3a7ab66aae5fbc9e4b905e'),
            ('last-modified', 'Wed, 14 Jun 2017 23:11:34 GMT'),
            ('server', 'Jetty(9.2.z-SNAPSHOT)'),
            # x-amz-meta-* get translated to X-Object-Meta-*
            ('x-object-meta-mtime', '1497315527.000000'),
        ]

        req = swob.Request.blank('/v1/AUTH_a/c/key', method='GET')
        status, headers, body_iter = self.sync_s3.shunt_object(req, key)
        self.assertEqual(status, 200)
        self.assertEqual(sorted(headers), expected_headers)
        self.assertEqual(b''.join(body_iter), body)
        self.assertEqual(self.mock_boto3_client.get_object.mock_calls,
                         [mock.call(Bucket=self.aws_bucket,
                                    Key=self.sync_s3.get_s3_name(key))])

        # Again, but with HEAD
        req.method = 'HEAD'
        status, headers, body_iter = self.sync_s3.shunt_object(req, key)
        self.assertEqual(status, 200)
        self.assertEqual(sorted(headers), expected_headers)
        self.assertEqual(b''.join(body_iter), b'')
        self.assertEqual(self.mock_boto3_client.head_object.mock_calls,
                         [mock.call(Bucket=self.aws_bucket,
                                    Key=self.sync_s3.get_s3_name(key))])

    def test_shunt_object_includes_some_client_headers(self):
        key = 'key'
        body = 'some content'
        # minimal response
        head_response = {
            'ResponseMetadata': {
                'HTTPHeaders': {},
                'HTTPStatusCode': 304,
            }
        }
        get_response = dict(head_response)
        get_response[u'Body'] = StringIO(body)
        self.mock_boto3_client.get_object.return_value = get_response
        self.mock_boto3_client.head_object.return_value = head_response

        req = swob.Request.blank('/v1/AUTH_a/c/key', method='GET', headers={
            'Range': 'r',
            'If-Match': 'im',
            'If-None-Match': 'inm',
            'If-Modified-Since': 'ims',
            'If-Unmodified-Since': 'ius',
        })
        status, headers, body_iter = self.sync_s3.shunt_object(req, key)
        self.assertEqual(status, 304)
        self.assertEqual(headers, [])
        self.assertEqual(b''.join(body_iter), body)
        self.assertEqual(self.mock_boto3_client.get_object.mock_calls,
                         [mock.call(Bucket=self.aws_bucket,
                                    Key=self.sync_s3.get_s3_name(key),
                                    Range='r',
                                    IfMatch='im',
                                    IfNoneMatch='inm',
                                    IfModifiedSince='ims',
                                    IfUnmodifiedSince='ius')])

        # Again, but with HEAD
        req.method = 'HEAD'
        status, headers, body_iter = self.sync_s3.shunt_object(req, key)
        self.assertEqual(status, 304)
        self.assertEqual(headers, [])
        self.assertEqual(b''.join(body_iter), b'')
        self.assertEqual(self.mock_boto3_client.head_object.mock_calls,
                         [mock.call(Bucket=self.aws_bucket,
                                    Key=self.sync_s3.get_s3_name(key),
                                    Range='r',
                                    IfMatch='im',
                                    IfNoneMatch='inm',
                                    IfModifiedSince='ims',
                                    IfUnmodifiedSince='ius')])

    def test_shunt_object_network_error(self):
        key = 'key'
        self.mock_boto3_client.get_object.side_effect = RequestException
        self.mock_boto3_client.head_object.side_effect = RequestException
        req = swob.Request.blank('/v1/AUTH_a/c/key', method='GET')
        status, headers, body_iter = self.sync_s3.shunt_object(req, key)
        self.assertEqual(status, 502)
        self.assertEqual(headers, [])
        self.assertEqual(b''.join(body_iter), 'Bad Gateway')
        self.assertEqual(self.mock_boto3_client.get_object.mock_calls,
                         [mock.call(Bucket=self.aws_bucket,
                                    Key=self.sync_s3.get_s3_name(key))])

        # Again, but with HEAD
        req.method = 'HEAD'
        status, headers, body_iter = self.sync_s3.shunt_object(req, key)
        self.assertEqual(status, 502)
        self.assertEqual(headers, [])
        self.assertEqual(b''.join(body_iter), b'')
        self.assertEqual(self.mock_boto3_client.head_object.mock_calls,
                         [mock.call(Bucket=self.aws_bucket,
                                    Key=self.sync_s3.get_s3_name(key))])

    def test_list_objects(self):
        prefix = '%s/%s/%s' % (self.sync_s3.get_prefix(), self.sync_s3.account,
                               self.sync_s3.container)
        now_date = datetime.datetime.now()
        self.mock_boto3_client.list_objects.return_value = {
            'Contents': [
                dict(Key='%s/%s' % (prefix, 'barù'.decode('utf-8')),
                     ETag='"badbeef"',
                     Size=42,
                     LastModified=now_date),
                dict(Key='%s/%s' % (prefix, 'foo'),
                     ETag='"deadbeef"',
                     Size=1024,
                     LastModified=now_date)
            ],
            'CommonPrefixes': [
                dict(Prefix='%s/afirstpref' % prefix),
                dict(Prefix='%s/preflast' % prefix),
            ]
        }

        status, ret = self.sync_s3.list_objects('marker', 10, 'prefix', '-')
        self.mock_boto3_client.list_objects.assert_called_once_with(
            Bucket=self.aws_bucket,
            Prefix='%s/prefix' % prefix,
            Delimiter='-',
            MaxKeys=10,
            Marker='%s/marker' % prefix)
        self.assertEqual(200, status)
        expected_location = 'AWS S3;%s;%s' % (self.aws_bucket, prefix)
        self.assertEqual(
            dict(subdir='afirstpref',
                 content_location=expected_location),
            ret[0])
        self.assertEqual(
            dict(subdir='preflast',
                 content_location=expected_location),
            ret[3])
        self.assertEqual(
            dict(hash='badbeef',
                 name=u'bar\xf9',
                 bytes=42,
                 last_modified=now_date.isoformat(),
                 content_type='application/octet-stream',
                 content_location=expected_location),
            ret[1])
        self.assertEqual(
            dict(hash='deadbeef',
                 name='foo',
                 bytes=1024,
                 last_modified=now_date.isoformat(),
                 content_type='application/octet-stream',
                 content_location=expected_location),
            ret[2])

    def test_list_objects_error(self):
        self.mock_boto3_client.list_objects.side_effect = ClientError(
            dict(Error=dict(Code=500)), 'failed to list!')
        prefix = '%s/%s/%s/' % (self.sync_s3.get_prefix(),
                                self.sync_s3.account, self.sync_s3.container)

        status, ret = self.sync_s3.list_objects('', 10, '', '')
        self.mock_boto3_client.list_objects.assert_called_once_with(
            Bucket=self.aws_bucket,
            Prefix=prefix,
            MaxKeys=10)
        self.assertEqual(500, status)
