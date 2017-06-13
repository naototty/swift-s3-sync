import json
import mock
import swiftclient
from utils import FakeStream
from s3_sync import utils
from s3_sync.sync_swift import SyncSwift
import unittest


class TestSyncSwift(unittest.TestCase):
    @mock.patch('s3_sync.sync_swift.swiftclient.client.Connection')
    def setUp(self, mock_swift):
        self.mock_swift_client = mock.Mock()

        mock_swift.return_value = self.mock_swift_client

        self.aws_bucket = 'bucket'
        self.scratch_space = 'scratch'
        self.sync_swift = SyncSwift(
            {'aws_bucket': self.aws_bucket,
             'aws_identity': 'identity',
             'aws_secret': 'credential',
             'account': 'account',
             'container': 'container',
             'aws_endpoint': 'http://swift.url/auth/v1.0'})

    @mock.patch('s3_sync.sync_swift.check_slo')
    @mock.patch('s3_sync.sync_swift.FileWrapper')
    def test_upload_new_object(self, mock_file_wrapper, mock_check_slo):
        key = 'key'
        storage_policy = 42
        swift_req_headers = {'X-Backend-Storage-Policy-Index': storage_policy,
                             'X-Newest': True}

        wrapper = mock.Mock()
        wrapper.__len__ = lambda s: 0
        wrapper.get_headers.return_value = {'etag': 'deadbeef'}
        mock_file_wrapper.return_value = wrapper
        not_found = swiftclient.exceptions.ClientException('not found',
                                                           http_status=404)
        self.mock_swift_client.head_object.side_effect = not_found

        mock_check_slo.return_value = False
        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = {}

        self.sync_swift.upload_object(key, storage_policy, mock_ic)
        mock_file_wrapper.assert_called_with(mock_ic,
                                             self.sync_swift.account,
                                             self.sync_swift.container,
                                             key, swift_req_headers)

        self.mock_swift_client.put_object.assert_called_with(
            self.aws_bucket, key, wrapper, headers={}, etag='deadbeef',
            content_length=0)

    @mock.patch('s3_sync.sync_swift.check_slo')
    @mock.patch('s3_sync.sync_swift.FileWrapper')
    def test_upload_unicode_object(self, mock_file_wrapper, mock_check_slo):
        key = 'monkey-\xf0\x9f\x90\xb5'
        storage_policy = 42
        swift_req_headers = {'X-Backend-Storage-Policy-Index': storage_policy,
                             'X-Newest': True}

        wrapper = mock.Mock()
        wrapper.__len__ = lambda s: 0
        wrapper.get_headers.return_value = {'etag': 'deadbeef'}
        mock_file_wrapper.return_value = wrapper
        not_found = swiftclient.exceptions.ClientException('not found',
                                                           http_status=404)
        self.mock_swift_client.head_object.side_effect = not_found

        mock_check_slo.return_value = False
        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = {}

        self.sync_swift.upload_object(key, storage_policy, mock_ic)
        mock_file_wrapper.assert_called_with(mock_ic,
                                             self.sync_swift.account,
                                             self.sync_swift.container,
                                             key, swift_req_headers)

        self.mock_swift_client.put_object.assert_called_with(
            self.aws_bucket, key, wrapper, headers={},
            etag='deadbeef', content_length=0)

    def test_upload_changed_meta(self):
        key = 'key'
        storage_policy = 42
        etag = '1234'
        swift_object_meta = {'x-object-meta-new': 'new',
                             'x-object-meta-old': 'updated',
                             'etag': etag}
        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = swift_object_meta
        self.mock_swift_client.head_object.return_value = {
            'x-object-meta-old': 'old', 'etag': '%s' % etag}

        self.sync_swift.upload_object(key, storage_policy, mock_ic)

        self.mock_swift_client.post_object.assert_called_with(
            self.aws_bucket, key,
            {'x-object-meta-new': 'new',
             'x-object-meta-old': 'updated'})

    def test_meta_unicode(self):
        key = 'key'
        storage_policy = 42
        etag = '1234'
        swift_object_meta = {'x-object-meta-new': '\xf0\x9f\x91\x8d',
                             'x-object-meta-old': 'updated',
                             'etag': etag}
        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = swift_object_meta
        self.mock_swift_client.head_object.return_value = {
            'x-object-meta-old': 'old', 'etag': '%s' % etag}

        self.sync_swift.upload_object(key, storage_policy, mock_ic)

        self.mock_swift_client.post_object.assert_called_with(
            self.aws_bucket,
            key,
            {'x-object-meta-new': '\xf0\x9f\x91\x8d',
             'x-object-meta-old': 'updated'})

    @mock.patch('s3_sync.sync_swift.FileWrapper')
    def test_upload_replace_object(self, mock_file_wrapper):
        key = 'key'
        storage_policy = 42
        swift_object_meta = {'x-object-meta-new': 'new',
                             'x-object-meta-old': 'updated',
                             'etag': '2'}
        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = swift_object_meta
        self.mock_swift_client.head_object.return_value = {
            'x-object-meta-old': 'old', 'etag': '1'}

        wrapper = mock.Mock()
        wrapper.get_headers.return_value = swift_object_meta
        wrapper.__len__ = lambda s: 42
        mock_file_wrapper.return_value = wrapper

        self.sync_swift.upload_object(key, storage_policy, mock_ic)

        self.mock_swift_client.put_object.assert_called_with(
            self.aws_bucket,
            key,
            wrapper,
            headers={'x-object-meta-new': 'new',
                     'x-object-meta-old': 'updated'},
            etag='2',
            content_length=42)

    def test_upload_same_object(self):
        key = 'key'
        storage_policy = 42
        etag = '1234'
        swift_object_meta = {'x-object-meta-foo': 'foo',
                             'etag': etag}
        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = swift_object_meta
        self.mock_swift_client.head_object.return_value = {
            'x-object-meta-foo': 'foo', 'etag': '%s' % etag}

        self.sync_swift.upload_object(key, storage_policy, mock_ic)

        self.mock_swift_client.post_object.assert_not_called()
        self.mock_swift_client.put_object.assert_not_called()

    def test_upload_slo(self):
        slo_key = 'slo-object'
        storage_policy = 42
        swift_req_headers = {'X-Backend-Storage-Policy-Index': storage_policy,
                             'X-Newest': True}
        manifest = [{'name': '/segment_container/slo-object/part1',
                     'hash': 'deadbeef',
                     'bytes': 1024},
                    {'name': '/segment_container/slo-object/part2',
                     'hash': 'beefdead',
                     'bytes': 1024}]

        not_found = swiftclient.exceptions.ClientException('not found',
                                                           http_status=404)
        self.mock_swift_client.head_object.side_effect = not_found
        self.mock_swift_client.get_object.side_effect = not_found

        def get_metadata(account, container, key, headers):
            if key == slo_key:
                return {utils.SLO_HEADER: 'True'}
            raise RuntimeError('Unknown key')

        def get_object(account, container, key, headers):
            if key == slo_key:
                return (200, {utils.SLO_HEADER: 'True'},
                        FakeStream(content=json.dumps(manifest)))
            if container == 'segment_container':
                if key == 'slo-object/part1':
                    return (200, {'Content-Length': 1024}, FakeStream(1024))
                elif key == 'slo-object/part2':
                    return (200, {'Content-Length': 1024}, FakeStream(1024))
            raise RuntimeError('Unknown key!')

        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.side_effect = get_metadata
        mock_ic.get_object.side_effect = get_object

        self.sync_swift.upload_object(slo_key, storage_policy, mock_ic)

        self.mock_swift_client.head_object.assert_called_once_with(
            self.aws_bucket, slo_key)
        segment_container = self.aws_bucket + '_segments'
        self.mock_swift_client.put_object.assert_has_calls([
            mock.call(segment_container,
                      'slo-object/part1', mock.ANY, etag='deadbeef',
                      content_length=1024),
            mock.call(self.aws_bucket + '_segments',
                      'slo-object/part2', mock.ANY, etag='beefdead',
                      content_length=1024),
            mock.call(self.aws_bucket, slo_key,
                      mock.ANY,
                      headers={},
                      query_string='multipart-manifest=put')
        ])

        expected_manifest = [
            {'path': '/%s/%s' % (segment_container,
                                 'slo-object/part1'),
             'size_bytes': 1024,
             'etag': 'deadbeef'},
            {'path': '/%s/%s' % (segment_container,
                                 'slo-object/part2'),
             'size_bytes': 1024,
             'etag': 'beefdead'}]

        called_manifest = json.loads(
            self.mock_swift_client.put_object.mock_calls[-1][1][2])
        self.assertEqual(len(expected_manifest), len(called_manifest))
        for index, segment in enumerate(expected_manifest):
            called_segment = called_manifest[index]
            self.assertEqual(set(segment.keys()), set(called_segment.keys()))
            for k in segment.keys():
                self.assertEqual(segment[k], called_segment[k])

        mock_ic.get_object_metadata.assert_called_once_with(
            'account', 'container', slo_key, headers=swift_req_headers)
        mock_ic.get_object.assert_has_calls([
            mock.call('account', 'container', slo_key,
                      headers=swift_req_headers),
            mock.call('account', 'segment_container', 'slo-object/part1',
                      headers=swift_req_headers),
            mock.call('account', 'segment_container', 'slo-object/part2',
                      headers=swift_req_headers)])

    def test_slo_metadata_update(self):
        key = 'key'
        storage_policy = 42
        etag = '1234'
        swift_object_meta = {'x-object-meta-new': 'new',
                             'x-object-meta-old': 'updated',
                             'x-static-large-object': 'True',
                             'etag': etag}
        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = swift_object_meta
        self.mock_swift_client.head_object.return_value = {
            'x-object-meta-old': 'old',
            'x-static-large-object': 'True',
            'etag': '%s' % etag}
        self.mock_swift_client.get_object.return_value = ({
            'etag': etag
        }, '')

        self.sync_swift.upload_object(key, storage_policy, mock_ic)

        self.mock_swift_client.post_object.assert_called_with(
            self.aws_bucket, key,
            {'x-object-meta-new': 'new',
             'x-object-meta-old': 'updated'})

    def test_slo_no_changes(self):
        key = 'key'
        storage_policy = 42
        etag = '1234'
        meta = {'x-object-meta-new': 'new',
                'x-object-meta-old': 'updated',
                'x-static-large-object': 'True',
                'etag': etag}
        mock_ic = mock.Mock()
        mock_ic.get_object_metadata.return_value = meta
        self.mock_swift_client.head_object.return_value = meta
        self.mock_swift_client.get_object.return_value = (meta, '')

        self.sync_swift.upload_object(key, storage_policy, mock_ic)

        self.mock_swift_client.post_object.assert_not_called()

    def test_delete_object(self):
        key = 'key'

        # When deleting in Swift, we have to do a HEAD in case it's an SLO
        self.mock_swift_client.head_object.return_value = {}
        self.sync_swift.delete_object(key)
        self.mock_swift_client.delete_object.assert_called_with(
            self.aws_bucket, key)

    def test_delete_non_existent_object(self):
        key = 'key'

        not_found = swiftclient.exceptions.ClientException(
            'not found', http_status=404)
        self.mock_swift_client.head_object.side_effect = not_found
        self.sync_swift.delete_object(key)
        self.mock_swift_client.delete_object.assert_not_called()

    def test_delete_slo(self):
        slo_key = 'slo-object'
        manifest = [{'name': '/segment_container/slo-object/part1',
                     'hash': 'deadbeef'},
                    {'name': '/segment_container/slo-object/part2',
                     'hash': 'beefdead'}]

        self.mock_swift_client.head_object.return_value = {
            'x-static-large-object': 'True',
            'etag': 'deadbeef'
        }
        self.mock_swift_client.get_object.return_value = (
            {}, json.dumps(manifest))

        self.sync_swift.delete_object(slo_key)

        self.mock_swift_client.delete_object.assert_called_once_with(
            self.aws_bucket, slo_key, query_string='multipart-manifest=delete')

        self.mock_swift_client.head_object.assert_called_once_with(
            self.aws_bucket, slo_key)
