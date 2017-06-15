import json
import mock
import tempfile
import unittest

from swift.common import swob

from s3_sync import shunt


class FakeSwift(object):
    def __init__(self):
        self.calls = []

    def __call__(self, env, start_response):
        self.calls.append(env)
        # Let the tests set up the responses they want
        status = env.get('__test__.status', '200 OK')
        headers = env.get('__test__.headers', [])
        body = env.get('__test__.headers', ['pass'])

        start_response(status, headers)
        return body


class TestShunt(unittest.TestCase):
    def setUp(self):
        self.patchers = [mock.patch(name) for name in (
            's3_sync.sync_swift.SyncSwift.shunt_object',
            's3_sync.sync_s3.SyncS3.shunt_object')]
        self.mock_shunt_swift = self.patchers[0].__enter__()
        self.mock_shunt_swift.return_value = (
            200, [
                ('Remote-x-openstack-request-id', 'also some trans id'),
                ('Remote-x-trans-id', 'some trans id'),
                ('CONNECTION', 'bad'),
                ('keep-alive', 'bad'),
                ('proxy-authenticate', 'bad'),
                ('proxy-authorization', 'bad'),
                ('te', 'bad'),
                ('trailer', 'bad'),
                ('Transfer-Encoding', 'bad'),
                ('Upgrade', 'bad'),
            ], ['remote swift'])
        self.mock_shunt_s3 = self.patchers[1].__enter__()
        self.mock_shunt_s3.return_value = (
            200, [
                ('Remote-x-amz-id-2', 'also some trans id'),
                ('Remote-x-amz-request-id', 'some trans id'),
                ('CONNECTION', 'bad'),
                ('keep-alive', 'bad'),
                ('proxy-authenticate', 'bad'),
                ('proxy-authorization', 'bad'),
                ('te', 'bad'),
                ('trailer', 'bad'),
                ('Transfer-Encoding', 'bad'),
                ('Upgrade', 'bad'),
            ], ['remote s3'])

        with tempfile.NamedTemporaryFile() as fp:
            json.dump({'containers': [
                {
                    'account': 'AUTH_a',
                    'container': u'sw\u00e9ft',
                    'propagate_delete': False,
                    'protocol': 'swift',
                    'aws_bucket': 'dest-container',
                    'aws_identity': 'user',
                    'aws_secret': 'key',
                    'aws_endpoint': 'https://swift.example.com/auth/v1.0',
                },
                {
                    'account': 'AUTH_a',
                    'container': 's3',
                    'propagate_delete': False,
                    'aws_bucket': 'dest-bucket',
                    'aws_identity': 'user',
                    'aws_secret': 'key',
                },
                {
                    'account': 'AUTH_b',
                    'container': '/*',
                    'propagate_delete': False,
                    'aws_bucket': 'dest-bucket',
                    'aws_identity': 'user',
                    'aws_secret': 'key',
                },
            ]}, fp)
            fp.flush()
            self.app = shunt.filter_factory(
                {'conf_file': fp.name})(FakeSwift())

    def tearDown(self):
        for patcher in self.patchers:
            patcher.__exit__()

    def test_init(self):
        self.maxDiff = None
        self.assertEqual(self.app.sync_profiles, {
            ('AUTH_a', 'sw\xc3\xa9ft'): {
                'account': 'AUTH_a',
                'container': 'sw\xc3\xa9ft',
                'propagate_delete': False,
                'protocol': 'swift',
                'aws_bucket': 'dest-container',
                'aws_identity': 'user',
                'aws_secret': 'key',
                'aws_endpoint': 'https://swift.example.com/auth/v1.0',
            },
            ('AUTH_a', 's3'): {
                'account': 'AUTH_a',
                'container': 's3',
                'propagate_delete': False,
                'aws_bucket': 'dest-bucket',
                'aws_identity': 'user',
                'aws_secret': 'key',
            },
            ('AUTH_b', '/*'): {
                'account': 'AUTH_b',
                'container': '/*',
                'propagate_delete': False,
                'aws_bucket': 'dest-bucket',
                'aws_identity': 'user',
                'aws_secret': 'key',
            },
        })

    def test_unshunted_requests(self):
        def _do_test(path, method='GET'):
            req = swob.Request.blank(path, method=method, environ={
                'swift.trans_id': 'local trans id'})
            req.call_application(self.app)
            self.assertEqual(self.mock_shunt_swift.mock_calls, [])
            self.assertEqual(self.mock_shunt_s3.mock_calls, [])

        def _test_methods(path, methods=('OPTIONS', 'GET', 'HEAD', 'PUT',
                                         'POST', 'DELETE', 'COPY')):
            if 'OPTIONS' in methods:
                _do_test(path, 'OPTIONS')
            if 'GET' in methods:
                _do_test(path, 'GET')
            if 'HEAD' in methods:
                _do_test(path, 'HEAD')
            if 'PUT' in methods:
                _do_test(path, 'PUT')
            if 'POST' in methods:
                _do_test(path, 'POST')
            if 'DELETE' in methods:
                _do_test(path, 'DELETE')
            # doesn't necessarily apply to all paths, but whatever
            if 'COPY' in methods:
                _do_test(path, 'COPY')

        # Only shunt object GETs
        _test_methods('/some/weird/non/swift/path')
        _test_methods('/v1/AUTH_a')
        _test_methods('/v1/AUTH_a/')
        # Not an affected container
        _test_methods('/v1/AUTH_a/c')
        _test_methods('/v1/AUTH_a/c/')
        _test_methods('/v1/AUTH_a/c/o')
        # Affected container, but not relevant methods
        _test_methods(u'/v1/AUTH_a/sw\u00e9ft',
                      ('OPTIONS', 'HEAD', 'PUT', 'POST', 'DELETE'))
        _test_methods(u'/v1/AUTH_a/sw\u00e9ft/',
                      ('OPTIONS', 'HEAD', 'PUT', 'POST', 'DELETE'))
        _test_methods('/v1/AUTH_a/s3',
                      ('OPTIONS', 'HEAD', 'PUT', 'POST', 'DELETE'))
        _test_methods('/v1/AUTH_a/s3/',
                      ('OPTIONS', 'HEAD', 'PUT', 'POST', 'DELETE'))
        _test_methods(u'/v1/AUTH_a/sw\u00e9ft/o',
                      ('OPTIONS', 'PUT', 'POST', 'DELETE'))
        _test_methods('/v1/AUTH_a/s3/o',
                      ('OPTIONS', 'PUT', 'POST', 'DELETE'))

    def test_object_shunt(self):
        def _test_no_shunt(path, status):
            req = swob.Request.blank(path, environ={
                '__test__.status': status,
                'swift.trans_id': 'local trans id'})
            req.call_application(self.app)
            self.assertEqual(self.mock_shunt_swift.mock_calls, [])
            self.assertEqual(self.mock_shunt_s3.mock_calls, [])
        # Not an affected container
        _test_no_shunt('/v1/AUTH_a/c/o', '404 Not Found')
        _test_no_shunt('/v1/AUTH_a/c/o', '404 Not Found')
        # Affected container, but not 404
        _test_no_shunt(u'/v1/AUTH_a/sw\u00e9ft/o', '200 OK')
        _test_no_shunt('/v1/AUTH_a/s3/o', '200 OK')
        _test_no_shunt(u'/v1/AUTH_a/sw\u00e9ft/o', '503 Not Available')
        _test_no_shunt('/v1/AUTH_a/s3/o', '400 Bad Request')

        # Do the shunt!
        def _test_shunted(path, expect_s3):
            req = swob.Request.blank(path, environ={
                '__test__.status': '404 Not Found',
                'swift.trans_id': 'local trans id'})
            status, headers, body_iter = req.call_application(self.app)
            if expect_s3:
                self.assertEqual(self.mock_shunt_swift.mock_calls, [])
                self.assertEqual(self.mock_shunt_s3.mock_calls, [
                    mock.call(mock.ANY, path.split('/', 4)[4])])
                received_req = self.mock_shunt_s3.mock_calls[0][1][0]
                self.assertEqual(req.environ, received_req.environ)
                self.assertEqual(status, '200 OK')
                self.assertEqual(headers, [
                    ('Remote-x-amz-id-2', 'also some trans id'),
                    ('Remote-x-amz-request-id', 'some trans id'),
                ])
                self.assertEqual(b''.join(body_iter), b'remote s3')
                self.mock_shunt_s3.reset_mock()
            else:
                self.assertEqual(self.mock_shunt_s3.mock_calls, [])
                self.assertEqual(self.mock_shunt_swift.mock_calls, [
                    mock.call(mock.ANY, path.split('/', 4)[4])])
                received_req = self.mock_shunt_swift.mock_calls[0][1][0]
                self.assertEqual(req.environ, received_req.environ)
                self.assertEqual(status, '200 OK')
                self.assertEqual(headers, [
                    ('Remote-x-openstack-request-id', 'also some trans id'),
                    ('Remote-x-trans-id', 'some trans id'),
                ])
                self.assertEqual(b''.join(body_iter), b'remote swift')
                self.mock_shunt_swift.reset_mock()
        _test_shunted(u'/v1/AUTH_a/sw\u00e9ft/o', False)
        _test_shunted('/v1/AUTH_a/s3/o', True)
