import json
import lxml
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
        body = env.get('__test__.body', ['pass'])

        start_response(status, headers)
        return body


class TestShunt(unittest.TestCase):
    def setUp(self):
        self.patchers = [mock.patch(name) for name in (
            's3_sync.sync_swift.SyncSwift.shunt_object',
            's3_sync.sync_s3.SyncS3.shunt_object',
            's3_sync.sync_swift.SyncSwift.list_objects',
            's3_sync.sync_s3.SyncS3.list_objects')]
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

        self.mock_list_swift = self.patchers[2].__enter__()
        self.mock_list_s3 = self.patchers[3].__enter__()

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

    def test_bad_config_noops(self):
        app = shunt.filter_factory(
            {'conf_file': '/etc/doesnt/exist'})(FakeSwift())
        self.assertEqual(app.sync_profiles, {})

        with tempfile.NamedTemporaryFile() as fp:
            # empty
            app = shunt.filter_factory(
                {'conf_file': fp.name})(FakeSwift())
            self.assertEqual(app.sync_profiles, {})

            # not json
            fp.write('{"containers":')
            fp.flush()
            app = shunt.filter_factory(
                {'conf_file': fp.name})(FakeSwift())
            self.assertEqual(app.sync_profiles, {})

    def test_init(self):
        self.maxDiff = None
        self.assertEqual(self.app.sync_profiles, {
            ('AUTH_a', 'sw\xc3\xa9ft'): {
                'account': 'AUTH_a',
                'container': 'sw\xc3\xa9ft'.decode('utf-8'),
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
        _test_shunted('/v1/AUTH_b/c1/o', True)
        _test_shunted('/v1/AUTH_b/c2/o', True)

    def test_list_container_no_shunt(self):
        req = swob.Request.blank(
            '/v1/AUTH_a/foo',
            environ={'__test__.status': '200 OK',
                     'swift.trans_id': 'id'})
        req.call_application(self.app)

        self.assertEqual(self.mock_list_swift.mock_calls, [])
        self.assertEqual(self.mock_list_s3.mock_calls, [])

    def test_list_container_shunt_s3(self):
        self.mock_list_s3.side_effect = [
            (200, [{'name': 'abc',
                    'hash': 'ffff',
                    'bytes': 42,
                    'last_modified': 'date',
                    'content_type': 'type'},
                   {'name': 'unicod\xc3\xa9',
                    'hash': 'ffff',
                    'bytes': 1000,
                    'last_modified': 'date',
                    'content_type': 'type'}]),
            (200, [])]
        req = swob.Request.blank(
            '/v1/AUTH_a/s3',
            environ={'__test__.status': '200 OK',
                     '__test__.body': '[]',
                     'swift.trans_id': 'id'})
        status, headers, body_iter = req.call_application(self.app)
        self.assertEqual(self.mock_shunt_swift.mock_calls, [])
        self.mock_list_s3.assert_has_calls([
            mock.call('', 10000, '', ''),
            mock.call('unicod\xc3\xa9', 10000, '', '')])
        names = body_iter.split('\n')
        self.assertEqual(['abc', 'unicod\xc3\xa9'], names)

    def test_list_container_shunt_s3_xml(self):
        elements = [{'name': 'abc',
                     'hash': 'ffff',
                     'bytes': 42,
                     'last_modified': 'date',
                     'content_type': 'type'},
                    {'name': u'unicod\xc3\xa9',
                     'hash': 'ffff',
                     'bytes': 1000,
                     'last_modified': 'date',
                     'content_type': 'type'}]
        self.mock_list_s3.side_effect = [(200, elements), (200, [])]
        req = swob.Request.blank(
            '/v1/AUTH_a/s3?format=xml',
            environ={'__test__.status': '200 OK',
                     '__test__.body': '[]',
                     'swift.trans_id': 'id'})
        status, headers, body_iter = req.call_application(self.app)
        self.assertEqual(self.mock_shunt_swift.mock_calls, [])
        self.mock_list_s3.assert_has_calls([
            mock.call('', 10000, '', ''),
            mock.call(u'unicod\xc3\xa9', 10000, '', '')])
        root = lxml.etree.fromstring(body_iter)
        context = lxml.etree.iterwalk(root, events=("start", "end"))
        element_index = 0
        cur_elem_properties = {}
        for action, elem in context:
            if action == 'end':
                if elem.tag == 'container':
                    self.assertEqual('s3', elem.get('name'))
                elif elem.tag == 'object':
                    self.assertEqual(elements[element_index],
                                     cur_elem_properties)
                    element_index += 1
                else:
                    try:
                        int_value = int(elem.text)
                        cur_elem_properties[elem.tag] = int_value
                    except ValueError:
                        cur_elem_properties[elem.tag] = elem.text

    def test_list_container_shunt_s3_json(self):
        elements = [{'name': 'abc',
                     'hash': 'ffff',
                     'bytes': 42,
                     'last_modified': 'date',
                     'content_type': 'type'},
                    {'name': u'unicod\xc3\xa9',
                     'hash': 'ffff',
                     'bytes': 1000,
                     'last_modified': 'date',
                     'content_type': 'type'}]
        self.mock_list_s3.side_effect = [(200, elements), (200, [])]
        req = swob.Request.blank(
            '/v1/AUTH_a/s3?format=json',
            environ={'__test__.status': '200 OK',
                     '__test__.body': '[]',
                     'swift.trans_id': 'id'})
        status, headers, body_iter = req.call_application(self.app)
        self.assertEqual(self.mock_shunt_swift.mock_calls, [])
        self.mock_list_s3.assert_has_calls([
            mock.call('', 10000, '', ''),
            mock.call(u'unicod\xc3\xa9', 10000, '', '')])
        results = json.loads(body_iter)
        for i, entry in enumerate(results):
            self.assertEqual(elements[i], entry)

    @mock.patch('s3_sync.shunt.create_provider')
    def test_list_container_shunt_all_containers(self, create_mock):
        create_mock.return_value = mock.Mock()
        create_mock.return_value.list_objects.return_value = (200, [])
        req = swob.Request.blank(
            '/v1/AUTH_b/s3',
            environ={'__test__.status': '200 OK',
                     '__test__.body': '[]',
                     'swift.trans_id': 'id'})
        status, headers, body_iter = req.call_application(self.app)
        create_mock.assert_called_once_with({
            'account': 'AUTH_b',
            'container': 's3',
            'propagate_delete': False,
            'aws_bucket': 'dest-bucket',
            'aws_identity': 'user',
            'aws_secret': 'key'}, max_conns=1, per_account=True)

        # Follow it up with another request to a *different* container to make
        # sure we didn't bleed state
        create_mock.reset_mock()
        req = swob.Request.blank(
            '/v1/AUTH_b/s4',
            environ={'__test__.status': '200 OK',
                     '__test__.body': '[]',
                     'swift.trans_id': 'id'})
        status, headers, body_iter = req.call_application(self.app)
        create_mock.assert_called_once_with({
            'account': 'AUTH_b',
            'container': 's4',
            'propagate_delete': False,
            'aws_bucket': 'dest-bucket',
            'aws_identity': 'user',
            'aws_secret': 'key'}, max_conns=1, per_account=True)

    def test_list_container_shunt_swift(self):
        self.mock_list_swift.side_effect = [
            (200, [{'name': 'abc',
                    'hash': 'ffff',
                    'bytes': 42,
                    'last_modified': 'date',
                    'content_type': 'type'},
                   {'name': u'unicod\xe9',
                    'hash': 'ffff',
                    'bytes': 1000,
                    'last_modified': 'date',
                    'content_type': 'type'}]),
            (200, [])]
        req = swob.Request.blank(
            '/v1/AUTH_a/sw\xc3\xa9ft',
            environ={'__test__.status': '200 OK',
                     '__test__.body': '[]',
                     'swift.trans_id': 'id'})
        status, headers, body_iter = req.call_application(self.app)
        self.assertEqual(self.mock_shunt_swift.mock_calls, [])
        self.mock_list_swift.assert_has_calls([
            mock.call('', 10000, '', ''),
            mock.call(u'unicod\xe9', 10000, '', '')])
        names = body_iter.split('\n')
        self.assertEqual(['abc', u'unicod\xe9'], names)
