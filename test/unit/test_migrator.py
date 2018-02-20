"""
Copyright 2017 SwiftStack

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
from contextlib import contextmanager
import datetime
import errno
import itertools
import json
import logging
import mock
from s3_sync.base_sync import ProviderResponse
import s3_sync.migrator
from StringIO import StringIO
from swift.common.internal_client import UnexpectedResponse
from swift.common.utils import Timestamp
import time
import unittest


def create_timestamp(epoch_ts):
    dt = datetime.datetime.utcfromtimestamp(epoch_ts)
    return dt.strftime(s3_sync.migrator.LAST_MODIFIED_FMT) + ' UTC'


def create_list_timestamp(epoch_ts):
    dt = datetime.datetime.utcfromtimestamp(epoch_ts)
    return dt.strftime(s3_sync.utils.SWIFT_TIME_FMT)


class TestMigratorUtils(unittest.TestCase):
    def test_migration_comparison(self):
        test_cases = [
            ({'account': 'AUTH_account',
              'aws_bucket': 'bucket',
              'aws_identity': 'id',
              'aws_credential': 'secret'},
             {'account': 'AUTH_account',
              'aws_bucket': 'bucket',
              'aws_identity': 'id',
              'aws_credential': 'secret',
              'status': {'moved': 100,
                         'scanned': 200}},
             True),
            ({'account': 'AUTH_account',
              'aws_bucket': 'bucket',
              'aws_identity': 'id',
              'aws_credential': 'secret'},
             {'account': 'AUTH_account',
              'aws_bucket': 'other_bucket',
              'aws_identity': 'id',
              'aws_credential': 'secret',
              'status': {'moved': 100,
                         'scanned': 200}},
             False),
            ({'account': 'AUTH_account',
              'aws_bucket': 'bucket',
              'aws_identity': 'id',
              'aws_credential': 'secret'},
             {'account': 'AUTH_account',
              'aws_bucket': 'bucket',
              'aws_identity': 'id',
              'aws_credential': 'secret',
              'aws_endpoint': 'http://s3-clone',
              'status': {'moved': 100,
                         'scanned': 200}},
             False)]

        for left, right, expected in test_cases:
            self.assertEqual(
                expected, s3_sync.migrator.equal_migration(left, right))

    def test_listing_comparison(self):
        test_cases = [
            ({'last_modified': '2000-01-01T00:00:00.00000',
              'hash': 'deadbeef'},
             {'last_modified': '2000-01-01T00:00:00.00000',
              'hash': 'deadbeef'},
             0),
            ({'last_modified': '2000-01-01T00:00:00.00000',
              'hash': 'deadbeef'},
             {'last_modified': '1999-12-31T11:59:59.99999',
              'hash': 'deadbeef'},
             1),
            ({'last_modified': '2000-01-01T00:00:00.00000',
              'hash': 'deadbeef'},
             {'last_modified': '2000-01-01T00:00:00.00000',
              'hash': 'beefdead'},
             s3_sync.migrator.MigrationError),
            ({'last_modified': '2000-01-01T00:00:00.00000',
              'hash': 'deadbeef'},
             {'last_modified': '2000-01-01T00:00:00.00001',
              'hash': 'deadbeef'},
             -1),
        ]
        for left, right, expected in test_cases:
            if type(expected) == int:
                self.assertEqual(
                    expected, s3_sync.migrator.cmp_object_entries(left, right))
            else:
                with self.assertRaises(expected):
                    s3_sync.migrator.cmp_object_entries(left, right)

    @mock.patch('s3_sync.migrator.open')
    def test_status_get(self, mock_open):
        mock_file = mock.Mock()
        mock_file.__enter__ = lambda *args: mock_file
        mock_file.__exit__ = lambda *args: None
        mock_open.return_value = mock_file

        test_cases = [
            [{'aws_bucket': 'testbucket',
              'aws_endpoint': '',
              'aws_identity': 'identity',
              'aws_secret': 'secret',
              'account': 'AUTH_test',
              'protocol': 's3',
              'status': {
                  'moved_count': 10,
                  'scanned_count': 20}}],
            [{'aws_bucket': 'testbucket',
              'aws_endpoint': 'http://swift',
              'aws_identity': 'identity',
              'aws_secret': 'secret',
              'account': 'AUTH_test',
              'protocol': 'swift',
              'status': {
                  'moved_count': 123,
                  'scanned_count': 100}}],
        ]
        for test in test_cases:
            mock_file.reset_mock()
            mock_file.read.return_value = json.dumps(test)

            status = s3_sync.migrator.Status('/fake/location')
            self.assertEqual({}, status.get_migration(
                {'aws_identity': '',
                 'aws_secret': ''}))
            for migration in test:
                query = dict(migration)
                del query['status']
                self.assertEqual(
                    migration['status'], status.get_migration(query))

    @mock.patch('s3_sync.migrator.open')
    def test_status_get_missing(self, mock_open):
        mock_open.side_effect = IOError(errno.ENOENT, 'missing file')

        status = s3_sync.migrator.Status('/fake/location')
        self.assertEqual({}, status.get_migration({}))

    @mock.patch('s3_sync.migrator.open')
    def test_status_get_errors(self, mock_open):
        mock_open.side_effect = IOError(errno.EPERM, 'denied')

        status = s3_sync.migrator.Status('/fake/location')
        with self.assertRaises(IOError) as e:
            status.get_migration({})
        self.assertEqual(errno.EPERM, e.exception.errno)

    @mock.patch('s3_sync.migrator.open')
    def test_status_save(self, mock_open):
        mock_file = mock.Mock()
        mock_file.__enter__ = lambda *args: mock_file
        mock_file.__exit__ = lambda *args: None
        mock_open.return_value = mock_file
        start = int(time.time()) + 1

        test_cases = [
            ([{'aws_bucket': 'testbucket',
               'aws_endpoint': '',
               'aws_identity': 'identity',
               'aws_secret': 'secret',
               'account': 'AUTH_test',
               'protocol': 's3',
               'status': {
                   'finished': start - 1,
                   'moved_count': 10,
                   'scanned_count': 20}}],
             {'marker': 'marker', 'moved_count': 1000, 'scanned_count': 1000,
              'reset_stats': False},
             {'finished': start, 'moved_count': 1010, 'scanned_count': 1020}),
            ([{'aws_bucket': 'testbucket',
               'aws_endpoint': '',
               'aws_identity': 'identity',
               'aws_secret': 'secret',
               'account': 'AUTH_test',
               'protocol': 's3',
               'status': {
                   'finished': start - 1,
                   'moved_count': 10,
                   'scanned_count': 20}}],
             {'marker': 'marker', 'moved_count': 1000,
              'scanned_count': 1000, 'reset_stats': True},
             {'finished': start, 'moved_count': 1000, 'scanned_count': 1000,
              'last_finished': start - 1, 'last_moved_count': 10,
              'last_scanned_count': 20}),
            ([],
             {'marker': 'marker', 'finished': start, 'moved_count': 1000,
              'scanned_count': 1000, 'reset_stats': False},
             {'finished': start, 'moved_count': 1000, 'scanned_count': 1000}),
        ]
        for status_list, test_params, write_status in test_cases:
            mock_file.reset_mock()
            status = s3_sync.migrator.Status('/fake/location')
            status.status_list = status_list

            if not status_list:
                migration = {}
            else:
                migration = dict(status_list[0])

            with mock.patch('time.time') as mock_time:
                mock_time.return_value = start
                status.save_migration(
                    migration, test_params['marker'],
                    test_params['moved_count'],
                    test_params['scanned_count'],
                    test_params['reset_stats'])
            write_status['marker'] = test_params['marker']
            migration['status'] = write_status
            # gets the 1st argument in the call argument list
            written = ''.join([call[1][0] for call in
                               mock_file.write.mock_calls])
            self.assertEqual(json.loads(written), [migration])

    @mock.patch('s3_sync.migrator.os.mkdir')
    @mock.patch('s3_sync.migrator.open')
    def test_status_save_create(self, mock_open, mock_mkdir):
        start = int(time.time()) + 1
        mock_file = mock.Mock()
        mock_file.__enter__ = lambda *args: mock_file
        mock_file.__exit__ = lambda *args: None
        mock_open.side_effect = [IOError(errno.ENOENT, 'not found'), mock_file]

        status = s3_sync.migrator.Status('/fake/location')
        status.status_list = []
        with mock.patch('time.time') as mock_time:
            mock_time.return_value = start
            status.save_migration({}, 'marker', 100, 100, False)
        mock_mkdir.assert_called_once_with('/fake', mode=0755)
        written = ''.join([call[1][0] for call in mock_file.write.mock_calls])
        self.assertEqual(json.loads(written),
                         [{'status': {
                             'marker': 'marker', 'moved_count': 100,
                             'scanned_count': 100, 'finished': start}}])

    @mock.patch('s3_sync.migrator.os.mkdir')
    @mock.patch('s3_sync.migrator.open')
    def test_status_save_create_raises(self, mock_open, mock_mkdir):
        mock_file = mock.Mock()
        mock_file.__enter__ = lambda *args: mock_file
        mock_file.__exit__ = lambda *args: None
        mock_open.side_effect = IOError(errno.ENOENT, 'not found')
        mock_mkdir.side_effect = IOError(errno.EPERM, 'denied')

        status = s3_sync.migrator.Status('/fake/location')
        status.status_list = []
        with self.assertRaises(IOError) as cm:
            status.save_migration({}, 'marker', 100, 100, False)
        mock_mkdir.assert_called_once_with('/fake', mode=0755)
        self.assertEqual(errno.EPERM, cm.exception.errno)

    @mock.patch('s3_sync.migrator.open')
    def test_status_save_raises(self, mock_open):
        mock_open.side_effect = IOError(errno.EPERM, 'denied')
        status = s3_sync.migrator.Status('/fake/location')
        status.status_list = []
        with self.assertRaises(IOError) as err:
            status.save_migration({}, 'marker', 100, 100, False)
        self.assertEqual(errno.EPERM, err.exception.errno)


class TestMigrator(unittest.TestCase):
    def setUp(self):
        config = {'aws_bucket': 'bucket',
                  'account': 'AUTH_test'}
        self.swift_client = mock.Mock()
        pool = mock.Mock()
        pool.item.return_value.__enter__ = lambda *args: self.swift_client
        pool.item.return_value.__exit__ = lambda *args: None
        pool.max_size = 11
        self.migrator = s3_sync.migrator.Migrator(
            config, None, 1000, pool, None, 0, 1)
        self.migrator.logger = mock.Mock()
        self.migrator.status = mock.Mock()

    @mock.patch('s3_sync.migrator.create_provider')
    def test_single_container(self, create_provider_mock):
        self.migrator._next_pass = mock.Mock()
        self.migrator.next_pass()
        create_provider_mock.assert_called_once_with(
            {'aws_bucket': 'bucket', 'container': 'bucket',
             'account': 'AUTH_test', 'native': True},
            self.migrator.ic_pool.max_size, False)
        self.migrator._next_pass.assert_called_once_with()

    @mock.patch('s3_sync.migrator.create_provider')
    def test_all_containers(self, create_provider_mock):
        provider_mock = mock.Mock()
        buckets = [{'name': 'bucket'}]
        provider_mock.list_buckets.return_value = ProviderResponse(
            True, 200, [], buckets)

        def check_provider(config, conns, per_account):
            # We have to check the arguments this way, as otherwise the
            # dictionary gets mutated and assert_called_once_with check will
            # fail.
            self.assertEqual('/*', config['aws_bucket'])
            self.assertEqual('.', config['container'])
            self.assertEqual(self.migrator.ic_pool.max_size, conns)
            return provider_mock

        create_provider_mock.side_effect = check_provider
        self.migrator.config = {'aws_bucket': '/*'}
        self.migrator._next_pass = mock.Mock()
        self.migrator.next_pass()
        self.assertEqual(buckets[0]['name'], self.migrator.config['container'])
        self.assertEqual(buckets[0]['name'],
                         self.migrator.config['aws_bucket'])
        self.assertEqual(buckets[0]['name'], provider_mock.aws_bucket)

    @mock.patch('s3_sync.migrator.create_provider')
    def test_list_buckets_error(self, create_provider_mock):
        create_provider_mock.return_value.list_buckets.return_value = \
            ProviderResponse(False, 404, [], 'Not Found')
        self.migrator.config = {'aws_bucket': '/*'}
        with self.assertRaises(s3_sync.migrator.MigrationError):
            self.migrator.next_pass()
        self.assertEqual({'aws_bucket': '/*', 'container': '.'},
                         self.migrator.config)
        create_provider_mock.assert_called_once_with(
            {'aws_bucket': '/*', 'container': '.'},
            self.migrator.ic_pool.max_size, False)

    @mock.patch('s3_sync.migrator.create_provider')
    def test_migrate_objects(self, create_provider_mock):
        provider = create_provider_mock.return_value
        self.migrator.status.get_migration.return_value = {}

        tests = [{
            'objects': {
                'foo': {
                    'remote_headers': {
                        'x-object-meta-custom': 'custom',
                        'last-modified': create_timestamp(1.5e9)},
                    'expected_headers': {
                        'x-object-meta-custom': 'custom',
                        'x-timestamp': Timestamp(1.5e9).internal},
                    'list-time': create_list_timestamp(1.5e9),
                    'hash': 'etag'
                },
                'bar': {
                    'remote_headers': {
                        'x-object-meta-custom': 'custom',
                        'last-modified': create_timestamp(1.4e9)},
                    'expected_headers': {
                        'x-object-meta-custom': 'custom',
                        'x-timestamp': Timestamp(1.4e9).internal},
                    'list-time': create_list_timestamp(1.4e9),
                    'hash': 'etag'
                }
            },
        }, {
            'objects': {
                'foo': {
                    'remote_headers': {
                        'x-object-meta-custom': 'custom',
                        'last-modified': create_timestamp(1.5e9)},
                    'expected_headers': {
                        'x-object-meta-custom': 'custom',
                        'x-timestamp': Timestamp(1.5e9).internal},
                    'list-time': create_list_timestamp(1.5e9),
                    'hash': 'etag'
                },
                'bar': {
                    'remote_headers': {
                        'x-object-meta-custom': 'custom',
                        'last-modified': create_timestamp(1.4e9)},
                    'expected_headers': {
                        'x-object-meta-custom': 'custom',
                        'x-timestamp': Timestamp(1.4e9).internal},
                    'list-time': create_list_timestamp(1.4e9),
                    'hash': 'etag'
                }
            },
            'config': {
                'aws_bucket': 'container',
                'protocol': 'swift'
            }
        }, {
            'objects': {
                'foo': {
                    'remote_headers': {
                        'x-object-meta-custom': 'custom',
                        'last-modified': create_timestamp(1.5e9)},
                    'expected_headers': {
                        'x-object-meta-custom': 'custom',
                        'x-timestamp': Timestamp(1.5e9).internal},
                    'list-time': create_list_timestamp(1.5e9),
                    'hash': 'etag'
                },
                'bar': {
                    'remote_headers': {
                        'x-object-meta-custom': 'custom',
                        'last-modified': create_timestamp(1.4e9)},
                    'expected_headers': {
                        'x-object-meta-custom': 'custom',
                        'x-timestamp': Timestamp(1.4e9).internal},
                    'list-time': create_list_timestamp(1.4e9),
                    'hash': 'etag'
                }
            },
            'config': {
                'aws_bucket': 'container',
                'protocol': 'swift'
            },
            'local_objects': [
                {'name': 'foo',
                 'last_modified': create_list_timestamp(1.5e9),
                 'hash': 'etag'},
                {'name': 'bar',
                 'last_modified': create_list_timestamp(1.4e9),
                 'hash': 'etag'}
            ],
            'migrated': []
        }, {
            'objects': {
                'foo': {
                    'remote_headers': {
                        'x-object-meta-custom': 'custom',
                        'last-modified': create_timestamp(1.5e9)},
                    'expected_headers': {
                        'x-object-meta-custom': 'custom',
                        'x-timestamp': Timestamp(1.5e9).internal},
                    'list-time': create_list_timestamp(1.5e9),
                    'hash': 'etag'
                },
                'bar': {
                    'remote_headers': {
                        'x-object-meta-custom': 'custom',
                        'last-modified': create_timestamp(1.4e9)},
                    'expected_headers': {
                        'x-object-meta-custom': 'custom',
                        'x-timestamp': Timestamp(1.4e9).internal},
                    'list-time': create_list_timestamp(1.4e9),
                    'hash': 'etag'
                }
            },
            'config': {
                'aws_bucket': 'container',
                'protocol': 'swift'
            },
            'local_objects': [
                {'name': 'foo',
                 'last_modified': create_list_timestamp(1.4e9),
                 'hash': 'old-etag'},
                {'name': 'bar',
                 'last_modified': create_list_timestamp(1.3e9),
                 'hash': 'old-etag'}
            ],
            'migrated': ['foo', 'bar']
        }]
        config = self.migrator.config

        for test in tests:
            objects = test['objects']
            if 'migrated' not in test:
                migrated = objects.keys()
            else:
                migrated = test['migrated']
            test_config = dict(config)
            for k, v in test.get('config', {}).items():
                test_config[k] = v
                if k == 'aws_bucket':
                    test_config['container'] = v
            self.migrator.config = test_config

            provider.reset_mock()
            self.swift_client.reset_mock()
            self.swift_client.container_exists.return_value = True

            local_objects = test.get('local_objects', [])

            def get_object(name, **args):
                if name not in objects.keys():
                    raise RuntimeError('Unknown object: %s' % name)
                if name not in migrated:
                    raise RuntimeError('Object should not be moved %s' % name)
                return ProviderResponse(
                    True, 200, objects[name]['remote_headers'],
                    StringIO('object body'))

            provider.list_objects.return_value = (
                200, [{'name': name,
                       'last_modified': objects[name]['list-time'],
                       'hash': objects[name]['hash']}
                      for name in objects.keys()])
            provider.get_object.side_effect = get_object
            self.swift_client.iter_objects.return_value = iter(local_objects)

            self.migrator.next_pass()

            self.swift_client.upload_object.assert_has_calls(
                [mock.call(mock.ANY, self.migrator.config['account'],
                           self.migrator.config['aws_bucket'], name,
                           objects[name]['expected_headers'])
                 for name in migrated])

            for call in self.swift_client.upload_object.mock_calls:
                self.assertEqual('object body', ''.join(call[1][0]))

    @mock.patch('s3_sync.migrator.create_provider')
    def test_migrate_objects_reset(self, create_provider_mock):
        provider = create_provider_mock.return_value

        self.migrator.status = mock.Mock()
        self.migrator.status.get_migration.return_value = {'marker': 'zzz'}
        provider.list_objects.return_value = (200, [])
        self.swift_client.iter_objects.return_value = iter([])

        self.migrator.next_pass()
        provider.list_objects.assert_has_calls(
            [mock.call('zzz', self.migrator.work_chunk, None),
             mock.call(None, self.migrator.work_chunk, None)])

    @mock.patch('s3_sync.migrator.create_provider')
    def test_missing_container(self, create_provider_mock):
        tests = [{'protocol': 'swift'}, {}]
        provider = create_provider_mock.return_value
        config = self.migrator.config

        for test in tests:
            test_config = dict(config, **test)
            self.migrator.config = test_config

            provider.reset_mock()
            self.swift_client.reset_mock()
            provider.list_objects.return_value = (200, [{'name': 'test'}])
            provider.get_object.return_value = ProviderResponse(
                True, 200, {'last-modified': create_timestamp(1.5e9)},
                StringIO(''))

            if self.migrator.config.get('protocol') == 'swift':
                resp = mock.Mock()
                resp.status = 200
                resp.headers = {'x-container-meta-foo': 'foo'}
                provider.head_bucket.return_value = resp
                headers = resp.headers
            else:
                headers = {}

            self.swift_client.iter_objects.return_value = iter([])
            self.swift_client.container_exists.side_effect = (False, True)

            self.migrator.status.get_migration.return_value = {}

            self.migrator.next_pass()
            if test.get('protocol') == 'swift':
                provider.list_objects.assert_called_once_with(
                    None, self.migrator.work_chunk, None)
                provider.head_bucket.assert_called_once_with(
                    self.migrator.config['container'])
            else:
                provider.list_objects.assert_called_once_with(
                    None, self.migrator.work_chunk, None)
            self.swift_client.create_container.assert_called_once_with(
                self.migrator.config['account'],
                self.migrator.config['container'], headers)

    @mock.patch('s3_sync.migrator.create_provider')
    def test_head_container_error(self, create_provider_mock):
        self.migrator.config['protocol'] = 'swift'

        provider = create_provider_mock.return_value
        provider.list_objects.return_value = (200, [{'name': 'test'}])

        resp = mock.Mock()
        resp.status = 404
        resp.headers = {}
        provider.head_bucket.return_value = resp
        self.swift_client.iter_objects.return_value = iter([])
        self.swift_client.container_exists.return_value = False

        self.migrator.status.get_migration.return_value = {}

        with self.assertRaises(s3_sync.migrator.MigrationError):
            self.migrator.next_pass()
        provider.head_bucket.assert_called_once_with(
            self.migrator.config['container'])

    @mock.patch('s3_sync.migrator.time')
    @mock.patch('s3_sync.migrator.create_provider')
    def test_create_container_timeout(self, create_provider_mock, time_mock):
        provider = create_provider_mock.return_value
        provider.list_objects.return_value = (200, [{'name': 'test'}])

        resp = mock.Mock()
        resp.status = 200
        resp.headers = {}
        provider.head_bucket.return_value = resp
        self.swift_client.iter_objects.return_value = iter([])
        self.swift_client.container_exists.return_value = False

        time_mock.time.side_effect = (0, 0, 1)
        self.migrator.status.get_migration.return_value = {}

        with self.assertRaises(s3_sync.migrator.MigrationError) as e:
            self.migrator.next_pass()
            self.assertEqual('Timeout', e.msg.split()[0])
        self.swift_client.create_container.assert_called_once_with(
            self.migrator.config['account'], self.migrator.config['container'],
            {})
        self.swift_client.container_exists.assert_has_calls(
            [mock.call(self.migrator.config['account'],
                       self.migrator.config['container'])] * 2)

    @mock.patch('s3_sync.migrator.create_provider')
    def test_migrate_slo(self, create_provider_mock):
        self.migrator.config['protocol'] = 'swift'
        provider = create_provider_mock.return_value
        self.migrator.status.get_migration.return_value = {}
        segments_container = '/slo-segments'

        manifest = [{'name': '/'.join([segments_container, 'part1'])},
                    {'name': '/'.join([segments_container, 'part2'])}]

        objects = {
            'slo': {
                'remote_headers': {
                    'x-object-meta-custom': 'slo-meta',
                    'last-modified': create_timestamp(1.5e9),
                    'x-static-large-object': True},
                'expected_headers': {
                    'x-object-meta-custom': 'slo-meta',
                    'x-timestamp': Timestamp(1.5e9).internal,
                    'x-static-large-object': True,
                    'Content-Length': len(json.dumps(manifest))}
            },
            'part1': {
                'remote_headers': {
                    'x-object-meta-part': 'part-1',
                    'last-modified': create_timestamp(1.4e9)},
                'expected_headers': {
                    'x-object-meta-part': 'part-1',
                    'x-timestamp': Timestamp(1.4e9).internal}
            },
            'part2': {
                'remote_headers': {
                    'x-object-meta-part': 'part-2',
                    'last-modified': create_timestamp(1.1e9)},
                'expected_headers': {
                    'x-object-meta-part': 'part-2',
                    'x-timestamp': Timestamp(1.1e9).internal}
            }
        }

        containers = {segments_container: False,
                      self.migrator.config['container']: False}

        def container_exists(_, container):
            return containers[container]

        def create_container(_, container, headers):
            containers[container] = True

        def get_object(name, **args):
            if name not in objects.keys():
                raise RuntimeError('Unknown object: %s' % name)
            return ProviderResponse(
                True, 200, objects[name]['remote_headers'],
                StringIO('object body'))

        def head_object(name, container):
            if container != segments_container[1:]:
                raise RuntimeError('wrong container: %s' % container)
            if name not in objects.keys():
                raise RuntimeError('unknown object: %s' % name)
            resp = mock.Mock()
            resp.status = 200
            resp.headers = objects[name]['remote_headers']
            return resp

        self.swift_client.container_exists.side_effect = container_exists
        self.swift_client.create_container.side_effect = create_container
        swift_head_resp = mock.Mock()
        swift_head_resp.status_int = 404
        self.swift_client.get_object_metadata.side_effect = UnexpectedResponse(
            '', swift_head_resp)

        bucket_resp = mock.Mock()
        bucket_resp.status = 200
        bucket_resp.headers = {}

        provider.head_bucket.return_value = bucket_resp
        provider.list_objects.return_value = (
            200, [{'name': 'slo'}])
        provider.get_object.side_effect = get_object
        provider.head_object.side_effect = head_object
        provider.get_manifest.return_value = manifest
        self.swift_client.iter_objects.return_value = iter([])

        self.migrator.next_pass()

        self.swift_client.upload_object.assert_has_calls(
            [mock.call(mock.ANY, self.migrator.config['account'],
                       'slo-segments',
                       'part1',
                       objects['part1']['expected_headers']),
             mock.call(mock.ANY, self.migrator.config['account'],
                       'slo-segments',
                       'part2',
                       objects['part2']['expected_headers']),
             mock.call(mock.ANY, self.migrator.config['account'],
                       self.migrator.config['container'],
                       'slo',
                       objects['slo']['expected_headers'])])

        for call in self.swift_client.upload_object.mock_calls:
            body, acct, cont, obj, headers = call[1]
            if obj.startswith('part'):
                self.assertEqual(segments_container[1:], cont)
                self.assertEqual('object body', ''.join(body))
            else:
                self.assertEqual(self.migrator.config['container'], cont)
                self.assertEqual(manifest, json.loads(''.join(body)))


class TestStatus(unittest.TestCase):

    def setUp(self):
        self.start = int(time.time()) + 1
        patcher = mock.patch('time.time')
        self.addCleanup(patcher.stop)
        self.mock_time = patcher.start()
        self.mock_time.side_effect = itertools.count(self.start)

    def test_update_status_fresh(self):
        status = {}
        # initial pass
        s3_sync.migrator._update_status_counts(status, 10, 10, True)
        self.assertEqual({
            'finished': self.start,
            'moved_count': 10,
            'scanned_count': 10,
        }, status)

    def test_update_status_update(self):
        # second pass finishes
        status = {
            'finished': self.start - 1,
            'moved_count': 10,
            'scanned_count': 10,
        }
        s3_sync.migrator._update_status_counts(status, 8, 8, False)
        self.assertEqual({
            'finished': self.start,
            'moved_count': 18,
            'scanned_count': 18,
        }, status)

    def test_update_status_set_last(self):
        # next pass has nothing to move
        status = {
            'finished': self.start - 1,
            'moved_count': 18,
            'scanned_count': 18,
        }
        s3_sync.migrator._update_status_counts(status, 0, 10, True)
        self.assertEqual({
            'finished': self.start,
            'moved_count': 0,
            'scanned_count': 10,
            'last_finished': self.start - 1,
            'last_moved_count': 18,
            'last_scanned_count': 18,
        }, status)

    def test_update_status_update_current_maintains_last(self):
        # still nothing
        status = {
            'finished': self.start - 1,
            'moved_count': 0,
            'scanned_count': 10,
            'last_finished': self.start - 2,
            'last_moved_count': 18,
            'last_scanned_count': 18,
        }
        s3_sync.migrator._update_status_counts(status, 0, 8, False)
        self.assertEqual({
            'finished': self.start,
            'moved_count': 0,
            'scanned_count': 18,
            'last_finished': self.start - 2,
            'last_moved_count': 18,
            'last_scanned_count': 18,
        }, status)

    def test_update_status_finished_resets_last(self):
        # fresh run, but nothing moved and scanned matches!
        status = {
            'finished': self.start - 1,
            'moved_count': 0,
            'scanned_count': 18,
            'last_finished': self.start - 3,
            'last_moved_count': 18,
            'last_scanned_count': 18,
        }
        s3_sync.migrator._update_status_counts(status, 0, 10, True)
        self.assertEqual({
            'finished': self.start,
            'moved_count': 0,
            'scanned_count': 10,
            'last_finished': self.start - 1,
            'last_moved_count': 0,
            'last_scanned_count': 18,
        }, status)

    def test_update_status_update_with_new_move(self):
        # oh weird, something new showed up!?
        status = {
            'finished': self.start - 3,
            'moved_count': 0,
            'scanned_count': 10,
            'last_finished': self.start - 4,
            'last_moved_count': 0,
            'last_scanned_count': 18,
        }
        s3_sync.migrator._update_status_counts(status, 1, 9, False)
        self.assertEqual({
            'finished': self.start,
            'moved_count': 1,
            'scanned_count': 19,
            'last_finished': self.start - 4,
            'last_moved_count': 0,
            'last_scanned_count': 18,
        }, status)

    def test_update_status_finished_new_move_resets_last(self):
        # ok, back to borning nothing
        status = {
            'finished': self.start - 3,
            'moved_count': 1,
            'scanned_count': 19,
            'last_finished': self.start - 5,
            'last_moved_count': 0,
            'last_scanned_count': 18,
        }
        s3_sync.migrator._update_status_counts(status, 0, 10, True)
        self.assertEqual({
            'finished': self.start,
            'moved_count': 0,
            'scanned_count': 10,
            'last_finished': self.start - 3,
            'last_moved_count': 1,
            'last_scanned_count': 19,
        }, status)

    def test_update_status_finished_no_moves_resets_last(self):
        # and we're done here...
        status = {
            'finished': self.start - 5,
            'moved_count': 0,
            'scanned_count': 19,
            'last_finished': self.start - 7,
            'last_moved_count': 1,
            'last_scanned_count': 19,
        }
        s3_sync.migrator._update_status_counts(status, 0, 10, True)
        self.assertEqual({
            'finished': self.start,
            'moved_count': 0,
            'scanned_count': 10,
            'last_finished': self.start - 5,
            'last_moved_count': 0,
            'last_scanned_count': 19,
        }, status)

    def test_update_status_clean_finish_does_not_reset_last(self):
        # and we'll stay this way, indefinately...
        status = {
            'finished': self.start - 2,
            'moved_count': 0,
            'scanned_count': 19,
            'last_finished': self.start - 7,
            'last_moved_count': 0,
            'last_scanned_count': 19,
        }
        s3_sync.migrator._update_status_counts(status, 0, 10, True)
        self.assertEqual({
            'finished': self.start,
            'moved_count': 0,
            'scanned_count': 10,
            'last_finished': self.start - 7,
            'last_moved_count': 0,
            'last_scanned_count': 19,
        }, status)

    def test_update_legacy(self):
        # we start with less info
        status = {
            'moved_count': 0,
            'scanned_count': 8,
        }
        s3_sync.migrator._update_status_counts(status, 0, 8, True)
        self.assertEqual({
            'finished': self.start,
            'moved_count': 0,
            'scanned_count': 8,
        }, status)
        s3_sync.migrator._update_status_counts(status, 0, 8, True)
        # ... but we get there eventually
        self.assertEqual({
            'finished': self.start + 1,
            'moved_count': 0,
            'scanned_count': 8,
            'last_finished': self.start,
            'last_moved_count': 0,
            'last_scanned_count': 8,
        }, status)


class TestMain(unittest.TestCase):

    def setUp(self):
        self.logger = logging.getLogger()
        self.stream = StringIO()
        self.logger.addHandler(logging.StreamHandler(self.stream))
        self.conf = {
            'migrations': [],
            'migration_status': None,
            'internal_pool': None,
            'logger': self.logger,
            'items_chunk': None,
            'node_id': 0,
            'nodes': 1,
            'poll_interval': 30,
            'once': True,
        }

    @contextmanager
    def patch(self, name):
        with mock.patch('s3_sync.migrator.' + name) as mocked:
            yield mocked

    def pop_log_lines(self):
        lines = self.stream.getvalue()
        self.stream.seek(0)
        self.stream.truncate()
        return lines

    def test_run_once(self):
        start = time.time()
        with self.patch('time') as mocktime:
            mocktime.time.side_effect = [start, start + 1]
            s3_sync.migrator.run(**self.conf)
            # with once = True we don't sleep
            self.assertEqual(mocktime.sleep.call_args_list, [])
            self.assertEqual('Finished cycle in 1.00s\n',
                             self.pop_log_lines())

    def test_run_forever(self):
        start = time.time()
        self.conf['once'] = False

        class StopDeamon(Exception):
            pass

        with self.patch('process_migrations') as mock_process, \
                self.patch('time') as mocktime:
            mock_process.side_effect = [None, None, StopDeamon()]
            mocktime.time.side_effect = [start + i for i in range(5)]
            with self.assertRaises(StopDeamon):
                s3_sync.migrator.run(**self.conf)
            self.assertEqual(mocktime.sleep.call_args_list,
                             [mock.call(29)] * 2)
            self.assertEqual([
                'Finished cycle in 1.00s, sleeping for 29.00s.',
                'Finished cycle in 1.00s, sleeping for 29.00s.',
            ], self.pop_log_lines().splitlines())
