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
import time
import mock
import logging
from StringIO import StringIO
from contextlib import contextmanager

import s3_sync.migrator
import unittest


class TestMigrator(unittest.TestCase):
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
