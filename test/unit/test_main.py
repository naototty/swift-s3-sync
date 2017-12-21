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

import mock
import s3_sync
import sys
import unittest


class TestMain(unittest.TestCase):

    @mock.patch('s3_sync.daemon_utils.os.path.exists')
    @mock.patch('s3_sync.daemon_utils.setup_logger')
    @mock.patch('s3_sync.__main__.ContainerCrawler')
    def test_log_lvl(self, crawler_mock, setup_logger_mock, exists_mock):
        exists_mock.return_value = True

        test_params = [
            {'conf_level': None,
             'args': [],
             'expected': 'INFO'},
            {'conf_level': 'debug',
             'args': [],
             'expected': 'DEBUG'},
            {'conf_level': 'warn',
             'args': ['--log-level', 'debug'],
             'expected': 'DEBUG'},
        ]

        try:
            # avoid loading boto3 and SyncContainer
            sys.modules['s3_sync.sync_container'] = mock.Mock()
            defaults = ['main', '--conf', '/sample/config']

            for params in test_params:
                with mock.patch('s3_sync.daemon_utils.load_config') \
                        as conf_mock, \
                        mock.patch('s3_sync.daemon_utils.sys') as sys_mock:
                    sys_mock.argv = defaults + params['args']

                    conf_mock.return_value = {}
                    if params['conf_level']:
                        conf_mock.return_value['log_level'] = \
                            params['conf_level']

                    s3_sync.__main__.main()
                    setup_logger_mock.assert_called_once_with(
                        console=False, level=params['expected'],
                        log_file=None)
                setup_logger_mock.reset_mock()
        finally:
            del sys.modules['s3_sync.sync_container']
