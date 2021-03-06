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

import logging
import os
import traceback

from container_crawler import ContainerCrawler
from .daemon_utils import load_swift, setup_context, setup_logger


def main():
    args, conf = setup_context()
    if args.log_level:
        conf['log_level'] = args.log_level
    conf['console'] = args.console
    logger_name = 's3-sync'
    setup_logger(logger_name, conf)
    load_swift(logger_name, args.once)

    from .sync_container import SyncContainer
    logger = logging.getLogger(logger_name)
    logger.debug('Starting S3Sync')

    if 'http_proxy' in conf:
        logger.debug('Using HTTP proxy %r', conf['http_proxy'])
        os.environ['http_proxy'] = conf['http_proxy']
    if 'https_proxy' in conf:
        logger.debug('Using HTTPS proxy %r', conf['https_proxy'])
        os.environ['https_proxy'] = conf['https_proxy']

    try:
        crawler = ContainerCrawler(conf, SyncContainer, logger)
        if args.once:
            crawler.run_once()
        else:
            crawler.run_always()
    except Exception as e:
        logger.error("S3Sync failed: %s" % repr(e))
        logger.error(traceback.format_exc(e))
        exit(1)


if __name__ == '__main__':
    main()
