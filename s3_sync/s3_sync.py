import eventlet
eventlet.patcher.monkey_patch(all=True)

import logging
import os.path
import sys
import time
import traceback

from swift.common.db import DatabaseConnectionError
from swift.common.ring import Ring
from swift.common.ring.utils import is_local_device
from swift.common.utils import whataremyips, hash_path, storage_directory
from swift.container.backend import DATADIR, ContainerBroker

from .sync_container import SyncContainer


class S3Sync(object):
    def __init__(self, conf):
        self.logger = logging.getLogger('s3-sync')
        self.containers = conf.get('containers', [])
        self.root = conf['devices']
        self.interval = 10
        self.swift_dir = '/etc/swift'
        self.container_ring = Ring(self.swift_dir, ring_name='container')
        self.retries = 3

        self.status_dir = conf['status_dir']
        self.myips = whataremyips('0.0.0.0')
        self.items_chunk = conf['items_chunk']
        self.poll_interval = conf.get('poll_interval', 5)
        self._setup_workers(conf)
        self.logger.debug('Created the S3Sync instance')

    def _setup_workers(self, conf):
        self.workers = conf.get('workers', 10)
        self.pool = eventlet.GreenPool(self.workers)
        self.work_queue = eventlet.queue.Queue(self.workers * 2)

        # max_size=None means a Queue is infinite
        self.error_queue = eventlet.queue.Queue(maxsize=None)
        self.stats_queue = eventlet.queue.Queue(maxsize=None)
        for _ in range(0, self.workers):
            self.pool.spawn_n(self.worker)

    def get_broker(self, account, container, part, node):
        db_hash = hash_path(account, container)
        db_dir = storage_directory(DATADIR, part, db_hash)
        db_path = os.path.join(self.root, node['device'], db_dir,
                               db_hash + '.db')
        return ContainerBroker(db_path, account=account, container=container)

    def worker(self):
        quit = False
        while 1:
            work = None
            try:
                work = self.work_queue.get()
                if not work:
                    quit = True
                    break
                row, sync_container = work

                if row['deleted']:
                    sync_container.delete_object(row['name'])
                else:
                    sync_container.upload_object(row['name'],
                                                 row['storage_policy_index'])
            except:
                self.error_queue.put((row, traceback.format_exc()))
            finally:
                if work or quit:
                    self.work_queue.task_done()

    def check_errors(self, account, container):
        if self.error_queue.empty():
            return
        while not self.error_queue.empty():
            row, error = self.error_queue.get()
            if self.logger:
                self.logger.error('Failed to propagate object %s/%s/%s: %s' % (
                    account, container, row['name'], repr(error)))
        raise RuntimeError('Failed to sync %s/%s' % (account, container))

    def sync_items(self, sync_container, rows, nodes_count, node_id):
        object_count = 0
        start = time.time()
        for row in rows:
            if (row['ROWID'] % nodes_count) != node_id:
                continue
            object_count += 1
            self.work_queue.put((row, sync_container))
            if self.logger:
                self.logger.debug('propagating %s' % row['ROWID'])
        self.work_queue.join()
        self.logger.info("Processed %d objects in %f seconds; %d errors" %
                         (object_count, time.time() - start,
                          self.error_queue.qsize()))
        self.check_errors(sync_container.account, sync_container.container)

        # TODO: Duplicated code that should be refactored.
        object_count = 0
        start = time.time()
        for row in rows:
            # Validate that changes from all other rows have also been sync'd.
            if (row['ROWID'] % nodes_count) == node_id:
                continue
            object_count += 1
            self.work_queue.put((row, sync_container))
            if self.logger:
                self.logger.debug('verifiying %s' % row['ROWID'])
        self.work_queue.join()
        self.logger.info("Verified %d objects in %f seconds; %d errors" %
                         (object_count, time.time() - start,
                          self.error_queue.qsize()))
        self.check_errors(sync_container.account, sync_container.container)

    def sync_container(self, container):
        part, container_nodes = self.container_ring.get_nodes(
            container.account, container.container)
        nodes_count = len(container_nodes)
        for index, node in enumerate(container_nodes):
            if not is_local_device(self.myips, None, node['ip'],
                                   node['port']):
                continue
            broker = self.get_broker(container.account, container.container,
                                     part, node)
            broker_info = broker.get_info()
            start = container.load_status(broker_info['id'])
            try:
                items = broker.get_items_since(start, self.items_chunk)
            except DatabaseConnectionError:
                continue
            if items:
                self.sync_items(container, items, nodes_count, index)
                container.save_status(items[-1]['ROWID'], broker_info['id'])
            return

    def stop_pool(self):
        for _ in range(0, self.workers):
            self.work_queue.put(None)
        self.pool.waitall()

    def run_always(self):
        self.logger.debug('Entering the poll loop')
        while True:
            start = time.time()
            self.run_once()
            elapsed = time.time() - start
            if elapsed < self.poll_interval:
                time.sleep(self.poll_interval - elapsed)

    def run_once(self):
        # Since we don't support reloading, the daemon should quit if there are
        # no containers configured
        if not self.containers:
            sys.exit(0)
        for sync_settings in self.containers:
            try:
                self.logger.debug('Sync container: %s/%s' % (
                    sync_settings['account'], sync_settings['container']))
                self.sync_container(SyncContainer(self.status_dir,
                                                  sync_settings, self.workers))
            except:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                account = sync_settings.get('account', 'N/A')
                container = sync_settings.get('container', 'N/A')
                bucket = sync_settings.get('aws_bucket', 'N/A')
                self.logger.error("Failed to sync %s/%s to %s" % (
                    account, container, bucket))
                self.logger.error(traceback.format_exc())
