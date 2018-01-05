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

import eventlet
import eventlet.pools
eventlet.patcher.monkey_patch(all=True)

import datetime
import errno
import json
import logging
import os
import time
import traceback

from container_crawler.utils import create_internal_client
from .daemon_utils import setup_context
from .provider_factory import create_provider
from .utils import convert_to_local_headers, SWIFT_TIME_FMT
from swift.common.internal_client import UnexpectedResponse
from swift.common.utils import FileLikeIter


def equal_migration(left, right):
    key_diff = set(left.keys()) ^ set(right.keys())
    if key_diff:
        if len(key_diff) > 1 or key_diff.pop() != 'status':
            return False
    for k in left.keys():
        if k == 'status':
            continue
        if left[k] != right[k]:
            return False
    return True


def cmp_object_entries(left, right):
    local_time = datetime.datetime.strptime(
        left['last_modified'], SWIFT_TIME_FMT)
    remote_time = datetime.datetime.strptime(
        right['last_modified'], SWIFT_TIME_FMT)
    if local_time == remote_time:
        if left['hash'] == right['hash']:
            return 0
        raise RuntimeError('Same time objects have different ETags!')
    return cmp(local_time, remote_time)


class Status(object):
    def __init__(self, status_location):
        self.status_location = status_location
        self.status_list = None

    def get_migration(self, migration):
        if not self.status_list:
            try:
                with open(self.status_location) as fh:
                    self.status_list = json.load(fh)
            except IOError as e:
                if e.errno == errno.ENOENT:
                    self.status_list = []
                else:
                    raise
        for entry in self.status_list:
            if equal_migration(entry, migration):
                return entry.get('status', {})
        return {}

    def save_migration(self, migration, marker, moved_count, scanned_count,
                       stats_reset=False):
        for entry in self.status_list:
            if equal_migration(entry, migration):
                if 'status' not in entry:
                    entry['status'] = {}
                status = entry['status']
                break
        else:
            entry = dict(migration)
            entry['status'] = {}
            self.status_list.append(entry)
            status = entry['status']

        status['marker'] = marker
        if not stats_reset:
            status['moved_count'] = status.get('moved_count', 0) + moved_count
            status['scanned_count'] = status.get('scanned_count', 0) + \
                scanned_count
        else:
            status['moved_count'] = moved_count
            status['scanned_count'] = scanned_count
        try:
            with open(self.status_location, 'w') as fh:
                fh.truncate()
                json.dump(self.status_list, fh)
        except IOError as e:
            if e.errno == errno.ENOENT:
                os.mkdir(os.path.dirname(self.status_location), mode=0755)
                with open(self.status_location, 'w') as fh:
                    json.dump(self.status_list, fh)
            else:
                raise


class GreenStack(object):
    def __init__(self):
        self.semaphore = eventlet.semaphore.Semaphore()
        self.items = []
        self.done_sem = eventlet.semaphore.Semaphore()
        self.outstanding = 0
        self.put_sem = eventlet.semaphore.Semaphore(0)

    def put(self, value):
        with self.semaphore:
            if not self.done_sem.locked():
                self.done_sem.acquire()
            self.put_sem.release()
            self.outstanding += 1
            self.items.append(value)

    def pop(self):
        self.put_sem.acquire()
        with self.semaphore:
            return self.items.pop()

    def task_done(self):
        with self.semaphore:
            self.outstanding -= 1
            if self.outstanding == 0:
                self.done_sem.release()

    def join(self):
        self.done_sem.acquire()
        self.done_sem.release()


class Migrator(object):
    '''List and move objects from a remote store into the Swift cluster'''
    def __init__(self, config, status, work_chunk, swift_pool, logger):
        self.config = dict(config)
        if 'container' not in self.config:
            # NOTE: in the future this may no longer be true, as we may allow
            # remapping buckets/containers during migrations.
            self.config['container'] = self.config['aws_bucket']
        self.status = status
        self.work_chunk = work_chunk
        self.max_conns = swift_pool.max_size
        self.object_queue = eventlet.queue.Queue(self.max_conns * 2)
        self.slos = GreenStack()
        self.ic_pool = swift_pool
        self.errors = eventlet.queue.Queue()
        self.workers = config.get('workers', 10)
        self.logger = logger

    def next_pass(self):
        if self.config['aws_bucket'] != '/*':
            self.provider = create_provider(
                self.config, self.max_conns, False)
            self._next_pass()
            return

        self.config['container'] = '.'
        self.provider = create_provider(
            self.config, self.max_conns, False)
        containers = self.provider.list_buckets()
        for container in containers:
            # NOTE: we cannot remap container names when migrating all
            # containers
            # TODO: avoid instantiating the class for every container by
            # recording that we should trawl all of them and changing the
            # provider's container attribute.
            self.config['aws_bucket'] = container['name']
            self.config['container'] = container['name']
            self.provider = create_provider(
                self.config, self.max_conns, False)
            self._next_pass()

    def _next_pass(self):
        is_reset, keys, local_marker = self._list_source_objects()

        worker_pool = eventlet.GreenPool(self.workers)
        for _ in xrange(self.workers):
            worker_pool.spawn_n(self._upload_worker)
        moved = self._find_missing_objects(keys, local_marker)
        self.object_queue.join()
        self._stop_workers(self.object_queue)

        for _ in xrange(self.workers):
            worker_pool.spawn_n(self._slo_upload_worker)
        self.slos.join()
        self._stop_workers(self.slos)

        self.check_errors()

        marker = keys[-1]['name']
        moved -= self.errors.qsize()
        # TODO: record the number of errors, as well
        self.status.save_migration(
            self.config, marker, moved, len(keys), is_reset)

    def check_errors(self):
        while not self.errors.empty():
            container, key, err = self.errors.get()
            self.logger.error('Failed to migrated %s/%s: %s' % (
                container, key, err))

    def _stop_workers(self, q):
        for _ in range(self.workers):
            q.put(None)
        q.join()

    def _list_source_objects(self):
        state = self.status.get_migration(self.config)
        reset = False
        _, keys = self.provider.list_objects(
            state.get('marker', ''),
            self.work_chunk,
            self.config.get('prefix'))
        if not keys and state.get('marker'):
            reset = True
            _, keys = self.provider.list_objects(
                None, self.work_chunk, self.config.get('prefix'))
        if not reset:
            local_marker = state.get('marker', '')
        else:
            local_marker = ''
        return reset, keys, local_marker

    def _create_container(self, container, internal_client, timeout=1):
        internal_client.create_container(self.config['account'], container)
        start = time.time()
        while time.time() - start < timeout:
            if not internal_client.container_exists(
                    self.config['account'], container):
                time.sleep(0.1)
            else:
                self.logger.debug('Created container %s' % container)
                return
        raise RuntimeError('Timeout while creating container %s' % container)

    def _find_missing_objects(self, source_list, marker):
        moved = 0
        with self.ic_pool.item() as ic:
            try:
                local_iter = ic.iter_objects(self.config['account'],
                                             self.config['container'],
                                             marker=marker)
                local = next(local_iter, None)
                if not local:
                    if not ic.container_exists(self.config['account'],
                                               self.config['container']):
                        self._create_container(self.config['container'], ic)
            except UnexpectedResponse as e:
                if e.resp.status_int != 404:
                    raise
                self._create_container(self.config['container'], ic)

            index = 0
            while index < len(source_list):
                # NOTE: the listing from the given marker may return fewer than
                # the number of items we should process. We will process all of
                # the keys that were returned in the listing and restart on the
                # following iteration.
                if not local or local['name'] > source_list[index]['name']:
                    self.object_queue.put((
                        self.config['container'], source_list[index]['name']))
                    index += 1
                    moved += 1
                elif local['name'] < source_list[index]:
                    local = next(local_iter, None)
                else:
                    cmp_ret = cmp_object_entries(local, source_list[index])
                    if cmp_ret < 0:
                        self.object_queue.put((self.config['container'],
                                               source_list[index]['name']))
                        moved += 1
                    local = next(local_iter, None)
                    index += 1
        return moved

    def _migrate_object(self, container, key):
        resp = self.provider.get_object(
            key, container=container, resp_chunk_size=65536)
        put_headers = convert_to_local_headers(resp.headers.items())
        if 'x-static-large-object' in resp.headers:
            # We have to move the segments and then move the manifest file
            resp.body.close()
            self._migrate_slo(container, key, put_headers)
        else:
            self._upload_object(
                container, key, FileLikeIter(resp.body), put_headers)

    def _migrate_slo(self, slo_container, key, headers):
        manifest = self.provider.get_manifest(key)
        for entry in manifest:
            container, segment_key = entry['name'][1:].split('/', 1)
            # TODO: check if the segment already exists
            self.object_queue.put((container, segment_key))
        manifest_blob = json.dumps(manifest)
        headers['Content-Length'] = len(manifest_blob)
        self.slos.put((slo_container, key, manifest_blob, headers))

    def _upload_object(self, container, key, content, headers):
        with self.ic_pool.item() as ic:
            try:
                ic.upload_object(
                    content, self.config['account'], container, key,
                    headers)
                self.logger.debug('Copied %s/%s' % (container, key))
            except UnexpectedResponse as e:
                if e.resp.status_int != 404:
                    raise
                self._create_container(container, ic)
                ic.upload_object(
                    content, self.config['account'], container, key,
                    headers)
                self.logger.debug('Copied %s/%s' % (container, key))

    def _upload_worker(self):
        while True:
            work = self.object_queue.get()
            try:
                if not work:
                    break
                container, key = work
                self._migrate_object(container, key)
            except Exception as e:
                self.errors.put((container, key, e))
            finally:
                self.object_queue.task_done()

    def _slo_upload_worker(self):
        while True:
            work = self.slos.pop()
            try:
                if not work:
                    break
                container, key, manifest, headers = work
                self._upload_object(
                    container, key, FileLikeIter(manifest), headers)
            except Exception as e:
                self.errors.put((container, key, e))
            finally:
                self.slos.task_done()


def main():
    args, conf = setup_context(
        description='Daemon to migrate objects into Swift',
        logger_name='swift-s3-migrator')

    logger = logging.getLogger('swift-s3-migrator')

    if 'migrator_settings' not in conf:
        print 'Missing migrator settings section'
        exit(-1)

    migrator_conf = conf['migrator_settings']
    if 'status_file' not in migrator_conf:
        print 'Missing status file location!'
        exit(-1 * errno.ENOENT)

    migration_status = Status(migrator_conf['status_file'])

    pool_size = migrator_conf.get('workers', 10)
    swift_dir = conf.get('swift_dir', '/etc/swift')
    internal_pool = eventlet.pools.Pool(
        create=lambda: create_internal_client(conf, swift_dir),
        min_size=0,
        max_size=pool_size)

    while True:
        for migration in conf.get('migrations'):
            # TODO: support splitting up the work across the nodes -- we can
            # assume all container nodes for now, but maybe have a configurable
            # count and ID?
            try:
                migrator = Migrator(migration, migration_status,
                                    migrator_conf['items_chunk'],
                                    internal_pool, logger)
                migrator.next_pass()
            except Exception as e:
                logger.error('Migration error: %r\n%s' % (
                    e, traceback.format_exc(e)))
        if args.once:
            break


if __name__ == '__main__':
    main()
