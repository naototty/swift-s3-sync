import eventlet
eventlet.patcher.monkey_patch(all=True)

import json
import logging
import os
import os.path
from swift.common.utils import decode_timestamps, Timestamp
from swift.common.internal_client import UnexpectedResponse
import time

import container_crawler.base_sync
from container_crawler import RetryError
from .sync_s3 import SyncS3
from .sync_swift import SyncSwift


class SyncContainer(container_crawler.base_sync.BaseSync):
    def __init__(self, status_dir, sync_settings, max_conns=10):
        super(SyncContainer, self).__init__(status_dir, sync_settings)
        self.logger = logging.getLogger('s3-sync')
        self.aws_bucket = sync_settings['aws_bucket']
        self.copy_after = int(sync_settings.get('copy_after', 0))
        self.retain_copy = sync_settings.get('retain_local', True)
        self.propagate_delete = sync_settings.get('propagate_delete', True)
        provider_type = sync_settings.get('protocol', None)
        if not provider_type or provider_type == 's3':
            self.provider = SyncS3(sync_settings, max_conns)
        elif provider_type == 'swift':
            self.provider = SyncSwift(sync_settings, max_conns)
        else:
            raise NotImplementedError()

    def get_last_row(self, db_id):
        if not os.path.exists(self._status_file):
            return 0
        with open(self._status_file) as f:
            try:
                status = json.load(f)
                # First iteration did not include the bucket and DB ID
                if 'last_row' in status:
                    return status['last_row']
                if db_id in status:
                    entry = status[db_id]
                    if entry['aws_bucket'] == self.aws_bucket:
                        return entry['last_row']
                    else:
                        return 0
                return 0
            except ValueError:
                return 0

    def save_last_row(self, row, db_id):
        if not os.path.exists(self._status_account_dir):
            os.mkdir(self._status_account_dir)
        if not os.path.exists(self._status_file):
            with open(self._status_file, 'w') as f:
                json.dump({db_id: dict(last_row=row,
                                       aws_bucket=self.aws_bucket)}, f)
                return

        with open(self._status_file, 'r+') as f:
            status = json.load(f)
            # The first version did not include the DB ID and aws_bucket in the
            # status entries
            if 'last_row' in status:
                status = {db_id: dict(last_row=row,
                                      aws_bucket=self.aws_bucket)}
            else:
                status[db_id] = dict(last_row=row,
                                     aws_bucket=self.aws_bucket)
            f.seek(0)
            json.dump(status, f)
            f.truncate()

    def handle(self, row, swift_client):
        if row['deleted'] and self.propagate_delete:
            self.provider.delete_object(row['name'], swift_client)
        else:
            # The metadata timestamp should always be the latest timestamp
            _, _, meta_ts = decode_timestamps(row['created_at'])
            if time.time() < self.copy_after + meta_ts.timestamp:
                raise RetryError('Object is not yet eligible for archive')
            self.provider.upload_object(row['name'],
                                        row['storage_policy_index'],
                                        swift_client)

            if not self.retain_copy:
                # NOTE: We rely on the DELETE object X-Timestamp header to
                # mitigate races where the object may be overwritten. We
                # increment the offset to ensure that we never remove new
                # customer data.
                self.logger.debug("Creating a new TS: %f %f" % (
                    meta_ts.offset, meta_ts.timestamp))
                delete_ts = Timestamp(meta_ts, offset=meta_ts.offset + 1)
                try:
                    swift_client.delete_object(
                        self._account, self._container, row['name'],
                        headers={'X-Timestamp': delete_ts.internal})
                except UnexpectedResponse as e:
                    if '409 Conflict' in e.message:
                        pass
