import eventlet
eventlet.patcher.monkey_patch(all=True)

import json
import logging
import os
import os.path
import container_crawler.base_sync
from .sync_s3 import SyncS3
from .sync_swift import SyncSwift


class SyncContainer(container_crawler.base_sync.BaseSync):
    def __init__(self, status_dir, sync_settings, max_conns=10):
        super(SyncContainer, self).__init__(status_dir, sync_settings)
        self.logger = logging.getLogger('s3-sync')
        self.aws_bucket = sync_settings['aws_bucket']
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
        if row['deleted']:
            self.provider.delete_object(row['name'], swift_client)
        else:
            self.provider.upload_object(row['name'],
                                        row['storage_policy_index'],
                                        swift_client)
