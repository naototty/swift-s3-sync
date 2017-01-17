import boto3
import botocore.exceptions
from botocore.handlers import conditionally_calculate_md5
import eventlet
import hashlib
import json
import logging
import os
import os.path

import container_crawler.base_sync
from .utils import (convert_to_s3_headers, FileWrapper,
                    is_object_meta_synced)


class SyncContainer(container_crawler.base_sync.BaseSync):
    # S3 prefix space: 6 16 digit characters
    PREFIX_LEN = 6
    PREFIX_SPACE = 16**PREFIX_LEN
    BOTO_CONN_POOL_SIZE = 10

    class BotoClientPoolEntry(object):
        def __init__(self, client):
            self.semaphore = eventlet.semaphore.Semaphore(
                SyncContainer.BOTO_CONN_POOL_SIZE)
            self.client = client

        def acquire(self):
            return self.semaphore.acquire(blocking=False)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            self.semaphore.release()
            return False

    class BotoClientPool(object):
        def __init__(self, boto_session, boto_config, endpoint, max_conns):
            self.get_semaphore = eventlet.semaphore.Semaphore(max_conns)
            self.client_pool = self._create_pool(boto_session, boto_config,
                                                 endpoint, max_conns)

        def _create_pool(self, boto_session, boto_config, endpoint, max_conns):
            clients = max_conns / SyncContainer.BOTO_CONN_POOL_SIZE
            if max_conns % SyncContainer.BOTO_CONN_POOL_SIZE:
                clients += 1
            return [SyncContainer.BotoClientPoolEntry(self._make_boto_client(
                        boto_session, boto_config, endpoint))
                    for _ in range(0, clients)]

        def _make_boto_client(self, boto_session, boto_config, endpoint):
            s3_client = boto_session.client('s3',
                                            endpoint_url=endpoint,
                                            config=boto_config)
            # Remove the Content-MD5 computation as we will supply the MD5
            # header ourselves
            s3_client.meta.events.unregister('before-call.s3.PutObject',
                                             conditionally_calculate_md5)
            return s3_client

        def get_client(self):
            with self.get_semaphore:
                # we are guaranteed that there is an open connection we can use
                for client in self.client_pool:
                    if client.acquire():
                        return client
                raise RuntimeError('Failed to get a client!')

    def __init__(self, status_dir, sync_settings, max_conns=10):
        super(SyncContainer, self).__init__(status_dir, sync_settings)
        self.account = sync_settings['account']
        self.container = sync_settings['container']
        self._status_dir = status_dir
        self._status_file = os.path.join(self._status_dir, self.account,
                                         self.container)
        self._status_account_dir = os.path.join(self._status_dir, self.account)
        self._init_s3(sync_settings, max_conns)
        self.logger = logging.getLogger('s3-sync')

    def _init_s3(self, settings, max_conns):
        self.aws_bucket = settings['aws_bucket']
        aws_identity = settings['aws_identity']
        aws_secret = settings['aws_secret']
        # assumes local swift-proxy
        aws_endpoint = settings.get('aws_endpoint', None)
        if not aws_endpoint or aws_endpoint.endswith('amazonaws.com'):
            # We always use v4 signer with Amazon, as it will support all
            # regions.
            boto_config = boto3.session.Config(signature_version='s3v4',
                                               s3={'aws_chunked': True})
        else:
            # For the other providers, we default to v2 signer, as a lot of
            # them don't support v4 (e.g. Google)
            boto_config = boto3.session.Config(s3={'addressing_style': 'path'})
        boto_session = boto3.session.Session(
            aws_access_key_id=aws_identity,
            aws_secret_access_key=aws_secret)
        self.boto_client_pool = self.BotoClientPool(
            boto_session, boto_config, aws_endpoint, max_conns)

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

    def get_s3_name(self, key):
        concat_key = u'%s/%s/%s' % (self.account, self.container,
                                    key.decode('utf-8'))
        md5_hash = hashlib.md5('%s/%s' % (
            self.account, self.container)).hexdigest()
        # strip off 0x and L
        prefix = hex(long(md5_hash, 16) % self.PREFIX_SPACE)[2:-1]
        return '%s/%s' % (prefix, concat_key)

    def handle(self, row):
        if row['deleted']:
            self.delete_object(row['name'])
        else:
            self.upload_object(row['name'], row['storage_policy_index'])

    def upload_object(self, swift_key, storage_policy_index):
        s3_key = self.get_s3_name(swift_key)
        missing = False
        try:
            with self.boto_client_pool.get_client() as boto_client:
                s3_client = boto_client.client
                resp = s3_client.head_object(Bucket=self.aws_bucket,
                                             Key=s3_key)
        except botocore.exceptions.ClientError as e:
            if int(e.response['Error']['Code']) == 404:
                missing = True
            else:
                raise e
        # TODO:  Handle large objects. Should we delete segments in S3?
        swift_req_hdrs = {
            'X-Backend-Storage-Policy-Index': storage_policy_index,
            'X-Newest': True
        }
        if not missing:
            metadata = self._swift_client.get_object_metadata(
                self.account, self.container, swift_key,
                headers=swift_req_hdrs)
            # S3 ETags are in quotes, whereas Swift ETags are not
            if resp['ETag'] == '"%s"' % metadata['etag']:
                if is_object_meta_synced(resp['Metadata'], metadata):
                    return

                # If metadata changes for objects expired to Glacier, we
                # have to re-upload them, unfortunately.
                if 'StorageClass' not in resp or \
                        resp['StorageClass'] != 'GLACIER':
                    self.logger.debug('Updating metadata for %s to %r' % (
                        s3_key, convert_to_s3_headers(metadata)))
                    with self.boto_client_pool.get_client() as boto_client:
                        s3_client = boto_client.client
                        s3_client.copy_object(
                            CopySource={'Bucket': self.aws_bucket,
                                        'Key': s3_key},
                            MetadataDirective='REPLACE',
                            Metadata=convert_to_s3_headers(metadata),
                            Bucket=self.aws_bucket,
                            Key=s3_key)
                        return

        wrapper_stream = FileWrapper(self._swift_client,
                                     self.account,
                                     self.container,
                                     swift_key,
                                     swift_req_hdrs)
        self.logger.debug('Uploading %s with meta: %r' % (
            s3_key, wrapper_stream.get_s3_headers()))
        with self.boto_client_pool.get_client() as boto_client:
            s3_client = boto_client.client
            s3_client.put_object(Bucket=self.aws_bucket,
                                 Key=s3_key,
                                 Body=wrapper_stream,
                                 Metadata=wrapper_stream.get_s3_headers(),
                                 ContentLength=len(wrapper_stream))

    def delete_object(self, swift_key):
        s3_key = self.get_s3_name(swift_key)
        self.logger.debug('Deleting object %s' % s3_key)
        with self.boto_client_pool.get_client() as boto_client:
            s3_client = boto_client.client
            s3_client.delete_object(Bucket=self.aws_bucket, Key=s3_key)
