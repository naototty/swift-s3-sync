import boto3
import botocore.exceptions
from botocore.handlers import conditionally_calculate_md5
import httplib
import json
import logging
import os
import os.path
import time

from swift.common.daemon import Daemon
from swift.common.db import DatabaseConnectionError
from swift.common.internal_client import InternalClient
from swift.common.ring import Ring
from swift.common.ring.utils import is_local_device
from swift.common.utils import whataremyips, hash_path, storage_directory
from swift.common.wsgi import ConfigString
from swift.container.backend import DATADIR, ContainerBroker

from utils import convert_to_s3_headers, get_s3_name, FileWrapper, is_object_meta_synced

internal_client_config = """
[DEFAULT]
# swift_dir = /etc/swift
# user = swift
# You can specify default log routing here if you want:
# log_name = swift
# log_facility = LOG_LOCAL0
# log_level = INFO
# log_address = /dev/log
#
# comma separated list of functions to call to setup custom log handlers.
# functions get passed: conf, name, log_to_console, log_route, fmt, logger,
# adapted_logger
# log_custom_handlers =
#
# If set, log_udp_host will override log_address
# log_udp_host =
# log_udp_port = 514
#
# You can enable StatsD logging here:
# log_statsd_host =
# log_statsd_port = 8125
# log_statsd_default_sample_rate = 1.0
# log_statsd_sample_rate_factor = 1.0
# log_statsd_metric_prefix =

[pipeline:main]
pipeline = catch_errors proxy-logging cache proxy-server

[app:proxy-server]
use = egg:swift#proxy
# See proxy-server.conf-sample for options

[filter:cache]
use = egg:swift#memcache
# See proxy-server.conf-sample for options

[filter:proxy-logging]
use = egg:swift#proxy_logging

[filter:catch_errors]
use = egg:swift#catch_errors
# See proxy-server.conf-sample for options
""".lstrip()


class S3Sync(Daemon):
    def __init__(self, conf, logger=None):
        self.conf = conf
        self.logger = logger
        self.root = conf.get('devices', '/mnt/swift-disk')
        self.interval = 10
        self.swift_dir = '/etc/swift'
        self.container_ring = Ring(self.swift_dir, ring_name='container')
        self.retries = 3
        ic_config = ConfigString(internal_client_config)
        self.swift = InternalClient(ic_config, 'S3 sync', 3)
        self.scratch = conf.get('scratch', '/home/vagrant/scratch')
        self.myips = whataremyips('0.0.0.0')
        # TODO: these settings should be per container?
        self.aws_id = conf.get('aws_id')
        self.aws_secret = conf.get('aws_secret')
        self.aws_bucket = conf.get('aws_bucket')
        # assumes local swift-proxy
        self.aws_endpoint = conf.get('aws_endpoint', 'http://127.0.0.1:8080')
        boto_session = boto3.session.Session(aws_access_key_id=self.aws_id,
                                             aws_secret_access_key=self.aws_secret)

        if not self.aws_endpoint.endswith('amazonaws.com'):
            # Required for supporting S3 clones
            config = boto3.session.Config(s3={'addressing_style': 'path'})
        self.s3_client = boto_session.client('s3',
                                             endpoint_url=self.aws_endpoint,
                                             config=config)
        # Remove the Content-MD5 computation as we will supply the MD5 header
        # ourselves
        self.s3_client.meta.events.unregister('before-call.s3.PutObject',
                                              conditionally_calculate_md5)

        self.items_chunk = conf.get('items_chunk', 10000)

    def get_broker(self, account, container, part, node):
        db_hash = hash_path(account, container)
        db_dir = storage_directory(DATADIR, part, db_hash)
        db_path = os.path.join(self.root, node['device'], db_dir,
            db_hash + '.db')
        return ContainerBroker(db_path, account=account, container=container)

    def load_sync_meta(self, account, container):
        scratch_file = os.path.join(self.scratch, account, container)
        if not os.path.exists(scratch_file):
            return {}
        with open(scratch_file) as f:
            try:
                return json.load(f)
            except ValueError:
                return {}

    def save_sync_meta(self, account, container, sync_meta):
        scratch_file = os.path.join(self.scratch, account, container)
        if not os.path.exists(os.path.join(self.scratch, account)):
            os.mkdir(os.path.join(self.scratch, account))
        with open(scratch_file, 'w') as f:
            json.dump(sync_meta, f)

    def is_object_synced(self, s3_info, swift_info):
        return ('"%s"' % swift_info['etag']) == s3_info['ETag']

    def upload_object(self, account, container, row):
        key = get_s3_name(account, container, row['name'])
        missing = False
        try:
            resp = self.s3_client.head_object(Bucket=self.aws_bucket, Key=key)
        except botocore.exceptions.ClientError as e:
            if int(e.response['Error']['Code']) == 404:
                missing = True
            else:
                raise e
        # TODO:  Handle large objects. Should we delete segments in S3?
        swift_req_hdrs = {
            'X-Backend-Storage-Policy-Index': row['storage_policy_index'],
            'X-Newest': True
        }
        if not missing:
            metadata = self.swift.get_object_metadata(account, container,
                row['name'], headers=swift_req_hdrs)
            if self.is_object_synced(resp, metadata):
                if is_object_meta_synced(resp['Metadata'], metadata):
                    return

                # Update the headers
                self.s3_client.copy_object(
                    CopySource={'Bucket': self.aws_bucket, 'Key': key},
                    MetadataDirective='REPLACE',
                    Metadata=convert_to_s3_headers(metadata),
                    Bucket=self.aws_bucket,
                    Key=key)
                return

        wrapper_stream = FileWrapper(self.swift, account, container,
            row['name'], swift_req_hdrs)
        self.s3_client.put_object(Bucket=self.aws_bucket,
                                  Key=key,
                                  Body=wrapper_stream,
                                  Metadata=wrapper_stream.get_s3_headers(),
                                 )
        return

    def delete_object(self, account, container, row):
        key = get_s3_name(account, container, row['name'])
        return self.s3_client.delete_object(Bucket=self.aws_bucket, Key=key)

    def sync_row(self, row, account, container):
        # Sync the row to S3 (PUT/POST/DELETE) and mark the sync flag if
        # necessary. Should do a HEAD first.
        # TODO: use boto to propagate changes
        if row['deleted']:
            return self.delete_object(account, container, row)
        return self.upload_object(account, container, row)

    # TODO: use green threads here: need to understand whether it makes sense
    def sync_items(self, account, container, rows, nodes_count, node_id):
        for row in rows:
            if (row['ROWID'] % nodes_count) != node_id:
                continue
            print 'propagating %s' % row['ROWID']
            self.sync_row(row, account, container)

        for row in rows:
            # Validate that changes from all other rows have also been sync'd.
            if (row['ROWID'] % nodes_count) == node_id:
                continue
            print 'verifiying %s' % row['ROWID']
            self.sync_row(row, account, container)

    def get_items_since(self, broker, since_start):
        return broker.get_items_since(since_start, 10)

    def sync_container(self, account, container):
        part, container_nodes = self.container_ring.get_nodes(account,
                                                              container)
        node_id = None
        nodes_count = len(container_nodes)
        for index, node in enumerate(container_nodes):
            if not is_local_device(self.myips, None, node['ip'],
                               node['port']):
                continue
            self.s3_sync_meta = self.load_sync_meta(account, container)
            if self.s3_sync_meta:
                start = self.s3_sync_meta['last_row']
            else:
                start = 0
            broker = self.get_broker(account, container, part, node)
            try:
                items = broker.get_items_since(start, self.items_chunk)
            except DatabaseConnectionError as e:
                continue
            if items:
                self.sync_items(account, container, items, nodes_count, index)
                self.s3_sync_meta['last_row'] = items[-1]['ROWID']
                self.save_sync_meta(account, container, self.s3_sync_meta)
            return

    def run_once(self):
        #while True:
        #    cycle_start = time.time()
        #    for container in self.conf.get('containers'):
        #        self.sync_container(container)
        #    if time.time() - cycle_start < interval:
        #        time.sleep(interval)
        self.sync_container('AUTH_test', 'sync-test-0')
