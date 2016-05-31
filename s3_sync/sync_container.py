import boto3
import botocore.exceptions
from botocore.handlers import conditionally_calculate_md5
import hashlib
import json
import os
import os.path

from swift.common.internal_client import InternalClient
from swift.common.wsgi import ConfigString
from .utils import (convert_to_s3_headers, FileWrapper,
                    is_object_meta_synced)


class SyncContainer(object):
    INTERNAL_CLIENT_CONFIG = """
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

    # S3 prefix space: 6 16 digit characters
    PREFIX_LEN = 6
    PREFIX_SPACE = 16**PREFIX_LEN

    def __init__(self, status_dir, sync_settings):
        self._settings = sync_settings
        self.account = sync_settings['account']
        self.container = sync_settings['container']
        self._status_dir = status_dir
        self._status_file = os.path.join(self._status_dir, self.account,
                                         self.container)
        self._status_account_dir = os.path.join(self._status_dir, self.account)
        self._init_s3()
        ic_config = ConfigString(self.INTERNAL_CLIENT_CONFIG)
        self.swift = InternalClient(ic_config, 'S3 sync', 3)

    def _init_s3(self):
        self.aws_identity = self._settings['aws_identity']
        self.aws_secret = self._settings['aws_secret']
        self.aws_bucket = self._settings['aws_bucket']
        # assumes local swift-proxy
        self.aws_endpoint = self._settings.get('aws_endpoint', None)
        boto_session = boto3.session.Session(
                aws_access_key_id=self.aws_identity,
                aws_secret_access_key=self.aws_secret)
        config = boto3.session.Config()
        if self.aws_endpoint:
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

    def load_status(self):
        if not os.path.exists(self._status_file):
            return {}
        with open(self._status_file) as f:
            try:
                return json.load(f)
            except ValueError:
                return {}

    def save_status(self, state):
        if not os.path.exists(self._status_account_dir):
            os.mkdir(self._status_account_dir)
        with open(self._status_file, 'w') as f:
            json.dump(state, f)

    def get_s3_name(self, key):
        concat_key = '%s/%s/%s' % (self.account, self.container, key)
        md5_hash = hashlib.md5(concat_key).hexdigest()
        # strip off 0x and L
        prefix = hex(long(md5_hash, 16) % self.PREFIX_SPACE)[2:-1]
        return '%s/%s' % (prefix, concat_key)

    def upload_object(self, swift_key, storage_policy_index):
        s3_key = self.get_s3_name(swift_key)
        missing = False
        try:
            resp = self.s3_client.head_object(Bucket=self.aws_bucket,
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
            metadata = self.swift.get_object_metadata(self.account,
                                                      self.container,
                                                      swift_key,
                                                      headers=swift_req_hdrs)
            if resp['ETag'] == metadata['etag']:
                if is_object_meta_synced(resp['Metadata'], metadata):
                    return

                # Update the headers
                self.s3_client.copy_object(
                    CopySource={'Bucket': self.aws_bucket, 'Key': s3_key},
                    MetadataDirective='REPLACE',
                    Metadata=convert_to_s3_headers(metadata),
                    Bucket=self.aws_bucket,
                    Key=s3_key)
                return

        wrapper_stream = FileWrapper(self.swift,
                                     self.account,
                                     self.container,
                                     swift_key,
                                     swift_req_hdrs)
        self.s3_client.put_object(Bucket=self.aws_bucket,
                                  Key=s3_key,
                                  Body=wrapper_stream,
                                  Metadata=wrapper_stream.get_s3_headers())

    def delete_object(self, swift_key):
        s3_key = self.get_s3_name(swift_key)
        return self.s3_client.delete_object(Bucket=self.aws_bucket, Key=s3_key)
